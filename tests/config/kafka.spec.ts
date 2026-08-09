import { EachMessagePayload } from "kafkajs";

jest.mock("kafkajs", () => {
  const consumer = {
    connect: jest.fn(),
    disconnect: jest.fn(),
    subscribe: jest.fn(),
    run: jest.fn(),
  };
  const Kafka = jest.fn().mockImplementation(() => ({
    consumer: jest.fn(() => consumer),
  }));
  return { Kafka, __consumer: consumer };
});

// The transport is replaced so the specs can read back exactly what would
// have been emailed. The template functions are left real, so this also
// covers the wiring between the consumer and them.
jest.mock("../../src/factories/notification-factory", () => ({
  createNotificationTransport: jest.fn(() => ({ send: jest.fn() })),
}));

import { Kafka } from "kafkajs";
import { KafkaBroker } from "../../src/config/kafka";
import { createNotificationTransport } from "../../src/factories/notification-factory";
import { OrderEvents, OrderStatus } from "../../src/types";
import { ORDER_ID, orderMessage } from "../utils/fixtures";

const kafkajs = jest.requireMock("kafkajs") as {
  __consumer: Record<string, jest.Mock>;
};

describe("KafkaBroker", () => {
  let send: jest.Mock;
  let logSpy: jest.SpyInstance;

  beforeEach(() => {
    jest.clearAllMocks();
    send = jest.fn();
    (createNotificationTransport as jest.Mock).mockReturnValue({ send });
    // consumeMessage logs every message it receives.
    logSpy = jest.spyOn(console, "log").mockImplementation(() => undefined);
  });

  afterEach(() => {
    logSpy.mockRestore();
  });

  describe("construction", () => {
    it("should build a plaintext client outside production", () => {
      new KafkaBroker("notification-service", ["localhost:9092"]);

      expect(Kafka).toHaveBeenCalledWith({
        clientId: "notification-service",
        brokers: ["localhost:9092"],
      });
    });

    it("should add ssl and sasl in production", () => {
      const previous = process.env.NODE_ENV;
      process.env.NODE_ENV = "production";

      try {
        new KafkaBroker("notification-service", ["broker:9092"]);

        const config = (Kafka as unknown as jest.Mock).mock.calls[0][0] as {
          ssl: boolean;
          connectionTimeout: number;
          sasl: { mechanism: string; username: string };
        };

        expect(config.ssl).toBe(true);
        expect(config.connectionTimeout).toBe(45000);
        expect(config.sasl.mechanism).toBe("plain");
        // From config/test.yaml, since node-config is already loaded.
        expect(config.sasl.username).toBe("test-user-kafka");
      } finally {
        process.env.NODE_ENV = previous;
      }
    });

    it("should hardcode ssl rather than read it from config", () => {
      // Worth pinning because order-service does the opposite:
      // `ssl: config.get("kafka.ssl")`. Here it is the literal `true`, so a
      // managed broker that does not speak TLS cannot be configured — and
      // `kafka.ssl` in config/production.yaml is dead weight.
      const previous = process.env.NODE_ENV;
      process.env.NODE_ENV = "production";

      try {
        new KafkaBroker("notification-service", ["broker:9092"]);

        expect(
          ((Kafka as unknown as jest.Mock).mock.calls[0][0] as { ssl: boolean })
            .ssl,
        ).toBe(true);
      } finally {
        process.env.NODE_ENV = previous;
      }
    });

    it("should connect and disconnect the consumer", async () => {
      const broker = new KafkaBroker("notification-service", []);

      await broker.connectConsumer();
      await broker.disconnectConsumer();

      expect(kafkajs.__consumer.connect).toHaveBeenCalledTimes(1);
      expect(kafkajs.__consumer.disconnect).toHaveBeenCalledTimes(1);
    });
  });

  describe("consumeMessage", () => {
    /** Subscribes, then returns a function that delivers one message. */
    const runConsumer = async (topics = ["order"]) => {
      const broker = new KafkaBroker("notification-service", []);
      await broker.consumeMessage(topics, false);

      const { eachMessage } = kafkajs.__consumer.run.mock.calls[0][0] as {
        eachMessage: (payload: EachMessagePayload) => Promise<void>;
      };

      return (topic: string, value: Buffer | null) =>
        eachMessage({
          topic,
          partition: 0,
          message: { value },
        } as unknown as EachMessagePayload);
    };

    /** The single argument passed to `transport.send`. */
    const sentMessage = (call = 0) =>
      send.mock.calls[call][0] as {
        to: string;
        subject: string;
        text: string;
        html: string;
      };

    it("should subscribe to the requested topics", async () => {
      await runConsumer();

      expect(kafkajs.__consumer.subscribe).toHaveBeenCalledWith({
        topics: ["order"],
        fromBeginning: false,
      });
    });

    it("should default fromBeginning to false", async () => {
      // On a fresh consumer group this means the service starts at the end of
      // the topic. A new deployment therefore never emails about orders placed
      // before it started, which is the behaviour you want here — replaying
      // the topic would resend every historical confirmation.
      const broker = new KafkaBroker("notification-service", []);
      await broker.consumeMessage(["order"], undefined as unknown as boolean);

      expect(kafkajs.__consumer.subscribe).toHaveBeenCalledWith({
        topics: ["order"],
        fromBeginning: false,
      });
    });

    it("should email the customer on the order topic", async () => {
      const deliver = await runConsumer();

      await deliver("order", orderMessage());

      expect(send).toHaveBeenCalledTimes(1);
      expect(sentMessage().to).toBe("swarup@test.com");
    });

    it("should ask the factory for a mail transport", async () => {
      const deliver = await runConsumer();

      await deliver("order", orderMessage());

      expect(createNotificationTransport).toHaveBeenCalledWith("mail");
    });

    it("should request the transport per message rather than once at startup", async () => {
      // Harmless only because the factory caches — see
      // tests/factories/notification-factory.spec.ts. Without that cache this
      // would open a new SMTP transporter for every event.
      const deliver = await runConsumer();

      await deliver("order", orderMessage());
      await deliver("order", orderMessage());

      expect(createNotificationTransport).toHaveBeenCalledTimes(2);
    });

    it("should use the rendered text and html bodies", async () => {
      const deliver = await runConsumer();

      await deliver("order", orderMessage());

      expect(sentMessage().text).toBe("Thank you for your order.");
      expect(sentMessage().html).toContain(
        `href="http://localhost:5173/order/${ORDER_ID}"`,
      );
    });

    it("should ignore a topic it is not subscribed to", async () => {
      const deliver = await runConsumer();

      await deliver("product", orderMessage());

      expect(send).not.toHaveBeenCalled();
    });

    it("should log every message it receives", async () => {
      // BUG, captured rather than asserted as correct. `console.log` on the
      // hot path of every order event, bypassing the winston logger the rest
      // of the service uses — so these lines carry no service name, level or
      // timestamp, and are not silenced in tests. They also print the full
      // payload, which includes the customer's name and email address.
      const deliver = await runConsumer();

      await deliver("order", orderMessage());

      expect(logSpy).toHaveBeenCalled();

      const logged = logSpy.mock.calls[0][0] as { value: string };
      expect(logged.value).toContain("swarup@test.com");
    });

    describe("Which events trigger an email", () => {
      it("should email on ORDER_CREATE", async () => {
        const deliver = await runConsumer();

        await deliver(
          "order",
          orderMessage({ event_type: OrderEvents.ORDER_CREATE }),
        );

        expect(send).toHaveBeenCalledTimes(1);
      });

      it("should email on every status change", async () => {
        // BUG, captured rather than asserted as correct. There is a
        // `todo: Decide whether to send notification or not // according to
        // event_type.` on this branch, and nothing filters. A single order
        // walking received -> confirmed -> prepared -> out_for_delivery ->
        // delivered sends the customer five emails.
        const deliver = await runConsumer();

        for (const orderStatus of [
          OrderStatus.CONFIRMED,
          OrderStatus.PREPARED,
          OrderStatus.OUT_FOR_DELIVERY,
          OrderStatus.DELIVERED,
        ]) {
          await deliver(
            "order",
            orderMessage(
              { event_type: OrderEvents.ORDER_STATUS_UPDATE },
              { orderStatus },
            ),
          );
        }

        expect(send).toHaveBeenCalledTimes(4);
      });

      it("should send an identical body for each of those emails", async () => {
        // The consequence of the missing filter: all five emails carry the
        // same subject and the same body, so the customer cannot tell what
        // changed — or that anything did.
        const deliver = await runConsumer();

        await deliver(
          "order",
          orderMessage({ event_type: OrderEvents.ORDER_CREATE }),
        );
        await deliver(
          "order",
          orderMessage(
            { event_type: OrderEvents.ORDER_STATUS_UPDATE },
            { orderStatus: OrderStatus.DELIVERED },
          ),
        );

        expect(sentMessage(1)).toEqual(sentMessage(0));
      });

      it("should email on PAYMENT_STATUS_UPDATE", async () => {
        const deliver = await runConsumer();

        await deliver(
          "order",
          orderMessage({ event_type: OrderEvents.PAYMENT_STATUS_UPDATE }),
        );

        expect(send).toHaveBeenCalledTimes(1);
      });

      it("should email on an event type it does not recognise", async () => {
        // The branch is on the *topic*, not the event, so anything published
        // to `order` becomes an email — including an event added later that
        // was never meant to notify anyone.
        const deliver = await runConsumer();

        await deliver("order", orderMessage({ event_type: "ORDER_ARCHIVED" }));

        expect(send).toHaveBeenCalledTimes(1);
      });

      it("should always use the same subject line", async () => {
        const deliver = await runConsumer();

        await deliver("order", orderMessage());

        expect(sentMessage().subject).toBe("Order update.");
      });
    });

    describe("Choosing the recipient", () => {
      it("should use the embedded customer's email", async () => {
        // notification-service has no database and no HTTP client, so the
        // customer embedded in the event is the only source for this.
        const deliver = await runConsumer();

        await deliver(
          "order",
          orderMessage(
            {},
            {
              customerId: { email: "someone@else.com", firstName: "Other" },
            },
          ),
        );

        expect(sentMessage().to).toBe("someone@else.com");
      });

      it("should fall back to the from address when the customer has no email", async () => {
        // BUG, captured rather than asserted as correct.
        // `order.data.customerId.email || config.get("mail.from")` means a
        // customer record without an email silently redirects the
        // notification to the service's own sending address. Nobody is told,
        // and the operator's inbox receives a confirmation addressed to a
        // customer who never got one.
        const deliver = await runConsumer();

        await deliver(
          "order",
          orderMessage({}, { customerId: { firstName: "No Email" } }),
        );

        expect(sentMessage().to).toBe("noreply@pizza-restaurant.com");
      });

      it("should throw when the order has no customer at all", async () => {
        // BUG, captured rather than asserted as correct. `customerId` is null
        // when order-service's `populate` hits a deleted customer, and
        // `.email` on null is a TypeError. Nothing catches it, so it rejects
        // out of `eachMessage` — see the poison-message tests below.
        const deliver = await runConsumer();

        await expect(
          deliver("order", orderMessage({}, { customerId: null })),
        ).rejects.toThrow();
      });
    });

    describe("Malformed messages", () => {
      it("should reject on a payload that is not JSON", async () => {
        // BUG, captured rather than asserted as correct. The `JSON.parse` has
        // no try/catch and `eachMessage` does not catch either, so kafkajs
        // retries the same offset indefinitely. One unparseable message stops
        // every later notification on that partition — and because this
        // service does nothing else, that is a total outage of order emails.
        const deliver = await runConsumer();

        await expect(
          deliver("order", Buffer.from("not json")),
        ).rejects.toThrow();
        expect(send).not.toHaveBeenCalled();
      });

      it("should reject on a tombstone with a null value", async () => {
        // `message.value.toString()` runs before anything else, so a
        // null-valued record is a TypeError rather than a skipped message.
        const deliver = await runConsumer();

        await expect(deliver("order", null)).rejects.toThrow();
      });

      it("should reject on an event with no data", async () => {
        const deliver = await runConsumer();

        await expect(
          deliver(
            "order",
            Buffer.from(JSON.stringify({ event_type: "ORDER_CREATE" })),
          ),
        ).rejects.toThrow();
      });

      it("should let an SMTP failure reject too", async () => {
        // Same consequence as a malformed payload: a transient mail outage
        // stalls the partition rather than dropping one notification.
        send.mockRejectedValue(new Error("SMTP 535 auth failed"));
        const deliver = await runConsumer();

        await expect(deliver("order", orderMessage())).rejects.toThrow(
          "SMTP 535 auth failed",
        );
      });
    });
  });
});
