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

/**
 * Same registry discipline as the notification factory spec: `KafkaBroker` and
 * the kafkajs mock have to be re-required after `resetModules`, or the
 * instanceof check and the call counts refer to objects from the previous
 * registry.
 */
const fresh = () => {
  jest.resetModules();
  const factory =
    require("../../src/factories/broker-factory") as typeof import("../../src/factories/broker-factory");
  const { KafkaBroker } =
    require("../../src/config/kafka") as typeof import("../../src/config/kafka");
  const { Kafka } = require("kafkajs") as { Kafka: jest.Mock };
  return { ...factory, KafkaBroker, Kafka };
};

describe("createMessageBroker", () => {
  let logSpy: jest.SpyInstance;

  beforeEach(() => {
    // The factory logs "connecting to kafka broker..." on every call.
    logSpy = jest.spyOn(console, "log").mockImplementation(() => undefined);
  });

  afterEach(() => {
    logSpy.mockRestore();
  });

  it("should return a KafkaBroker", () => {
    const { createMessageBroker, KafkaBroker } = fresh();

    expect(createMessageBroker()).toBeInstanceOf(KafkaBroker);
  });

  it("should return the same instance on a second call", () => {
    // Only `server.ts` calls this, once — unlike order-service, where two
    // routers and the entry point all need the same producer. The singleton is
    // defensive here rather than load-bearing.
    const { createMessageBroker } = fresh();

    expect(createMessageBroker()).toBe(createMessageBroker());
  });

  it("should build the Kafka client once, from config", () => {
    const { createMessageBroker, Kafka } = fresh();

    createMessageBroker();
    createMessageBroker();

    expect(Kafka).toHaveBeenCalledTimes(1);
    // Broker list from config/test.yaml.
    expect(Kafka).toHaveBeenCalledWith({
      clientId: "notification-service",
      brokers: ["localhost:9092"],
    });
  });

  it("should name the consumer group after the service", () => {
    // The groupId is the clientId, so every replica shares the partitions of
    // the `order` topic rather than each getting a full copy — which is what
    // stops a two-replica deployment emailing every customer twice.
    const { createMessageBroker, Kafka } = fresh();

    createMessageBroker();

    const kafkaInstance = Kafka.mock.results[0].value as {
      consumer: jest.Mock;
    };

    expect(kafkaInstance.consumer).toHaveBeenCalledWith({
      groupId: "notification-service",
    });
  });

  it("should log on every call, even the cached one", () => {
    // BUG, captured rather than asserted as correct. The `console.log` sits
    // above the cache check rather than inside it, so it announces
    // "connecting to kafka broker..." even when no connection is being made.
    // It also bypasses the winston logger the rest of the service uses.
    const { createMessageBroker } = fresh();

    createMessageBroker();
    createMessageBroker();

    expect(logSpy).toHaveBeenCalledTimes(2);
    expect(logSpy).toHaveBeenCalledWith("connecting to kafka broker...");
  });
});
