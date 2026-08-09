jest.mock("../src/factories/broker-factory", () => ({
  createMessageBroker: jest.fn(),
}));

import type { Logger } from "winston";

type BrokerStub = {
  connectConsumer: jest.Mock;
  disconnectConsumer: jest.Mock;
  consumeMessage: jest.Mock;
};

/**
 * `server.ts` calls `void startServer()` at module load, so importing it *is*
 * running it — which means each test needs a fresh module registry.
 *
 * Everything the test arranges or observes has to live inside that same
 * registry. `jest.isolateModules` gives `server.ts` its own copy of the broker
 * factory and of the logger, so a mock configured on a top-level import would
 * be a different object than the one `server.ts` actually calls, and every
 * assertion would see zero interactions.
 */
const bootServer = async (
  arrange: (factory: { createMessageBroker: jest.Mock }) => void,
) => {
  let errorSpy!: jest.SpyInstance;

  jest.isolateModules(() => {
    const factory = require("../src/factories/broker-factory") as {
      createMessageBroker: jest.Mock;
    };
    const logger = (require("../src/config/logger") as { default: Logger })
      .default;

    errorSpy = jest.spyOn(logger, "error").mockImplementation(() => logger);
    arrange(factory);

    require("../server");
  });

  // startServer is async and nothing awaits it; give it a turn to settle.
  await new Promise((resolve) => setImmediate(resolve));

  return errorSpy;
};

const makeBroker = (): BrokerStub => ({
  connectConsumer: jest.fn().mockResolvedValue(undefined),
  disconnectConsumer: jest.fn().mockResolvedValue(undefined),
  consumeMessage: jest.fn().mockResolvedValue(undefined),
});

describe("server startup", () => {
  let exitSpy: jest.SpyInstance;

  beforeEach(() => {
    exitSpy = jest
      .spyOn(process, "exit")
      .mockImplementation(() => undefined as never);
  });

  afterEach(() => {
    exitSpy.mockRestore();
    jest.restoreAllMocks();
  });

  describe("on a clean start", () => {
    it("should connect the consumer", async () => {
      const broker = makeBroker();
      await bootServer((f) => f.createMessageBroker.mockReturnValue(broker));

      expect(broker.connectConsumer).toHaveBeenCalledTimes(1);
    });

    it("should subscribe to the order topic only", async () => {
      // This service consumes nothing else — the product and topping topics
      // are order-service's business.
      const broker = makeBroker();
      await bootServer((f) => f.createMessageBroker.mockReturnValue(broker));

      expect(broker.consumeMessage).toHaveBeenCalledWith(["order"], false);
    });

    it("should connect before subscribing", async () => {
      const broker = makeBroker();
      await bootServer((f) => f.createMessageBroker.mockReturnValue(broker));

      expect(broker.connectConsumer.mock.invocationCallOrder[0]).toBeLessThan(
        broker.consumeMessage.mock.invocationCallOrder[0],
      );
    });

    it("should not exit", async () => {
      const broker = makeBroker();
      await bootServer((f) => f.createMessageBroker.mockReturnValue(broker));

      expect(exitSpy).not.toHaveBeenCalled();
    });

    it("should not disconnect anything", async () => {
      const broker = makeBroker();
      await bootServer((f) => f.createMessageBroker.mockReturnValue(broker));

      expect(broker.disconnectConsumer).not.toHaveBeenCalled();
    });
  });

  describe("when the broker cannot connect", () => {
    const arrangeFailure = (broker: BrokerStub) => {
      broker.connectConsumer.mockRejectedValue(new Error("ECONNREFUSED"));
      return (f: { createMessageBroker: jest.Mock }) =>
        f.createMessageBroker.mockReturnValue(broker);
    };

    it("should log the failure", async () => {
      const broker = makeBroker();
      const errorSpy = await bootServer(arrangeFailure(broker));

      expect(errorSpy).toHaveBeenCalledWith("Error happened: ", "ECONNREFUSED");
    });

    it("should disconnect the consumer it had built", async () => {
      const broker = makeBroker();
      await bootServer(arrangeFailure(broker));

      expect(broker.disconnectConsumer).toHaveBeenCalledTimes(1);
    });

    it("should exit with a non-zero code", async () => {
      const broker = makeBroker();
      await bootServer(arrangeFailure(broker));

      expect(exitSpy).toHaveBeenCalledWith(1);
    });

    it("should never subscribe", async () => {
      const broker = makeBroker();
      await bootServer(arrangeFailure(broker));

      expect(broker.consumeMessage).not.toHaveBeenCalled();
    });

    it("should exit without waiting for the log to flush", async () => {
      // BUG, captured rather than asserted as correct. order-service's
      // equivalent waits for winston's `finish` event before exiting; this one
      // calls `process.exit(1)` immediately. winston's File transport writes
      // asynchronously, so the line naming *why* startup failed can be lost
      // exactly when it is most needed — a crash-looping container with an
      // empty error log.
      const broker = makeBroker();
      const errorSpy = await bootServer(arrangeFailure(broker));

      expect(errorSpy).toHaveBeenCalled();
      expect(exitSpy).toHaveBeenCalledWith(1);
    });
  });

  describe("when subscribing fails", () => {
    it("should still disconnect and exit", async () => {
      const broker = makeBroker();
      broker.consumeMessage.mockRejectedValue(
        new Error("Topic order does not exist"),
      );

      await bootServer((f) => f.createMessageBroker.mockReturnValue(broker));

      expect(broker.disconnectConsumer).toHaveBeenCalledTimes(1);
      expect(exitSpy).toHaveBeenCalledWith(1);
    });
  });

  describe("when the factory itself throws", () => {
    it("should exit without trying to disconnect", async () => {
      // `broker` is still null at that point, and the null guard is what stops
      // the error handler throwing a second, more confusing error over the top
      // of the real one.
      const broker = makeBroker();

      await bootServer((f) =>
        f.createMessageBroker.mockImplementation(() => {
          throw new Error("kafka.broker is not defined");
        }),
      );

      expect(broker.disconnectConsumer).not.toHaveBeenCalled();
      expect(exitSpy).toHaveBeenCalledWith(1);
    });
  });
});
