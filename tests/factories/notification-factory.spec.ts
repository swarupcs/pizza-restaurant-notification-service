jest.mock("nodemailer", () => {
  const sendMail = jest.fn().mockResolvedValue({ messageId: "msg-1" });
  const createTransport = jest.fn(() => ({ sendMail }));
  return { createTransport, __sendMail: sendMail };
});

/**
 * `transports` is a module-level array inside the factory, so its cache
 * survives between calls within one module registry. Each test resets the
 * registry to get a clean one.
 *
 * Everything the test compares against has to come from that same fresh
 * registry: after `resetModules`, a top-level `import { MailTransport }` would
 * refer to a *different* class object than the one the re-required factory
 * instantiates, and `toBeInstanceOf` would fail with the baffling
 * "Expected constructor: MailTransport, Received constructor: MailTransport".
 */
const fresh = () => {
  jest.resetModules();
  const factory =
    require("../../src/factories/notification-factory") as typeof import("../../src/factories/notification-factory");
  const { MailTransport } =
    require("../../src/mail") as typeof import("../../src/mail");
  const nodemailer = require("nodemailer") as { createTransport: jest.Mock };
  return { ...factory, MailTransport, nodemailer };
};

describe("createNotificationTransport", () => {
  it("should return a MailTransport for 'mail'", () => {
    const { createNotificationTransport, MailTransport } = fresh();

    expect(createNotificationTransport("mail")).toBeInstanceOf(MailTransport);
  });

  it("should return the same instance on a second call", () => {
    // This is the only thing stopping a new SMTP transporter being built for
    // every single Kafka message — the consumer calls the factory inside
    // `eachMessage`, not once at startup.
    const { createNotificationTransport } = fresh();

    const first = createNotificationTransport("mail");
    const second = createNotificationTransport("mail");

    expect(second).toBe(first);
  });

  it("should build the underlying transporter only once", () => {
    const { createNotificationTransport, nodemailer } = fresh();

    createNotificationTransport("mail");
    createNotificationTransport("mail");
    createNotificationTransport("mail");

    expect(nodemailer.createTransport).toHaveBeenCalledTimes(1);
  });

  it("should throw for 'sms'", () => {
    // Declared in the union type but deliberately unimplemented, so the
    // failure is explicit rather than a silent no-op.
    const { createNotificationTransport } = fresh();

    expect(() => createNotificationTransport("sms")).toThrow(
      "Sms notification is not supported.",
    );
  });

  it("should throw for a type outside the union", () => {
    // Only reachable from untyped code — the consumer passes the literal
    // "mail" — but the default arm names the offending type, which is the
    // right behaviour if a config value ever drives this.
    const { createNotificationTransport } = fresh();

    expect(() =>
      (createNotificationTransport as unknown as (t: string) => unknown)(
        "push",
      ),
    ).toThrow("push notification provider is not supported.");
  });

  it("should not cache anything for an unsupported type", () => {
    const { createNotificationTransport, nodemailer } = fresh();

    expect(() => createNotificationTransport("sms")).toThrow();
    expect(() => createNotificationTransport("sms")).toThrow();
    expect(nodemailer.createTransport).not.toHaveBeenCalled();
  });
});
