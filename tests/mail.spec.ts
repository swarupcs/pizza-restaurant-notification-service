// nodemailer is replaced wholesale: these tests are about what MailTransport
// asks it to do, not about reaching an SMTP server.
//
// No `__esModule: true` here on purpose. `mail.ts` does
// `import nodemailer from "nodemailer"`, which under esModuleInterop compiles
// to `__importDefault(require(...)).default` — and without the flag that
// helper wraps the whole mock as the default export, which is exactly the CJS
// shape the real nodemailer has.
jest.mock("nodemailer", () => {
  const sendMail = jest.fn().mockResolvedValue({ messageId: "msg-1" });
  const createTransport = jest.fn(() => ({ sendMail }));
  return { createTransport, __sendMail: sendMail };
});

import nodemailer from "nodemailer";
import { MailTransport } from "../src/mail";

const mailer = jest.requireMock("nodemailer") as { __sendMail: jest.Mock };

describe("MailTransport", () => {
  let logSpy: jest.SpyInstance;

  beforeEach(() => {
    jest.clearAllMocks();
    mailer.__sendMail.mockResolvedValue({ messageId: "msg-1" });
    // `send` logs the message id with console.log.
    logSpy = jest.spyOn(console, "log").mockImplementation(() => undefined);
  });

  afterEach(() => {
    logSpy.mockRestore();
  });

  describe("construction", () => {
    it("should build the transport from config", () => {
      // Values from config/test.yaml.
      new MailTransport();

      expect(nodemailer.createTransport).toHaveBeenCalledWith({
        host: "smtp.ethereal.email",
        port: 587,
        secure: true,
        auth: { user: "test-user", pass: "test-pass" },
      });
    });

    it("should ask for implicit TLS on a STARTTLS port", () => {
      // BUG, captured rather than asserted as correct. `secure` is hardcoded
      // `true`, and the comment beside it says "Use `true` for port 465,
      // `false` for all other ports" — but every environment file
      // (development, production and test) configures port **587**, which is
      // the STARTTLS submission port and does not speak TLS from the first
      // byte. The connection will not negotiate.
      //
      // The fix is to derive it: `secure: config.get("mail.port") === 465`.
      const call = (nodemailer.createTransport as jest.Mock).mock;
      new MailTransport();

      const options = call.calls[0][0] as { port: number; secure: boolean };

      expect(options.port).toBe(587);
      expect(options.secure).toBe(true);
    });

    it("should build a new transporter per instance", () => {
      // The caching that avoids this lives in the notification factory, not
      // here — see tests/factories/notification-factory.spec.ts.
      new MailTransport();
      new MailTransport();

      expect(nodemailer.createTransport).toHaveBeenCalledTimes(2);
    });
  });

  describe("send", () => {
    const message = {
      to: "swarup@test.com",
      subject: "Order update.",
      text: "Thank you for your order.",
      html: "<h3>Thank you for your order.</h3>",
    };

    it("should pass the message through to nodemailer", async () => {
      await new MailTransport().send(message);

      expect(mailer.__sendMail).toHaveBeenCalledWith({
        from: "noreply@pizza-restaurant.com",
        to: "swarup@test.com",
        subject: "Order update.",
        text: "Thank you for your order.",
        html: "<h3>Thank you for your order.</h3>",
      });
    });

    it("should take the from address from config, not the message", async () => {
      // `Message` has no `from` field — the sender is fixed per deployment.
      await new MailTransport().send(message);

      const sent = mailer.__sendMail.mock.calls[0][0] as { from: string };

      expect(sent.from).toBe("noreply@pizza-restaurant.com");
    });

    it("should send text and html together", async () => {
      // Both are populated so a plain-text client still gets a readable body.
      await new MailTransport().send(message);

      const sent = mailer.__sendMail.mock.calls[0][0] as {
        text: string;
        html: string;
      };

      expect(sent.text).toBeTruthy();
      expect(sent.html).toBeTruthy();
    });

    it("should send an undefined html when the message omits it", async () => {
      // `html` is optional on `Message`.
      await new MailTransport().send({ to: "a@b.com", text: "hi" });

      const sent = mailer.__sendMail.mock.calls[0][0] as {
        html?: string;
        subject?: string;
      };

      expect(sent.html).toBeUndefined();
      expect(sent.subject).toBeUndefined();
    });

    it("should log the message id rather than use the logger", async () => {
      // BUG, captured rather than asserted as correct. There is a
      // `// use logger` comment on the line. This is the only record that a
      // notification was delivered, and it goes to stdout with no service
      // name, level or timestamp — and unlike the winston logger it is not
      // silenced under NODE_ENV=test.
      await new MailTransport().send(message);

      expect(logSpy).toHaveBeenCalledWith("Message sent: %s", "msg-1");
    });

    it("should send to an unvalidated recipient", async () => {
      // BUG, captured rather than asserted as correct. There is a
      // `todo: validate for valid email.` above `to`. Whatever arrives in the
      // Kafka payload is handed straight to the SMTP layer.
      await new MailTransport().send({ to: "not-an-email", text: "hi" });

      const sent = mailer.__sendMail.mock.calls[0][0] as { to: string };

      expect(sent.to).toBe("not-an-email");
    });

    it("should let an SMTP failure reject", async () => {
      // Nothing catches this. It propagates out of `eachMessage`, so kafkajs
      // retries the same offset — see tests/config/kafka.spec.ts for why that
      // matters.
      mailer.__sendMail.mockRejectedValue(new Error("SMTP 535 auth failed"));

      await expect(new MailTransport().send(message)).rejects.toThrow(
        "SMTP 535 auth failed",
      );
    });
  });
});
