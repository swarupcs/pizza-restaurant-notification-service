# notification-service — code walkthrough

How this service is put together, file by file and function by function.

This is the **internals** document. For the event contract — which topics are
consumed, what the payloads look like, how to run it — see
[SERVICE-DOCS.md](./SERVICE-DOCS.md). This one explains *why the code looks the
way it does*, which is the part that is hard to recover from reading it cold.

---

## 1. What this service owns

notification-service tells the customer what happened to their order. That is
the whole job.

| Responsibility | Detail |
| --- | --- |
| Consuming order events | Subscribes to the `order` Kafka topic |
| Composing the message | A plain-text body and an HTML body, both from `src/handlers` |
| Sending it | One SMTP transport, via nodemailer |

It is the smallest service in the platform by a wide margin — six source files,
one of which is empty — and the only one with **no HTTP server at all**. There
is no `app.ts`, no express, no port, no `/health` endpoint. `server.ts` connects
a Kafka consumer and then the process just sits in kafkajs's poll loop. Nothing
can call it; nothing can probe it.

Three consequences of that shape run through everything below:

- **No database.** It stores nothing and remembers nothing between messages. Any
  fact it needs about an order — including the customer's email address — has to
  arrive inside the Kafka payload.
- **No HTTP client.** It cannot go and ask order-service or auth-service
  anything. If the event is incomplete, the email is incomplete.
- **No authentication of any kind.** There is no `authenticate` middleware and
  no JWKS validation, because there is no request to authenticate. Trust is
  entirely in the Kafka topic: anything that can write to `order` can make this
  service send email to any address it likes.

### What is conspicuously absent

Reading this after order-service, three things are missing:

- **No error handling around message processing.** No try/catch anywhere in
  `eachMessage`. §9 is largely about what that costs.
- **No filtering on `event_type`.** The consumer branches on the *topic* only.
  The `event_type` field is read by `handleOrderText` and ignored by everything
  else. (SERVICE-DOCS.md says the service "reads the `event_type` field from
  every Kafka message" and lists per-event behaviour — that describes the
  intent, not the code.)
- **No `logger` on the hot path.** winston is configured and used in exactly one
  place, `server.ts`. The consumer and the mail transport use `console.log`.

---

## 2. The shape of a message

```
order-service publishes to topic "order"
    │
    ▼
kafkajs consumer, groupId "notification-service"
    │
    ▼
eachMessage({ topic, partition, message })
    │
    ├── console.log({ value, topic, partition })      ← always, unconditionally
    │
    └── if (topic === "order")
            │
            ├── createNotificationTransport("mail")   ← cached singleton
            ├── JSON.parse(message.value.toString())  ← no try/catch
            │
            └── transport.send({
                    to:      order.data.customerId.email || config mail.from
                    subject: "Order update."          ← a constant
                    text:    handleOrderText(order)
                    html:    handleOrderHtml(order)
                })
                    │
                    └── nodemailer sendMail → SMTP
```

That is the entire runtime. There are no other paths.

---

## 3. Bootstrapping

### `server.ts`

Twenty lines, and the only entry point:

```ts
const startServer = async () => {
  let broker: MessageBroker | null = null;
  try {
    broker = createMessageBroker();
    await broker.connectConsumer();
    await broker.consumeMessage(["order"], false);
  } catch (err) {
    logger.error("Error happened: ", err.message);
    if (broker) {
      await broker.disconnectConsumer();
    }
    process.exit(1);
  }
};

void startServer();
```

Four details worth naming:

- **`void startServer()` at module load** means importing this file *is* running
  it. That is why every test of it lives inside `jest.isolateModules` — see §11.
- **The `if (broker)` guard matters.** If `createMessageBroker()` itself throws
  (e.g. `kafka.broker` missing from config), `broker` is still `null`, and
  without the guard the error handler would throw a second, more confusing
  `TypeError` over the top of the real one.
- **`fromBeginning: false`.** A fresh consumer group starts at the *end* of the
  topic. Deploy this service for the first time and it will not email every
  customer about every order in the retention window — which is the right
  default, and worth knowing before anyone "helpfully" flips it.
- **`process.exit(1)` is called immediately after `logger.error`.** order-service
  waits for winston's `finish` event first. This one does not, and winston's
  File transport writes asynchronously, so the line naming *why* startup failed
  can be lost exactly when it is most needed. See §9.

Note also that `err.message` on an `unknown` catch variable compiles here only
because `tsconfig.json` sets `"strict": false`.

### `src/config/logger.ts`

Standard winston setup: JSON format, timestamp, `serviceName:
"notification-service"` as default metadata, three transports (`combined.log`,
`error.log`, console), each `silent` when `NODE_ENV === "test"`. Identical in
spirit to the other services.

The thing to notice is how little uses it.

### `src/factories/broker-factory.ts`

```ts
let broker: MessageBroker | null = null;

export const createMessageBroker = (): MessageBroker => {
  console.log("connecting to kafka broker...");
  // singleton
  if (!broker) {
    broker = new KafkaBroker("notification-service", config.get("kafka.broker"));
  }
  return broker;
};
```

A module-level singleton, so the whole process shares one consumer. The
client id `"notification-service"` is passed twice over: once as kafkajs's
`clientId` and again as the consumer `groupId`. Sharing a group id across every
replica is what makes horizontal scaling work — partitions get divided between
instances and each message is handled once.

The `console.log` sits *above* the null check, so it prints "connecting to kafka
broker..." on every call including the cached ones, when nothing is being
connected. Cosmetic, but misleading in a log.

### `src/config/kafka.ts` — `KafkaBroker`

The constructor builds a base config and then, only when
`process.env.NODE_ENV === "production"`, layers on the hosted-broker settings:

```ts
if (process.env.NODE_ENV === "production") {
  kafkaConfig = {
    ...kafkaConfig,
    ssl: true,
    connectionTimeout: 45000,
    sasl: {
      mechanism: "plain",
      username: config.get("kafka.sasl.username"),
      password: config.get("kafka.sasl.password"),
    },
  };
}
```

Two things differ from order-service's otherwise-identical class:

- **`ssl: true` is hardcoded**, where order-service reads `config.get("kafka.ssl")`.
  `production.yaml` here declares a `kafka.ssl` key that nothing ever reads.
- **`connectionTimeout: 45000`** is set, because a cold serverless broker
  (Upstash, Confluent Cloud) routinely takes longer than kafkajs's 1s default.

The branch is gated on `process.env.NODE_ENV` read directly, not on a config
value, so it cannot be exercised by pointing `NODE_ENV=test` at a production
config file. It is only reachable by setting the env var — which is what
`tests/config/kafka.spec.ts` does.

`connectConsumer` and `disconnectConsumer` are one-line delegations.
`consumeMessage` is the whole service, and gets its own section.

---

## 4. `consumeMessage` — the only real code path

```ts
async consumeMessage(topics: string[], fromBeginning: boolean = false) {
  await this.consumer.subscribe({ topics, fromBeginning });

  await this.consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({ value: message.value.toString(), topic, partition });

      if (topic === "order") {
        //todo: Decide whether to send notification or not. // according to event_type.
        const transport = createNotificationTransport("mail");
        const order = JSON.parse(message.value.toString());

        await transport.send({
          to: order.data.customerId.email || config.get("mail.from"),
          subject: "Order update.",
          text: handleOrderText(order),
          html: handleOrderHtml(order),
        });
      }
    },
  });
}
```

Reading it line by line, because almost every line has a consequence:

**`console.log({ value: ... })` runs first, for every message, on the hot path.**
It prints the entire payload — which includes the customer's name and email
address — to stdout. It bypasses winston, so it is not JSON-structured, not
level-filtered, and not silenced by the `NODE_ENV === "test"` flag that every
winston transport respects.

**`message.value.toString()` runs before anything checks whether `value` exists.**
A tombstone record (a message with a null value, which compaction produces) is a
`TypeError` here.

**`topic === "order"` is the only routing.** `consumeMessage` accepts a
`topics: string[]`, so the class is written as if it could serve several — but
any topic other than `order` is silently consumed and dropped, offset advanced,
nothing sent. Today `server.ts` only ever passes `["order"]`, so this is latent
rather than live.

**The `todo` on the next line is the service's biggest functional gap.** Nothing
branches on `event_type`. A single order walking through received → confirmed →
prepared → out_for_delivery → delivered publishes five events, and this sends
five emails, all with the subject `"Order update."` and byte-for-byte identical
bodies. See §9.

**`createNotificationTransport("mail")` is called inside `eachMessage`,** not
once at startup. The factory's cache (§5) is the only thing preventing a new
nodemailer transporter — and therefore a new SMTP connection pool — being
constructed for every single message.

**`JSON.parse` has no try/catch, and neither does `eachMessage`.** kafkajs
treats a rejected `eachMessage` as a failed batch and retries the same offset.
Since the message will not parse the second time either, the partition stalls
permanently. Because this service does nothing else, one unparseable record is a
total outage of order email. The same is true of a transient SMTP failure: a
rejected `sendMail` blocks the partition until the broker succeeds or the
process is restarted.

**`order.data.customerId.email || config.get("mail.from")`** is doing two
different jobs, both badly. If the customer has no email, the notification is
sent to `noreply@pizza-restaurant.com` — the operator's own sending address —
and nobody is told the customer did not get it. And if `customerId` is `null`
rather than an object, which is what order-service's `populate` produces when
the referenced customer has been deleted, this is a `TypeError` before the `||`
can help.

**`subject: "Order update."`** is a constant. Every email this platform sends
has that subject line.

---

## 5. The transports

### `src/types/notification-types.ts`

```ts
export interface Message {
  to: string;
  text: string;
  html?: string;
  subject?: string;
}

export interface NotificationTransport {
  send(message: Message): Promise<void>;
}
```

Two lines of indirection that buy the service its shape: the consumer depends on
`NotificationTransport`, not on nodemailer. Swapping in SMS or push means adding
a class, not touching `kafka.ts`.

### `src/factories/notification-factory.ts`

```ts
const transports: NotificationTransport[] = [];

export const createNotificationTransport = (type: "mail" | "sms") => {
  switch (type) {
    case "mail": {
      const cached = transports.find((t) => t instanceof MailTransport);
      if (cached) return cached;
      const instance = new MailTransport();
      transports.push(instance);
      return instance;
    }
    case "sms":
      throw new Error("Sms notification is not supported.");
    default:
      throw new Error(`${type} notification provider is not supported.`);
  }
};
```

An array-plus-`instanceof` cache rather than a plain `let mailTransport`. It
generalises to several transport types without a variable per type, at the cost
of a linear scan — irrelevant at a cache size of one.

`"sms"` is in the union type but deliberately unimplemented, so calling it fails
loudly rather than silently sending nothing. The `default` arm is unreachable
from typed code but names the offending type, which is the right behaviour if a
config value ever drives this.

Nothing is cached on the throwing paths, so a bad type does not poison the
cache.

### `src/mail.ts` — `MailTransport`

```ts
this.transporter = nodemailer.createTransport({
  host: config.get("mail.host"),
  port: config.get("mail.port"),
  secure: true, // Use `true` for port 465, `false` for all other ports
  auth: { user: config.get("mail.auth.user"), pass: config.get("mail.auth.pass") },
});
```

The comment beside `secure: true` states the rule the line breaks. Every
environment file — development, production and test — configures port **587**,
the STARTTLS submission port, which does *not* speak TLS from the first byte.
`secure: true` on 587 makes the handshake hang until it times out. The fix is
literally `secure: config.get("mail.port") === 465`.

Config is read in the **constructor**, so it is read once, at first message, not
at startup. A missing `mail.*` key therefore surfaces as a failed message rather
than a failed boot — and, per §4, as a stalled partition.

`send` passes `from` from config and the other four fields straight through,
with a `todo: validate for valid email.` above `to`. It then reports success
with `console.log("Message sent: %s", info.messageId)` — again not winston,
again not silenced in tests, again on the path taken for every message.

### `src/handlers/orderHander.ts`

The filename typo is real, and it is what every import uses.

```ts
export const handleOrderText = (order) => {
  // todo: put proper check logic
  if (
    order.event_type === OrderEvents.ORDER_CREATE &&
    order.data.PaymentMode === PaymentMode.CASH
  ) {
    return `Thank you for your order.\n Your order id is: ${order.data._id}`;
  }
  return "Thank you for your order.";
};
```

**`order.data.PaymentMode` has a capital P. order-service publishes
`paymentMode`.** So the condition is comparing `undefined` to `"cash"`, it has
never once been true, and the branch is dead code. No customer has ever received
their order id in the plain-text body. Correcting the casing is the entire fix,
and it is the single highest-value change in this repo.

```ts
export const handleOrderHtml = (order) => {
  // todo: think about proper checks
  return `
    <h3>Thank you for your order.</h3>
    <div>Your order id is: <a href="${config.get("frontend.clientUI")}/order/${order.data._id}">${order.data._id}</a></div>
`;
};
```

Ignores `event_type` completely. Everything a customer might actually want to
confirm — the total, the delivery address, the current status, the payment
outcome — is sitting in `order.data` and none of it is used. `order.data._id` is
interpolated with no guard, so a payload without one produces
`href=".../order/undefined"` as a live link rather than failing.

### `src/utils.ts`

Empty. Zero bytes, nothing imports it. It exists because it exists in the other
services.

---

## 6. Types

`src/types/index.ts` re-declares four enums — `OrderEvents`, `OrderStatus`,
`PaymentStatus`, `PaymentMode` — that are also declared in order-service. They
are copy-pasted, not shared: there is no common package, so the two services'
notion of an order event is kept in sync by hand. The `PaymentMode` casing bug
in §5 is exactly the class of defect that arrangement invites — the enum is
imported correctly and the *field name* is what drifted, so TypeScript sees
nothing wrong.

`src/types/broker.ts` is the `MessageBroker` interface `server.ts` programs
against. Note its `consumeMessage(topics, fromBeginning)` declares `fromBeginning`
as required while the implementation defaults it — harmless, but it means callers
must pass it explicitly, which is why `server.ts` writes the `false` out.

---

## 7. The Kafka contract

### Consumed — topic `order`, group `notification-service`

Everything order-service publishes, keyed by order id. The payload:

```jsonc
{
  "event_type": "ORDER_CREATE",       // or ORDER_STATUS_UPDATE, PAYMENT_STATUS_UPDATE
  "data": {
    "_id": "670000000000000000000001",
    "total": 1162,
    "orderStatus": "received",
    "paymentMode": "cash",            // note: lower-case p — see §5
    "paymentStatus": "pending",
    "address": "...",
    "customerId": {                   // populated, not an id
      "firstName": "...",
      "email": "customer@example.com"
    }
  }
}
```

The **populated `customerId`** is load-bearing. This service has no database, so
that embedded object is the only way it can learn where to send the email. If
order-service ever stops populating it, every notification silently goes to the
operator's own `mail.from` address.

### Produced

Nothing. This is a leaf.

---

## 8. Configuration

Four files in `config/`, loaded by node-config off `NODE_ENV`:

| File | Loaded when |
| --- | --- |
| `development.yaml` | `NODE_ENV=development` |
| `production.yaml` | `NODE_ENV=production` |
| `test.yaml` | `NODE_ENV=test` |
| `custom-environment-variables.yaml` | always, layered on top |

**There is no `default.yaml`**, so each environment file has to be complete on
its own. A key added to one and forgotten in another is not a fallback — it is a
crash, because node-config **throws** on `get()` of an undefined key rather than
returning `undefined`.

Keys: `kafka.broker` (array), `kafka.sasl.{username,password}`, `mail.{host,port,from}`,
`mail.auth.{user,pass}`, `frontend.clientUI`.

`custom-environment-variables.yaml` maps every one of them to an env var, with
`kafka.broker` declared `__format: "json"` so `KAFKA_BROKER` can carry a JSON
array. Nothing sensitive is committed here — unlike catelog-service,
`development.yaml` has empty strings for the SMTP credentials.

Two config oddities already noted above: `production.yaml` declares `kafka.ssl`
which no code reads, and every file sets `mail.port: 587` against a hardcoded
`secure: true`.

---

## 9. Known issues

Each of these is captured by a test that asserts the *current* behaviour, with a
comment naming the fix. None has been silently corrected — changing any of them
changes runtime behaviour.

### The one to fix first

**`handleOrderText` reads `order.data.PaymentMode`; order-service publishes
`paymentMode`.** The cash branch has never executed. One character.

### Messaging

**No `event_type` filtering.** Five status transitions produce five identical
emails, subject `"Order update."`, bodies byte-for-byte the same. Pinned
directly by `expect(sentMessage(1)).toEqual(sentMessage(0))`.

**A failed payment gets "Thank you for your order."** `PAYMENT_STATUS_UPDATE`
with `paymentStatus: "failed"` produces the same cheerful text and HTML as a
successful order. The customer is not told their card was declined.

**The HTML body says almost nothing.** No total, no address, no status, no line
items — all present in `order.data`, none used.

**A payload without `_id` produces `/order/undefined` as a live link.**

### Reliability

**One poison message stops all email.** No try/catch around `JSON.parse` or
inside `eachMessage`; kafkajs retries the same offset forever. Since this
service has exactly one job, that is a total outage.

**A transient SMTP failure has the same effect,** for the same reason.

**A null-valued message is a `TypeError`,** because `message.value.toString()`
runs before any guard.

**`process.exit(1)` does not wait for winston to flush,** so a crash-looping
container can have an empty `error.log`.

### Correctness and privacy

**A missing customer email silently redirects to the operator.**
`order.data.customerId.email || config.get("mail.from")` — no log, no alert.

**A null `customerId` throws** before the `||` can apply.

**`secure: true` against port 587.** The connection cannot complete as
configured.

**`console.log` on the hot path prints the full payload,** customer name and
email included, unstructured and not silenced in tests.

**No validation of `to`.** There is a `todo` for it. Whatever arrives in the
event is handed to the SMTP server.

### Operational

**`ssl: true` is hardcoded** where order-service reads it from config, making
`kafka.ssl` in `production.yaml` dead weight.

**`createMessageBroker` logs "connecting to kafka broker..." on cached calls,**
when nothing is being connected.

**Any topic other than `order` is consumed and dropped silently,** despite
`consumeMessage` taking an array.

**`src/utils.ts` is empty** and unreferenced.

### Dependencies

**`nodemailer@7.0.9` carries six open high-severity advisories**, including SMTP
command injection via CRLF in header values and TLS validation weaknesses in the
OAuth2 token fetch. The fix is `nodemailer@9`, a breaking major upgrade. This is
the item on the list with a clock on it.

---

## 10. Where the tests live

72 tests across 6 suites, ~9s. No `--runInBand`, no `globalSetup`, no database
and no network — every suite is a unit test against mocked `kafkajs` and
`nodemailer`.

```
tests/utils/fixtures.ts                        orderEvent() and its Buffer form
tests/config/kafka.spec.ts                 25  the consumer: routing, payload, ssl branch
tests/handlers/order-handler.spec.ts       14  both templates, every event type
tests/server.spec.ts                       12  startup, and all three failure paths
tests/mail.spec.ts                         10  what MailTransport asks nodemailer to do
tests/factories/notification-factory.spec.ts 6  caching, and the unsupported types
tests/factories/broker-factory.spec.ts      5  singleton behaviour
```

Coverage is 93.98% statements / 92.5% branches. Every executable source file is
at 100%; the shortfall is `src/utils.ts` (empty) and the two pure type modules.

Four harness details that are not obvious:

**The jest module-registry trap, twice.** Both the factories and `server.ts`
hold module-level state — a transport cache, a broker singleton, and a
`void startServer()` that runs on import — so each test needs a fresh module
registry. The trap is that after `jest.resetModules()` or inside
`jest.isolateModules()`, a *top-level* import refers to the **previous**
registry. Comparing against it fails with the memorable
`Expected constructor: MailTransport, Received constructor: MailTransport`, and
mocks arranged on it show zero interactions. Every spec that resets the registry
therefore re-requires its collaborators — `MailTransport`, `KafkaBroker`,
`kafkajs`, `nodemailer`, the logger — from inside that same registry.

**`server.spec.ts` arranges everything inside the `isolateModules` callback,**
including the `jest.spyOn` on the logger, for exactly that reason. It then
awaits a `setImmediate` tick, because `startServer` is async and nothing awaits
it.

**`tests/mail.spec.ts` mocks nodemailer *without* `__esModule: true`.** That is
deliberate: under `esModuleInterop`, a mock lacking that flag gets wrapped so the
whole object becomes the default export — which is precisely how a real CommonJS
package like nodemailer behaves, and what `import nodemailer from "nodemailer"`
in `src/mail.ts` expects.

**`config/test.yaml` declares `kafka.sasl`** even though the test environment has
no SASL broker. Without those keys the production branch of `KafkaBroker`'s
constructor cannot be entered at all, because node-config throws on `get()` of an
undefined key. There is deliberately no `kafka.ssl` key — this service hardcodes
it.

One dependency note: `config` is pinned to exact `4.4.2`. Version 5 is ESM in
practice behind a CommonJS entry point (it `require`s a `.mjs`), which cannot
load under Jest's CommonJS runtime; `4.4.2` is the last CJS-native release.
