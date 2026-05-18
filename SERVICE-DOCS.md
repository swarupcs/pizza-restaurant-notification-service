# Notification Service — Documentation

> **Type:** Kafka Consumer (no HTTP REST API)  
> **Default Port:** N/A — this service has no HTTP server  
> **Kafka Consumer Group ID:** `notification-service`

## Overview

The Notification Service is a **background consumer-only** process. It subscribes to the `order` Kafka topic and sends **email notifications** to customers whenever an order event occurs. There are no HTTP endpoints — it cannot be called directly via Postman.

```
┌─────────────┐   Kafka (order topic)   ┌─────────────────────┐   SMTP   ┌─────────────┐
│ Order Svc   │ ──────────────────────▶ │ Notification Service │ ───────▶ │ Customer    │
│             │                         │  (Kafka Consumer)    │          │   Email     │
└─────────────┘                         └─────────────────────┘          └─────────────┘
```

---

## 📨 Kafka Consumer

### Topic Subscribed

| Topic | Description |
|---|---|
| `order` | All order lifecycle events published by order-service |

### Consumer Group

```
notification-service
```

Each message is processed exactly once within this consumer group.

---

## 📨 Handled Events

The service reads the `event_type` field from every Kafka message on the `order` topic.

### Kafka Message Structure

```json
{
  "event_type": "ORDER_CREATE | ORDER_STATUS_UPDATE | PAYMENT_STATUS_UPDATE",
  "data": {
    "_id": "65f1a2b3c4d5e6f7a8b9c0d4",
    "cart": [ ... ],
    "tenantId": "1",
    "total": 584,
    "paymentMode": "cash",
    "orderStatus": "received",
    "paymentStatus": "pending",
    "customerId": {
      "_id": "65f1a2b3c4d5e6f7a8b9c0d5",
      "email": "swarup@example.com",
      "firstName": "Swarup",
      "lastName": "Das"
    }
  }
}
```

---

### `ORDER_CREATE`

Triggered when a new order is successfully placed.

**Published by:** order-service → `POST /orders`

**Action:** Sends an order confirmation email to the customer.

**Email recipient:** `data.customerId.email` (falls back to `mail.from` config if not present)

**Email subject:** `Order update.`

**Email text body** (cash payment only):

```
Thank you for your order.
Your order id is: 65f1a2b3c4d5e6f7a8b9c0d4
```

**Email HTML body:**

```html
<h3>Thank you for your order.</h3>
<div>
  Your order id is:
  <a href="http://localhost:5173/order/65f1a2b3c4d5e6f7a8b9c0d4">
    65f1a2b3c4d5e6f7a8b9c0d4
  </a>
</div>
```

> The order link uses `frontend.clientUI` config value as the base URL.

---

### `ORDER_STATUS_UPDATE`

Triggered when the order status changes (e.g., `received` → `preparing`).

**Published by:** order-service → `PATCH /orders/change-status/:orderId`

**Current action:** Email is sent with generic "Thank you for your order." content.

> ⚠️ **TODO in codebase:** The handler currently sends a generic email for all event types. Status-specific messaging (e.g., "Your order is being prepared") is not yet implemented.

---

### `PAYMENT_STATUS_UPDATE`

Triggered when Stripe confirms payment success or failure.

**Published by:** order-service → `POST /payments/webhook`

**Current action:** Same generic email as above.

> ⚠️ **TODO in codebase:** Payment-specific messaging (e.g., payment confirmation/failure) is not yet implemented.

---

## 📧 Email Transport (SMTP)

Emails are sent via **Nodemailer** using SMTP.

### SMTP Configuration

| Config Key | Env Variable | Description |
|---|---|---|
| `mail.host` | `MAIL_HOST` | SMTP server hostname |
| `mail.port` | `MAIL_PORT` | SMTP port (587 for TLS, 465 for SSL) |
| `mail.auth.user` | `MAIL_USER` | SMTP username |
| `mail.auth.pass` | `MAIL_PASS` | SMTP password |
| `mail.from` | `MAIL_FROM` | Sender address (e.g., `noreply@pizza-restaurant.com`) |

### Development / Testing with Ethereal

For local development, use [Ethereal Email](https://ethereal.email) — a free fake SMTP service:

1. Go to https://ethereal.email and click **Create Ethereal Account**
2. Copy the generated SMTP credentials
3. Fill them into `.env` or `config/development.yaml`
4. Sent emails appear in your Ethereal inbox (not actually delivered)

```yaml
# config/development.yaml
mail:
  host: "smtp.ethereal.email"
  port: 587
  auth:
    user: "your.ethereal.user@ethereal.email"
    pass: "yourEtherealPassword"
  from: "noreply@pizza-restaurant.com"
```

---

## 🔧 Configuration Reference

All config is loaded by the `config` npm package from `config/` directory.

| Config Key | Env Variable | Default (dev) | Description |
|---|---|---|---|
| `kafka.broker` | `KAFKA_BROKER` | `["localhost:9092"]` | Kafka broker address(es) (JSON array) |
| `kafka.sasl.username` | `KAFKA_SASL_USERNAME` | — | Kafka SASL username (production only) |
| `kafka.sasl.password` | `KAFKA_SASL_PASSWORD` | — | Kafka SASL password (production only) |
| `mail.host` | `MAIL_HOST` | `smtp.ethereal.email` | SMTP host |
| `mail.port` | `MAIL_PORT` | `587` | SMTP port |
| `mail.auth.user` | `MAIL_USER` | — | SMTP username |
| `mail.auth.pass` | `MAIL_PASS` | — | SMTP password |
| `mail.from` | `MAIL_FROM` | `noreply@pizza-restaurant.com` | Sender email address |
| `frontend.clientUI` | `CLIENT_UI_DOMAIN` | `http://localhost:5173` | Client UI base URL (used in email links) |

> In **production**, Kafka uses SSL + SASL/PLAIN authentication. Set `NODE_ENV=production` to activate this.

---

## 🚀 How to Run

```bash
# Install dependencies
npm install

# Development (watches for changes)
NODE_ENV=development npm run dev

# Production
NODE_ENV=production npm start
```

Ensure **Kafka** is running before starting this service. With Docker Compose:

```bash
docker-compose up -d kafka
```

---

## 🧪 How to Test

Since there are no HTTP endpoints, testing is done end-to-end:

### Step 1 — Start all required services
```bash
# Start Kafka
docker-compose up -d kafka

# Start auth-service, order-service
# Start notification-service
NODE_ENV=development npm run dev
```

### Step 2 — Place an order via order-service
```
POST http://localhost:5503/orders
Idempotency-Key: <unique-uuid>
Content-Type: application/json

{ "cart": [...], "paymentMode": "cash", ... }
```

### Step 3 — Check your Ethereal inbox
Go to https://ethereal.email → **Messages** → confirm the email arrived.

---

## 🗂️ Project Structure

```
notification-service/
├── server.ts                   # Entry point — connects Kafka consumer
├── src/
│   ├── config/
│   │   ├── kafka.ts            # KafkaBroker class (consumer + event dispatch)
│   │   └── logger.ts           # Winston logger
│   ├── factories/
│   │   ├── broker-factory.ts   # Singleton Kafka broker factory
│   │   └── notification-factory.ts  # Creates MailTransport
│   ├── handlers/
│   │   └── orderHander.ts      # Builds email text/HTML for order events
│   ├── mail.ts                 # Nodemailer SMTP transport
│   └── types/                  # TypeScript interfaces
├── config/
│   ├── development.yaml        # Dev config (committed)
│   ├── test.yaml               # Test config (committed)
│   ├── production.yaml         # Prod config (gitignored — use env vars)
│   └── custom-environment-variables.yaml  # Maps env vars to config keys
└── .env.example                # All required environment variables
```
