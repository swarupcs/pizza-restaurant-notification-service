import {
  OrderEvents,
  OrderStatus,
  PaymentMode,
  PaymentStatus,
} from "../../src/types";

export const ORDER_ID = "670000000000000000000001";

/**
 * The shape order-service actually publishes to the `order` topic.
 *
 * Two things about it are worth knowing before reading the specs:
 *
 * 1. `data` is the whole order document flattened, with `customerId` replaced
 *    by the populated customer — not left as an id. notification-service has
 *    no database and no HTTP client, so the embedded customer is the only way
 *    it can learn an email address.
 * 2. The field is `paymentMode`, lower-case p. `handleOrderText` reads
 *    `order.data.PaymentMode`. See tests/handlers/order-handler.spec.ts.
 */
export const orderEvent = (
  overrides: Record<string, unknown> = {},
  dataOverrides: Record<string, unknown> = {},
) => ({
  event_type: OrderEvents.ORDER_CREATE,
  data: {
    _id: ORDER_ID,
    total: 1162,
    discount: 0,
    taxes: 162,
    deliveryCharges: 100,
    address: "12 Park Street, Kolkata",
    comment: "Ring the bell twice",
    tenantId: "1",
    orderStatus: OrderStatus.RECEIVED,
    paymentMode: PaymentMode.CASH,
    paymentStatus: PaymentStatus.PENDING,
    customerId: {
      _id: "670000000000000000000002",
      userId: "1",
      firstName: "Swarup",
      lastName: "Das",
      email: "swarup@test.com",
    },
    ...dataOverrides,
  },
  ...overrides,
});

/** The same event as the Buffer kafkajs hands to `eachMessage`. */
export const orderMessage = (
  overrides: Record<string, unknown> = {},
  dataOverrides: Record<string, unknown> = {},
) => Buffer.from(JSON.stringify(orderEvent(overrides, dataOverrides)));
