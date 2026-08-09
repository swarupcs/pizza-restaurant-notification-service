import {
  handleOrderHtml,
  handleOrderText,
} from "../../src/handlers/orderHander";
import { OrderEvents, OrderStatus, PaymentMode } from "../../src/types";
import { ORDER_ID, orderEvent } from "../utils/fixtures";

/**
 * The two template functions. They are the entire content of every email this
 * service sends, so what they do and do not say is the whole customer-facing
 * behaviour of notification-service.
 *
 * (The filename is `orderHander.ts` in the source — the typo is real.)
 */
describe("Order message templates", () => {
  describe("handleOrderText", () => {
    it("should return the generic thank-you for a cash order create", () => {
      // BUG, captured rather than asserted as correct — and it is the reason
      // this whole branch is dead code.
      //
      // The condition reads `order.data.PaymentMode === PaymentMode.CASH`
      // with a capital P, but order-service publishes `paymentMode`. So
      // `order.data.PaymentMode` is always undefined, the branch never runs,
      // and no customer has ever received the order id in the plain-text body.
      //
      // Correcting the casing makes this return the branch asserted in the
      // next test.
      const text = handleOrderText(
        orderEvent({ event_type: OrderEvents.ORDER_CREATE }),
      );

      expect(text).toBe("Thank you for your order.");
      expect(text).not.toContain(ORDER_ID);
    });

    it("should include the order id once the casing matches", () => {
      // The same event with the field the condition actually looks for. This
      // is the behaviour the test above should have, and will have after the
      // fix.
      const text = handleOrderText(
        orderEvent(
          { event_type: OrderEvents.ORDER_CREATE },
          { PaymentMode: PaymentMode.CASH },
        ),
      );

      expect(text).toBe(
        `Thank you for your order.\n Your order id is: ${ORDER_ID}`,
      );
    });

    it("should return the generic thank-you for a card order", () => {
      // Correct even after the casing fix: the branch is scoped to cash.
      const text = handleOrderText(
        orderEvent(
          { event_type: OrderEvents.ORDER_CREATE },
          { PaymentMode: PaymentMode.CARD },
        ),
      );

      expect(text).toBe("Thank you for your order.");
    });

    it("should say the same thing for a status update as for a new order", () => {
      // BUG, captured rather than asserted as correct. There is a
      // `todo: put proper check logic` on this function. A customer whose
      // order moves to "out for delivery" is emailed "Thank you for your
      // order." — the text never mentions the status, so the three event
      // types are indistinguishable.
      const text = handleOrderText(
        orderEvent(
          { event_type: OrderEvents.ORDER_STATUS_UPDATE },
          { orderStatus: OrderStatus.OUT_FOR_DELIVERY },
        ),
      );

      expect(text).toBe("Thank you for your order.");
    });

    it("should say the same thing for a payment update", () => {
      const text = handleOrderText(
        orderEvent({ event_type: OrderEvents.PAYMENT_STATUS_UPDATE }),
      );

      expect(text).toBe("Thank you for your order.");
    });

    it("should say the same thing for a failed payment", () => {
      // Worth pinning separately: a customer whose card was declined gets
      // "Thank you for your order." and no indication anything went wrong.
      const text = handleOrderText(
        orderEvent(
          { event_type: OrderEvents.PAYMENT_STATUS_UPDATE },
          { paymentStatus: "failed" },
        ),
      );

      expect(text).toBe("Thank you for your order.");
    });

    it("should not throw on an unknown event type", () => {
      expect(() =>
        handleOrderText(orderEvent({ event_type: "SOMETHING_ELSE" })),
      ).not.toThrow();
    });
  });

  describe("handleOrderHtml", () => {
    it("should link to the order on the client UI", () => {
      // The host comes from config/test.yaml.
      const html = handleOrderHtml(orderEvent());

      expect(html).toContain(`href="http://localhost:5173/order/${ORDER_ID}"`);
    });

    it("should show the order id as the link text", () => {
      const html = handleOrderHtml(orderEvent());

      expect(html).toContain(`>${ORDER_ID}</a>`);
    });

    it("should carry the thank-you heading", () => {
      const html = handleOrderHtml(orderEvent());

      expect(html).toContain("<h3>Thank you for your order.</h3>");
    });

    it("should produce identical html for every event type", () => {
      // BUG, captured rather than asserted as correct. There is a
      // `todo: think about proper checks` here. The html body ignores
      // `event_type` entirely, so the delivery notification, the payment
      // confirmation and the original order acknowledgement are byte-for-byte
      // the same message.
      const create = handleOrderHtml(
        orderEvent({ event_type: OrderEvents.ORDER_CREATE }),
      );
      const status = handleOrderHtml(
        orderEvent({ event_type: OrderEvents.ORDER_STATUS_UPDATE }),
      );
      const payment = handleOrderHtml(
        orderEvent({ event_type: OrderEvents.PAYMENT_STATUS_UPDATE }),
      );

      expect(status).toBe(create);
      expect(payment).toBe(create);
    });

    it("should mention neither the total nor the address", () => {
      // Everything the customer might want to confirm is available in
      // `order.data` and none of it is used.
      const html = handleOrderHtml(orderEvent());

      expect(html).not.toContain("1162");
      expect(html).not.toContain("Park Street");
    });

    it("should build a broken link when the order carries no id", () => {
      // BUG, captured rather than asserted as correct. `order.data._id` is
      // interpolated with no guard, so a payload missing it produces
      // "/order/undefined" as a live link rather than failing.
      const html = handleOrderHtml(orderEvent({}, { _id: undefined }));

      expect(html).toContain("/order/undefined");
    });

    it("should throw when the event has no data at all", () => {
      expect(() => handleOrderHtml({ event_type: "ORDER_CREATE" })).toThrow();
    });
  });
});
