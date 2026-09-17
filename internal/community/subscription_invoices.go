package community

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	stripe "github.com/stripe/stripe-go/v86"
)

func invoiceSubscription(inv *stripe.Invoice) (string, string) {
	if inv.Parent == nil || inv.Parent.Type != "subscription_details" || inv.Parent.SubscriptionDetails == nil || inv.Parent.SubscriptionDetails.Subscription == nil {
		return "", ""
	}
	d := inv.Parent.SubscriptionDetails
	return d.Subscription.ID, d.Metadata["cosift_order_id"]
}

func (s *Server) knownSubscriptionInvoice(ctx context.Context, inv *stripe.Invoice) (bool, error) {
	sub, order := invoiceSubscription(inv)
	if sub == "" {
		return false, nil
	}
	if order != "" {
		return true, nil
	}
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM billing_subscriptions WHERE id=?`, sub).Scan(&n)
	return n > 0, err
}

func (s *Server) loadSubscriptionInvoice(ctx context.Context, id string) (*stripe.Invoice, *billingSubscription, error) {
	if !strings.HasPrefix(id, "in_") || !stripeIDPattern.MatchString(id) {
		return nil, nil, errors.New("invalid invoice id")
	}
	var inv stripe.Invoice
	if err := s.stripeRequest(ctx, "GET", "/v1/invoices/"+id, nil, "", &inv); err != nil {
		return nil, nil, err
	}
	if inv.ID != id || inv.Livemode != s.stripeLive() {
		return nil, nil, errors.New("invoice environment mismatch")
	}
	known, err := s.knownSubscriptionInvoice(ctx, &inv)
	if err != nil {
		return nil, nil, err
	}
	if !known {
		return nil, nil, nil
	}
	subID, orderID := invoiceSubscription(&inv)
	b, err := s.syncSubscription(ctx, subID)
	if err != nil {
		return nil, nil, err
	}
	if b == nil || (orderID != "" && orderID != b.OrderID) || inv.Customer == nil || inv.Customer.ID != b.CustomerID {
		return nil, nil, errors.New("invoice ownership mismatch")
	}
	return &inv, b, nil
}

func validatePaidSubscriptionInvoice(inv *stripe.Invoice, b *billingSubscription) (int64, error) {
	if inv.Status != "paid" || inv.AmountPaid != packAmountCents || inv.AmountRemaining != 0 || inv.AmountDue != packAmountCents || inv.Total != packAmountCents || inv.Subtotal != packAmountCents || inv.AmountPaidOffStripe != 0 || inv.Currency != "usd" {
		return 0, errors.New("invoice is not a full paid monthly plan")
	}
	if inv.BillingReason != "subscription_create" && inv.BillingReason != "subscription_cycle" {
		return 0, errors.New("invoice is not a subscription billing period")
	}
	if inv.Lines == nil || inv.Lines.HasMore || len(inv.Lines.Data) != 1 {
		return 0, errors.New("invoice must contain exactly one monthly plan line")
	}
	line := inv.Lines.Data[0]
	if line == nil || line.ID == "" || line.Amount != packAmountCents || line.Currency != "usd" || line.Quantity != 1 || (line.QuantityDecimal != 0 && line.QuantityDecimal != 1) || line.Pricing == nil || line.Pricing.Type != "price_details" || line.Pricing.PriceDetails == nil || line.Pricing.PriceDetails.Price == nil || line.Pricing.PriceDetails.Price.ID != b.PriceID {
		return 0, errors.New("invoice line price or quantity mismatch")
	}
	if line.Parent == nil || line.Parent.Type != "subscription_item_details" || line.Parent.SubscriptionItemDetails == nil || line.Parent.SubscriptionItemDetails.SubscriptionItem == "" || line.Parent.SubscriptionItemDetails.Subscription != b.ID || line.Parent.SubscriptionItemDetails.Proration {
		return 0, errors.New("invoice line subscription mismatch or proration")
	}
	if line.Period == nil || line.Period.Start <= 0 || line.Period.End-line.Period.Start < 27*86400 || line.Period.End-line.Period.Start > 32*86400 {
		return 0, errors.New("invoice period is not monthly")
	}
	return line.Period.End, nil
}

func (s *Server) invoiceEvent(ctx context.Context, event stripeEvent, fulfill bool) error {
	var snapshot stripe.Invoice
	if json.Unmarshal(event.Data.Object, &snapshot) != nil {
		return errors.New("invalid invoice event")
	}
	known, err := s.knownSubscriptionInvoice(ctx, &snapshot)
	if err != nil {
		return err
	}
	if !known {
		return nil
	}
	inv, b, err := s.loadSubscriptionInvoice(ctx, snapshot.ID)
	if err != nil {
		return err
	}
	if b == nil {
		return nil
	}
	if !fulfill {
		return nil
	} // Current subscription status was still synchronized.
	end, err := validatePaidSubscriptionInvoice(inv, b)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO subscription_invoices(id,subscription_id,user_id,amount_cents,credits,currency,period_end,livemode) VALUES(?,?,?,?,?,'usd',?,?) ON CONFLICT(id) DO NOTHING`, inv.ID, b.ID, b.UserID, packAmountCents, packCredits, end, s.stripeLive())
	if err != nil {
		return err
	}
	var storedSub, storedUser string
	var storedEnd int64
	if err = tx.QueryRowContext(ctx, `SELECT subscription_id,user_id,period_end FROM subscription_invoices WHERE id=?`, inv.ID).Scan(&storedSub, &storedUser, &storedEnd); err != nil {
		return err
	}
	if storedSub != b.ID || storedUser != b.UserID || storedEnd != end {
		return errors.New("invoice was already bound to different content")
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO credit_ledger(id,user_id,delta,reason,created_at) VALUES(?,?,?,'stripe_purchase',?) ON CONFLICT(id) DO NOTHING`, "stripe-invoice:"+inv.ID, b.UserID, packCredits, time.Now().Unix())
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE billing_subscriptions SET paid_until=COALESCE((SELECT MAX(period_end) FROM subscription_invoices WHERE subscription_id=? AND refunded_cents<amount_cents),0) WHERE id=?`, b.ID, b.ID)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO payment_events(provider,event_id,user_id,credits,created_at) VALUES('stripe',?,?,?,?) ON CONFLICT(provider,event_id) DO NOTHING`, event.ID, b.UserID, packCredits, time.Now().Unix())
	if err != nil {
		return err
	}
	return tx.Commit()
}

// Since Basil, a Charge no longer embeds its invoice. Resolve the paid
// InvoicePayment association using the charge's PaymentIntent, then inspect
// the invoice's subscription parent. Never guess ownership from an email.
func (s *Server) refundSubscriptionCharge(ctx context.Context, event stripeEvent) error {
	var charge stripe.Charge
	if json.Unmarshal(event.Data.Object, &charge) != nil {
		return errors.New("invalid refund charge")
	}
	if charge.PaymentIntent == nil || charge.PaymentIntent.ID == "" {
		return nil
	}
	if charge.Livemode != s.stripeLive() || charge.AmountRefunded < 0 || charge.AmountRefunded > charge.Amount {
		return errors.New("invalid refund amount")
	}
	pi := charge.PaymentIntent.ID
	if !strings.HasPrefix(pi, "pi_") || !stripeIDPattern.MatchString(pi) {
		return errors.New("invalid refund payment intent")
	}
	values := url.Values{"payment[type]": {"payment_intent"}, "payment[payment_intent]": {pi}, "status": {"paid"}, "limit": {"100"}}
	for page := 0; page < 100; page++ {
		var payments stripe.InvoicePaymentList
		if err := s.stripeRequest(ctx, "GET", "/v1/invoice_payments?"+values.Encode(), nil, "", &payments); err != nil {
			return err
		}
		for _, payment := range payments.Data {
			if payment == nil || payment.Invoice == nil || payment.Payment == nil || payment.Payment.Type != "payment_intent" || payment.Payment.PaymentIntent == nil || payment.Payment.PaymentIntent.ID != pi || payment.Status != "paid" {
				return errors.New("invalid invoice payment association")
			}
			inv, b, err := s.loadSubscriptionInvoice(ctx, payment.Invoice.ID)
			if err != nil {
				return err
			}
			if b == nil {
				continue
			}
			if charge.Customer == nil || charge.Customer.ID != b.CustomerID || charge.Amount != packAmountCents || charge.Currency != "usd" || payment.AmountPaid != packAmountCents || payment.Livemode != s.stripeLive() {
				return errors.New("refund does not match subscription invoice payment")
			}
			if _, err := validatePaidSubscriptionInvoice(inv, b); err != nil {
				return err
			}
			if err := s.refundSubscriptionInvoice(ctx, event.ID, inv.ID, b, charge.AmountRefunded); err != nil {
				return err
			}
		}
		if !payments.HasMore {
			return nil
		}
		if len(payments.Data) == 0 || payments.Data[len(payments.Data)-1].ID == "" {
			return errors.New("invalid invoice payment pagination")
		}
		values.Set("starting_after", payments.Data[len(payments.Data)-1].ID)
	}
	return errors.New("invoice payment pagination limit exceeded")
}

func (s *Server) refundSubscriptionInvoice(ctx context.Context, eventID, invoiceID string, b *billingSubscription, refunded int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var user, sub string
	var amount, credits, previous int64
	err = tx.QueryRowContext(ctx, `SELECT user_id,subscription_id,amount_cents,credits,refunded_cents FROM subscription_invoices WHERE id=?`, invoiceID).Scan(&user, &sub, &amount, &credits, &previous)
	if err != nil {
		return err
	} // A refund preceding invoice.paid must retry.
	if sub != b.ID || user != b.UserID || amount <= 0 || refunded > amount {
		return errors.New("refund invoice binding mismatch")
	}
	if refunded <= previous {
		return nil
	}
	revoke := credits*refunded/amount - credits*previous/amount
	_, err = tx.ExecContext(ctx, `INSERT INTO credit_ledger(id,user_id,delta,reason,created_at) VALUES(?,?,?,'stripe_refund',?)`, fmt.Sprintf("stripe-invoice-refund:%s:%d", invoiceID, refunded), user, -revoke, time.Now().Unix())
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE subscription_invoices SET refunded_cents=? WHERE id=?`, refunded, invoiceID)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE billing_subscriptions SET paid_until=COALESCE((SELECT MAX(period_end) FROM subscription_invoices WHERE subscription_id=? AND refunded_cents<amount_cents),0) WHERE id=?`, sub, sub)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO payment_events(provider,event_id,user_id,credits,created_at) VALUES('stripe',?,?,?,?) ON CONFLICT(provider,event_id) DO NOTHING`, eventID, user, -revoke, time.Now().Unix())
	if err != nil {
		return err
	}
	return tx.Commit()
}
