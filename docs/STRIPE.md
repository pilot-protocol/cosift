# Stripe subscriptions and credit top-ups

Every account has a **Free plan: 1,000 credits per UTC calendar month plus 60
shared free requests/minute**, with no subscription required. The optional
**$5/month paid plan adds 50,000 credits per paid month** and permits **one-time
$5/50,000-credit top-ups**. Subscribers still receive their free monthly credits.
Unused free, earned, and purchased credits carry over.

After the free request allowance, Search costs 1 credit, Answer 2, and Research 3.
At this pack price, 1,000 paid requests cost $0.10 for Search, $0.20 for Answer, or
$0.30 for Research. Credits do not bypass request caps. There are no automatic
top-ups; the subscription itself renews monthly until canceled.

## Live configuration

Live billing needs:

- `STRIPE_SECRET_KEY`: a live Stripe secret API key, or a suitable live restricted
  key with access to the Checkout, subscription, invoice, charge, and billing
  portal operations used by this service.
- `STRIPE_WEBHOOK_SECRET`: the `whsec_...` signing secret for this service's live
  snapshot webhook endpoint.
- `COSIFT_STRIPE_PORTAL_CONFIGURATION_ID`: a dedicated `bpc_...` billing portal
  configuration allowing cancellation, payment-method updates, and invoices.
  Disable subscription price/product changes, quantity changes, coupons, and
  other plan changes; Cosift supports one fixed monthly plan.

Store values in the community service's root-owned environment file. Never put
secrets in frontend code, git, logs, shell arguments, or a public issue. No
publishable key or Stripe.js is needed: Stripe-hosted Checkout collects payment
details. The server owns the USD price, credit amount, and monthly recurrence;
no product or price ID needs to be provisioned manually.

Without a usable API key and matching webhook setup, leave purchases unavailable.
`GET /api/credits` reports `payments_enabled` and `payment_mode` (`live`, `test`, or
`unavailable`). The billing portal also requires its dedicated configuration ID.
A nonempty configuration is not proof that a real payment and webhook work.

Create a **snapshot event** webhook at the exact production URL:

```text
https://cosift.pilotprotocol.network/api/payments/webhook
```

Enable these eight events:

| Event | Purpose |
| --- | --- |
| `checkout.session.completed` | Bind a completed subscription checkout or fulfill a paid top-up |
| `checkout.session.async_payment_succeeded` | Fulfill delayed paid Checkout sessions |
| `invoice.paid` | Grant one monthly subscription credit allocation per paid invoice |
| `invoice.payment_failed` | Reconcile payment/subscription status |
| `customer.subscription.created` | Reconcile subscription identity and state |
| `customer.subscription.updated` | Reconcile renewals and cancellation state |
| `customer.subscription.deleted` | Reconcile cancellation without deleting the credit balance |
| `charge.refunded` | Reverse the corresponding purchased credits |

Use this endpoint's signing secret from the same Stripe environment as the API
key. Thin-event destinations are unsupported. The handler verifies the signature
against the raw request body. The exact webhook path is exempt from browser CSRF
headers; other payment mutations still require the normal authenticated client
and origin checks. API object retrieval uses the installed official Stripe Go
SDK's API version.

See Stripe's [hosted Checkout guide](https://docs.stripe.com/checkout/quickstart),
[subscription lifecycle](https://docs.stripe.com/billing/subscriptions/webhooks),
[billing portal configuration](https://docs.stripe.com/customer-management/configure-portal),
[signature verification](https://docs.stripe.com/webhooks/signature), and
[idempotent requests](https://docs.stripe.com/api/idempotent_requests).

## Browser and API flow

1. A member chooses **Subscribe** or, with a paid current subscription period,
   **Top up** on Billing.
2. `POST /api/payments/checkout` accepts
   `{ "kind": "subscription", "idempotency_key": "..." }` or
   `{ "kind": "topup", "idempotency_key": "..." }`. The key is 16–64 letters,
   digits, underscores, or hyphens. Prices and credit quantities come only from
   the server. Persisted orders and Stripe idempotency prevent duplicate charges
   from retrying the same checkout.
3. The browser follows a validated `https://checkout.stripe.com/` URL. Subscription
   Checkout establishes a recurring monthly agreement; top-up Checkout is a
   one-time payment. Abandoning Checkout grants no credits.
4. Signed webhooks reconcile the stored owner, order, Stripe identity, amount,
   currency, and test/live environment. **Only a verified `invoice.paid` grants
   subscription credits.** Subscription Checkout completion never grants them.
   A verified paid top-up Checkout event grants its pack once. Duplicate events
   must not duplicate credits.
5. The return page refreshes the account. A URL such as `?payment=success` has no
   financial authority. If delivery is delayed, the balance changes after a valid
   webhook succeeds. The same balance is visible with `cosift contribute -credits`.

`GET /api/credits` includes:

- `subscription`: `status`, `active`, `cancel_at_period_end`, and
  `current_period_end` (Unix seconds).
- `can_top_up` and `portal_available`.
- `subscription_plan`: server-owned `amount_cents`, `currency`, `credits`, and
  `interval` (`month`), alongside the existing one-time `credit_pack`.
- `monthly_free_credits`, monthly activity, and `request_credit_costs`.

Top-up eligibility requires an active subscription and a paid current period;
merely starting Checkout or holding a credit balance is insufficient. The
backend rechecks eligibility when a top-up starts. A member can use
`POST /api/payments/portal` with `{}` to obtain a validated
`https://billing.stripe.com/` URL for their own customer record. No customer ID
is accepted from the browser.

Retry failed checkout requests using the same idempotency key. The app will not
reuse an expired Stripe idempotency window: after 23 hours, begin a new checkout.
A missing local order, ownership mismatch, or storage failure returns a retryable
failure to Stripe rather than silently claiming fulfillment. Unrelated Stripe
objects without Cosift ownership are not credited.

## Cancellation, refunds, and operations

Cancellation changes the recurring agreement; it does not erase unused free,
earned, subscription, or top-up credits. Existing credits remain subject to the
same request caps. New top-ups require paid subscription access, even if the
account still has a positive balance.

Issue refunds in the Stripe Dashboard. Signed refund notifications reverse the
corresponding purchased credit allocation. Duplicate or out-of-order events must
not reverse it twice, and a late duplicate payment event must not restore refunded
credits. Refunding credits already spent may leave a negative balance; further
credit-funded requests need sufficient credit. Cancellation alone is not a refund.

Back up payment orders, subscription/invoice records, the credit ledger, and
webhook receipts together with the account database. Monitor non-2xx deliveries
and replay them from Stripe after correcting configuration or storage problems.
The basic integration does not automate disputes, tax calculation, prorations,
plan changes, or metered billing. Keep the restricted portal configuration in
place and review merchant tax/receipt settings before live activation.

## Validation before live activation

Test keys are rejected by default. Only an isolated QA service with a separate
account database may set `COSIFT_ALLOW_TEST_PAYMENTS=1` and use test API/webhook
credentials. Never enable public test-card purchases on the production ledger.
The API and UI must label the isolated test payment mode explicitly.

Automated tests use controlled Stripe responses and signed events to check
server-owned pricing, account isolation, login/CSRF, checkout idempotency,
subscription gating, paid-invoice fulfillment, duplicate/out-of-order handling,
and refund behavior. They do not prove that merchant configuration or external
webhook delivery works.

Before activating live billing, use the isolated test environment to:

1. Complete a subscription Checkout and confirm exactly one 50,000-credit grant
   from its paid invoice. Replaying Checkout or invoice events must not add more.
2. Verify a free account cannot buy top-ups, a paid subscriber can, and a paid
   top-up adds its credits once.
3. Exercise a renewal, failed payment, cancellation, and the restricted portal.
   Confirm the free monthly allowance and remaining balance survive cancellation.
4. Exercise partial/full refunds and duplicate deliveries; inspect the balance
   and Stripe delivery status.
5. Confirm public production still reports unavailable until live credentials,
   the correct live webhook, and the dedicated portal configuration are ready.

Record actual hosted Checkout and webhook outcomes separately from local tests.
Do not describe payment activation as verified merely because keys were supplied.
