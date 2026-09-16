# Stripe credit purchases

A single one-time pack: **US$5 buys 50,000 credits**. One credit pays for one
additional Search, Answer or Research request after the shared free allowance.
That is **$0.10 per 1,000 paid requests**. Buying credits does not bypass the
existing mode caps. Earned credits and purchased credits use the same balance.
There are no subscriptions, automatic top-ups, recurring charges, saved-card
billing, or Stripe product/price IDs to provision.

## Pricing basis, checked 2026-09-16

[Parallel's pricing](https://docs.parallel.ai/getting-started/pricing) lists
`turbo`/`fast` Search at $1 per 1,000 requests (10 results), and `basic`/`advanced`
at $5. [Exa's pricing](https://exa.ai/pricing) lists Search at $7 per 1,000
requests (up to 10 results), Answer at $5, and Deep Search at $12–15.

Cosift's $0.10 rate is one tenth of Parallel's cheapest listed search rate and
less than one tenth of Exa's search rate. This is a posted request-price
comparison, not a claim of equivalent coverage, quality, latency or research
capabilities. Cosift applies the same simple credit price to all three modes;
free requests and credits earned by contributing lower a user's cash spend.
Competitor prices are a dated comparison, not dynamically synchronized pricing.

## Configuration (after review and deployment approval)

Purchases are disabled until both environment variables are configured:

- `STRIPE_SECRET_KEY`: a Stripe secret API key (`sk_test_...` for testing,
  `sk_live_...` for live charges; suitable restricted keys are also supported).
- `STRIPE_WEBHOOK_SECRET`: the `whsec_...` signing secret for this app's webhook
  endpoint in the same Stripe test/live environment.

Put these in the community service's root-owned environment file, alongside
`COSIFT_COMMUNITY_ADMIN_TOKEN`. Do not put secret values in command arguments,
frontend code, logs or git. `deploy/community.env.example` contains empty fields.
Blank or partial configuration leaves the rest of the app usable and hides the
purchase button; `GET /api/credits` reports `payments_enabled: false`.

Create a **snapshot event** webhook endpoint at:

`https://YOUR-COMMUNITY-HOST/api/payments/webhook`

Subscribe to `checkout.session.completed`,
`checkout.session.async_payment_succeeded`, and `charge.refunded`. Use the
endpoint's signing secret, which is separate from the API key. Stripe-hosted
Checkout collects the card details; Cosift never handles card numbers. It uses
USD and card payments with price localization disabled. No publishable key or
Stripe.js is needed. The API request pins the version supplied by the installed
Stripe Go SDK. Thin-event destinations are not supported by this handler.

See Stripe's [hosted Checkout guide](https://docs.stripe.com/checkout/quickstart),
[fulfillment guide](https://docs.stripe.com/checkout/fulfillment),
[signature verification](https://docs.stripe.com/webhooks/signature), and
[idempotent requests](https://docs.stripe.com/api/idempotent_requests).

## Payment flow

1. A signed-in member clicks “Buy 50,000 credits · $5.00”.
2. `POST /api/payments/checkout` accepts only an `idempotency_key` (16–64 letters,
   digits, underscores or hyphens). The amount, currency and credit quantity
   come from the server, never the browser. A stored order and Stripe's
   idempotency key keep retries from creating another checkout session.
3. The browser follows the validated `https://checkout.stripe.com/` URL.
4. A verified Stripe webhook must report a paid, completed, one-time Checkout
   Session whose owner, order, session, amount, currency and test/live mode
   match. The order, ledger credit and event receipt commit in one transaction.
   Deduplication uses the session ID as well as the event receipt, so different
   notifications of the same payment cannot add credits twice.
5. The return page refreshes the balance. It cannot grant credits: adding
   `?payment=success` to a URL has no financial effect. If webhook delivery is
   delayed, credits appear after it succeeds. The balance is also available to
   the CLI through `cosift contribute -credits`.

Payments start only after the member completes Stripe Checkout. An abandoned
checkout does not grant credits or initiate an automatic retry charge. Failed
API requests can be retried with the same idempotency key. After 23 hours, reload
the page to start a new checkout; the app will not reuse Stripe's expired
idempotency window. Missing/mismatched local orders or storage errors return a
retryable failure to Stripe rather than silently acknowledging an unfulfilled
purchase. Ignore unrelated Stripe events with no Cosift order metadata.

## Refunds and operations

Issue refunds manually in the Stripe Dashboard. Signed `charge.refunded`
notifications revoke the corresponding fraction of purchased credits. They use
cumulative refunded cents, so duplicates, partial refunds and notifications
arriving out of order cannot revoke twice. A refund received before fulfillment
returns a retryable error. A late duplicate payment event cannot restore refunded
credits. A member who has already spent refunded credits may have a negative
balance; further credit-funded requests require replenishing it.

Back up payment orders, the ledger and webhook receipts together with the account
database. Monitor non-2xx webhook deliveries and retry them from Stripe after
fixing configuration or storage issues. This basic integration does not automate
chargeback/dispute handling, tax calculation, or subscription management; those
remain operator responsibilities. Tax and receipt settings should be reviewed in
the merchant's Stripe account before enabling live purchases.

## Validation before live activation

Automated tests use a fake Stripe HTTP transport and signatures generated by the
official SDK. They cover server-owned pricing, missing configuration, login/CSRF,
idempotent checkout, invalid/old signatures, unpaid or mismatched events,
concurrent/repeated fulfillment, restart persistence, transaction rollback and
partial/full/out-of-order refunds. They do not move money or contact Stripe.

After keys are supplied, use Stripe **test mode** to complete a hosted Checkout,
confirm one 50,000-credit grant, replay its event, cancel another checkout, and
perform a partial then full refund. Confirm the dashboard's delivery status and
Cosift's ledger balance. This credentialed end-to-end check is still required;
local tests do not claim it has happened. Production remains unchanged until a
new explicit deployment instruction.
