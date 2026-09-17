# Auth deployment configuration companion

The companion merged in [cosift-auth PR #1](https://github.com/pilot-protocol/cosift-auth/pull/1)
at `e6ce91911df8670e6913dd18ca8084236af428a5` and is deployed. The retained
patch and commands below reproduce the historical change; do not apply the
patch again to current auth main. Current endpoints and deployment evidence are
in [SHARED-ACCOUNTS.md](../../docs/SHARED-ACCOUNTS.md).

`proxy-configuration.patch` applies to cosift-auth
`61435108d41789e08ec3832c0ea3cd2c97f520e7`. It allows the existing CIDR client-IP
resolver to be selected by the deploy script; it changes no auth/token runtime.

Use `git apply --check` and `git apply` in a separate clean checkout. Validate
with `bash -n infra/deploy.sh` and `GOWORK=off go test ./internal/clientip ./internal/config`.
Do not run the deploy script as a test.

Set `AUTH_XFF_MODE=cidr` and `AUTH_TRUSTED_PROXIES` only after measuring and
reviewing the staging ingress chain and gateway egress addresses, as described
in `docs/SHARED-ACCOUNTS.md`. Raising a public service's hop count is not equivalent.
