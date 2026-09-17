# Auth deployment configuration companion

`proxy-configuration.patch` applies to cosift-auth
`61435108d41789e08ec3832c0ea3cd2c97f520e7`. It allows the existing CIDR client-IP
resolver to be selected by the deploy script; it changes no auth/token runtime.

Use `git apply --check` and `git apply` in a separate clean checkout. Validate
with `bash -n infra/deploy.sh` and `GOWORK=off go test ./internal/clientip ./internal/config`.
Do not run the deploy script as a test.

Set `AUTH_XFF_MODE=cidr` and `AUTH_TRUSTED_PROXIES` only after measuring and
reviewing the staging ingress chain and gateway egress addresses, as described
in `docs/SHARED-ACCOUNTS.md`. Raising a public service's hop count is not equivalent.
