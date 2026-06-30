# cosift deploy — PULL-based self-update (signed releases)

How a new cosift build reaches the GH200 **without CI ever holding the
production SSH key**. CI builds, signs, and publishes a GitHub Release; the
box pulls, verifies the signature against a public key baked locally, snapshots,
atomically swaps the binary, restarts, and health-gates with auto-rollback.

```
  tag push (vX.Y.Z)                         GH200 box (every ~5 min)
        │                                          │
  ┌─────▼─────────────────────┐            ┌───────▼────────────────────────┐
  │ .github/workflows/         │            │ cosift-self-update.timer        │
  │   deploy.yml               │            │   → cosift-self-update.service  │
  │ verify → build → sign      │  Release   │   → cosift-self-update.sh       │
  │ (minisign, secret key)     │ ─────────► │ poll latest release             │
  │ upload binary+.sha256+.sig │  assets    │ verify sha256 + minisign(pub)   │
  └────────────────────────────┘            │ snapshot → swap → restart       │
                                            │ health-gate /healthz, rollback  │
                                            └─────────────────────────────────┘
```

No SSH key, no box credentials, ever live in GitHub Actions. The trust anchor
is the minisign keypair: the **private** key lives only as a GitHub secret; the
**public** key is baked on the box and is the only thing that authorizes an
install.

## One-time setup

### 1. Generate the minisign keypair (do this locally, once)

```bash
minisign -G -p minisign.pub -s minisign.key
# -G generates a keypair. Choose a passphrase OR use -W for an unencrypted
# secret key. CI signs non-interactively, so the secret key stored in the
# GitHub secret must be UNENCRYPTED — generate with -W:
minisign -G -W -p minisign.pub -s minisign.key
```

This produces two files:
- `minisign.pub`  — public key (safe to commit / publish / bake on the box)
- `minisign.key`  — secret key (NEVER commit; goes into the GitHub secret)

### 2. Store the secret key as a GitHub Actions secret

Repo → Settings → Secrets and variables → Actions → New repository secret:

- **Name:** `MINISIGN_SECRET_KEY`
- **Value:** the full contents of `minisign.key`

`deploy.yml` writes it to a temp file, signs with `minisign -S -W`, and deletes
it. Because the workflow signs non-interactively, the secret key must be the
**unencrypted** form (`minisign -G -W`).

> Anyone who can edit `deploy.yml` and trigger it can sign a release. Protect
> the workflow with branch/tag protection and required reviews accordingly.

### 3. Bake the PUBLIC key on the box

```bash
sudo mkdir -p /etc/cosift
sudo cp minisign.pub /etc/cosift/minisign.pub
sudo chmod 0644 /etc/cosift/minisign.pub
```

The self-updater verifies every download against `/etc/cosift/minisign.pub`
and refuses to install anything that doesn't verify.

### 4. Install the self-update script + timer on the box

```bash
# script (the serve unit expects it under /home/ubuntu/scripts/)
sudo install -o ubuntu -g ubuntu -m 0755 \
  deploy/scripts/cosift-self-update.sh /home/ubuntu/scripts/cosift-self-update.sh

# the snapshot script it reuses must also be present + executable
sudo install -o ubuntu -g ubuntu -m 0755 \
  scripts/snapshot.sh /home/ubuntu/scripts/snapshot.sh

# systemd units
sudo cp deploy/systemd/cosift-self-update.service /etc/systemd/system/
sudo cp deploy/systemd/cosift-self-update.timer   /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now cosift-self-update.timer

# verify
systemctl status cosift-self-update.timer
systemctl list-timers cosift-self-update.timer
```

Dependencies on the box: `curl`, `jq`, `sha256sum` (coreutils), `minisign`,
`systemctl`. Install minisign with `sudo apt-get install -y minisign`.

## Cutting a release

```bash
git tag v0.1.0
git push origin v0.1.0
```

The `deploy.yml` workflow runs `verify` (vet + race tests + smoke), `build`
(static `linux/arm64`, sha256, minisign signature), and `release` (uploads
`cosift-linux-arm64`, `.sha256`, `.minisig` as Release assets). Within ~5 min
the box's timer fires, verifies, and rolls the update out. `workflow_dispatch`
builds + signs the artifact for inspection but does **not** cut a Release
(only tag pushes do).

## Operating the self-updater

```bash
# trigger a poll right now (don't wait for the timer)
sudo systemctl start cosift-self-update.service

# watch what it did
journalctl -u cosift-self-update.service -n 200 --no-pager

# pause / resume automatic polling
sudo systemctl disable --now cosift-self-update.timer
sudo systemctl enable  --now cosift-self-update.timer
```

Safety properties baked into `cosift-self-update.sh`:
- `set -euo pipefail` + `flock` (no concurrent runs racing the swap).
- Installs only if sha256 **and** minisign signature verify against the baked
  public key; also cross-checks the downloaded binary's `version` against the
  release tag.
- Pre-restart GCS snapshot via `scripts/snapshot.sh`.
- Atomic swap: stage `cosift.new` → keep `cosift.prev` → `mv` over `cosift`.
- Health gate: polls `/healthz` every 10s for up to 10 min (the listener binds
  only after the ~4-5 min HNSW load, so a 200 is true readiness).
- Auto-rollback: on health-gate failure, restores `cosift.prev`, restarts, and
  exits non-zero.
