# Cosift Open Contribution Network — Flow Diagrams

Companion to `PLAN-contrib-network.md`. Each diagram answers one question and
references the workstreams (WS0-WS15) implementing it. Mermaid renders natively
on GitHub.

Diagram index:

1. [System overview — layers and trust boundaries](#1-system-overview)
2. [End-to-end story — developer publishes → AI agent retrieves](#2-end-to-end-story)
3. [Component map of the broker](#3-component-map-of-the-broker)
4. [Publisher: claim + verify a docs prefix](#4-publisher-claim-and-verify)
5. [Publisher: heartbeat + lease lifecycle](#5-publisher-heartbeats-and-lease-state)
6. [Publisher: lease state machine](#6-lease-state-machine)
7. [Worker: install → register → ready](#7-worker-install-and-register)
8. [Worker: fetch contribution lifecycle](#8-fetch-contribution-lifecycle)
9. [Worker: embed contribution lifecycle](#9-embed-contribution-lifecycle)
10. [Frontier lanes and work distribution](#10-frontier-lanes-and-work-distribution)
11. [Quarantine → verify → promote pipeline](#11-quarantine-verify-promote-pipeline)
12. [Retrieval path with gating](#12-retrieval-path-with-gating)
13. [Transport binding — Pilot tunnel identity check (I9)](#13-transport-binding-i9)
14. [Principal reputation state machine](#14-principal-reputation)
15. [Multi-broker federation (forward-compat)](#15-multi-broker-federation-future)

---

## 1. System overview

Three planes — humans/agents at the top, Pilot overlay in the middle, the broker (cosift on GH200) at the bottom. Every contribution byte crosses the middle plane (**I9**).

```mermaid
flowchart TB
    subgraph H[Humans and agents]
        DEV[Developer<br/>publishes docs]
        AI[Claude Code / AI agent<br/>retrieves answers]
    end

    subgraph A[Apps on contributor / consumer machines]
        PUB[cosift-node app<br/>publisher role]
        WRK[cosift-node app<br/>worker role + bundled embedder]
        MCP[cosift MCP shim]
    end

    subgraph P[Pilot overlay - tunnels authenticated by ed25519 pubkey]
        T1((overlay tunnel))
        T2((overlay tunnel))
        T3((overlay tunnel))
    end

    subgraph B[Broker - cosift on GH200]
        LIS[Pilot listener<br/>virt port 7700]
        RT[Router + middleware<br/>envelope verify, peer-pubkey binding]
        BL[Business logic<br/>collections - leases - principals - verifier]
        ST[(Pebble store<br/>P, C, A, S, W, docs, frontier, HNSW)]
        VLM[vLLM<br/>chat]
        EMB[Ollama replicas x8<br/>nomic-embed-text-v1.5]
    end

    DEV --> PUB
    AI --> MCP

    PUB --> T1
    WRK --> T2
    MCP --> T3

    T1 --> LIS
    T2 --> LIS
    T3 --> LIS

    LIS --> RT --> BL --> ST
    BL --> VLM
    BL --> EMB

    style P fill:#eef,stroke:#88a,stroke-width:2px
    style B fill:#efe,stroke:#8a8,stroke-width:2px
```

Trust boundaries: every arrow crossing into the broker is overlay-authenticated; the envelope signing key equals the tunnel peer pubkey.

---

## 2. End-to-end story

The full loop from a dev shipping docs to an AI agent retrieving an answer. Time flows top-to-bottom; only happy-path events shown.

```mermaid
sequenceDiagram
    autonumber
    actor DEV as Developer
    participant PUB as cosift-node (publisher)
    participant ORG as docs.foo.dev origin
    participant BRK as Cosift broker
    participant WRK as cosift-node (worker, elsewhere)
    participant MCP as cosift MCP shim
    actor AI as Claude Code user

    DEV->>PUB: install via pilotctl appstore
    PUB->>BRK: register principal (overlay, signed)
    PUB->>BRK: claim_prefix("docs.foo.dev")
    BRK-->>PUB: challenge token T
    PUB->>DEV: place T at /.well-known/cosift-verify.txt
    DEV->>ORG: deploy T
    BRK->>ORG: GET /.well-known (verification worker)
    ORG-->>BRK: T matches
    BRK->>BRK: collection verified, active

    PUB->>BRK: submit URLs (lane 0)
    BRK->>BRK: push to frontier lane 0

    WRK->>BRK: claim work (kinds: fetch, embed)
    BRK-->>WRK: leased URLs and chunks
    WRK->>ORG: fetch pages
    WRK->>WRK: embed chunks (bundled sidecar)
    WRK->>BRK: submit signed results
    BRK->>BRK: quarantine, verify, promote

    Note over PUB,BRK: heartbeat every 6h keeps lease active

    AI->>MCP: cosift_search("how to use foo v2.3")
    MCP->>BRK: /search (overlay)
    BRK->>BRK: BM25 + HNSW + rerank + judge<br/>+ gate (collection state, banned principals)
    BRK-->>MCP: hits with provenance
    MCP-->>AI: results in Claude Code
```

The lease is what makes "active only while installed" true: the developer's app stops heartbeating when uninstalled, lease lapses, docs leave the index.

---

## 3. Component map of the broker

Zoom into one cosift instance. Routes split by auth surface; background loops sit on the side.

```mermaid
flowchart LR
    subgraph IN[Inbound]
        OVL[Overlay listener<br/>port 7700]
        LOOP[Loopback HTTP :7777<br/>admin only, not exposed]
    end

    subgraph MW[Middleware]
        ENV[Envelope verify +<br/>peer-pubkey binding]
        ADM[Bearer admin]
    end

    subgraph RT[Routes]
        PR[/pub/* publisher API/]
        WR[/work/* worker API/]
        SR[/search /query /answer /research/]
        PUB[/queue /healthz /stats /metrics/]
        AR[/admin/* operator/]
    end

    subgraph BL[Core logic]
        COL[collections + leases]
        PRN[principal registry + reputation]
        WL[work-lease + frontier lanes]
        VR[verification scheduler]
        RET[retrieval + filter + judge]
    end

    subgraph ST[Pebble families]
        FAM["'P' principals<br/>'C' collections<br/>'A' attribution<br/>'S' staging<br/>'W' work leases<br/>'d/u/i/h' docs<br/>'f' frontier (lane-prefixed)<br/>'v/q' HNSW + PQ"]
    end

    subgraph BG[Background loops]
        SWP[lease sweeper]
        REV[re-verify domain]
        VWK[verification workers]
        REC[re-crawl scheduler]
        SNP[snapshot timer]
    end

    OVL --> ENV
    LOOP --> ADM
    ENV --> PR
    ENV --> WR
    ENV --> SR
    ENV --> PUB
    ADM --> AR

    PR --> COL
    WR --> WL
    SR --> RET
    AR --> WL
    AR --> PRN

    COL --> ST
    PRN --> ST
    WL --> ST
    VR --> ST
    RET --> ST

    SWP --> COL
    REV --> COL
    VWK --> VR
    REC --> WL
    SNP --> ST
```

`/admin/*` routes only exist on loopback HTTP behind the operator's admin token — the overlay listener refuses them outright (route allowlist).

---

## 4. Publisher claim and verify

Domain ownership without a central account system. The well-known token is the proof; the broker re-verifies every 30 days.

```mermaid
sequenceDiagram
    autonumber
    participant DEV as Developer
    participant APP as cosift-node app
    participant BRK as Broker
    participant ORG as docs.foo.dev

    DEV->>APP: node.claim_prefix(prefix="docs.foo.dev")
    APP->>APP: build envelope (kind=pub_claim,<br/>principal=K_pub, seq=N)
    APP->>BRK: POST /pub/claim (overlay, envelope)
    BRK->>BRK: verify envelope, allocate collection_id,<br/>store record state=pending,<br/>generate token T
    BRK-->>APP: { collection_id, token T, expires }
    APP-->>DEV: "Place T at https://docs.foo.dev/.well-known/cosift-verify.txt"

    DEV->>ORG: deploy verification file
    Note over BRK: verification worker tick (every 60s)
    BRK->>ORG: GET /.well-known/cosift-verify.txt
    ORG-->>BRK: contents = T (match)
    BRK->>BRK: state = verified -> active<br/>verified_at = now

    APP->>BRK: GET /pub/verify-status (envelope)
    BRK-->>APP: { state: "active" }
    APP-->>DEV: "Verified. You may submit."

    Note over BRK: every 30 days re-verify. failure leads to state=unverified<br/>(serving continues, writes blocked)
```

Conflicting claims: longest-prefix nesting; first verified wins. A second principal claiming the same exact prefix → 409 immediately.

---

## 5. Publisher heartbeats and lease state

Nonce-chained heartbeats (**D1**) — the response carries the next nonce; replay-proof without inbound delivery. App uninstall = identity destroyed = heartbeats stop = lease lapses.

```mermaid
sequenceDiagram
    autonumber
    participant APP as cosift-node (publisher)
    participant BRK as Broker

    Note over APP,BRK: every 6h while installed

    APP->>APP: load nonce_N from local state
    APP->>APP: sign envelope (kind=heartbeat,<br/>seq=S, body=nonce_N)
    APP->>BRK: POST /pub/heartbeat (overlay, envelope)
    BRK->>BRK: verify envelope, assert nonce_N matches<br/>expected pending_nonce, assert seq advances
    BRK->>BRK: update last_renewal, generate nonce_{N+1}, store as pending
    BRK-->>APP: { next_nonce: nonce_{N+1}, lease_state: active }
    APP->>APP: persist nonce_{N+1}

    Note over APP,BRK: no heartbeat for 72h leads to grace.<br/>plus 7d to suspended. plus 60d to purged
```

Sweeper goroutine ticks once a minute; transitions are flag flips on the `'C'` record + an in-memory suspended-set update consumed by retrieval gating (WS6).

---

## 6. Lease state machine

Two state machines live on a collection: the **lease** (presence-driven, this diagram) and the **verification** state (active vs unverified, which gates writes but never serving).

```mermaid
stateDiagram-v2
    [*] --> pending : POST /pub/claim
    pending --> verified : well-known token matches
    pending --> [*] : token expires (24h)
    verified --> active : first heartbeat received
    active --> grace : 72h no renewal
    grace --> active : renewal arrives
    grace --> suspended : 7d total no renewal<br/>(remove from retrieval)
    suspended --> active : renewal arrives<br/>(instant reactivation, no re-crawl)
    suspended --> purged : 60d total no renewal<br/>(walk attribution, delete docs)
    purged --> [*]

    note right of active
      serving normally
      writes accepted
    end note
    note right of grace
      score multiplier 0.5
      writes accepted
    end note
    note right of suspended
      docs hidden in retrieval
      writes blocked
      still on disk
    end note
```

Generous timings: a laptop on vacation doesn't nuke a publisher's docs.

---

## 7. Worker install and register

Self-contained — no ollama, no python. The bundle ships the embed runtime and model weights.

```mermaid
sequenceDiagram
    autonumber
    actor OP as Operator
    participant CLI as pilotctl
    participant SUP as appstore supervisor
    participant APP as cosift-node binary
    participant SIDE as llama.cpp sidecar
    participant BRK as Broker

    OP->>CLI: pilotctl appstore install io.pilot.cosift-node
    CLI->>SUP: fetch bundle (~307 MB)
    SUP->>SUP: verify bundle sha256 vs catalogue<br/>verify each data entry sha256 vs manifest
    SUP->>APP: spawn cosift-node --identity $APP/identity.json
    APP->>APP: load or create identity (ed25519)
    APP->>SIDE: spawn embed-runtime<br/>--model models/nomic-...gguf<br/>--uds $APP/embed.sock
    SIDE-->>APP: ready
    APP->>SIDE: embed(fixture_text)
    SIDE-->>APP: vector v
    APP->>APP: assert cosine(v, fixture_vector) at least 0.9999
    APP->>BRK: POST /work/register (overlay, envelope<br/>kind=register, reports model_blob_sha256)
    BRK->>BRK: store principal record 'P' family<br/>tier=new, sampling=1.0
    BRK-->>APP: { ok, principal_id }
    APP-->>SUP: ready signal
    SUP-->>CLI: state: ready
    CLI-->>OP: installed and ready
```

If the fixture check fails, the app stays alive for `node.status` but `node.worker_start` returns `runtime_not_ready` — never enters the loop with a tampered model.

---

## 8. Fetch contribution lifecycle

Claim → fetch → submit → quarantine → sampled verify → promote (or reject). Politeness stays server-controlled.

```mermaid
sequenceDiagram
    autonumber
    participant WRK as Worker app
    participant BRK as Broker
    participant ORG as Target site
    participant VR as Verifier (background)

    WRK->>BRK: POST /work/claim<br/>{kinds: [fetch], lanes: [1,2,3], n: 10}
    BRK->>BRK: drain lanes by weighted RR<br/>respect host fairness + politeness deadline
    BRK->>BRK: write 'W' leases (TTL 10min)
    BRK-->>WRK: [{claim_id, url, robots_ok, deadline, max_body}, ...]

    loop per claim
        WRK->>ORG: GET url (own egress)
        ORG-->>WRK: HTML/PDF
        WRK->>WRK: extract title/text/lang/published_at
        WRK->>BRK: POST /work/submit<br/>(envelope, claim_id,<br/>raw_sha256, extracted)
        BRK->>BRK: schema check, body size check,<br/>content-type allowlist
        BRK->>BRK: stage in 'S' family
        BRK-->>WRK: { staged: true }
    end

    Note over BRK,VR: sampling rate r per principal tier<br/>r=1.0 fresh, 0.05 trusted

    VR->>VR: pick fraction r of staged items
    VR->>ORG: re-fetch url (same path)
    VR->>VR: normalize via parse.go pipeline<br/>(drop tags, boilerplate, whitespace)
    VR->>VR: 64-bit simhash compare<br/>(match at most 6, partial at most 14, else mismatch)
    VR->>VR: authority check (Score(host) floor)<br/>judge gate (LLM, fail-open)<br/>ContentSHA dedup

    alt all gates pass
        VR->>VR: UpsertDocument + IndexDocument<br/>write attribution row 'A'<br/>bump principal verified_ok
    else any gate fails or mismatch pattern
        VR->>VR: discard from 'S'<br/>bump principal verified_fail<br/>(pattern leads to tier drop or ban)
    end
```

Same churn-aware grading prevents single-doc slashing — a contributor goes down only on a *pattern* of mismatches.

---

## 9. Embed contribution lifecycle

Verification is sharper here than for content — same model + same blob = deterministic match, cosine ≥ 0.9999.

```mermaid
sequenceDiagram
    autonumber
    participant WRK as Worker app
    participant SIDE as Bundled embed sidecar
    participant BRK as Broker
    participant EMB as Broker's local embedder
    participant VR as Verifier

    WRK->>BRK: POST /work/claim<br/>{kinds: [embed],<br/>model_blob_sha256: 970aa74c...,<br/>n: 100}
    BRK->>BRK: assert blob sha matches index pin<br/>(if not -> 409, never hand work)
    BRK->>BRK: scan docs lacking vectors<br/>chunk via internal/index/chunk.go<br/>truncate via truncateForEmbedLite
    BRK-->>WRK: [{claim_id, doc_id, offset, length, text}, ...]

    loop per chunk
        WRK->>SIDE: embed(text)
        SIDE-->>WRK: vector (768 float32)
    end

    WRK->>BRK: POST /work/submit<br/>(envelope, claim_id, vectors)
    BRK->>BRK: dim check, finite check,<br/>unit-normalize server-side
    BRK->>BRK: stage in 'S' family

    VR->>VR: pick fraction r of vectors
    VR->>EMB: re-embed(same text)
    EMB-->>VR: vector v_local
    VR->>VR: cosine(v_submitted, v_local) at least 0.9999?
    alt all sampled pass
        VR->>VR: HNSW.AddPassageBatch<br/>write attribution row 'A'<br/>bump verified_ok
    else any fails
        VR->>VR: reject whole batch<br/>bump verified_fail<br/>(tier drop on pattern)
    end
```

Server defines the text (**I6**) — contributors never get to choose the input, which collapses the verification problem into "did you compute the right vector for this text."

---

## 10. Frontier lanes and work distribution

The frontier is a single keyspace with a lane byte; weighted drain prevents bulk from starving submissions.

```mermaid
flowchart LR
    subgraph SRC[Sources]
        S0[publisher submit<br/>POST /pub/submit]
        S1[RSS / sitemap refresh<br/>internal/crawler/rss.go]
        S2[crawler discovery]
        S3[WET / bulk import]
    end

    subgraph FR[Frontier 'f' family - lane-prefixed]
        L0["lane 0 submitted<br/>weight 50"]
        L1["lane 1 refresh<br/>weight 30"]
        L2["lane 2 discovered<br/>weight 15"]
        L3["lane 3 bulk<br/>weight 5"]
    end

    subgraph CLM[ClaimWork - weighted RR]
        WRR["pop by lane weights<br/>host fairness within lane<br/>per-lane cursor"]
    end

    subgraph CONS[Consumers]
        SC[server-side crawler<br/>internal/crawler]
        WC[external workers<br/>via /work/claim]
    end

    subgraph LEA[Work leases 'W' family]
        WL["claim_id ->{principal, expiry, attempts}<br/>TTL 10min fetch / 5min embed"]
    end

    S0 --> L0
    S1 --> L1
    S2 --> L2
    S3 --> L3

    L0 --> WRR
    L1 --> WRR
    L2 --> WRR
    L3 --> WRR

    WRR --> SC
    WRR --> WC
    WC -.lease.-> WL
    WL -.expiry -> attempts++.-> WRR

    style L0 fill:#dfd
    style L3 fill:#fdd

    L0 -. "lane 0 fetch -> trusted-tier only<br/>(or server-side)" .- SC
```

If lane 0 is empty its share redistributes to other lanes; deterministic weighted RR keeps statistical fairness across many claims.

---

## 11. Quarantine, verify, promote pipeline

The atomic gate: nothing reaches the main index without passing every check for its kind.

```mermaid
flowchart TB
    SUB["POST /work/submit<br/>envelope verified"]
    SCH["schema + size + content-type<br/>fail fast"]
    STG[("'S' family<br/>quarantine, TTL 14d")]
    SCD["verification scheduler<br/>picks at rate r per principal tier"]

    subgraph FETCH[Fetch path]
        REF["re-fetch via crawler stack<br/>+ remote_fetcher CF transport"]
        NORM["normalize via parse.go<br/>drop tags, boilerplate, collapseWS"]
        SIM["64-bit simhash compare<br/>match, partial, mismatch"]
    end

    subgraph EMBED[Embed path]
        RCMP["recompute via local embedder<br/>same blob sha"]
        COS{"cosine at least 0.9999?"}
    end

    subgraph GATES[Promotion gates - both paths]
        AUTH["authority.Score(host)<br/>lane-0 exempt, else floor 0.2"]
        JDG["judge LLM relevance<br/>fail-open"]
        DEDUP["ContentSHA exact-dup check"]
    end

    PROM["UpsertDocument + IndexDocument<br/>write 'A' attribution row<br/>bump verified_ok"]
    REJ["discard from 'S'<br/>bump verified_fail<br/>pattern leads to tier drop or ban then purge"]

    SUB --> SCH
    SCH -- ok --> STG
    SCH -- bad --> REJ
    STG --> SCD

    SCD -- fetch kind --> REF --> NORM --> SIM
    SCD -- embed kind --> RCMP --> COS

    SIM -- match or partial --> AUTH
    SIM -- mismatch --> REJ
    COS -- yes --> AUTH
    COS -- no --> REJ

    AUTH --> JDG --> DEDUP
    DEDUP -- new --> PROM
    DEDUP -- exact-dup --> REJ

    style REJ fill:#fee
    style PROM fill:#efe
```

`partial` for content is neutral (page churn — ads, timestamps); only a *pattern* of mismatches drops a tier.

---

## 12. Retrieval path with gating

One choke point covers `/search`, `/query`, `/answer`, `/research`: the post-`retrieve()` filter loop. Suspended collections + banned principals filtered there cheaply via the docMeta v2 packed blob.

```mermaid
flowchart TB
    Q["/query  /search  /answer  /research"]
    PARSE["parse params: q, k, filters,<br/>decay, mode, max_passes"]

    subgraph RTV[retrieve and rerank]
        BM[BM25 lexical]
        DN[HNSW dense ANN]
        FUSE[hybrid fuse RRF]
        RR["rerank Cohere or Voyage<br/>or LLMReranker"]
    end

    subgraph GATE[post-retrieve gate - the single choke point]
        DM["lookup docMeta v2<br/>collection_id + principal"]
        SUS{"collection<br/>suspended?"}
        BAN{"principal<br/>banned?"}
        GR{"collection<br/>in grace?"}
    end

    JDG["judge LLM relevance<br/>drop low-score hits"]
    MMR[MMR diversify]
    SYN["synthesize answer<br/>multi-pass research"]
    OUT["SSE stream:<br/>phase events + answer + sources"]

    Q --> PARSE --> BM --> FUSE
    PARSE --> DN --> FUSE
    FUSE --> RR --> DM
    DM --> SUS
    SUS -- yes --> DROP1["drop hit"]
    SUS -- no --> BAN
    BAN -- yes --> DROP2["drop hit"]
    BAN -- no --> GR
    GR -- yes --> DECAY["score x 0.5<br/>reuses decay multiplier"]
    GR -- no --> KEEP["keep at full score"]
    KEEP --> JDG
    DECAY --> JDG
    JDG --> MMR --> SYN --> OUT

    style GATE fill:#ffe,stroke:#aa6
```

Same machinery for every retrieval endpoint — invariant **I8** enforced by sharing one function, not four.

---

## 13. Transport binding (I9)

The contribution-path proof: the envelope's signing key must equal the Pilot tunnel's authenticated peer pubkey. Forge either side, broker refuses.

```mermaid
sequenceDiagram
    autonumber
    participant APP as Worker / publisher app<br/>(principal pubkey K)
    participant D1 as Pilot daemon<br/>(contributor side)
    participant D2 as Pilot daemon<br/>(broker side, on GH200)
    participant LIS as Broker pilot_listener
    participant MW as Envelope middleware
    participant H as Handler

    APP->>D1: net.dial(broker_overlay_addr, 7700)
    D1->>D2: tunnel handshake (Pilot mutual auth)
    Note over D1,D2: cryptographically proves<br/>peer pubkey is K
    D2->>LIS: accept stream, remote_pubkey = K
    LIS->>LIS: stamp ctx[pilotPeerPubKey] = K

    APP->>APP: build envelope (PrincipalPub = K, sig by K)
    APP->>LIS: POST /work/claim (envelope in header)
    LIS->>MW: call handler with ctx
    MW->>MW: verify envelope sig (ed25519)
    MW->>MW: assert envelope.PrincipalPub == ctx.pilotPeerPubKey
    alt match
        MW->>MW: assert seq advances, persist new seq
        MW->>H: invoke with principal in ctx
        H-->>APP: 200 OK
    else mismatch
        MW->>MW: bump Sybil counter on K<br/>(forger used wrong key, real K caught)
        MW-->>APP: 401 envelope_peer_mismatch
    end

    Note over LIS,MW: bypass only via Contrib.AllowHTTPDev=true<br/>and 127.0.0.1 (dev only)
```

Public HTTPS via Caddy 404s these routes — overlay is the only network-reachable path.

---

## 14. Principal reputation

Sampling rate is the lever — fresh keys verified at 100%, earned trust lowers it; mismatch patterns drop you back or ban you.

```mermaid
stateDiagram-v2
    [*] --> new : POST /work/register
    new --> basic : 200+ verified_ok<br/>AND age 7d+<br/>AND 0 mismatches / 30d
    basic --> trusted : 2000+ verified_ok<br/>AND age 30d+<br/>AND operator approves
    basic --> new : mismatch pattern<br/>(3+ in 24h)
    trusted --> basic : mismatch pattern
    new --> banned : mismatch pattern<br/>OR operator action
    basic --> banned : mismatch pattern persists
    trusted --> banned : mismatch pattern persists
    banned --> [*] : purge all attributed docs<br/>(WS1.3 PurgePrincipal)

    note right of new
      sampling 1.0
      lanes 2, 3
      200 URL / 5k chunks per day
    end note
    note right of basic
      sampling 0.25
      lanes 1, 2, 3
      2k URL / 50k chunks per day
    end note
    note right of trusted
      sampling 0.05 (floor)
      lanes 0, 1, 2, 3
      10k URL / 250k chunks per day
    end note
```

Expected un-caught bad submissions per burned identity ≈ (1−r)/r → 0 / 3 / 19. Purge (**I4**) reverses survivors.

---

## 15. Multi-broker federation (future)

What v2 looks like if **D10** says go. Phase 1-5 keeps the runway clear (WS11a) but ships single-broker.

```mermaid
flowchart TB
    subgraph DIR[Pilot directory - list-agents]
        D["keyword: cosift-broker<br/>each broker advertises<br/>region, capacity, age"]
    end

    subgraph B[Broker pool on Pilot overlay]
        BA[Broker A<br/>GH200 USA]
        BB[Broker B<br/>Jetson EU]
        BC[Broker C<br/>operator X JP]
    end

    subgraph APPS[Worker / publisher apps]
        W1[node W1]
        W2[node W2]
        P1[node P1]
    end

    APPS -.discover.-> DIR
    DIR -.candidates.-> APPS
    W1 -- "pick lowest-latency<br/>broker_overlay_addrs plural" --> BA
    W2 --> BB
    P1 --> BC

    subgraph GOSS[Inter-broker gossip - signed]
        G1[principal records<br/>self-signed by principal<br/>portable across brokers]
        G2[collection claims<br/>well-known token<br/>re-verifiable per broker]
        G3[reputation exports<br/>signed counter deltas<br/>opt-in import]
        G4["attribution<br/>doc carries principal_sig + broker_sig"]
    end

    BA <-->|signed sync| BB
    BB <-->|signed sync| BC
    BA <-->|signed sync| BC

    BA -.uses.-> GOSS
    BB -.uses.-> GOSS
    BC -.uses.-> GOSS

    subgraph FRO[Frontier sharding - reuses existing code]
        SH["URL hash FNV-32 -> shard<br/>internal/config/cluster.go<br/>ForwardFn for cross-broker forward"]
    end

    BA -.shard.-> SH
    BB -.shard.-> SH
    BC -.shard.-> SH

    style GOSS fill:#eef,stroke:#88a,stroke-dasharray:5 5
    style DIR fill:#fef,stroke:#a8a
```

The portability invariants from WS11a are what make this additive instead of a rewrite: principals, collections, attribution, and frontier sharding all already work cross-broker; v2 just adds discovery + gossip + import logic.

---

## Reading order if you only have ten minutes

1. **Diagram 1** — see the three planes and where Pilot sits.
2. **Diagram 2** — see the developer-to-AI-agent loop end to end.
3. **Diagram 13** — see the I9 transport binding (the security spine).
4. **Diagram 11** — see the verification pipeline (the trust spine).
5. **Diagram 6** — see the lease lifecycle (the adoption flywheel).
