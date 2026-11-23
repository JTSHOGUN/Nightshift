# Nightshift — Solana paper desk

A live, read-only Solana discovery engine and a simulated USD portfolio. Six deterministic agents enforce explicit handoffs. **No wallet connection, private key, transaction signing, order routing, or live-trading mode.** The web terminal is a view of the actual worker and SQLite ledger, not a demo-data generator.

## Operate the overnight session

Open the **Nightshift · Solana paper desk** live preview. The worker runs independently of the browser. A new database starts with **$10,000 paper cash** and a cutoff at the **next 07:00 in Africa/Kampala (EAT, UTC+3)**. The running workspace session began on 23 November 2026; its cutoff is 24 November 2026 at 07:00 EAT.

- **Desk overview:** the radar plots actual in-range pairs. Angle = pair age over 72h; radius = logarithmic market cap. A radar point is a detection, not a buy. Click a point or token for evidence.
- **Candidate feed:** search by mint/name/symbol, sort numeric columns, and filter for qualified or rejected pairs. Complete scores sort first, then hourly volume. A dash means **not scored**, not zero. Both “Killed” and “Unverified” prevent entry.
- **Paper portfolio:** open/closed positions, costs, marks, stops, trail status, reference swaps and paper P&L. Market-reference transactions do not belong to this desk.
- **Audit log:** filter/search all events, inspect evidence at decision time, and export the complete current-session CSV. A repeated rejection on a later cycle is a separate logged decision.
- **Pause** disables discovery/new entries; in-flight evidence may finish, but a paused session cannot fill. Open-position protection continues. **Resume** queues a fresh scan. **Scan now** cannot start overlapping cycles.
- At cutoff, new entries are blocked. Risk requests liquidation of every open position at a valid quote. Missing prices/liquidity leave the book **closing**, not fictitiously flat. Once reconciled, `runtime/session-<id>.md` contains the morning report, including the strongest fully qualified signals, kills, evidence gaps and executions. Download it from the completed session. A new night requires an explicit **New session**.

**Uptime:** an open browser is not required, but a running host is. An Arena workspace can be suspended; this is not a guarantee of unattended overnight uptime. Use the persistent deployment below for a reliable run. The database resumes on restart; it does not backfill fictional trades during downtime.

## Paper execution readiness & performance

**Paper portfolio → Execution readiness** separates worker readiness from coin eligibility. “Paper controls armed” does not mean there is a qualified coin or a live-trading connection. The panel and `GET /api/readiness` show the risk heartbeat, reconciled cash, unverified checks in the last completed cycle, optional read-only observations of recent buys/sells, and session performance. Diagnostic observations on a rejected coin cannot open positions or affect P&L.

Before both authorization and fill the worker requires a completed, current risk sweep (≤60s), cash reconciliation to within half a cent, the original complete Filter/Analyst handoffs, a fresh exact-pool buy and sell (≤90s each), and a valid stop. A modeled immediate liquidation has to recover more than 94% of the all-in cost. This extra check rejects trades where estimated friction already consumes the entire 6% loss budget. A recent sell is evidence of two-sided activity, **not** a guarantee of executable depth. Actual paper exits use the lower of the fresh sell reference and current mark, reduced by modeled impact/slippage/fees. No recent sell means the exit stays pending.

A monitor interruption over 60s is recorded. Affected open positions are durably queued for a protective exit at the next valid sell reference; no fills are invented during downtime. The operator can still pause new entries at any time. Those safeguards do not bound loss if a market gaps or disappears.

**Forward paper scorecard:** realized net P&L, completed round trips, win rate, average net dollars per close, return on closed cost, modeled fees already included, and maximum drawdown on sampled marks. Opening an unclosed position is not a win. Zero completed trades means win rate/average return are **unavailable**, not 0% or a projection. The morning report includes the same measurements. Unmodeled network/priority costs, latency and actual fills can make live results worse. There is no profit guarantee or automatic move to live trading.

Upstream request pacing, `Retry-After` handling, and timeout backoff reduce pressure on public providers. Missing bundle/history fields remain blockers even when the executor and risk monitor are healthy. If a read-only SolanaTracker subscription is available, configure its key privately on the host rather than in chat. It enables an additional data source; it does not bypass checks.

## Run on an always-on host

### Windows PC

Install Docker Desktop with Linux containers, extract the whole ZIP, then open **Start Nightshift.cmd** in the folder containing `compose.yaml`. See **WINDOWS-START-HERE.txt** for the manual PowerShell commands and overnight power settings. No separate Python installation is needed.

### Docker Compose

```sh
cp .env.example .env
# Optional: set a read-only provider key or RPC URL in .env, not in the UI/chat.
docker compose up -d --build
docker compose logs -f nightshift
```

Open `http://127.0.0.1:8080`. Compose restricts the host port to loopback, uses `restart: unless-stopped`, and persists the ledger in a named volume. For a remote server, use an SSH tunnel or authenticated HTTPS reverse proxy. The UI is **not** a multi-user/authenticated service: anyone with access can pause/resume the paper worker. Do not expose it unauthenticated on the public internet. `docker compose down` retains data; **do not use `down -v`** unless you intend to remove the ledger.

### Without Docker (Linux/macOS, Python 3.10+)

```sh
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
# Native Python reads process environment variables; it does not implicitly load .env.
python server.py
```

Keep the process under an OS service supervisor on an always-on machine. `PORT` defaults to `8080`. `DESK_DB` defaults to `runtime/desk.sqlite`. Only one worker can own a database at a time. The source-only ZIP starts a fresh book; the active workspace’s real session remains in its `runtime` directory.

To move the **existing** book to a durable host, make a transactionally consistent backup while the worker runs:

```sh
python backup.py /path/to/desk-checkpoint.sqlite
```

Stop the old worker before starting the new one. On the new host, set `DESK_DB` to the copied checkpoint. It retains the same cash, positions, stop state, journal and cutoff. Do not run two copies of a checkpoint as if they were one portfolio.

## Data coverage — deliberately fail closed

| Source | Use |
|---|---|
| DexScreener public API | Latest Solana profiles/boosts, chain-specific token batches, exact-pair market cap, pair creation time, hourly USD volume, USD liquidity, buy/sell counts and price change. FDV is never substituted for market cap. |
| GeckoTerminal public API | New/trending pool seeds, exact-pool unique buyers, real swap signers/timestamps, base-token USD prices and on-demand OHLCV. The latest swap sample is bounded; it is not complete wallet history. |
| Solana JSON-RPC, `confirmed` | Parsed SPL/Token-2022 mint authority fields; a full token-account census, aggregated by wallet. Returned balances must reconcile exactly with mint supply. Slots are retained as evidence. |
| RugCheck | Attributed creator, exact-pool LP state and indexed prior deployments. A missing/partial list is not a clean history. A prior mint must have an explicit verdict, and a report must match the queried mint. |
| TrenchBot | Initial bundled-supply percentage for supported launch mints. Unavailable, malformed or inconsistent responses block entry. **The public endpoint timed out during initial live validation.** |
| Optional SolanaTracker | `SOLANA_TRACKER_API_KEY` enables `/tokens/{mint}/bundlers` initial percentage and a complete paginated `/deployer/{creator}?format=full` fallback. Current bundler holdings never substitute for launch allocations. Requires suitable provider coverage/entitlement. |

A paid provider adapter is implemented, **not a promise that any individual mint is covered**. It was not exercised against a paid account in the workspace. Missing inputs leave the filter closed. HTTP failures, rate limits, source age and explicit unknowns are surfaced. Never paste a signing key into this project. `SOLANA_RPC_URL` can point to a read-only provider that supports mint-filtered `getProgramAccounts`; many shared RPCs limit that method.

Discovery is bounded: recent profiles, boosts, new/trending pools and retained mints (up to 210 per sweep), then the actual DEX-returned Solana markets. This is not exhaustive full-chain discovery, proof of a token’s cultural category, or verification that a developer has never used another wallet. Indexers can lag or misclassify. The on-chain owner census includes pool vaults conservatively. Conflicting census/supply snapshots and unsupported extensions are not estimated away.

## The six hard handoffs

**Scout → Filter → Analyst → Risk → Executor. Logger records the whole path**, including early kills. A score alone can never authorize a fill.

1. **Scout:** `30 minutes ≤ pair age ≤ 72 hours` and `$300,000 ≤ market cap ≤ $20,000,000`. Out-of-universe decisions and discovery mints without usable pairs are journaled.
2. **Filter:** both mint and freeze authority fields must be explicitly null; the selected pool must have positive LP issuance, 100% burned/locked LP (roundoff tolerance only) and zero unlocked LP; liquidity ≥ $40,000; 1h volume ≥ $50,000; top ten owners ≤25%; attributed creator ≤3%; no reported current/prior rug; initial bundled supply ≤10%; and change **strictly below +80%** in 1h. Cheap failures short-circuit costly lookups. Unchecked gates are not described as passed.
3. **Analyst:** all six inputs are required. No partial-score renormalization. Scores below 70 are killed. Factors increase linearly between their limits and are bounded at zero and the listed maximum.

   | Factor | Points | Full-credit measurement |
   |---|---:|---|
   | Momentum | 20 | +40% in 1h; zero credit at or below 0% |
   | Distinct buyers | 20 | 300 indexed unique buyers in 1h, not 300 buy transactions |
   | Buy/sell ratio | 15 | 2.0×; zero at or below 0.75×; buys without sells earn maximum, no trades is unknown |
   | Holder growth | 20 | +10% relative to a prior matched-source owner census, closest to 15m within a 12–20m window |
   | Liquidity / cap | 15 | 20% depth relative to observed market cap |
   | Smart-wallet buying | 10 | 3 distinct qualifying buyers in the most recent ≤300 indexed pool swaps, limited to 1h |

   The smart-wallet cohort is **defined**, not guessed: optional operator-supplied public addresses in `SMART_WALLETS`, plus wallets with ≥3 observed completed round trips across ≥2 *other* mints, positive aggregate net result, and ≥2/3 profitable round trips in the previous seven days. Only observations before the candidate buy count. Sampled profitability is not lifetime P&L. A successful fresh sample with no matches earns zero; an unavailable/stale sample blocks scoring. Holder warm-up is an explicit evidence kill, never assumed growth.

4. **Risk:** ranks concurrent admissions by score, re-fetches the exact market and checks the universe/market gates again. Requires a matching non-anomalous swap ≤90s old, fresh portfolio marks, no pending protective exit, and an active session. The all-in budget is **min(available cash, 8% × marked equity)**, rounded down to cents. A concrete stop at **94% of the modeled entry fill** exists before Executor can book the position. Screening evidence expires after 180s; the Risk permit expires after 8s.
5. **Executor:** paper only, no leverage, one entry per mint per session. Cash debit and the fill are one SQLite transaction, preceded by the logged stop authorization. Simulation uses a 0.30% per-side fee, 0.25% adverse slippage and a size/depth impact approximation. This is not a DEX routing quote.
6. **Logger:** append-only event rows (database triggers disallow updates/deletes), exact source/check evidence, identifiers, timestamps, scored factors, reference swaps and realized exits. The mutable latest-pair view never replaces the historical journal.

**Protection:** Risk polls every 20s and processes available intrapoll swap observations. After an observed +12% gain on entry, the default trail is **6% below the high-water mark**. Stops never loosen. A detected breach is latched durably before looking up exit liquidity, so a rebound/restart cannot cancel it. Exits use the latest valid adverse-adjusted price, **not the trigger price**. Polling gaps, missing trades, outages and illiquidity can produce losses greater than 6%. Paper equity based on a stale last mark is flagged, and further entries stop.

## Verify the build

```sh
python -m unittest discover -s tests -v
```

The isolated tests use temporary databases and synthetic inputs **only inside the tests**. They cover exact thresholds, missing/active authorities, pool identity, LP protection, bundle evidence, prior rugs, owner aggregation, score bounds and missing inputs, smart-wallet lookahead, stop-before-fill ordering, all-in sizing, duplicate prevention, stale permits, pause/cutoff, gap exits, intrapoll breaches, ratcheting trails, durable exit intents and append-only logging. No test trades enter the live ledger.

`GET /api/health` exposes the session mode, worker heartbeat and last completed scan. `GET /api/state` supplies the terminal. `GET /api/ledger.csv` exports raw decisions with UTC epoch timestamps and evidence JSON. All browser requests are same-origin. Fonts are bundled with their OFL licenses; token thumbnails are optional, sanitized cosmetic assets.
