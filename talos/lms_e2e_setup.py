#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = ["requests"]
# ///
"""
Talos LMS — end-to-end client setup

Provisions a complete Loan Management System structure for a single borrower
on a Talos sandbox, starting from a name and an EVM wallet address:

  1.  Create Transfer market account  (evmchain/<wallet>)
  2.  Create collateral sub-account   (<name>-collateral)
  3.  Create loan sub-account         (<name>-loan)
  4.  Create net rollup               (<name>-net)
  5.  Wire rollup memberships
  6.  Link permission filter          (collateral SA → wallet MA, Action: Write)
  7.  Create borrower counterparty    (<name>-borrower)
  8.  Post initial USDC collateral    to the collateral sub-account
  9.  Book loan deal                  (ETH lent, USDC collateral, SOFR + spread)
  10. Activate deal
  11. Verify positions

All steps are idempotent: existing resources are detected via GET and skipped.

Configuration
─────────────
Set the following environment variables before running:

  TALOS_HOST        Sandbox hostname, e.g. tal-123.sandbox.talostrading.com
  TALOS_API_KEY     API key with roles: org.trading.config, org.trader,
                    org.treasury, org.user.admin, org.viewer
  TALOS_API_SECRET  Corresponding API secret

Usage
─────
  export TALOS_HOST=tal-123.sandbox.talostrading.com
  export TALOS_API_KEY=...
  export TALOS_API_SECRET=...
  uv run --with requests --python 3.12 lms_e2e_setup.py

Notes
─────
- The wallet address must be fresh — never previously attempted on this sandbox,
  even in a failed run. A failed POST /v1/market-accounts still inserts a row in
  market_account_histories, causing a 500 on retry with the same address.
  Always use a new address per client/demo.

- Always pass an explicit Name (evmchain/<wallet>) in the market account request.
  Without it, the server auto-generates a name from the market name and description,
  which collides across separate attempts on the same sandbox.

- The evmchain gateway polls the wallet every ~15 seconds once the market account
  is active. On-chain balance changes are NOT automatically posted to the
  sub-account ledger (Q3 2026 roadmap). Post balances manually via
  PUT /v1/positions/subaccounts when collateral moves.

- GET /v1/contracts/deals does not exist. Deals are readable via WebSocket only.

References
──────────
  Talos LMS KB:    https://kb.talostrading.com/portfolio-management/lms
  Talos API docs:  https://docs.talostrading.com
"""

from __future__ import annotations

import base64
import datetime
import hashlib
import hmac
import json
import os
import sys

import requests

# ── Inputs ───────────────────────────────────────────────────────────────────

NAME   = "tangerine"      # used as a prefix for all created resources
WALLET = "0xdadB0d80178819F2319190D340ce9A924f783711"  # EVM wallet — must be fresh

INITIAL_USDC = "50000"    # USDC posted to collateral sub-account at origination
LOAN_ETH     = "10"       # ETH lent to the borrower

# ── Credentials (from environment) ───────────────────────────────────────────

HOST       = os.environ.get("TALOS_HOST", "")
API_KEY    = os.environ.get("TALOS_API_KEY", "")
API_SECRET = os.environ.get("TALOS_API_SECRET", "")

if not HOST or not API_KEY or not API_SECRET:
    sys.exit(
        "Set TALOS_HOST, TALOS_API_KEY, and TALOS_API_SECRET before running.\n"
        "Example:\n"
        "  export TALOS_HOST=tal-123.sandbox.talostrading.com\n"
        "  export TALOS_API_KEY=...\n"
        "  export TALOS_API_SECRET=..."
    )

BASE = f"https://{HOST}"

# Flush stdout immediately so step headers appear before any stderr output.
sys.stdout.reconfigure(line_buffering=True)

# ── Derived names ─────────────────────────────────────────────────────────────

CP_NAME            = f"{NAME}-borrower"
LOAN_SA_NAME       = f"{NAME}-loan"
COLLATERAL_SA_NAME = f"{NAME}-collateral"
ROLLUP_NAME        = f"{NAME}-net"
WALLET_MA_NAME     = f"evmchain/{WALLET}"

TODAY          = datetime.date.today()
TRADE_TIME     = TODAY.strftime("%Y-%m-%dT00:00:00.000000Z")
EFFECTIVE_TIME = TRADE_TIME
EXPIRY_TIME    = (TODAY + datetime.timedelta(days=31)).strftime("%Y-%m-%dT00:00:00.000000Z")
SETTLE_TIME    = (TODAY + datetime.timedelta(days=32)).strftime("%Y-%m-%dT00:00:00.000000Z")

# ── Signed HTTP ───────────────────────────────────────────────────────────────

def _sign(method: str, path: str, body: str = "") -> dict:
    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"
    parts = [method, ts, HOST, path]
    if body:
        parts.append(body)
    sig = base64.urlsafe_b64encode(
        hmac.new(API_SECRET.encode("ascii"), "\n".join(parts).encode("ascii"),
                 hashlib.sha256).digest()
    ).decode()
    return {"TALOS-KEY": API_KEY, "TALOS-SIGN": sig, "TALOS-TS": ts,
            "Content-Type": "application/json"}


def _parse(r: requests.Response) -> dict:
    return r.json() if r.text.strip() else {}


def _die(method: str, path: str, r: requests.Response) -> None:
    print(f"\n  ✗ {method} {path} → {r.status_code}: {r.text[:400]}", file=sys.stderr)
    sys.exit(1)


def get(path: str) -> dict:
    r = requests.get(f"{BASE}{path}", headers=_sign("GET", path), timeout=20)
    if not r.ok:
        _die("GET", path, r)
    return _parse(r)


def post(path: str, body: dict) -> dict:
    raw = json.dumps(body, separators=(",", ":"))
    r = requests.post(f"{BASE}{path}", headers=_sign("POST", path, raw), data=raw, timeout=20)
    if not r.ok:
        _die("POST", path, r)
    return _parse(r)


def put(path: str, body) -> dict:
    raw = json.dumps(body, separators=(",", ":"))
    r = requests.put(f"{BASE}{path}", headers=_sign("PUT", path, raw), data=raw, timeout=20)
    if not r.ok:
        _die("PUT", path, r)
    return _parse(r)


def _first(resp: dict) -> dict:
    data = resp.get("data", [])
    return data[0] if isinstance(data, list) and data else resp


def _items(resp: dict) -> list:
    data = resp.get("data", [])
    return data if isinstance(data, list) else []


def _step(n: int, msg: str) -> None:
    print(f"\n{'─'*64}\nStep {n:>2}  {msg}\n{'─'*64}")


_summary: dict = {}


# ── Step 1 — Confirm evmchain credential ─────────────────────────────────────

_step(1, "Confirm evmchain credential exists")

creds    = _items(get("/v1/credentials"))
evm_cred = next((c for c in creds if "evm" in (c.get("Name") or "").lower()
                  or c.get("Type") == "temporal_api_key"), None)
if evm_cred:
    print(f"  ✓ {evm_cred.get('Name')}  id={evm_cred.get('CredentialID')}")
else:
    print("  ⚠ No evmchain credential found — on-chain balance polling will not work.")
    print("    Ask Talos support to provision a temporal_api_key credential.")


# ── Step 2 — Create Transfer market account ───────────────────────────────────

_step(2, f"Create Transfer market account  ({WALLET_MA_NAME})")

all_mas   = _items(get("/v1/market-accounts"))
wallet_ma = next((ma for ma in all_mas
                  if (ma.get("Name") or "").lower() == WALLET_MA_NAME.lower()), None)

if wallet_ma:
    print(f"  · already exists  id={wallet_ma.get('MarketAccountID')}")
else:
    resp = post("/v1/market-accounts", {
        "SourceAccountID":  WALLET,
        "Market":           "evmchain",
        "Name":             WALLET_MA_NAME,   # always explicit — avoids auto-name collisions
        "DisplayName":      f"{NAME.capitalize()} — Collateral Wallet",
        "InternalMetadata": {"TokenSymbols": ["ETH", "USDC"]},
    })
    wallet_ma = _first(resp)
    print(f"  ✓ created  id={wallet_ma.get('MarketAccountID')}  name={wallet_ma.get('Name')}")

WALLET_MA_ID   = wallet_ma.get("MarketAccountID")
WALLET_MA_NAME = wallet_ma.get("Name")
_summary["wallet_ma_id"]   = WALLET_MA_ID
_summary["wallet_ma_name"] = WALLET_MA_NAME


# ── Step 3 — Create collateral sub-account ────────────────────────────────────

_step(3, f"Create collateral sub-account  ({COLLATERAL_SA_NAME})")

all_sas    = _items(get("/v1/subaccounts"))
sa_by_name = {sa.get("Name"): sa for sa in all_sas}

if COLLATERAL_SA_NAME in sa_by_name:
    collateral_sa = sa_by_name[COLLATERAL_SA_NAME]
    print(f"  · already exists  id={collateral_sa.get('SubaccountID')}")
else:
    collateral_sa = _first(post("/v1/subaccounts", {
        "Name": COLLATERAL_SA_NAME, "DisplayName": f"{NAME.capitalize()} — Collateral",
    }))
    print(f"  ✓ created  id={collateral_sa.get('SubaccountID')}")

COLLATERAL_SA_ID = collateral_sa.get("SubaccountID")
_summary["collateral_sa_id"] = COLLATERAL_SA_ID


# ── Step 4 — Create loan sub-account ─────────────────────────────────────────

_step(4, f"Create loan sub-account  ({LOAN_SA_NAME})")

all_sas    = _items(get("/v1/subaccounts"))
sa_by_name = {sa.get("Name"): sa for sa in all_sas}

if LOAN_SA_NAME in sa_by_name:
    loan_sa = sa_by_name[LOAN_SA_NAME]
    print(f"  · already exists  id={loan_sa.get('SubaccountID')}")
else:
    loan_sa = _first(post("/v1/subaccounts", {
        "Name": LOAN_SA_NAME, "DisplayName": f"{NAME.capitalize()} — Loan",
    }))
    print(f"  ✓ created  id={loan_sa.get('SubaccountID')}")

LOAN_SA_ID = loan_sa.get("SubaccountID")
_summary["loan_sa_id"] = LOAN_SA_ID


# ── Step 5 — Create net rollup ────────────────────────────────────────────────

_step(5, f"Create net rollup  ({ROLLUP_NAME})")

all_rollups    = _items(get("/v1/subaccounts/rollups"))
rollup_by_name = {r.get("DisplayName"): r for r in all_rollups}

if ROLLUP_NAME in rollup_by_name:
    net_rollup = rollup_by_name[ROLLUP_NAME]
    print(f"  · already exists  id={net_rollup.get('SubaccountID')}")
else:
    net_rollup = _first(post("/v1/subaccounts/rollups", {
        "Name": ROLLUP_NAME, "DisplayName": ROLLUP_NAME,
    }))
    print(f"  ✓ created  id={net_rollup.get('SubaccountID')}")

ROLLUP_ID = net_rollup.get("SubaccountID")
_summary["rollup_id"] = ROLLUP_ID


# ── Step 6 — Wire rollup memberships ─────────────────────────────────────────

_step(6, f"Wire memberships  ({ROLLUP_NAME} ← {LOAN_SA_NAME} + {COLLATERAL_SA_NAME})")

_mem = requests.get(f"{BASE}/v1/subaccounts/rollups/memberships/{ROLLUP_ID}",
                    headers=_sign("GET", f"/v1/subaccounts/rollups/memberships/{ROLLUP_ID}"),
                    timeout=20)
current = sorted(int(m.get("ChildID") or 0)
                 for m in (_mem.json().get("data") or [] if _mem.ok else []))
desired = sorted([int(LOAN_SA_ID), int(COLLATERAL_SA_ID)])

if current == desired:
    print(f"  · already set")
else:
    put(f"/v1/subaccounts/rollups/memberships/{ROLLUP_ID}", desired)
    print(f"  ✓ wired  {desired}")


# ── Step 7 — Permission filter ────────────────────────────────────────────────

_step(7, f"Link permission filter  ({COLLATERAL_SA_NAME} → {WALLET_MA_NAME})")

# Action: Write is required — the portfolio recon engine reads Write-action
# filters to build its sub-account ↔ market-account groupings.
all_filters   = _items(get("/v1/permission-filters/marketaccounts"))
filter_exists = any(
    (f.get("Subject") or {}).get("SubAccount") == COLLATERAL_SA_NAME
    and (f.get("Filter") or {}).get("MarketAccount") == WALLET_MA_NAME
    for f in all_filters
)

if filter_exists:
    print(f"  · already exists")
else:
    pf = _first(post("/v1/permission-filters/marketaccounts", {
        "Subject": {"SubAccount":    COLLATERAL_SA_NAME},
        "Action":  "Write",
        "Filter":  {"MarketAccount": WALLET_MA_NAME},
    }))
    print(f"  ✓ created  FilterID={pf.get('FilterID')}")


# ── Step 8 — Create borrower counterparty ────────────────────────────────────

_step(8, f"Create borrower counterparty  ({CP_NAME})")

all_cps = _items(get("/v1/counterparties"))
cp      = next((c for c in all_cps if c.get("Name") == CP_NAME), None)

if cp:
    print(f"  · already exists  id={cp.get('CounterpartyID')}")
else:
    cp = _first(post("/v1/counterparties", {
        "Name": CP_NAME, "DisplayName": f"{NAME.capitalize()} Borrower", "Type": "Customer",
    }))
    print(f"  ✓ created  id={cp.get('CounterpartyID')}")

_summary["counterparty_id"] = cp.get("CounterpartyID")


# ── Step 9 — Post initial collateral balance ──────────────────────────────────

_step(9, f"Post initial USDC collateral  ({INITIAL_USDC} USDC → {COLLATERAL_SA_NAME})")

_pos = requests.get(
    f"{BASE}/v1/positions/subaccounts?SubAccounts={COLLATERAL_SA_NAME}&ShowZeroPositions=true",
    headers=_sign("GET", f"/v1/positions/subaccounts?SubAccounts={COLLATERAL_SA_NAME}&ShowZeroPositions=true"),
    timeout=20,
)
if _pos.ok:
    current_usdc = next(
        (float(p.get("Amount") or p.get("Quantity") or 0)
         for p in _items(_pos.json())
         if (p.get("Asset") or p.get("Currency") or "").upper() == "USDC"),
        0.0,
    )
else:
    current_usdc = 0.0

if current_usdc > 0:
    print(f"  · balance already present ({current_usdc} USDC) — skipping")
else:
    put("/v1/positions/subaccounts", [
        {
            "Type":        "Balance",
            "Asset":       "USDC",
            "AccountName": COLLATERAL_SA_NAME,
            "Amount":      INITIAL_USDC,
            "Comments":    f"Initial USDC collateral — wallet {WALLET}, posted {TODAY}",
        },
        {
            "Type":        "Balance",
            "Asset":       "ETH",
            "AccountName": COLLATERAL_SA_NAME,
            "Amount":      "0",
            "Comments":    f"Initial ETH — zero, posted {TODAY}",
        },
    ])
    print(f"  ✓ posted  {INITIAL_USDC} USDC + 0 ETH")


# ── Step 10 — Book loan deal ──────────────────────────────────────────────────

_step(10, f"Book loan deal  ({LOAN_ETH} ETH lent, USDC collateral, SOFR + 3 %)")

# Note: GET /v1/contracts/deals does not exist — deals are read via WebSocket.
# Each run creates a new deal; re-running is intentional for demo purposes.
resp = post("/v1/contracts/deals", {
    "Counterparty":     CP_NAME,
    "CounterpartyRole": "Borrower",
    "DealSide":         "Lend",
    "SubAccount":       LOAN_SA_NAME,
    "MarketAccount":    WALLET_MA_NAME,

    "FinancialInstrument": {
        "RequestType":    "OTC",
        "ProductType":    "OTC",
        "SubProductType": "OTCFixedRateLoan",
        "BaseCurrency":   "ETH",
        "ProductDetails": {
            "Term":               "1M",
            "PaymentPeriod":      "1M",
            "UnitLoanAmount":     "1",
            "BorrowedAsset":      "ETH",
            "CollateralAsset":    "USDC",
            "AccrualPeriod":      "1M",
            "DayCountConvention": "Actual365",
            "LoanPaymentCcy":     "ETH",
            "TermType":           "Fixed",
        },
    },

    "DealInfo": {
        "Side":                         "Lend",
        "LoanAmount":                   LOAN_ETH,
        "CollateralAsset":              "USDC",
        "EnteredQuantity":              LOAN_ETH,
        "TradeTime":                    TRADE_TIME,
        "EffectiveTime":                EFFECTIVE_TIME,
        "ExpiryTime":                   EXPIRY_TIME,
        "SettleTime":                   SETTLE_TIME,
        "SettleCycle":                  "1D",
        "BusinessDayHolidayConvention": "Following",
        "HolidayCalendars":             [{"Name": "NYSE"}],
        "PricingMode":                  "SpreadToFixing",
        "RateSource":                   "SOFR",
        "Rate":                         "0.0664",
        "Spread":                       "0.03",
        "LoanOriginationFee":           "0",
        "LoanOriginationFeeSpread":     "0",
        "CollateralReleaseLTV":         "0.67",
        "MinimumLTV":                   "0.68",
        "MarginCallLTV":                "0.70",
        "LiquidationLTV":               "0.71",
    },
})
deal        = _first(resp)
DEAL_ID     = deal.get("DealID")
DEAL_STATUS = deal.get("Status")
print(f"  ✓ created  id={DEAL_ID}  status={DEAL_STATUS}")
_summary["deal_id"] = DEAL_ID


# ── Step 11 — Activate deal ───────────────────────────────────────────────────

_step(11, f"Activate deal  ({DEAL_ID})")

if DEAL_STATUS == "Live":
    print(f"  · already Live")
else:
    _act = requests.post(
        f"{BASE}/v1/contracts/deals/{DEAL_ID}/activate",
        headers=_sign("POST", f"/v1/contracts/deals/{DEAL_ID}/activate", "{}"),
        data="{}", timeout=20,
    )
    if _act.ok:
        DEAL_STATUS = _first(_parse(_act)).get("Status", "Live")
        print(f"  ✓ status → {DEAL_STATUS}")
    elif "status Live" in _act.text or "already" in _act.text.lower():
        print(f"  · already transitioned to Live")
    else:
        _die("POST", f"/v1/contracts/deals/{DEAL_ID}/activate", _act)

_summary["deal_status"] = DEAL_STATUS


# ── Step 12 — Verify positions ────────────────────────────────────────────────

_step(12, "Verify positions")

for sa_name in (LOAN_SA_NAME, COLLATERAL_SA_NAME):
    _path = f"/v1/positions/subaccounts?SubAccounts={sa_name}&ShowZeroPositions=true"
    _r    = requests.get(f"{BASE}{_path}", headers=_sign("GET", _path), timeout=20)
    print(f"\n  {sa_name}:")
    if not _r.ok:
        print(f"    ({_r.status_code} — check API key roles include org.treasury)")
    else:
        for p in _items(_r.json()):
            asset = p.get("Asset") or p.get("Currency") or p.get("Symbol") or "?"
            amt   = p.get("Amount") or p.get("Quantity") or p.get("NetPosition") or "?"
            print(f"    {asset:<12}  {amt}")


# ── Summary ───────────────────────────────────────────────────────────────────

print(f"""
{'═'*64}
LMS setup complete — {NAME.upper()}
{'═'*64}

  Wallet              {WALLET}
  Market account      {_summary.get('wallet_ma_name')}  (id={_summary.get('wallet_ma_id')})

  Sub-accounts
    {LOAN_SA_NAME:<30}  id={_summary.get('loan_sa_id')}
    {COLLATERAL_SA_NAME:<30}  id={_summary.get('collateral_sa_id')}

  Rollup
    {ROLLUP_NAME:<30}  id={_summary.get('rollup_id')}

  Counterparty        {CP_NAME:<22}  id={_summary.get('counterparty_id')}
  Initial collateral  {INITIAL_USDC} USDC → {COLLATERAL_SA_NAME}

  Deal                {_summary.get('deal_id')}
    {LOAN_ETH} ETH lent  /  USDC collateral  /  SOFR + 3%
    {TRADE_TIME[:10]} → {EXPIRY_TIME[:10]}  status={_summary.get('deal_status')}

  Permission filter
    {COLLATERAL_SA_NAME} → {_summary.get('wallet_ma_name')} [Write]
    Enables IBOR reconciliation — recon pairs this sub-account
    with the evmchain wallet market account at each checkpoint.
{'═'*64}
""")
