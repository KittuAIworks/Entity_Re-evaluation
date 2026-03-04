import os
import time
import csv
import uuid
import re
import datetime as dt
from typing import Dict, Any, Tuple, List, Optional, Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import streamlit as st

# ---------------------------------------------
# APP CONFIG
# ---------------------------------------------
st.set_page_config(page_title="Entity Re-evaluation (Bulk)", page_icon="🔄", layout="wide")
st.title("🔄 Entity Re-evaluation — Single / Multiple / All Types")

# Runtime knobs
MOCK_MODE = False
REQUEST_TIMEOUT = 40
MAX_RETRIES = 3
BACKOFF_SECONDS = 1.25

# API paths
MODEL_GET_PATH = "/api/entitymodelservice/get"
APP_GET_PATH   = "/api/entityappservice/get"
REEVAL_PATH    = "/api/entitygovernservice/reevaluate"

# Performance
PRIMARY_PAGE_SIZE   = 2000
RECOVERY_PAGE_SIZES = [1000, 500]     # sweeps if we still miss records
REEVAL_WORKERS     = 10               # concurrent re-evaluation calls

# Audit
AUDIT_DIR = "audit_logs"
os.makedirs(AUDIT_DIR, exist_ok=True)

# ---------------------------------------------
# HELPERS
# ---------------------------------------------
def build_headers(user_id: str, client_id: str, client_secret: str, tenant: str = "") -> Dict[str, str]:
    headers = {
        "Content-Type": "application/json",
        "x-rdp-version": "8.1",
        "x-rdp-clientId": "rdpclient",
        "x-rdp-userId": user_id or "system",
        "auth-client-id": client_id,
        "auth-client-secret": client_secret,
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    if tenant:
        headers["x-tenant-id"] = tenant
    return headers

def audit_path_today() -> str:
    return os.path.join(AUDIT_DIR, f"reevaluate_audit_{dt.datetime.now().strftime('%Y%m%d')}.csv")

def write_audit(row: Dict[str, Any]):
    path = audit_path_today()
    exists = os.path.isfile(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "timestamp_iso", "tenant",
                "user_id", "entity_id", "entity_type", "request_id",
                "status", "http_status", "latency_sec", "message", "backend_request_id",
            ],
        )
        if not exists:
            writer.writeheader()
        writer.writerow(row)

def read_recent_audit(n: int = 20):
    path = audit_path_today()
    if not os.path.isfile(path): return []
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))[-n:]

def make_request_payload(entity_id: str, entity_type: str) -> Dict[str, Any]:
    return {"entity": {"id": entity_id, "type": entity_type}, "requestId": str(uuid.uuid4())}

def robust_post(url: str, headers: Dict[str, str], body: Dict[str, Any],
                timeout: int, max_retries: int, backoff: float) -> Tuple[Dict[str, Any], int, float]:
    start = time.perf_counter()
    last_status, last_msg = 0, ""
    attempt = 0
    while attempt <= max_retries:
        attempt += 1
        try:
            r = requests.post(url, headers=headers, json=body, timeout=timeout)
            status = r.status_code
            if 200 <= status < 300:
                try:
                    return r.json(), status, time.perf_counter() - start
                except Exception:
                    return {}, status, time.perf_counter() - start
            if status in (429, 500, 502, 503, 504) and attempt <= max_retries:
                time.sleep(backoff * attempt); continue
            return {"error": r.text}, status, time.perf_counter() - start
        except requests.exceptions.Timeout:
            last_status, last_msg = 408, "Request timed out."
            if attempt <= max_retries:
                time.sleep(backoff * attempt); continue
            break
        except requests.exceptions.RequestException as e:
            last_status, last_msg = 520, f"Request exception: {str(e)}"
            if attempt <= max_retries:
                time.sleep(backoff * attempt); continue
            break
    return {"error": last_msg}, last_status, time.perf_counter() - start

# ---- extractors tuned to your API ----
def find_total_records(j: Any) -> Optional[int]:
    try:
        return int(j.get("response", {}).get("totalRecords"))
    except Exception:
        return None

def extract_entity_type_names(j: Any) -> List[str]:
    names: List[str] = []
    seen = set()
    try:
        models = j.get("response", {}).get("entityModels", [])
        for m in models:
            if isinstance(m, dict) and m.get("type") == "entityType":
                nm = str(m.get("name", "")).strip()
                if nm and nm not in seen:
                    names.append(nm); seen.add(nm)
    except Exception:
        pass
    return names

def extract_entity_ids_from_page(j: Any, expected_type: str) -> List[str]:
    ids: List[str] = []
    try:
        entities = j.get("response", {}).get("entities", [])
        for e in entities:
            if isinstance(e, dict) and e.get("type") == expected_type and "id" in e:
                val = str(e["id"]).strip()
                if val:
                    ids.append(val)
    except Exception:
        pass
    return ids

def fmt_min_sec(seconds: float) -> str:
    """Format seconds -> 'Xm Ys' (minutes and seconds)."""
    total_sec = int(seconds)
    m = total_sec // 60
    s = total_sec % 60
    return f"{m}m {s}s"

# ---------------------------------------------
# Paging strategies
# ---------------------------------------------
def _post_entities(base_url: str, headers: Dict[str, str], entity_type: str, options: Dict[str, Any]) -> Tuple[List[str], int]:
    """Helper to call entityappservice/get with the given options and return (ids, http_status)."""
    body = {
        "params": {
            "query": {"filters": {"typesCriterion": [entity_type]}},
            "fields": {"attributes": ["_ALL"]},
            "options": options
        }
    }
    url = f"{base_url}{APP_GET_PATH}"
    j, status, _ = robust_post(url, headers, body, REQUEST_TIMEOUT, MAX_RETRIES, BACKOFF_SECONDS)
    if status >= 400:
        return [], status
    return extract_entity_ids_from_page(j, entity_type), status

def iter_pages_pageNumber(base_url: str, headers: Dict[str, str], entity_type: str, page_size: int) -> Iterable[List[str]]:
    """Yield pages of IDs using pageNumber-based paging."""
    page = 1
    while True:
        ids, status = _post_entities(base_url, headers, entity_type, {"maxRecords": page_size, "pageNumber": page})
        if status >= 400 or not ids:
            break
        yield ids
        if len(ids) < page_size:
            break
        page += 1

def iter_pages_offset(base_url: str, headers: Dict[str, str], entity_type: str, page_size: int) -> Iterable[List[str]]:
    """Yield pages of IDs using offset-based paging."""
    offset = 0
    while True:
        ids, status = _post_entities(base_url, headers, entity_type, {"maxRecords": page_size, "offset": offset})
        if status >= 400 or not ids:
            break
        yield ids
        offset += len(ids)
        if len(ids) < page_size:
            break

def fetch_all_ids_resilient(base_url: str, headers: Dict[str, str], entity_type: str, expected_total: Optional[int], heartbeat_cb=None) -> Iterable[List[str]]:
    """
    Robust generator that yields chunks of *new* IDs for the entity_type.
    Strategy:
      Phase A: pageNumber with big pages.
      Phase B: offset with big pages.
      Phase C: recovery sweeps (pageNumber and offset) with smaller pages (1000, then 500).
    """
    seen_ids: set = set()

    def hb(msg: str):
        if heartbeat_cb:
            heartbeat_cb(msg)

    # helper to yield only new IDs from any iterator
    def yield_new_from(iterator: Iterable[List[str]], label: str) -> int:
        new_gained = 0
        for ids in iterator:
            new_ids = [x for x in ids if x not in seen_ids]
            if new_ids:
                for x in new_ids: seen_ids.add(x)
                yield new_ids
                new_gained += len(new_ids)
            else:
                hb(f"{label}: received a duplicate-only page; switching strategy soon if this persists…")
        return new_gained

    # Phase A: pageNumber @2000
    hb("Phase A (pageNumber) — fetching large pages…")
    for new_ids in yield_new_from(iter_pages_pageNumber(base_url, headers, entity_type, PRIMARY_PAGE_SIZE), "pageNumber"):
        yield new_ids
    if expected_total is not None and len(seen_ids) >= expected_total:
        return

    # Phase B: offset @2000
    hb("Phase B (offset) — fetching large pages…")
    for new_ids in yield_new_from(iter_pages_offset(base_url, headers, entity_type, PRIMARY_PAGE_SIZE), "offset"):
        yield new_ids
    if expected_total is not None and len(seen_ids) >= expected_total:
        return

    # Phase C: recovery sweeps with smaller page sizes
    for ps in RECOVERY_PAGE_SIZES:
        hb(f"Phase C (recovery) — smaller pages ({ps}) via pageNumber…")
        for new_ids in yield_new_from(iter_pages_pageNumber(base_url, headers, entity_type, ps), f"pageNumber/{ps}"):
            yield new_ids
            if expected_total is not None and len(seen_ids) >= expected_total:
                return

        hb(f"Phase C (recovery) — smaller pages ({ps}) via offset…")
        for new_ids in yield_new_from(iter_pages_offset(base_url, headers, entity_type, ps), f"offset/{ps}"):
            yield new_ids
            if expected_total is not None and len(seen_ids) >= expected_total:
                return

    return

# ---------------------------------------------
# Re-evaluation worker
# ---------------------------------------------
def reeval_worker(base_url: str, headers: Dict[str, str], iid: str, et: str) -> Dict[str, Any]:
    payload = make_request_payload(iid, et)
    url = f"{base_url}{REEVAL_PATH}"
    j, status, latency = robust_post(url, headers, {"entity": payload["entity"]},
                                     REQUEST_TIMEOUT, MAX_RETRIES, BACKOFF_SECONDS)
    success = (200 <= status < 300) and bool(j.get("success", True))
    msg = j.get("message") or j.get("error","")
    backend_rid = j.get("requestId") or j.get("backendRequestId") or ""
    return {
        "entityType": et, "id": iid, "httpStatus": status, "success": success,
        "latencySec": round(latency, 2), "message": msg[:200],
        "backendRequestId": backend_rid, "requestId": payload["requestId"]
    }

# ---------------------------------------------
# SIDEBAR — Connection (tenant only)
# ---------------------------------------------
with st.sidebar:
    st.header("🔐 Connection")
    tenant = st.text_input("Tenant", value="", placeholder="your-tenant")
    user_id = st.text_input("User ID", value="system")
    client_id = st.text_input("Client ID", value="")
    client_secret = st.text_input("Client Secret", value="", type="password")

BASE_URL = f"https://{tenant}.syndigo.com" if tenant else ""

# ---------------------------------------------
# STEP 1 — Discover entity types
# ---------------------------------------------
st.subheader("① Discover Entity Types")
btn_fetch_types = st.button("Fetch entity types", type="primary", disabled=not BASE_URL or not client_id or not client_secret)

if "entity_types" not in st.session_state: st.session_state.entity_types = []
if "type_counts" not in st.session_state: st.session_state.type_counts = {}
if "selected_types" not in st.session_state: st.session_state.selected_types = []

if btn_fetch_types:
    headers = build_headers(user_id, client_id, client_secret, tenant)
    url = f"{BASE_URL}{MODEL_GET_PATH}"
    body = {
        "params": {
            "query": {
                "domain": "thing",
                "filters": {"typesCriterion": ["entityType"]}
            }
        }
    }
    with st.spinner("Fetching entity types..."):
        j, status, _ = robust_post(url, headers, body, REQUEST_TIMEOUT, MAX_RETRIES, BACKOFF_SECONDS)
    if status >= 400:
        st.error(f"Failed to fetch types (HTTP {status}). {j.get('error','')[:300]}")
    else:
        names = sorted(set(extract_entity_type_names(j)))
        st.session_state.entity_types = names
        if names:
            st.success(f"Found {len(names)} entity type(s).")
        else:
            st.warning("No entity types found.")

if st.session_state.entity_types:
    st.dataframe([{"entityType": n} for n in st.session_state.entity_types], use_container_width=True, hide_index=True)

# ---------------------------------------------
# STEP 2 — Get counts (optional but improves progress accuracy)
# ---------------------------------------------
st.subheader("② Get Counts per Entity Type")
btn_counts = st.button("Load counts", disabled=not st.session_state.entity_types)

if btn_counts:
    headers = build_headers(user_id, client_id, client_secret, tenant)
    counts = {}
    prog = st.progress(0.0)
    for idx, et in enumerate(st.session_state.entity_types, start=1):
        url = f"{BASE_URL}{APP_GET_PATH}"
        body = {"params": {"query": {"filters": {"typesCriterion": [et]}}}}
        j, status, _ = robust_post(url, headers, body, REQUEST_TIMEOUT, MAX_RETRIES, BACKOFF_SECONDS)
        if status >= 400:
            counts[et] = {"totalRecords": None, "error": j.get("error","")[:160]}
        else:
            counts[et] = {"totalRecords": find_total_records(j)}
        prog.progress(idx / max(1, len(st.session_state.entity_types)))
    st.session_state.type_counts = counts
    st.success("Counts loaded.")

if st.session_state.type_counts:
    table = [{"entityType": et, "totalRecords": (st.session_state.type_counts.get(et) or {}).get("totalRecords")}
             for et in st.session_state.entity_types]
    st.dataframe(table, use_container_width=True, hide_index=True)

# ---------------------------------------------
# STEP 3 — Select types
# ---------------------------------------------
st.subheader("③ Select Types")
left, right = st.columns([3, 1])
with left:
    selected = st.multiselect("Select entity type(s)", st.session_state.entity_types,
                              default=st.session_state.selected_types)
with right:
    sel_all = st.checkbox("Select all", value=(len(selected)==len(st.session_state.entity_types) and len(selected)>0))

if sel_all:
    selected = st.session_state.entity_types[:]
st.session_state.selected_types = selected

# ---------------------------------------------
# STEP 4 — Bulk Re-evaluation (with preflight + heartbeat + adaptive pager)
# ---------------------------------------------
st.subheader("④ Re-evaluation")
btn_reeval = st.button("Start Re-evaluation", type="primary",
                       disabled=(not selected or not BASE_URL or not client_id or not client_secret))

results: List[Dict[str, Any]] = []
if btn_reeval:
    headers = build_headers(user_id, client_id, client_secret, tenant)

    # Expected global (if counts loaded)
    counts_available = bool(st.session_state.type_counts)
    expected_global = sum(int((st.session_state.type_counts.get(et) or {}).get("totalRecords") or 0) for et in selected) if counts_available else None

    # status areas
    overall = st.progress(0.0)
    status_area = st.empty()
    hb_area = st.empty()           # heartbeat line (updates during long requests)

    # show immediate start note
    status_area.info("Starting re-evaluation… preparing preflight checks. This ensures you see progress right away even if the first API call is slow.")
    st.write("")  # a small spacer

    # --- PREFLIGHT (fast ping) so the UI updates immediately ---
    preflight_fail = False
    for et in selected:
        tiny_body = {
            "params": {
                "query": {"filters": {"typesCriterion": [et]}},
                "options": {"maxRecords": 1}
            }
        }
        j, s, _ = robust_post(f"{BASE_URL}{APP_GET_PATH}", headers, tiny_body, timeout=15, max_retries=1, backoff=0.5)
        if s >= 400:
            preflight_fail = True
            st.error(f"Preflight failed for type `{et}` (HTTP {s}). Please verify tenant/credentials and try again.")
            break
        else:
            st.info(f"Preflight OK: `{et}` reachable.")
    if preflight_fail:
        st.stop()

    # heartbeat helper
    last_hb = time.time()
    def heartbeat(msg: str):
        nonlocal last_hb
        now = time.time()
        if now - last_hb >= 2.0:  # update at most once every ~2s
            hb_area.info(f"⏳ {msg}")
            last_hb = now

    # Track per-type exacts for final validation
    per_type_exact: Dict[str, int] = {}
    done_global = 0
    denom = expected_global if (counts_available and expected_global and expected_global > 0) else None

    with ThreadPoolExecutor(max_workers=REEVAL_WORKERS) as pool:
        for et in selected:
            per_type_t0 = time.perf_counter()
            expected_total = None
            if counts_available:
                try:
                    expected_total = int((st.session_state.type_counts.get(et) or {}).get("totalRecords") or 0)
                except Exception:
                    expected_total = None

            # stream + re-evaluate
            seen_for_type: set = set()
            status_area.info(f"Re-evaluating: {et} — 0/{expected_total if expected_total else '?'}")

            for new_ids in fetch_all_ids_resilient(BASE_URL, headers, et, expected_total, heartbeat_cb=heartbeat):
                # schedule in parallel
                futures = [pool.submit(reeval_worker, BASE_URL, headers, iid, et) for iid in new_ids]

                # consume
                for fut in as_completed(futures):
                    res = fut.result()
                    results.append(res)

                    # audit
                    write_audit({
                        "timestamp_iso": dt.datetime.now().isoformat(timespec="seconds"),
                        "tenant": tenant or "",
                        "user_id": user_id or "system",
                        "entity_id": res["id"],
                        "entity_type": et,
                        "request_id": res.get("requestId") or "",
                        "status": "success" if res["success"] else "failure",
                        "http_status": res["httpStatus"],
                        "latency_sec": f"{res['latencySec']:.2f}",
                        "message": res["message"],
                        "backend_request_id": res.get("backendRequestId") or "",
                    })

                # mark processed (unique)
                for iid in new_ids:
                    if iid not in seen_for_type:
                        seen_for_type.add(iid)
                        done_global += 1

                # update UI
                total_display = expected_total if (isinstance(expected_total, int) and expected_total > 0) else "?"
                if denom:
                    overall.progress(min(1.0, done_global / max(1, denom)))
                else:
                    # if counts not loaded, keep progress bounded by what we've processed
                    overall.progress(0.0 if done_global == 0 else min(1.0, done_global / (done_global + 1)))
                status_area.info(f"Re-evaluating: {et} — {len(seen_for_type)}/{total_display} (global {done_global}/{expected_global or '?'})")

                # heartbeat (still working)
                heartbeat(f"Processing `{et}` — {len(seen_for_type)} processed so far…")

                # stop early if exact reached
                if expected_total is not None and len(seen_for_type) >= expected_total:
                    break

            # finalize per-type
            per_type_exact[et] = len(seen_for_type)
            elapsed_label = fmt_min_sec(time.perf_counter() - per_type_t0)
            st.success(f"[{et}] Completed re-evaluation for {per_type_exact[et]} entities in {elapsed_label}.")

    # ---------- Final validation & messaging ----------
    if counts_available:
        missing_types = []
        for et in selected:
            exp = int((st.session_state.type_counts.get(et) or {}).get("totalRecords") or 0)
            got = int(per_type_exact.get(et, 0))
            if got < exp:
                missing_types.append((et, exp - got, exp, got))

        total_expected = sum(int((st.session_state.type_counts.get(et) or {}).get("totalRecords") or 0) for et in selected)
        total_got = sum(int(per_type_exact.get(et, 0)) for et in selected)

        if missing_types:
            st.warning("⚠️ Re-evaluation finished with deficits: the following entity types did not reach their expected counts:")
            for et, deficit, exp, got in missing_types:
                st.write(f"- **{et}**: processed **{got}** / expected **{exp}** (missing **{deficit}**)")
            st.warning(f"Global processed: **{total_got}/{total_expected}**")
        else:
            st.success(f"✅ Re-evaluation run complete (all counts matched). Global processed: **{total_got}/{total_expected}**")
    else:
        st.success("Re-evaluation run complete (counts not loaded → global total unknown).")

if results:
    st.subheader("Run Results")
    results_sorted = sorted(results, key=lambda x: (x["entityType"], x["id"]))
    st.dataframe(results_sorted, use_container_width=True, hide_index=True)

st.markdown("---")
st.subheader("Recent Audit Entries (today)")
recent = read_recent_audit(20)
if recent:
    st.dataframe(recent, use_container_width=True, hide_index=True)
else:
    st.info("No activity yet today.")
