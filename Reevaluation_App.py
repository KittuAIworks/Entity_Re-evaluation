import os
import time
import csv
import uuid
import re
import math
import datetime as dt
from typing import Dict, Any, Tuple, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import streamlit as st

# ---------------------------------------------
# APP CONFIG
# ---------------------------------------------
st.set_page_config(page_title="Entity Reevaluation (Bulk)", page_icon="🔄", layout="wide")
st.title("🔄 Entity Reevaluation — Single / Multiple / All Types")

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
PAGE_SIZE = 2000          # fetch large pages (works for <=2000 in one call; multi-page for >2000)
REEVAL_WORKERS = 10       # concurrent reevaluation calls

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

def fmt_hms(seconds: float) -> str:
    """Format seconds -> 'HH:MM:SS.mmm'"""
    ms = int((seconds - int(seconds)) * 1000)
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    return f"{h:02d}:{m:02d}:{s:02d}.{ms:03d}"

# Worker for threaded reevaluation
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
# STEP 4 — Bulk Reevaluation (stream pages + 10 threads + accurate counts + per-type total time)
# ---------------------------------------------
st.subheader("④ Reevaluation")
btn_reeval = st.button("Start Reevaluation", type="primary",
                       disabled=(not selected or not BASE_URL or not client_id or not client_secret))

results: List[Dict[str, Any]] = []
if btn_reeval:
    headers = build_headers(user_id, client_id, client_secret, tenant)

    # Total estimated jobs (if counts loaded)
    total_estimated = 0
    counts_available = True
    for et in selected:
        tr = (st.session_state.type_counts.get(et) or {}).get("totalRecords") if st.session_state.type_counts else None
        try:
            total_estimated += int(tr) if tr is not None else 0
            if tr is None: counts_available = False
        except Exception:
            counts_available = False

    done = 0
    overall = st.progress(0.0)
    status_area = st.empty()

    with ThreadPoolExecutor(max_workers=REEVAL_WORKERS) as pool:
        for et in selected:
            per_type_t0 = time.perf_counter()  # <-- start timer for this entity type

            seen_ids: set = set()
            offset = 0
            page_number = 1
            processed_this_type = 0
            count_et = None
            if st.session_state.type_counts:
                try:
                    count_et = int((st.session_state.type_counts.get(et) or {}).get("totalRecords") or 0)
                except Exception:
                    count_et = None

            while True:
                # Pull a large page (2000) using BOTH pageNumber and offset to handle backends that ignore one of them
                body = {
                    "params": {
                        "query": {"filters": {"typesCriterion": [et]}},
                        "fields": {"attributes": ["_ALL"]},
                        "options": {
                            "maxRecords": PAGE_SIZE,
                            "pageNumber": page_number,   # try pageNumber-based paging
                            "offset": offset             # keep offset for back-compat backends
                        }
                    }
                }
                url = f"{BASE_URL}{APP_GET_PATH}"
                j, status, _ = robust_post(url, headers, body, REQUEST_TIMEOUT, MAX_RETRIES, BACKOFF_SECONDS)
                if status >= 400:
                    status_area.error(f"[{et}] Failed to fetch page #{page_number} (offset={offset}) — HTTP {status}. {j.get('error','')[:160]}")
                    break

                ids = extract_entity_ids_from_page(j, et)

                # STOP if API gave no more data
                if not ids:
                    break

                # De-dup (handles flaky paging)
                new_ids = [x for x in ids if x not in seen_ids]
                for x in new_ids:
                    seen_ids.add(x)

                # If backend returned only duplicates, try advancing pageNumber/offset once more; if still dupes, stop.
                if not new_ids:
                    # advance both and try one more time
                    page_number += 1
                    offset += PAGE_SIZE
                    j2, status2, _ = robust_post(url, headers, {
                        "params": {
                            "query": {"filters": {"typesCriterion": [et]}},
                            "fields": {"attributes": ["_ALL"]},
                            "options": {"maxRecords": PAGE_SIZE, "pageNumber": page_number, "offset": offset}
                        }
                    }, REQUEST_TIMEOUT, MAX_RETRIES, BACKOFF_SECONDS)
                    ids2 = extract_entity_ids_from_page(j2, et) if status2 < 400 else []
                    new_ids2 = [x for x in ids2 if x not in seen_ids]
                    for x in new_ids2:
                        seen_ids.add(x)
                    if not new_ids2:
                        break  # nothing new even after advancing -> end
                    new_ids = new_ids2  # proceed with the new set

                # Schedule reevaluations in parallel (10 workers)
                futures = [pool.submit(reeval_worker, BASE_URL, headers, iid, et) for iid in new_ids]

                # Consume results as they complete
                for fut in as_completed(futures):
                    res = fut.result()
                    results.append(res)

                    # Audit (main thread)
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

                    # Exact counters
                    processed_this_type = len(seen_ids)     # unique processed for this type
                    done += 1

                    # Global progress
                    denom = total_estimated if (counts_available and total_estimated > 0) else max(1, done)
                    overall.progress(min(1.0, done / denom))

                    # Precise per-type progress text
                    display_total = count_et if (isinstance(count_et, int) and count_et > 0) else "?"
                    status_area.info(
                        f"Reevaluating: {et} — {processed_this_type}/{display_total} (global {done}/{total_estimated or '?'})"
                    )

                # Advance paging for next pull
                # Use what the API returned (len(ids)) for offset; increment page_number too.
                offset += len(ids)
                page_number += 1

                # STOP when we reached known total
                if count_et is not None and len(seen_ids) >= count_et:
                    break

                # STOP when last page had fewer than PAGE_SIZE (likely end)
                if len(ids) < PAGE_SIZE:
                    break

            # Per-type completion with exact count + total time
            exact_total_for_type = len(seen_ids)
            per_type_elapsed = time.perf_counter() - per_type_t0
            st.success(f"[{et}] Completed reevaluation for {exact_total_for_type} entities in {fmt_hms(per_type_elapsed)}.")

    st.success("Reevaluation run complete.")

if results:
    st.subheader("Run Results")
    # Sorted by entityType then id for readability
    results_sorted = sorted(results, key=lambda x: (x["entityType"], x["id"]))
    st.dataframe(results_sorted, use_container_width=True, hide_index=True)

st.markdown("---")
st.subheader("Recent Audit Entries (today)")
recent = read_recent_audit(20)
if recent:
    st.dataframe(recent, use_container_width=True, hide_index=True)
else:
    st.info("No activity yet today.")
