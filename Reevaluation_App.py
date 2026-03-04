import os
import time
import csv
import uuid
import re
import math
import datetime as dt
from typing import Dict, Any, Tuple, List, Optional

import requests
import streamlit as st

# ---------------------------------------------
# APP CONFIG
# ---------------------------------------------
st.set_page_config(page_title="Entity Reevaluation (Bulk)", page_icon="🔄", layout="wide")
st.title("🔄 Entity Reevaluation — Single / Multiple / All Types")

# Minimal runtime (Streamlit + Requests only)
MOCK_MODE = False
REQUEST_TIMEOUT = 40
MAX_RETRIES = 3
BACKOFF_SECONDS = 1.25

# API paths
MODEL_GET_PATH = "/api/entitymodelservice/get"
APP_GET_PATH   = "/api/entityappservice/get"
REEVAL_PATH    = "/api/entitygovernservice/reevaluate"

# Fixed page size for streaming pulls (no UI control)
PAGE_SIZE = 500

# Audit
AUDIT_DIR = "audit_logs"
os.makedirs(AUDIT_DIR, exist_ok=True)

# ---------------------------------------------
# HELPERS
# ---------------------------------------------
def build_headers(user_id: str, client_id: str, client_secret: str, tenant: str = "") -> Dict[str, str]:
    """
    Matches your previous header style; optional x-tenant-id included if you need it.
    """
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
    """Fast path for counts: response.totalRecords"""
    try:
        return int(j.get("response", {}).get("totalRecords"))
    except Exception:
        return None

def extract_entity_type_names(j: Any) -> List[str]:
    """
    Extracts type names from response.entityModels where item.type == 'entityType' and 'name' exists.
    """
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
    """
    Extract ids from `response.entities` where `type == expected_type` (single page).
    """
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

# ---------------------------------------------
# SIDEBAR — Connection (tenant only)
# ---------------------------------------------
with st.sidebar:
    st.header("🔐 Connection")
    tenant = st.text_input("Tenant", value="", placeholder="your-tenant")
    user_id = st.text_input("User ID", value="system")
    client_id = st.text_input("Client ID", value="")
    client_secret = st.text_input("Client Secret", value="", type="password")

# Build base URL (tenant subdomain only)
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
# STEP 2 — Load counts per entity type (optional, used for progress)
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
# STEP 3 — Select types (no ID prefetch)
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
# STEP 4 — Bulk Reevaluation (stream IDs on-the-fly)
# ---------------------------------------------
st.subheader("④ Reevaluation")
btn_reeval = st.button("Start Reevaluation", type="primary",
                       disabled=(not selected or not BASE_URL or not client_id or not client_secret))

results: List[Dict[str, Any]] = []
if btn_reeval:
    headers = build_headers(user_id, client_id, client_secret, tenant)

    # Determine total jobs for progress (best effort, based on counts if available)
    def get_count(et: str) -> Optional[int]:
        try:
            v = st.session_state.type_counts.get(et, {}).get("totalRecords")
            return int(v) if v is not None else None
        except Exception:
            return None

    total_estimated = 0
    counts_available = True
    for et in selected:
        c = get_count(et)
        if c is None:
            counts_available = False
            break
        total_estimated += c

    # Progress trackers
    done = 0
    overall = st.progress(0.0)
    status_area = st.empty()
    per_type_box = st.empty()

    for et in selected:
        # If we know the count, we can show per-type page plan
        count_et = get_count(et)
        per_type_pages = math.ceil(count_et / PAGE_SIZE) if (count_et and count_et > 0) else None
        fetched_ids_this_type = 0

        offset = 0
        page_idx = 0
        while True:
            page_idx += 1
            body = {
                "params": {
                    "query": {"filters": {"typesCriterion": [et]}},
                    "fields": {"attributes": ["_ALL"]},
                    "options": {"maxRecords": PAGE_SIZE, "offset": offset}
                }
            }
            url = f"{BASE_URL}{APP_GET_PATH}"
            j, status, _ = robust_post(url, headers, body, REQUEST_TIMEOUT, MAX_RETRIES, BACKOFF_SECONDS)
            if status >= 400:
                status_area.error(f"[{et}] Failed to fetch page offset={offset} (HTTP {status}). {j.get('error','')[:160]}")
                break

            # Stream IDs from this page
            ids = extract_entity_ids_from_page(j, et)
            if not ids:
                # no more data
                break

            # Immediately reevaluate for each ID (no storing)
            for iid in ids:
                payload = make_request_payload(iid, et)
                reeval_url = f"{BASE_URL}{REEVAL_PATH}"
                rj, rstatus, rlat = robust_post(reeval_url, headers, {"entity": payload["entity"]},
                                                REQUEST_TIMEOUT, MAX_RETRIES, BACKOFF_SECONDS)
                success = (200 <= rstatus < 300) and bool(rj.get("success", True))
                msg = rj.get("message") or rj.get("error","")
                backend_rid = rj.get("requestId") or rj.get("backendRequestId") or ""

                results.append({
                    "entityType": et, "id": iid, "httpStatus": rstatus,
                    "success": success, "latencySec": round(rlat, 2), "message": msg[:200],
                    "backendRequestId": backend_rid
                })

                write_audit({
                    "timestamp_iso": dt.datetime.now().isoformat(timespec="seconds"),
                    "tenant": tenant or "",
                    "user_id": user_id or "system",
                    "entity_id": iid,
                    "entity_type": et,
                    "request_id": payload["requestId"],
                    "status": "success" if success else "failure",
                    "http_status": rstatus,
                    "latency_sec": f"{rlat:.2f}",
                    "message": msg,
                    "backend_request_id": backend_rid,
                })

                # Update progress
                done += 1
                fetched_ids_this_type += 1
                if counts_available and total_estimated > 0:
                    overall.progress(min(1.0, done / total_estimated))
                status_area.info(f"Reevaluating: {et} — {fetched_ids_this_type}/{count_et or '?'} (global {done}/{total_estimated or '?'})")

            # Advance offset by the number of ids processed
            offset += len(ids)

            # Stop if we reached the known count
            if count_et is not None and offset >= count_et:
                break

            # If the API returned fewer than PAGE_SIZE, we've hit the end
            if len(ids) < PAGE_SIZE:
                break

        per_type_box.success(f"[{et}] Completed reevaluation for ~{fetched_ids_this_type} entities.")

    st.success("Reevaluation run complete.")

if results:
    st.subheader("Run Results")
    st.dataframe(results, use_container_width=True, hide_index=True)

st.markdown("---")
st.subheader("Recent Audit Entries (today)")
recent = read_recent_audit(20)
if recent:
    st.dataframe(recent, use_container_width=True, hide_index=True)
else:
    st.info("No activity yet today.")
