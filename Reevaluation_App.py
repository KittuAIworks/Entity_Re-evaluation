import os
import time
import csv
import uuid
import re
import datetime as dt
from typing import Dict, Any, Tuple, List, Optional

import requests
import streamlit as st

# ---------------------------------------------
# APP CONFIG
# ---------------------------------------------
st.set_page_config(page_title="Entity Reevaluation (Bulk)", page_icon="🔄", layout="wide")
st.title("🔄 Entity Reevaluation — Single / Multiple / All Types")

# Keep runtime simple (no external deps beyond streamlit/requests)
MOCK_MODE = False
REQUEST_TIMEOUT = 40
MAX_RETRIES = 3
BACKOFF_SECONDS = 1.25

# API paths (as per your spec)
MODEL_GET_PATH = "/api/entitymodelservice/get"
APP_GET_PATH   = "/api/entityappservice/get"
REEVAL_PATH    = "/api/entitygovernservice/reevaluate"

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

def sanitize_entity_id(entity_id: str) -> str:
    """Allow alphanumerics, dash, underscore, colon, dot."""
    return re.sub(r"[^A-Za-z0-9_:\-\.]", "", (entity_id or "").strip())

def audit_path_today() -> str:
    return os.path.join(AUDIT_DIR, f"reevaluate_audit_{dt.datetime.now().strftime('%Y%m%d')}.csv")

def write_audit(row: Dict[str, Any]):
    path = audit_path_today()
    exists = os.path.isfile(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "timestamp_iso", "tenant", "base_mode", "base_host",
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
                time.sleep(backoff * attempt)
                continue
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

def build_base_url(base_mode: str, http: str, apiurl: str, tenant: str) -> str:
    """
    base_mode:
      - 'api'    -> {{HTTP}}://{{APIURL}}
      - 'tenant' -> https://{tenant}.syndigo.com
    """
    http = (http or "https").rstrip(":/")
    apiurl = (apiurl or "").strip().rstrip("/")
    if base_mode == "api":
        return f"{http}://{apiurl}" if apiurl else ""
    tenant = (tenant or "").strip()
    return f"https://{tenant}.syndigo.com" if tenant else ""

# ---- extractors tuned to the sample you shared ----
def find_total_records(j: Any) -> Optional[int]:
    """Fast path for counts: response.totalRecords"""
    try:
        return int(j.get("response", {}).get("totalRecords"))
    except Exception:
        # Fallback search if structure differs
        if isinstance(j, dict):
            for v in j.values():
                tr = find_total_records(v)
                if tr is not None:
                    return tr
        elif isinstance(j, list):
            for it in j:
                tr = find_total_records(it)
                if tr is not None:
                    return tr
    return None

def extract_entity_type_names(j: Any) -> List[str]:
    """
    Extracts type names from response.entities where item.type == "entityType" and 'name' exists.
    """
    names: List[str] = []
    seen = set()
    try:
        entities = j.get("response", {}).get("entities", [])
        for e in entities:
            if isinstance(e, dict) and e.get("type") == "entityType":
                nm = str(e.get("name", "")).strip()
                if nm and nm not in seen:
                    names.append(nm); seen.add(nm)
    except Exception:
        pass
    return names

def extract_entity_ids(j: Any, expected_type: str) -> List[str]:
    """
    Extract ids from `response.entities` where `type == expected_type`.
    Sample shape:
    {
      "response": {
        "totalRecords": 838,
        "entities": [
          {"id": "...", "type": "tertiarypackaging", ...},
          ...
        ]
      }
    }
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
# UI — Connection & Mode
# ---------------------------------------------
with st.sidebar:
    st.header("🔐 Connection")
    base_mode = st.radio("Base URL mode", ["API host (HTTP+APIURL)", "Tenant subdomain"], index=0)
    http = st.text_input("HTTP", value="https", help="http or https (used in API-host mode)")
    apiurl = st.text_input("APIURL", value="", help="e.g., api.mycompany.com (used in API-host mode)")
    tenant = st.text_input("Tenant (header + subdomain mode)", value="")

    st.divider()
    user_id = st.text_input("User ID", value="system")
    client_id = st.text_input("Client ID", value="")
    client_secret = st.text_input("Client Secret", value="", type="password")

    st.caption("In API-host mode we call {{HTTP}}://{{APIURL}}/... endpoints. "
               "In Tenant-subdomain mode we call https://{tenant}.syndigo.com/...")

# Build base URL
mode_key = "api" if base_mode.startswith("API") else "tenant"
BASE_URL = build_base_url(mode_key, http, apiurl, tenant)

# ---------------------------------------------
# STEP 1 — Discover entity types
# ---------------------------------------------
st.subheader("① Discover Entity Types")
c1, c2 = st.columns([1, 3])
with c1:
    btn_fetch_types = st.button("Fetch entity types", type="primary", use_container_width=True,
                                disabled=not BASE_URL or not client_id or not client_secret)
with c2:
    st.write("Calls `/api/entitymodelservice/get` with a filter on `entityType` and extracts type names.")

if "entity_types" not in st.session_state: st.session_state.entity_types = []
if "type_counts" not in st.session_state: st.session_state.type_counts = {}
if "selected_types" not in st.session_state: st.session_state.selected_types = []
if "ids_by_type" not in st.session_state: st.session_state.ids_by_type = {}

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
        names = extract_entity_type_names(j)
        names = sorted(set(names))
        if not names:
            st.warning("No entity types found. If your API returns names under a different path, we can adjust the extractor.")
        st.session_state.entity_types = names
        st.success(f"Found {len(names)} entity type(s).")

if st.session_state.entity_types:
    st.dataframe([{"entityType": n} for n in st.session_state.entity_types], use_container_width=True, hide_index=True)

# ---------------------------------------------
# STEP 2 — Load counts per entity type
# ---------------------------------------------
st.subheader("② Get Counts per Entity Type")
cols = st.columns([1,1,3])
with cols[0]:
    btn_counts = st.button("Load counts", disabled=not st.session_state.entity_types)
with cols[1]:
    delay_between_calls = st.number_input("Delay (sec) between count calls", min_value=0.0, max_value=5.0, value=0.1, step=0.1)

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
            tr = find_total_records(j)
            counts[et] = {"totalRecords": tr}
        prog.progress(idx / max(1, len(st.session_state.entity_types)))
        if delay_between_calls > 0: time.sleep(delay_between_calls)
    st.session_state.type_counts = counts
    st.success("Counts loaded.")

if st.session_state.type_counts:
    table = [{"entityType": et, "totalRecords": (st.session_state.type_counts.get(et) or {}).get("totalRecords")}
             for et in st.session_state.entity_types]
    st.dataframe(table, use_container_width=True, hide_index=True)

# ---------------------------------------------
# STEP 3 — Select types & fetch IDs
# ---------------------------------------------
st.subheader("③ Select Types and Fetch IDs")
csel1, csel2, csel3, csel4 = st.columns([2,1,1,2])
with csel1:
    selected = st.multiselect("Select entity type(s)", st.session_state.entity_types, default=st.session_state.selected_types)
with csel2:
    sel_all = st.checkbox("Select all", value=(len(selected)==len(st.session_state.entity_types) and len(selected)>0))
with csel3:
    per_type_limit = st.number_input("Max IDs per type", min_value=1, max_value=10000, value=100, step=50)
with csel4:
    page_size = st.number_input("Fetch batch size", min_value=10, max_value=2000, value=500, step=50,
                                help="Sent as options.maxRecords; pagination via options.offset.")

if sel_all:
    selected = st.session_state.entity_types[:]
st.session_state.selected_types = selected

cbtn1, _ = st.columns([1,4])
with cbtn1:
    btn_fetch_ids = st.button("Fetch IDs", type="primary", disabled=not selected)

def fetch_ids_for_type(base_url: str, headers: Dict[str, str], entity_type: str,
                       wanted: int, batch: int) -> List[str]:
    """
    Page through /api/entityappservice/get, collecting IDs from response.entities.
    Stops when:
      - we've collected `wanted` IDs, or
      - a page returns 0 entities, or
      - last page has fewer than requested.
    """
    got: List[str] = []
    offset = 0

    while len(got) < wanted:
        take = min(batch, wanted - len(got))
        body = {
            "params": {
                "query": {"filters": {"typesCriterion": [entity_type]}},
                # Keep fields lean if your API accepts; adjust to your contract if needed
                "fields": {"attributes": ["_ALL"]},
                "options": {"maxRecords": int(take), "offset": int(offset)}
            }
        }
        url = f"{base_url}{APP_GET_PATH}"
        j, status, _ = robust_post(url, headers, body, REQUEST_TIMEOUT, MAX_RETRIES, BACKOFF_SECONDS)
        if status >= 400:
            break

        ids = extract_entity_ids(j, entity_type)
        if not ids:
            # no more data
            break

        # append unique
        for iid in ids:
            if iid not in got:
                got.append(iid)

        # Advance offset by what the API actually returned
        offset += len(ids)

        # If API returned fewer than we asked, we've hit the end
        if len(ids) < take:
            break

    return got[:wanted]

if btn_fetch_ids:
    if not BASE_URL:
        st.error("Base URL is not configured.")
        st.stop()
    headers = build_headers(user_id, client_id, client_secret, tenant)
    ids_by_type: Dict[str, List[str]] = {}
    prog = st.progress(0.0)
    for idx, et in enumerate(selected, start=1):
        ids = fetch_ids_for_type(BASE_URL, headers, et, per_type_limit, page_size)
        ids_by_type[et] = ids
        prog.progress(idx / max(1, len(selected)))
    st.session_state.ids_by_type = ids_by_type
    st.success("Fetched IDs for selected type(s).")

if st.session_state.ids_by_type:
    preview_rows = []
    total_ids = 0
    for et, ids in st.session_state.ids_by_type.items():
        total_ids += len(ids or [])
        preview_rows.append({"entityType": et, "idsFound": len(ids or []), "sampleId": (ids[0] if ids else "")})
    st.dataframe(preview_rows, use_container_width=True, hide_index=True)
    st.caption(f"Total IDs fetched across selection: **{total_ids}**")

# ---------------------------------------------
# STEP 4 — Bulk Reevaluation
# ---------------------------------------------
st.subheader("④ Reevaluation")
rate_delay = st.number_input("Delay (sec) between reevaluate calls", min_value=0.0, max_value=5.0, value=0.1, step=0.1)
btn_reeval = st.button("Start Reevaluation", type="secondary",
                       disabled=(not st.session_state.ids_by_type or not BASE_URL or not client_id or not client_secret))

results: List[Dict[str, Any]] = []
if btn_reeval:
    headers = build_headers(user_id, client_id, client_secret, tenant)
    jobs = sum(len(v or []) for v in st.session_state.ids_by_type.values())
    done = 0
    prog = st.progress(0.0)
    status_area = st.empty()

    for et, ids in st.session_state.ids_by_type.items():
        for iid in ids:
            payload = make_request_payload(iid, et)
            url = f"{BASE_URL}{REEVAL_PATH}"
            j, status, latency = robust_post(url, headers, {"entity": payload["entity"]}, REQUEST_TIMEOUT, MAX_RETRIES, BACKOFF_SECONDS)
            success = (200 <= status < 300) and bool(j.get("success", True))
            msg = j.get("message") or j.get("error","")
            backend_rid = j.get("requestId") or j.get("backendRequestId") or ""

            results.append({
                "entityType": et, "id": iid, "httpStatus": status,
                "success": success, "latencySec": round(latency, 2), "message": msg[:200],
                "backendRequestId": backend_rid
            })

            write_audit({
                "timestamp_iso": dt.datetime.now().isoformat(timespec="seconds"),
                "tenant": tenant or "",
                "base_mode": mode_key,
                "base_host": f"{http}://{apiurl}" if mode_key == "api" else f"{tenant}.syndigo.com",
                "user_id": user_id or "system",
                "entity_id": iid,
                "entity_type": et,
                "request_id": payload["requestId"],
                "status": "success" if success else "failure",
                "http_status": status,
                "latency_sec": f"{latency:.2f}",
                "message": msg,
                "backend_request_id": backend_rid,
            })

            done += 1
            prog.progress(done / max(1, jobs))
            status_area.info(f"Processed {done}/{jobs}: {et} / {iid} — {'✅' if success else '❌'}")
            if rate_delay > 0: time.sleep(rate_delay)

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
