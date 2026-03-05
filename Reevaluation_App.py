import os
import time
import csv
import uuid
import datetime as dt
from typing import Dict, Any, List
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import streamlit as st

# ---------------------------------------------------------
# APP CONFIG
# ---------------------------------------------------------
st.set_page_config(page_title="Entity Re-evaluation (Bulk)", page_icon="🔄", layout="wide")
st.title("🔄 Entity Re-evaluation — Single / Multiple / All Types")

REQUEST_TIMEOUT = 40
MAX_RETRIES     = 3
BACKOFF         = 1.25
REEVAL_WORKERS  = 10

MODEL_GET_PATH   = "/api/entitymodelservice/get"
APP_GET_PATH     = "/api/entityappservice/get"
CLEAR_SCROLL_PATH= "/api/entityappservice/clearscroll"
REEVAL_PATH      = "/api/entitygovernservice/reevaluate"

AUDIT_DIR = "audit_logs"
os.makedirs(AUDIT_DIR, exist_ok=True)

# ---------------------------------------------------------
# HELPERS
# ---------------------------------------------------------
def build_headers(user_id: str, client_id: str, client_secret: str, tenant: str) -> Dict[str, str]:
    return {
        "Content-Type":      "application/json",
        "x-rdp-version":     "8.1",
        "x-rdp-clientId":    "rdpclient",
        "x-rdp-userId":      user_id or "system",
        "auth-client-id":    client_id,
        "auth-client-secret":client_secret,
        "Cache-Control":     "no-cache",
        "Pragma":            "no-cache",
        "x-tenant-id":       tenant
    }

def post_json(url: str, headers: Dict[str,str], body: Dict[str,Any]) -> (Dict[str,Any], int, float):
    start = time.perf_counter()
    for attempt in range(MAX_RETRIES+1):
        try:
            r = requests.post(url, headers=headers, json=body, timeout=REQUEST_TIMEOUT)
            # try parse JSON, but guard if body is empty
            try:
                data = r.json()
            except Exception:
                data = {}
            return data, r.status_code, time.perf_counter() - start
        except Exception as e:
            if attempt == MAX_RETRIES:
                return {"error": str(e)}, 520, time.perf_counter() - start
            time.sleep(BACKOFF*(attempt+1))
    return {}, 520, time.perf_counter() - start

def write_audit(row: Dict[str,Any]):
    path = os.path.join(AUDIT_DIR, f"audit_{dt.datetime.now().strftime('%Y%m%d')}.csv")
    new = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "timestamp","tenant","entity_id","entity_type",
            "status","http_status","latency","message","request_id"
        ])
        if new: w.writeheader()
        w.writerow(row)

def extract_entity_type_names(j: Dict[str,Any]) -> List[str]:
    names, seen = [], set()
    for m in j.get("response",{}).get("entityModels",[]):
        if isinstance(m, dict) and m.get("type") == "entityType":
            nm = str(m.get("name","")).strip()
            if nm and nm not in seen:
                seen.add(nm); names.append(nm)
    return names

def extract_ids(j: Dict[str,Any], etype: str) -> List[str]:
    ids=[]
    for e in j.get("response",{}).get("entities",[]):
        if isinstance(e, dict) and e.get("type")==etype and "id" in e:
            ids.append(str(e["id"]).strip())
    return ids

def fmt_min_sec(seconds: float) -> str:
    s = int(seconds)
    return f"{s//60}m {s%60}s"

# ---------------------------------------------------------
# SCROLL-BASED ID FETCH (3-step: prepareScroll → scrollId → clearScroll)
# ---------------------------------------------------------
def fetch_all_ids_via_scroll(base: str, headers: Dict[str,str], etype: str) -> List[str]:
    all_ids: List[str] = []
    seen: set = set()
    latest_scroll: str = ""

    # STEP 1 — Prepare Scroll (first 2000 + scrollId)
    body = {
        "params": {
            "prepareScroll": True,
            "query": {
                "filters": {
                    "typesCriterion": [etype]
                }
            }
        }
    }
    j, s, _ = post_json(f"{base}{APP_GET_PATH}", headers, body)
    if s >= 400:
        return []

    batch = extract_ids(j, etype)
    for i in batch:
        if i not in seen: seen.add(i); all_ids.append(i)

    # The scrollId might be under response.scrollId; use latest every time
    scroll_id = j.get("response", {}).get("scrollId")

    # STEP 2 — Loop with scrollId until no objects found / invalid
    while scroll_id and isinstance(scroll_id, str) and scroll_id.lower() != "invalid":
        latest_scroll = scroll_id
        body = {
            "params": {
                "scrollId": scroll_id,
                "query": {
                    "filters": {
                        "typesCriterion": [etype]
                    }
                }
            }
        }
        j, s, _ = post_json(f"{base}{APP_GET_PATH}", headers, body)
        if s >= 400:
            break

        # collect ids from this page
        batch = extract_ids(j, etype)
        if not batch:
            # service might already be at the end
            break

        for i in batch:
            if i not in seen: seen.add(i); all_ids.append(i)

        # IMPORTANT: scrollId can be regenerated each response; always capture the *latest*
        scroll_id = j.get("response", {}).get("scrollId")

        # safety: if service starts repeating same scrollId (rare), break to avoid loop
        if scroll_id == latest_scroll:
            break

    # STEP 3 — Clear Scroll (best effort)
    if latest_scroll and latest_scroll.lower() != "invalid":
        clear_body = {"params": {"scrollId": latest_scroll}}
        post_json(f"{base}{CLEAR_SCROLL_PATH}", headers, clear_body)

    return all_ids

# ---------------------------------------------------------
# Re-evaluation worker
# ---------------------------------------------------------
def reeval_worker(base: str, headers: Dict[str,str], iid: str, et: str) -> Dict[str,Any]:
    body = {"entity": {"id": iid, "type": et}}
    j, s, lat = post_json(f"{base}{REEVAL_PATH}", headers, body)
    success = (200 <= s < 300) and bool(j.get("success", True))
    msg = j.get("message") or j.get("error","")
    return {
        "id": iid,
        "success": success,
        "latency": round(lat, 2),
        "http": s,
        "msg": msg,
        "request_id": str(uuid.uuid4())
    }

# ---------------------------------------------------------
# SIDEBAR
# ---------------------------------------------------------
with st.sidebar:
    st.header("🔐 Connection")
    tenant        = st.text_input("Tenant", "")
    user_id       = st.text_input("User ID", "system")
    client_id     = st.text_input("Client ID", "")
    client_secret = st.text_input("Client Secret", "", type="password")

BASE_URL = f"https://{tenant}.syndigo.com" if tenant else ""

# ---------------------------------------------------------
# STEP 1 — Discover Entity Types
# ---------------------------------------------------------
st.subheader("① Discover Entity Types")
btn_types = st.button("Fetch entity types", type="primary",
                      disabled=not BASE_URL or not client_id or not client_secret)

if "types" not in st.session_state:
    st.session_state.types = []
if "counts" not in st.session_state:
    st.session_state.counts = {}
if "selected_types" not in st.session_state:
    st.session_state.selected_types = []

if btn_types:
    headers = build_headers(user_id, client_id, client_secret, tenant)
    body = {
        "params": {
            "query": {
                "domain": "thing",
                "filters": {"typesCriterion": ["entityType"]}
            }
        }
    }
    j, s, _ = post_json(f"{BASE_URL}{MODEL_GET_PATH}", headers, body)
    if s >= 400:
        st.error(f"Failed to fetch types (HTTP {s}).")
    else:
        st.session_state.types = extract_entity_type_names(j)
        st.success(f"Found {len(st.session_state.types)} types")

if st.session_state.types:
    st.dataframe([{"entityType": t} for t in st.session_state.types],
                 use_container_width=True, hide_index=True)

# ---------------------------------------------------------
# STEP 2 — Get Counts (optional, used for global progress denominator)
# ---------------------------------------------------------
st.subheader("② Get Counts per Entity Type")
btn_counts = st.button("Load counts", disabled=not st.session_state.types)

if btn_counts:
    headers = build_headers(user_id, client_id, client_secret, tenant)
    out = {}
    prog = st.progress(0.0)
    for i, et in enumerate(st.session_state.types, 1):
        body = {"params": {"query": {"filters": {"typesCriterion": [et]}}}}
        j, s, _ = post_json(f"{BASE_URL}{APP_GET_PATH}", headers, body)
        total = j.get("response", {}).get("totalRecords") if s < 400 else None
        out[et] = total
        prog.progress(i/len(st.session_state.types))
    st.session_state.counts = out
    st.success("Counts loaded.")

if st.session_state.counts:
    st.dataframe(
        [{"entityType": et, "totalRecords": st.session_state.counts.get(et)} for et in st.session_state.types],
        use_container_width=True, hide_index=True
    )

# ---------------------------------------------------------
# STEP 3 — Select Types (safe default cleanup)
# ---------------------------------------------------------
st.subheader("③ Select Types")

valid_types = st.session_state.types
# SAFE DEFAULTS: keep only defaults that exist in options to avoid StreamlitAPIException
clean_defaults = [x for x in st.session_state.get("selected_types", []) if x in valid_types]
st.session_state.selected_types = clean_defaults

selected = st.multiselect("Select entity type(s)", valid_types, default=clean_defaults)
st.session_state.selected_types = selected

# ---------------------------------------------------------
# STEP 4 — Re-evaluation (scroll-based ID retrieval + live progress)
# ---------------------------------------------------------
st.subheader("④ Re-evaluation")
btn_start = st.button("Start Re-evaluation", type="primary",
                      disabled=not selected or not BASE_URL or not client_id or not client_secret)

results: List[Dict[str,Any]] = []

if btn_start:
    headers = build_headers(user_id, client_id, client_secret, tenant)

    expected_global = sum((st.session_state.counts.get(et, 0) or 0) for et in selected) if st.session_state.counts else None
    overall = st.progress(0.0)
    status  = st.empty()
    done_global = 0
    denom = expected_global if (expected_global and expected_global > 0) else None

    with ThreadPoolExecutor(max_workers=REEVAL_WORKERS) as pool:
        for et in selected:
            t0 = time.perf_counter()

            # ------- SCROLL: get ALL IDs for this entity type -------
            ids = fetch_all_ids_via_scroll(BASE_URL, headers, et)
            total = len(ids)

            status.info(f"Re-evaluating: {et} — 0/{total} (global {done_global}/{expected_global or '?'})")

            futures = [pool.submit(reeval_worker, BASE_URL, headers, iid, et) for iid in ids]
            processed = 0

            for fut in as_completed(futures):
                res = fut.result()
                results.append(res)

                write_audit({
                    "timestamp": dt.datetime.now().isoformat(timespec="seconds"),
                    "tenant": tenant,
                    "entity_id": res["id"],
                    "entity_type": et,
                    "status": "success" if res["success"] else "failure",
                    "http_status": res["http"],
                    "latency": res["latency"],
                    "message": res["msg"],
                    "request_id": res["request_id"],
                })

                processed += 1
                done_global += 1

                if denom:
                    overall.progress(min(1.0, done_global/max(1,denom)))
                else:
                    # if counts not loaded, keep progress bounded without a denominator
                    overall.progress(0.0 if done_global == 0 else min(1.0, done_global/(done_global+1)))

                status.info(f"Re-evaluating: {et} — {processed}/{total} (global {done_global}/{expected_global or '?'})")

            elapsed = time.perf_counter() - t0
            st.success(f"[{et}] Completed re-evaluation for {processed} entities in {fmt_min_sec(elapsed)}.")

    st.success("Re-evaluation complete.")

if results:
    st.subheader("Run Results")
    results_sorted = sorted(results, key=lambda x: (x["id"]))
    st.dataframe(results_sorted, use_container_width=True, hide_index=True)

st.markdown("---")
st.subheader("Recent Audit (today)")
# (Optional) show last 20 rows of today's audit if exists
aud_path = os.path.join(AUDIT_DIR, f"audit_{dt.datetime.now().strftime('%Y%m%d')}.csv")
if os.path.exists(aud_path):
    with open(aud_path, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    st.dataframe(rows[-20:], use_container_width=True, hide_index=True)
else:
    st.info("No activity yet today.")
