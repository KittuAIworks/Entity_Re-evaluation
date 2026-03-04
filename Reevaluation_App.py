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
st.set_page_config(page_title="Entity Re-evaluation", page_icon="🔄", layout="wide")
st.title("🔄 Entity Re-evaluation — Single / Multiple / All Types")

REQUEST_TIMEOUT = 40
MAX_RETRIES = 3
BACKOFF = 1.0
REEVAL_WORKERS = 10

MODEL_GET_PATH = "/api/entitymodelservice/get"
APP_GET_PATH   = "/api/entityappservice/get"
REEVAL_PATH    = "/api/entitygovernservice/reevaluate"

AUDIT_DIR = "audit_logs"
os.makedirs(AUDIT_DIR, exist_ok=True)

# ---------------------------------------------------------
# HELPERS
# ---------------------------------------------------------
def build_headers(user_id, client_id, client_secret, tenant):
    return {
        "Content-Type": "application/json",
        "x-rdp-version": "8.1",
        "x-rdp-clientId": "rdpclient",
        "x-rdp-userId": user_id or "system",
        "auth-client-id": client_id,
        "auth-client-secret": client_secret,
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "x-tenant-id": tenant
    }

def audit_path():
    return os.path.join(AUDIT_DIR, f"reevaluation_{dt.datetime.now().strftime('%Y%m%d')}.csv")

def write_audit(row: Dict[str, Any]):
    path = audit_path()
    new = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "timestamp","tenant","entity_id","entity_type",
                "status","http_status","latency","message","req_id"
            ]
        )
        if new:
            w.writeheader()
        w.writerow(row)

def robust_post(url, headers, body):
    for attempt in range(MAX_RETRIES+1):
        try:
            r = requests.post(url, headers=headers, json=body, timeout=REQUEST_TIMEOUT)
            return r.json(), r.status_code, r.elapsed.total_seconds()
        except Exception as e:
            if attempt == MAX_RETRIES:
                return {"error":str(e)}, 520, 0
            time.sleep(BACKOFF*(attempt+1))
    return {}, 520, 0

def extract_types(resp):
    out=[]
    seen=set()
    try:
        for m in resp.get("response",{}).get("entityModels",[]):
            if m.get("type")=="entityType":
                name = m.get("name","").strip()
                if name and name not in seen:
                    seen.add(name)
                    out.append(name)
    except:
        pass
    return out

def extract_ids(resp, etype):
    out=[]
    try:
        for e in resp.get("response",{}).get("entities",[]):
            if e.get("type")==etype and "id" in e:
                out.append(str(e["id"]).strip())
    except:
        pass
    return out

# ---------------------------------------------------------
# UI SIDEBAR
# ---------------------------------------------------------
with st.sidebar:
    st.header("🔐 Connection")
    tenant        = st.text_input("Tenant")
    user_id       = st.text_input("User ID", value="system")
    client_id     = st.text_input("Client ID")
    client_secret = st.text_input("Client Secret", type="password")

BASE_URL = f"https://{tenant}.syndigo.com" if tenant else ""

# ---------------------------------------------------------
# STEP 1: fetch entity types
# ---------------------------------------------------------
st.subheader("① Discover Entity Types")
btn_types = st.button("Fetch entity types", type="primary",
    disabled=not BASE_URL or not client_id or not client_secret)

if "entity_types" not in st.session_state:
    st.session_state.entity_types=[]
if "type_counts" not in st.session_state:
    st.session_state.type_counts={}
if "selected_types" not in st.session_state:
    st.session_state.selected_types=[]

if btn_types:
    headers = build_headers(user_id,client_id,client_secret,tenant)
    body={
        "params":{
            "query":{
                "domain":"thing",
                "filters":{"typesCriterion":["entityType"]}
            }
        }
    }
    j,s,_ = robust_post(f"{BASE_URL}{MODEL_GET_PATH}", headers, body)
    if s>=400:
        st.error("Failed")
    else:
        types = extract_types(j)
        st.session_state.entity_types = types
        st.success(f"Found {len(types)} types")

if st.session_state.entity_types:
    st.dataframe([{"entityType":t} for t in st.session_state.entity_types],
                 use_container_width=True, hide_index=True)

# ---------------------------------------------------------
# STEP 2: get counts
# ---------------------------------------------------------
st.subheader("② Get Counts per Entity Type")
btn_counts = st.button("Load counts",
    disabled=not st.session_state.entity_types)

if btn_counts:
    headers = build_headers(user_id,client_id,client_secret,tenant)
    out={}
    prog=st.progress(0)
    for i,et in enumerate(st.session_state.entity_types,1):
        body={"params":{"query":{"filters":{"typesCriterion":[et]}}}}
        j,s,_ = robust_post(f"{BASE_URL}{APP_GET_PATH}", headers, body)
        if s>=400:
            out[et]=None
        else:
            out[et]=j.get("response",{}).get("totalRecords")
        prog.progress(i/len(st.session_state.entity_types))
    st.session_state.type_counts = out

if st.session_state.type_counts:
    st.dataframe(
        [{"entityType":et,"totalRecords":st.session_state.type_counts.get(et)} 
         for et in st.session_state.entity_types],
        use_container_width=True,
        hide_index=True
    )

# ---------------------------------------------------------
# STEP 3: select types
# ---------------------------------------------------------
st.subheader("③ Select Types")
selected = st.multiselect("Select entity type(s)",
    st.session_state.entity_types,
    default=st.session_state.selected_types)

st.session_state.selected_types = selected

# ---------------------------------------------------------
# STEP 4: final simple re-evaluation (fetchAll = TRUE)
# ---------------------------------------------------------
st.subheader("④ Re-evaluation")

btn_start = st.button("Start Re-evaluation", type="primary",
    disabled=not selected or not BASE_URL)

results=[]

if btn_start:
    headers = build_headers(user_id,client_id,client_secret,tenant)

    # Expected global total
    counts_available = bool(st.session_state.type_counts)
    expected_global  = sum(
        st.session_state.type_counts.get(et,0) or 0 for et in selected
    ) if counts_available else None

    overall = st.progress(0.0)
    status  = st.empty()

    done_global=0
    denom = expected_global if (counts_available and expected_global) else None

    with ThreadPoolExecutor(max_workers=REEVAL_WORKERS) as pool:
        for et in selected:
            t0=time.perf_counter()

            expected_total = st.session_state.type_counts.get(et,0) or None

            # --- REAL FIX: fetchAll = true ---
            body={
                "params":{
                    "query":{"filters":{"typesCriterion":[et]}},
                    "fields":{"attributes":["_ALL"]},
                    "options":{"fetchAll": True}
                }
            }
            j,s,_ = robust_post(f"{BASE_URL}{APP_GET_PATH}", headers, body)
            if s>=400:
                st.error(f"Cannot fetch entities for {et}")
                continue

            ids = extract_ids(j, et)
            unique_ids = list(dict.fromkeys(ids))  # remove dupes if any
            total = len(unique_ids)

            status.info(f"Re-evaluating: {et} — 0/{total} (global {done_global}/{expected_global or '?'})")

            futures=[pool.submit(reeval_worker,BASE_URL,headers,iid,et) for iid in unique_ids]

            processed=0

            for fut in as_completed(futures):
                res=fut.result()
                results.append(res)

                write_audit({
                    "timestamp":dt.datetime.now().isoformat(timespec="seconds"),
                    "tenant":tenant,
                    "entity_id":res["id"],
                    "entity_type":et,
                    "request_id":res["request_id"],
                    "status":"success" if res["success"] else "failure",
                    "http_status":res["httpStatus"],
                    "latency":res["latencySec"],
                    "message":res["message"],
                    "req_id":res["backend_request_id"],
                })

                processed+=1
                done_global+=1

                if denom:
                    overall.progress(min(1.0, done_global/max(1,denom)))
                else:
                    overall.progress(0.5)

                status.info(
                    f"Re-evaluating: {et} — {processed}/{total} "
                    f"(global {done_global}/{expected_global or '?'})"
                )

            elapsed = time.perf_counter()-t0
            st.success(f"[{et}] Completed {processed} entities in {elapsed//60:.0f}m {elapsed%60:.0f}s.")

    st.success("Re-evaluation complete.")
    st.dataframe(results,use_container_width=True,hide_index=True)
