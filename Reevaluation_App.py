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

# Simple, reliable paging
CHUNK_SIZE   = 1000        # pull 1000 IDs per request
REEVAL_WORKERS = 10        # concurrent re-evaluation calls

# Audit
AUDIT_DIR = "audit_logs"
os.makedirs(AUDIT_DIR, exist_ok=True)

# ---------------------------------------------
# HELPERS
# ---------------------------------------------
def build_headers(user_id: str, client_id: str, client_secret: str, tenant: str) -> Dict[str, str]:
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

def audit_path_today():
    return os.path.join(AUDIT_DIR, f"reevaluation_audit_{dt.datetime.now().strftime('%Y%m%d')}.csv")

def write_audit(row: Dict[str, Any]):
    path = audit_path_today()
    exists = os.path.isfile(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "timestamp_iso","tenant","user_id",
                "entity_id","entity_type","request_id",
                "status","http_status","latency_sec",
                "message","backend_request_id"
            ],
        )
        if not exists:
            writer.writeheader()
        writer.writerow(row)

def read_recent_audit(n=20):
    path = audit_path_today()
    if not os.path.isfile(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))[-n:]

def make_request_payload(entity_id, entity_type):
    return {
        "entity": {"id": entity_id, "type": entity_type},
        "requestId": str(uuid.uuid4())
    }

def robust_post(url, headers, body, timeout, max_retries, backoff):
    start = time.perf_counter()
    last_status = 0
    last_msg = ""
    for attempt in range(max_retries + 1):
        try:
            r = requests.post(url, headers=headers, json=body, timeout=timeout)
            status = r.status_code
            if 200 <= status < 300:
                try:
                    return r.json(), status, time.perf_counter() - start
                except:
                    return {}, status, time.perf_counter() - start
            if status in (429,500,502,503,504) and attempt < max_retries:
                time.sleep(backoff * (attempt+1))
                continue
            return {"error": r.text}, status, time.perf_counter() - start
        except requests.exceptions.Timeout:
            last_status = 408
            last_msg = "timeout"
            if attempt < max_retries:
                time.sleep(backoff*(attempt+1))
                continue
            break
        except requests.exceptions.RequestException as e:
            last_status = 520
            last_msg = str(e)
            if attempt < max_retries:
                time.sleep(backoff*(attempt+1))
                continue
            break
    return {"error": last_msg}, last_status, time.perf_counter() - start

def find_total_records(j):
    try: return int(j.get("response", {}).get("totalRecords"))
    except: return None

def extract_entity_type_names(j):
    out = []
    seen = set()
    try:
        for m in j.get("response", {}).get("entityModels", []):
            if isinstance(m,dict) and m.get("type")=="entityType":
                nm = str(m.get("name","")).strip()
                if nm and nm not in seen:
                    out.append(nm); seen.add(nm)
    except:
        pass
    return out

def extract_ids(j, etype):
    out=[]
    try:
        for e in j.get("response",{}).get("entities",[]):
            if isinstance(e,dict) and e.get("type")==etype and "id" in e:
                out.append(str(e["id"]).strip())
    except:
        pass
    return out

def fmt_min_sec(seconds):
    total = int(seconds)
    return f"{total//60}m {total%60}s"

# ---------------------------------------------
# Simple offset-only pager (no caps)
# ---------------------------------------------
def fetch_ids_offset_only(base, headers, etype) -> Iterable[List[str]]:
    """
    Offset-only paging:
      - options: {maxRecords: CHUNK_SIZE, offset: offset}
      - offset += len(ids)
      - stop when len(ids) == 0
    """
    offset = 0
    while True:
        body = {
            "params":{
                "query":{"filters":{"typesCriterion":[etype]}},
                "fields":{"attributes":["_ALL"]},
                "options":{"maxRecords": CHUNK_SIZE, "offset": offset}
            }
        }
        j, status, _ = robust_post(f"{base}{APP_GET_PATH}", headers, body,
                                   REQUEST_TIMEOUT, MAX_RETRIES, BACKOFF_SECONDS)
        if status >= 400:
            # stop on error; the caller will have partial results logged already
            break
        ids = extract_ids(j, etype)
        if not ids:
            break
        yield ids
        offset += len(ids)
        # continue until the API says no more

# ---------------------------------------------
# Worker (re-evaluation call)
# ---------------------------------------------
def reeval_worker(base, headers, iid, et):
    payload = make_request_payload(iid, et)
    j, status, lat = robust_post(f"{base}{REEVAL_PATH}", headers,
        {"entity": payload["entity"]},
        REQUEST_TIMEOUT, MAX_RETRIES, BACKOFF_SECONDS)
    success = (200<=status<300) and bool(j.get("success",True))
    msg = j.get("message") or j.get("error","")
    return {
        "entityType":et,
        "id":iid,
        "httpStatus":status,
        "success":success,
        "latencySec":round(lat,2),
        "message":msg[:200],
        "backend_request_id": j.get("backendRequestId") or j.get("requestId"),
        "request_id":payload["requestId"]
    }

# ---------------------------------------------
# UI — SIDEBAR
# ---------------------------------------------
with st.sidebar:
    st.header("🔐 Connection")
    tenant       = st.text_input("Tenant", value="")
    user_id      = st.text_input("User ID", value="system")
    client_id    = st.text_input("Client ID", value="")
    client_secret= st.text_input("Client Secret", value="", type="password")

BASE_URL = f"https://{tenant}.syndigo.com" if tenant else ""

# ---------------------------------------------
# STEP 1: Fetch types
# ---------------------------------------------
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
    headers = build_headers(user_id, client_id, client_secret, tenant)
    body = {
        "params":{
            "query":{
                "domain":"thing",
                "filters":{"typesCriterion":["entityType"]}
            }
        }
    }
    j,status,_= robust_post(f"{BASE_URL}{MODEL_GET_PATH}", headers, body,
                             REQUEST_TIMEOUT, MAX_RETRIES, BACKOFF_SECONDS)
    if status>=400:
        st.error(f"Failed to fetch types (HTTP {status}).")
    else:
        names = extract_entity_type_names(j)
        st.session_state.entity_types = sorted(names)
        st.success(f"Found {len(names)} entity type(s).")

if st.session_state.entity_types:
    st.dataframe([{"entityType":n} for n in st.session_state.entity_types],
                 use_container_width=True, hide_index=True)

# ---------------------------------------------
# STEP 2: Load counts (optional)
# ---------------------------------------------
st.subheader("② Get Counts per Entity Type")
btn_counts = st.button("Load counts",disabled=not st.session_state.entity_types)

if btn_counts:
    headers = build_headers(user_id,client_id,client_secret,tenant)
    counts={}
    prog=st.progress(0.0)
    for i,et in enumerate(st.session_state.entity_types,1):
        body={"params":{"query":{"filters":{"typesCriterion":[et]}}}}
        j,status,_ = robust_post(f"{BASE_URL}{APP_GET_PATH}", headers, body,
                                 REQUEST_TIMEOUT, MAX_RETRIES, BACKOFF_SECONDS)
        if status>=400:
            counts[et]={"totalRecords":None}
        else:
            counts[et]={"totalRecords":find_total_records(j)}
        prog.progress(i/len(st.session_state.entity_types))
    st.session_state.type_counts = counts
    st.success("Counts loaded.")

if st.session_state.type_counts:
    st.dataframe(
        [{"entityType":et,"totalRecords":st.session_state.type_counts.get(et,{}).get("totalRecords")} 
         for et in st.session_state.entity_types],
        use_container_width=True, hide_index=True
    )

# ---------------------------------------------
# STEP 3: Select types
# ---------------------------------------------
st.subheader("③ Select Types")
left,right = st.columns([3,1])
with left:
    selected = st.multiselect("Select entity type(s)", st.session_state.entity_types,
                              default=st.session_state.selected_types)
with right:
    sel_all = st.checkbox("Select all",
        value=(len(selected)==len(st.session_state.entity_types) and len(selected)>0))

if sel_all:
    selected = st.session_state.entity_types[:]
st.session_state.selected_types = selected

# ---------------------------------------------
# STEP 4: Re-evaluation (offset-only; live per-entity progress)
# ---------------------------------------------
st.subheader("④ Re-evaluation")
btn_start = st.button("Start Re-evaluation", type="primary",
                      disabled=not selected or not BASE_URL or not client_id or not client_secret)

results=[]
if btn_start:
    headers = build_headers(user_id,client_id,client_secret,tenant)

    counts_available = bool(st.session_state.type_counts)
    expected_global = sum(
        int(st.session_state.type_counts.get(et,{}).get("totalRecords") or 0)
        for et in selected
    ) if counts_available else None

    overall = st.progress(0.0)
    status_line = st.empty()

    # Silent reachability probe (no "Preflight OK" text)
    for et in selected:
        tiny={"params":{"query":{"filters":{"typesCriterion":[et]}},
                        "options":{"maxRecords":1}}}
        _, s, _ = robust_post(f"{BASE_URL}{APP_GET_PATH}", headers, tiny,
                              timeout=10, max_retries=1, backoff=0.5)
        if s>=400:
            st.error(f"Cannot reach {et} (HTTP {s}). Check tenant/credentials and try again.")
            st.stop()

    per_type_exact={}
    done_global=0
    denom = expected_global if (counts_available and expected_global and expected_global>0) else None

    with ThreadPoolExecutor(max_workers=REEVAL_WORKERS) as pool:
        for et in selected:
            t0=time.perf_counter()
            expected_total = st.session_state.type_counts.get(et,{}).get("totalRecords") if counts_available else None

            processed_this_type = 0
            unique_seen_ids=set()

            status_line.info(f"Re-evaluating: {et} — 0/{expected_total or '?'} (global {done_global}/{expected_global or '?'})")

            # OFFSET-ONLY ID streaming
            for id_chunk in fetch_ids_offset_only(BASE_URL, headers, et):
                futures=[pool.submit(reeval_worker,BASE_URL,headers,iid,et) for iid in id_chunk]

                # Update UI after each entity completes
                for fut in as_completed(futures):
                    res=fut.result()
                    results.append(res)

                    write_audit({
                        "timestamp_iso":dt.datetime.now().isoformat(timespec="seconds"),
                        "tenant":tenant,
                        "user_id":user_id,
                        "entity_id":res["id"],
                        "entity_type":et,
                        "request_id":res["request_id"],
                        "status":"success" if res["success"] else "failure",
                        "http_status":res["httpStatus"],
                        "latency_sec":res["latencySec"],
                        "message":res["message"],
                        "backend_request_id":res["backend_request_id"],
                    })

                    if res["id"] not in unique_seen_ids:
                        unique_seen_ids.add(res["id"])
                        processed_this_type += 1
                        done_global += 1

                        if denom:
                            overall.progress(min(1.0, done_global/max(1,denom)))
                        else:
                            overall.progress(0.0 if done_global==0 else min(1.0, done_global/(done_global+1)))

                        status_line.info(
                            f"Re-evaluating: {et} — {processed_this_type}/{expected_total or '?'} "
                            f"(global {done_global}/{expected_global or '?'})"
                        )

                # If we know the exact expected count, allow an early stop when reached
                if expected_total and processed_this_type >= expected_total:
                    break

            per_type_exact[et]=processed_this_type
            t_elapsed=fmt_min_sec(time.perf_counter()-t0)
            st.success(f"[{et}] Completed re-evaluation for {per_type_exact[et]} entities in {t_elapsed}.")

    # Final validation
    if counts_available:
        missing=[]
        for et in selected:
            exp = int(st.session_state.type_counts.get(et,{}).get("totalRecords") or 0)
            got = per_type_exact.get(et,0)
            if got < exp:
                missing.append((et, exp-got, exp, got))
        total_exp = sum(int(st.session_state.type_counts.get(et,{}).get("totalRecords") or 0) for et in selected)
        total_got = sum(per_type_exact.get(et,0) for et in selected)
        if missing:
            st.warning("⚠️ Some types did not reach expected totals:")
            for et,miss,exp,got in missing:
                st.write(f"- {et}: {got}/{exp} (missing {miss})")
            st.warning(f"Global processed: {total_got}/{total_exp}")
        else:
            st.success(f"✅ Re-evaluation run complete. Global processed: {total_got}/{total_exp}")
    else:
        st.success("Re-evaluation run complete.")

if results:
    st.subheader("Run Results")
    results_sorted = sorted(results, key=lambda x:(x["entityType"],x["id"]))
    st.dataframe(results_sorted, use_container_width=True, hide_index=True)

st.markdown("---")
st.subheader("Recent Audit (today)")
recent=read_recent_audit()
if recent:
    st.dataframe(recent,use_container_width=True,hide_index=True)
else:
    st.info("No activity yet today.")

