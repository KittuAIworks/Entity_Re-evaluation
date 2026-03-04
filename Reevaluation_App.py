import os
import time
import csv
import uuid
import datetime as dt
from typing import Dict, Any, List
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import streamlit as st

# ------------------------------------------
# APP CONFIG
# ------------------------------------------
st.set_page_config(page_title="Entity Re-evaluation", page_icon="🔄", layout="wide")
st.title("🔄 Entity Re-evaluation — Single / Multiple / All Types")

REQUEST_TIMEOUT = 40
MAX_RETRIES = 3
BACKOFF = 1.25
PAGE_SIZE = 1000          # SAFE VALUE (your backend works with this)
REEVAL_WORKERS = 10

MODEL_GET_PATH = "/api/entitymodelservice/get"
APP_GET_PATH   = "/api/entityappservice/get"
REEVAL_PATH    = "/api/entitygovernservice/reevaluate"

AUDIT_DIR = "audit_logs"
os.makedirs(AUDIT_DIR, exist_ok=True)

# ------------------------------------------
# HELPERS
# ------------------------------------------
def build_headers(uid, cid, secret, tenant):
    return {
        "Content-Type": "application/json",
        "x-rdp-version": "8.1",
        "x-rdp-clientId": "rdpclient",
        "x-rdp-userId": uid or "system",
        "auth-client-id": cid,
        "auth-client-secret": secret,
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "x-tenant-id": tenant
    }

def post_json(url, headers, body):
    start=time.perf_counter()
    for a in range(MAX_RETRIES+1):
        try:
            r=requests.post(url,headers=headers,json=body,timeout=REQUEST_TIMEOUT)
            return (r.json(), r.status_code, time.perf_counter()-start)
        except Exception as e:
            if a==MAX_RETRIES:
                return ({"error":str(e)},520,0)
            time.sleep(BACKOFF*(a+1))

def write_audit(row):
    path=os.path.join(AUDIT_DIR,f"audit_{dt.datetime.now().strftime('%Y%m%d')}.csv")
    new = not os.path.exists(path)
    with open(path,"a",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=[
            "timestamp","tenant","entity_id","entity_type","status",
            "http_status","latency","message","request_id"
        ])
        if new: w.writeheader()
        w.writerow(row)

def extract_types(j):
    out=[]
    for m in j.get("response",{}).get("entityModels",[]):
        if m.get("type")=="entityType":
            out.append(m.get("name",""))
    return out

def extract_ids(j, etype):
    out=[]
    for e in j.get("response",{}).get("entities",[]):
        if e.get("type")==etype and "id" in e:
            out.append(str(e["id"]).strip())
    return out

def fmt(seconds):
    s=int(seconds)
    return f"{s//60}m {s%60}s"

# ------------------------------------------
# OFFSET PAGER (your previously working logic)
# ------------------------------------------
def fetch_all_ids(base, headers, etype):
    all_ids=[]
    offset=0
    while True:
        body={
            "params":{
                "query":{"filters":{"typesCriterion":[etype]}},
                "fields":{"attributes":["_ALL"]},
                "options":{"maxRecords":PAGE_SIZE,"offset":offset}
            }
        }
        j,s,_=post_json(f"{base}{APP_GET_PATH}", headers, body)
        if s>=400:
            break
        ids = extract_ids(j,etype)
        if not ids:
            break
        all_ids.extend(ids)
        offset += len(ids)
        if len(ids) < PAGE_SIZE:
            break
    # remove duplicates
    return list(dict.fromkeys(all_ids))

# ------------------------------------------
# Worker
# ------------------------------------------
def reeval_worker(base, headers, iid, et):
    body={"entity":{"id":iid,"type":et}}
    j,s,lat = post_json(f"{base}{REEVAL_PATH}", headers, body)
    success = (200<=s<300) and bool(j.get("success",True))
    msg=j.get("message") or j.get("error","")
    return {
        "id":iid,
        "success":success,
        "latency":round(lat,2),
        "http":s,
        "msg":msg
    }

# ------------------------------------------
# UI SIDEBAR
# ------------------------------------------
with st.sidebar:
    st.header("🔐 Connection")
    tenant  = st.text_input("Tenant","")
    user_id = st.text_input("User ID","system")
    cid     = st.text_input("Client ID","")
    secret  = st.text_input("Client Secret","",type="password")

BASE_URL = f"https://{tenant}.syndigo.com" if tenant else ""

# ------------------------------------------
# STEP 1 — TYPES
# ------------------------------------------
st.subheader("① Discover Entity Types")
bt1 = st.button("Fetch entity types", type="primary",disabled=not BASE_URL or not cid or not secret)

if "types" not in st.session_state: st.session_state.types=[]
if bt1:
    h=build_headers(user_id,cid,secret,tenant)
    j,s,_=post_json(f"{BASE_URL}{MODEL_GET_PATH}",h,{
        "params":{
            "query":{"domain":"thing","filters":{"typesCriterion":["entityType"]}}
        }
    })
    st.session_state.types = extract_types(j)
    st.success(f"Found {len(st.session_state.types)} types")

if st.session_state.types:
    st.write(st.session_state.types)

# ------------------------------------------
# STEP 2 — COUNTS
# ------------------------------------------
st.subheader("② Get Counts")
bt2 = st.button("Load counts", disabled=not st.session_state.types)

if "counts" not in st.session_state: st.session_state.counts={}

if bt2:
    h=build_headers(user_id,cid,secret,tenant)
    counts={}
    prog=st.progress(0.0)
    for i,et in enumerate(st.session_state.types,1):
        j,s,_=post_json(f"{BASE_URL}{APP_GET_PATH}",h,{
            "params":{"query":{"filters":{"typesCriterion":[et]}}}
        })
        counts[et]=j.get("response",{}).get("totalRecords")
        prog.progress(i/len(st.session_state.types))
    st.session_state.counts=counts
    st.success("Counts loaded")

# ------------------------------------------
# STEP 3 — SELECT
# ------------------------------------------
st.subheader("③ Select Types")
selected = st.multiselect("Select entity type(s)", st.session_state.types,
                          default=st.session_state.selected_types if "selected_types" in st.session_state else [])
st.session_state.selected_types=selected

# ------------------------------------------
# STEP 4 — RE-EVALUATION
# ------------------------------------------
st.subheader("④ Re-evaluation")
bt3 = st.button("Start Re-evaluation", type="primary",
    disabled=not selected or not BASE_URL)

results=[]

if bt3:
    h=build_headers(user_id,cid,secret,tenant)
    expected_global = sum(st.session_state.counts.get(et,0) or 0 for et in selected)
    overall = st.progress(0.0)
    status  = st.empty()
    done_global=0

    with ThreadPoolExecutor(max_workers=REEVAL_WORKERS) as pool:
        for et in selected:
            t0=time.perf_counter()

            # --- WORKING LOGIC YOU APPROVED EARLIER ---
            ids = fetch_all_ids(BASE_URL,h,et)
            total = len(ids)

            status.info(f"Re-evaluating: {et} — 0/{total} (global {done_global}/{expected_global})")

            futures=[pool.submit(reeval_worker,BASE_URL,h,iid,et) for iid in ids]

            processed=0

            for fut in as_completed(futures):
                res=fut.result()
                results.append(res)

                write_audit({
                    "timestamp":dt.datetime.now().isoformat(timespec="seconds"),
                    "tenant":tenant,
                    "entity_id":res["id"],
                    "entity_type":et,
                    "status":"success" if res["success"] else "failure",
                    "http_status":res["http"],
                    "latency":res["latency"],
                    "message":res["msg"],
                    "request_id":str(uuid.uuid4())
                })

                processed+=1
                done_global+=1

                overall.progress(min(1.0, done_global/max(1,expected_global)))
                status.info(f"Re-evaluating: {et} — {processed}/{total} (global {done_global}/{expected_global})")

            elapsed = time.perf_counter()-t0
            st.success(f"[{et}] Completed {processed} entities in {fmt(elapsed)}.")

    st.success("Re-evaluation complete.")
    st.dataframe(results,use_container_width=True,hide_index=True)
