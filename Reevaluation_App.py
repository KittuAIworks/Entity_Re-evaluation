import os
import time
import csv
import uuid
import re
import datetime as dt
from typing import Dict, Any, Tuple

import requests
import streamlit as st

# ---------------------------------------------
# APP CONFIG
# ---------------------------------------------
st.set_page_config(page_title="Entity Reevaluation", page_icon="🔄", layout="centered")
st.title("🔄 Entity Reevaluation")

MOCK_MODE = False
REQUEST_TIMEOUT = 40
MAX_RETRIES = 3
BACKOFF_SECONDS = 1.25

REEVALUATE_PATH = "/api/entitygovernservice/reevaluate"
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

def sanitize_entity_id(entity_id: str) -> str:
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
                "timestamp_iso",
                "tenant",
                "user_id",
                "entity_id",
                "entity_type",
                "request_id",
                "status",
                "http_status",
                "latency_sec",
                "message",
                "backend_request_id",
            ],
        )
        if not exists:
            writer.writeheader()
        writer.writerow(row)

def read_recent_audit(n: int = 20):
    path = audit_path_today()
    if not os.path.isfile(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    return rows[-n:]

def make_request_payload(entity_id: str, entity_type: str) -> Dict[str, Any]:
    return {
        "entity": {"id": entity_id, "type": entity_type},
        "requestId": str(uuid.uuid4())
    }

# ---------------------------------------------
# Backend client
# ---------------------------------------------
class BackendClient:
    def __init__(self, timeout: int = 40, mock: bool = False):
        self.timeout = timeout
        self.mock = mock

    def reevaluate(
        self,
        tenant: str,
        payload: Dict[str, Any],
        user_id: str,
        client_id: str,
        client_secret: str,
        max_retries: int = 3,
        backoff_seconds: float = 1.25,
    ) -> Tuple[Dict[str, Any], int, float]:
        """
        POST https://{tenant}.syndigo.com/api/entitygovernservice/reevaluate
        """
        start = time.perf_counter()

        if self.mock:
            time.sleep(0.2)
            latency = time.perf_counter() - start
            return {
                "success": True,
                "message": f"[MOCK] Reevaluate accepted for {payload['entity'].get('id')} ({payload['entity'].get('type')})",
                "backendRequestId": f"mock-{uuid.uuid4()}",
                "data": {"echo": payload},
            }, 200, latency

        if not tenant:
            return {"success": False, "message": "Tenant not provided."}, 400, time.perf_counter() - start

        url = f"https://{tenant}.syndigo.com{REEVALUATE_PATH}"
        headers = build_headers(user_id=user_id, client_id=client_id, client_secret=client_secret, tenant=tenant)

        attempt = 0
        last_status = 0
        last_message = ""

        while attempt <= max_retries:
            attempt += 1
            try:
                r = requests.post(
                    url,
                    headers=headers,
                    json={"entity": payload["entity"]},  # only the required body
                    timeout=self.timeout,
                )
                last_status = r.status_code

                if 200 <= r.status_code < 300:
                    try:
                        body = r.json()
                    except Exception:
                        body = {"success": True, "message": "Reevaluate accepted"}
                    latency = time.perf_counter() - start
                    return body, r.status_code, latency

                if r.status_code in (429, 500, 502, 503, 504):
                    last_message = f"Retryable error {r.status_code}: {r.text[:300]}"
                    if attempt <= max_retries:
                        time.sleep(backoff_seconds * attempt)
                        continue

                last_message = f"Backend error {r.status_code}: {r.text[:300]}"
                break

            except requests.exceptions.Timeout:
                last_status = 408
                last_message = "Request timed out."
                if attempt <= max_retries:
                    time.sleep(backoff_seconds * attempt)
                    continue
                break
            except requests.exceptions.RequestException as e:
                last_status = 520
                last_message = f"Request exception: {str(e)}"
                if attempt <= max_retries:
                    time.sleep(backoff_seconds * attempt)
                    continue
                break

        latency = time.perf_counter() - start
        return {"success": False, "message": last_message}, last_status, latency

# ---------------------------------------------
# UI
# ---------------------------------------------
with st.form("reeval_form", clear_on_submit=False):
    st.subheader("🔐 Connection Details")
    tenant = st.text_input("Tenant", value="", placeholder="your-tenant")
    user_id = st.text_input("User ID", value="system", help="Defaults to 'system'")
    client_id = st.text_input("Client ID", value="")
    client_secret = st.text_input("Client Secret", value="", type="password")

    st.subheader("🧩 Entity")
    entity_type = st.text_input("Entity Type", placeholder="e.g., PRODUCT")
    entity_id = st.text_input("Entity ID", placeholder="e.g., PRD-12345")

    submitted = st.form_submit_button("Reevaluate", type="primary")

if submitted:
    if not tenant or not client_id or not client_secret:
        st.error("Please provide Tenant, Client ID, and Client Secret.")
        st.stop()

    clean_id = sanitize_entity_id(entity_id)
    clean_type = (entity_type or "").strip()
    if not clean_id or not clean_type:
        st.error("Both Entity ID and Entity Type are required.")
        st.stop()

    req_payload = make_request_payload(clean_id, clean_type)
    client = BackendClient(timeout=REQUEST_TIMEOUT, mock=MOCK_MODE)

    with st.spinner("Contacting backend..."):
        body, http_status, latency = client.reevaluate(
            tenant=tenant.strip(),
            payload=req_payload,
            user_id=(user_id or "system").strip() or "system",
            client_id=client_id.strip(),
            client_secret=client_secret.strip(),
            max_retries=MAX_RETRIES,
            backoff_seconds=BACKOFF_SECONDS,
        )

    success = bool(body.get("success", 200 <= http_status < 300))
    message = body.get("message", "")
    backend_req_id = body.get("requestId") or body.get("backendRequestId") or ""

    if success:
        st.success(f"✅ Reevaluate accepted. (HTTP {http_status}, {latency:.2f}s)")
    else:
        st.error(f"❌ Reevaluate failed. (HTTP {http_status}, {latency:.2f}s)")

    with st.expander("Response details"):
        st.json({
            "httpStatus": http_status,
            "latencySec": round(latency, 2),
            "message": message,
            "backendRequestId": backend_req_id,
            "data": body.get("data"),
            "request": req_payload,
            "headersUsed": {
                "x-rdp-version": "8.1",
                "x-rdp-clientId": "rdpclient",
                "x-rdp-userId": (user_id or 'system'),
                "auth-client-id": client_id,
                "auth-client-secret": "***masked***",
                "x-tenant-id": tenant or None,
            }
        })

    write_audit({
        "timestamp_iso": dt.datetime.now().isoformat(timespec="seconds"),
        "tenant": tenant or "",
        "user_id": user_id or "system",
        "entity_id": req_payload["entity"]["id"],
        "entity_type": req_payload["entity"]["type"],
        "request_id": req_payload["requestId"],
        "status": "success" if success else "failure",
        "http_status": http_status,
        "latency_sec": f"{latency:.2f}",
        "message": message,
        "backend_request_id": backend_req_id,
    })

st.markdown("---")
st.markdown("### Recent Activity (today)")
recent = read_recent_audit(20)
if recent:
    st.dataframe(recent, use_container_width=True, hide_index=True)
else:
    st.info("No activity yet today.")
