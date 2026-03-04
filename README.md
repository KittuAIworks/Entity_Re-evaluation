# Entity Reevaluation App (Streamlit)

A lightweight Streamlit app to trigger entity reevaluation when a new business condition is added to an entity type.

- **Inputs:** Tenant, User ID (defaults to `system`), Client ID, Client Secret, Entity Type, Entity ID
- **Endpoint:** `POST https://{tenant}.syndigo.com/api/entitygovernservice/reevaluate`
- **Headers:** `x-rdp-*` and `auth-*` exactly like the existing internal tools
- **Audit:** Appends activity to `audit_logs/reevaluate_audit_YYYYMMDD.csv`

> This app follows the same UI/input pattern as your existing Streamlit apps that collect connection details directly from the sidebar, without environment files. [1](https://syndigo-my.sharepoint.com/personal/kishore_reddy_syndigo_com/Documents/Microsoft%20Copilot%20Chat%20Files/UI_Config_Lineage_Report.py)

---

## Requirements

See `requirements.txt`.

## Run

```bash
# 1) (optional) create a virtual env
python -m venv .venv
source .venv/bin/activate    # Windows: .venv\Scripts\activate

# 2) Install dependencies
pip install -r requirements.txt

# 3) Launch Streamlit
streamlit run Reevaluation_App.py
