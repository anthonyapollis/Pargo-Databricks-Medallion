"""
Uploads all 3 Pargo medallion notebooks to Databricks workspace
and creates a scheduled Workflow job to run them in sequence.

Usage:
    python upload_to_databricks.py <YOUR_PAT_TOKEN>

Get a token: Databricks → User Settings → Developer → Access Tokens → Generate new token
"""
import sys, base64, json, pathlib, urllib.request, urllib.error

HOST  = "https://dbc-ea48b979-9753.cloud.databricks.com"
TOKEN = sys.argv[1] if len(sys.argv) > 1 else input("Paste your Databricks PAT token: ").strip()
FOLDER = "/Pargo_Medallion"

NOTEBOOKS = [
    ("01_bronze_ingestion",  "01_bronze_ingestion.py"),
    ("02_silver_cleansing",  "02_silver_cleansing.py"),
    ("03_gold_analytics",    "03_gold_analytics.py"),
]

def api(path, body=None, method=None):
    url = HOST + path
    data = json.dumps(body).encode() if body else None
    req  = urllib.request.Request(url, data=data,
           headers={"Authorization": f"Bearer {TOKEN}",
                    "Content-Type": "application/json"},
           method=method or ("POST" if data else "GET"))
    try:
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        err = e.read().decode()
        print(f"  HTTP {e.code}: {err[:300]}")
        return {}

# 1. Create workspace folder
print(f"\nCreating folder {FOLDER}...")
r = api("/api/2.0/workspace/mkdirs", {"path": FOLDER})
print("  OK" if not r.get("error_code") else f"  {r}")

# 2. Upload notebooks
script_dir = pathlib.Path(__file__).parent
notebook_ids = []

for name, filename in NOTEBOOKS:
    path = script_dir / filename
    content = path.read_text(encoding="utf-8")
    encoded = base64.b64encode(content.encode()).decode()
    nb_path = f"{FOLDER}/{name}"

    print(f"Uploading {name}...")
    r = api("/api/2.0/workspace/import", {
        "path":      nb_path,
        "format":    "SOURCE",
        "language":  "PYTHON",
        "content":   encoded,
        "overwrite": True
    })
    if r.get("error_code"):
        print(f"  ERROR: {r}")
    else:
        print(f"  OK -> {nb_path}")
    notebook_ids.append(nb_path)

# 3. Create Workflow job (serverless compute for cost savings)
print("\nCreating Workflow job 'Pargo Medallion Pipeline'...")
job_body = {
    "name": "Pargo Medallion Pipeline",
    "tags": {"project": "pargo", "layer": "medallion"},
    "schedule": {
        "quartz_cron_expression": "0 0 3 * * ?",  # 03:00 daily
        "timezone_id": "Africa/Johannesburg",
        "pause_status": "PAUSED"  # starts paused — unpause when ready
    },
    "tasks": [
        {
            "task_key": "bronze_ingestion",
            "notebook_task": {"notebook_path": notebook_ids[0], "source": "WORKSPACE"},
            "job_cluster_key": "pargo_cluster"
        },
        {
            "task_key": "silver_cleansing",
            "depends_on": [{"task_key": "bronze_ingestion"}],
            "notebook_task": {"notebook_path": notebook_ids[1], "source": "WORKSPACE"},
            "job_cluster_key": "pargo_cluster"
        },
        {
            "task_key": "gold_analytics",
            "depends_on": [{"task_key": "silver_cleansing"}],
            "notebook_task": {"notebook_path": notebook_ids[2], "source": "WORKSPACE"},
            "job_cluster_key": "pargo_cluster"
        }
    ],
    "job_clusters": [
        {
            "job_cluster_key": "pargo_cluster",
            "new_cluster": {
                "spark_version":  "15.4.x-scala2.12",
                "node_type_id":   "m5d.large",        # smallest available on AWS
                "num_workers":    1,
                "autotermination_minutes": 30,
                "spark_conf": {
                    "spark.databricks.delta.optimizeWrite.enabled": "true",
                    "spark.databricks.delta.autoCompact.enabled":   "true"
                }
            }
        }
    ],
    "email_notifications": {},
    "max_concurrent_runs": 1
}

r = api("/api/2.1/jobs/create", job_body)
if r.get("job_id"):
    jid = r["job_id"]
    print(f"  Workflow job created: job_id={jid}")
    print(f"  View at: {HOST}/#job/{jid}")
else:
    print(f"  Job creation response: {r}")

print("\nDone! Open Databricks → Workflows to see 'Pargo Medallion Pipeline'")
print("Run notebooks manually in order to test: 01 → 02 → 03")
