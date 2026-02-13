# File: dags/report_dag.py
#
# One-shot DAG triggered by Airflow_Lab2.  Queries the Airflow REST API
# for the latest pipeline run, renders a static HTML report to a shared
# volume served by nginx, and exits.  Both DAGs finish with green checkmarks.

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="Airflow_Lab2_Report",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    default_args={"retries": 0},
)
def airflow_lab2_report():
    """Triggered by Airflow_Lab2 — generates a static status report."""

    @task
    def generate_report():
        """Query the Airflow API and write a static HTML report."""
        import os

        import requests

        webserver = os.getenv("AIRFLOW_WEBSERVER", "http://airflow-apiserver:8080")
        af_user = os.getenv("AIRFLOW_USERNAME", "airflow")
        af_pass = os.getenv("AIRFLOW_PASSWORD", "airflow")
        target_dag = os.getenv("TARGET_DAG_ID", "Airflow_Lab2")
        report_dir = "/opt/airflow/report"
        os.makedirs(report_dir, exist_ok=True)

        # --- Get bearer token ---
        token = None
        try:
            r = requests.post(
                f"{webserver}/auth/token",
                json={"username": af_user, "password": af_pass},
                headers={"Content-Type": "application/json"},
                timeout=10,
            )
            if r.status_code in (200, 201):
                token = r.json().get("access_token")
        except Exception as e:
            print(f"Token fetch failed: {e}")

        # --- Query latest run ---
        state = "unknown"
        run_id = "N/A"
        logical_date = "N/A"
        start_date = "N/A"
        end_date = "N/A"
        note = ""

        if token:
            try:
                url = f"{webserver}/api/v2/dags/{target_dag}/dagRuns?order_by=-run_after&limit=1"
                headers = {
                    "Accept": "application/json",
                    "Authorization": f"Bearer {token}",
                }
                r = requests.get(url, headers=headers, timeout=10)
                if r.status_code == 200:
                    runs = r.json().get("dag_runs", [])
                    if runs:
                        run = runs[0]
                        state = run.get("state", "unknown")
                        run_id = run.get("dag_run_id", "N/A")
                        logical_date = run.get("logical_date") or run.get(
                            "run_after", "N/A"
                        )
                        start_date = run.get("start_date", "N/A")
                        end_date = run.get("end_date", "N/A")
                else:
                    note = f"API Error {r.status_code}"
            except Exception as e:
                note = f"Network issue: {e}"
        else:
            note = "Could not obtain API token"

        # --- Render HTML ---
        is_success = state == "success"

        if is_success:
            color = "#10b981"
            icon = "✓"
            title = "Pipeline Success"
            subtitle = "The model training and deployment cycle completed successfully."
        else:
            color = "#ef4444"
            icon = "!"
            title = "Execution Interrupted"
            subtitle = f"The DAG <b>{run_id}</b> encountered a critical error during execution."
            if note:
                subtitle += f"<br><small>{note}</small>"

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="refresh" content="30">
    <title>Pipeline Report</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600&display=swap" rel="stylesheet">
    <style>
        :root {{ --accent: {color}; }}
        body {{ font-family: 'Inter', sans-serif; background: #f8fafc; color: #1e293b;
               margin: 0; display: flex; justify-content: center; align-items: center; min-height: 100vh; }}
        .container {{ width: 100%; max-width: 500px; background: #fff; border-radius: 16px;
                     box-shadow: 0 10px 25px rgba(0,0,0,.05); padding: 40px; text-align: center;
                     border-top: 8px solid var(--accent); }}
        .icon {{ width: 80px; height: 80px; background: color-mix(in srgb, var(--accent) 15%, white);
                color: var(--accent); border-radius: 50%; display: flex; justify-content: center;
                align-items: center; margin: 0 auto 20px; font-size: 40px; font-weight: bold; }}
        h1 {{ margin: 0 0 10px; font-weight: 600; }}
        p {{ color: #64748b; margin-bottom: 30px; }}
        .grid {{ background: #f1f5f9; border-radius: 12px; padding: 20px; text-align: left; margin-bottom: 30px; }}
        .row {{ display: flex; justify-content: space-between; margin-bottom: 8px; font-size: 14px; }}
        .label {{ font-weight: 600; color: #64748b; }}
        .value {{ color: #1e293b; font-family: monospace; }}
        .btn {{ display: inline-block; padding: 12px 24px; border-radius: 8px; border: none;
               font-weight: 600; cursor: pointer; text-decoration: none; font-size: 14px;
               background: var(--accent); color: white; }}
        .btn:hover {{ opacity: .9; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="icon">{icon}</div>
        <h1>{title}</h1>
        <p>{subtitle}</p>
        <div class="grid">
            <div class="row"><span class="label">Run ID</span><span class="value">{run_id}</span></div>
            <div class="row"><span class="label">State</span><span class="value" style="color:var(--accent)">{state}</span></div>
            <div class="row"><span class="label">Logical Date</span><span class="value">{logical_date}</span></div>
            <div class="row"><span class="label">Started</span><span class="value">{start_date}</span></div>
            <div class="row"><span class="label">Ended</span><span class="value">{end_date}</span></div>
        </div>
        <a href="http://localhost:8080" class="btn">Open Airflow</a>
    </div>
</body>
</html>"""

        out_path = os.path.join(report_dir, "index.html")
        with open(out_path, "w") as f:
            f.write(html)

        print(f"Report written to {out_path} — state: {state}")

    generate_report()


airflow_lab2_report()
