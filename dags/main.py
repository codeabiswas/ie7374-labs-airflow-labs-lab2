# File: dags/main.py
from __future__ import annotations

import os

import pendulum
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, task
from airflow.task.trigger_rule import TriggerRule

NOTIFY_EMAIL = os.getenv("NOTIFY_EMAIL", "default@example.com")


@dag(
    dag_id="Airflow_Lab2",
    description="Airflow-Lab2 DAG Description",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
    owner_links={"Ramin Mohammadi": "https://github.com/raminmohammadi/MLOps/"},
    max_active_runs=1,
    default_args={"retries": 0},
)
def airflow_lab2():
    """ML pipeline: load → preprocess → train → evaluate → notify → trigger report."""

    @task
    def load_data() -> str:
        """Load CSV and persist raw dataframe to a pickle file."""
        import os
        import pickle

        import pandas as pd

        working_dir = "/opt/airflow/working_data"
        os.makedirs(working_dir, exist_ok=True)

        csv_path = os.path.join(
            os.path.dirname(__file__),
            "data",
            "advertising.csv",
        )
        df = pd.read_csv(csv_path)

        out_path = os.path.join(working_dir, "raw.pkl")
        with open(out_path, "wb") as f:
            pickle.dump(df, f)
        return out_path

    @task
    def data_preprocessing(file_path: str) -> str:
        """Load dataframe, split, scale, and save to pickle."""
        import os
        import pickle

        from sklearn.compose import make_column_transformer
        from sklearn.model_selection import train_test_split
        from sklearn.preprocessing import StandardScaler

        working_dir = "/opt/airflow/working_data"

        with open(file_path, "rb") as f:
            df = pickle.load(f)

        X = df.drop(
            ["Timestamp", "Clicked on Ad", "Ad Topic Line", "Country", "City"],
            axis=1,
        )
        y = df["Clicked on Ad"]

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.3, random_state=42
        )

        num_columns = [
            "Daily Time Spent on Site",
            "Age",
            "Area Income",
            "Daily Internet Usage",
            "Male",
        ]

        ct = make_column_transformer(
            (StandardScaler(), num_columns),
            remainder="passthrough",
        )

        X_train_tr = ct.fit_transform(X_train)
        X_test_tr = ct.transform(X_test)

        out_path = os.path.join(working_dir, "preprocessed.pkl")
        with open(out_path, "wb") as f:
            pickle.dump((X_train_tr, X_test_tr, y_train.values, y_test.values), f)
        return out_path

    @task
    def build_model(file_path: str, filename: str = "model.sav") -> str:
        """Train a Random Forest classifier and save to disk."""
        import os
        import pickle

        from sklearn.ensemble import RandomForestClassifier

        model_dir = "/opt/airflow/model"
        os.makedirs(model_dir, exist_ok=True)

        with open(file_path, "rb") as f:
            X_train, X_test, y_train, y_test = pickle.load(f)

        model = RandomForestClassifier(n_estimators=100, random_state=42)
        model.fit(X_train, y_train)

        model_path = os.path.join(model_dir, filename)
        with open(model_path, "wb") as f:
            pickle.dump(model, f)

        return model_path

    @task
    def load_model(file_path: str, model_path: str) -> int:
        """Load saved model and test set, print score, return first prediction."""
        import pickle

        with open(file_path, "rb") as f:
            X_train, X_test, y_train, y_test = pickle.load(f)

        with open(model_path, "rb") as f:
            model = pickle.load(f)

        score = model.score(X_test, y_test)
        print(f"Model score on test data: {score}")

        pred = model.predict(X_test)
        return int(pred[0])

    # -- Classic operators --
    owner_task = BashOperator(
        task_id="task_using_linked_owner",
        bash_command="echo 1",
        owner="Ramin Mohammadi",
    )

    # Email on success — runs only if all upstream tasks succeeded
    email_success = EmailOperator(
        task_id="email_success",
        to=NOTIFY_EMAIL,
        subject="✅ Airflow_Lab2 pipeline succeeded",
        html_content="""
        <h3>Pipeline Success</h3>
        <p>The ML pipeline <b>Airflow_Lab2</b> completed successfully.</p>
        <p><b>Run ID:</b> {{ dag_run.run_id }}</p>
        <p><b>Logical Date:</b> {{ dag_run.logical_date }}</p>
        """,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # Email on failure — runs if any upstream task failed
    email_failure = EmailOperator(
        task_id="email_failure",
        to=NOTIFY_EMAIL,
        subject="❌ Airflow_Lab2 pipeline failed",
        html_content="""
        <h3>Pipeline Failure</h3>
        <p>The ML pipeline <b>Airflow_Lab2</b> encountered an error.</p>
        <p><b>Run ID:</b> {{ dag_run.run_id }}</p>
        <p>Check the Airflow UI for details.</p>
        """,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    trigger_report = TriggerDagRunOperator(
        task_id="trigger_report",
        trigger_dag_id="Airflow_Lab2_Report",
        conf={"message": "Pipeline complete, generate report"},
        reset_dag_run=True,
        wait_for_completion=False,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # -- Wire the pipeline --
    raw_path = load_data()
    preprocessed_path = data_preprocessing(raw_path)
    model_path = build_model(preprocessed_path)
    prediction = load_model(preprocessed_path, model_path)

    owner_task >> raw_path
    prediction >> [email_success, email_failure] >> trigger_report


airflow_lab2()
