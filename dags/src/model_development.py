# File: dags/src/model_development.py
#
# Reference only — the DAG no longer imports from here.
# Task logic lives inline in main.py via @task decorators.

import os
import pickle

import pandas as pd
from sklearn.compose import make_column_transformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

WORKING_DIR = "/opt/airflow/working_data"
MODEL_DIR = "/opt/airflow/model"
os.makedirs(WORKING_DIR, exist_ok=True)
os.makedirs(MODEL_DIR, exist_ok=True)


def load_data() -> str:
    csv_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "data", "advertising.csv"
    )
    df = pd.read_csv(csv_path)
    out_path = os.path.join(WORKING_DIR, "raw.pkl")
    with open(out_path, "wb") as f:
        pickle.dump(df, f)
    return out_path


def data_preprocessing(file_path: str) -> str:
    with open(file_path, "rb") as f:
        df = pickle.load(f)
    X = df.drop(
        ["Timestamp", "Clicked on Ad", "Ad Topic Line", "Country", "City"], axis=1
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
        (StandardScaler(), num_columns), remainder="passthrough"
    )
    X_train_tr = ct.fit_transform(X_train)
    X_test_tr = ct.transform(X_test)
    out_path = os.path.join(WORKING_DIR, "preprocessed.pkl")
    with open(out_path, "wb") as f:
        pickle.dump((X_train_tr, X_test_tr, y_train.values, y_test.values), f)
    return out_path


def build_model(file_path: str, filename: str = "model.sav") -> str:
    with open(file_path, "rb") as f:
        X_train, X_test, y_train, y_test = pickle.load(f)
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    model_path = os.path.join(MODEL_DIR, filename)
    with open(model_path, "wb") as f:
        pickle.dump(model, f)
    return model_path


def load_model(file_path: str, filename: str = "model.sav") -> int:
    with open(file_path, "rb") as f:
        X_train, X_test, y_train, y_test = pickle.load(f)
    model_path = os.path.join(MODEL_DIR, filename)
    with open(model_path, "rb") as f:
        model = pickle.load(f)
    score = model.score(X_test, y_test)
    print(f"Model score on test data: {score}")
    return int(model.predict(X_test)[0])
