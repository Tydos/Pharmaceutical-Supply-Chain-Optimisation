"""Training script – migrated from notebook/Supply_Chain_Optimisation.ipynb.

Exposes a ``train`` function that builds the full sklearn Pipeline (cleaning →
feature engineering → preprocessing → classifier), fits it on the raw supply
chain CSV, evaluates on a held-out test split, and persists the fitted pipeline
to disk with joblib.
"""

import logging
import os
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer, OneHotEncoder, StandardScaler, TargetEncoder

from src.config import load_config
from src.import_data import import_data

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Column groups (mirrors the notebook)
# ---------------------------------------------------------------------------

NUMERIC_COLS = ["Weight (Kilograms)", "Freight Cost (USD)"]

ONE_HOT_COLS = [
    "Managed By",
    "Fulfill Via",
    "Vendor INCO Term",
    "Shipment Mode",
    "Product Group",
    "Sub Classification",
    "Dosage Form",
    "First Line Designation",
]

TARGET_ENCODE_COLS = [
    "Country",
    "Brand",
    "Dosage",
    "Project Code",
    "Vendor",
    "Item Description",
    "Molecule/Test Type",
    "Manufacturing Site",
]

# ---------------------------------------------------------------------------
# Preprocessing helpers (module-level so they are picklable via joblib)
# ---------------------------------------------------------------------------


def _to_numeric(X: pd.DataFrame) -> pd.DataFrame:
    """Coerce all columns to numeric, turning unparseable values into NaN."""
    return X.apply(pd.to_numeric, errors="coerce")


def _get_string_date_columns(df: pd.DataFrame) -> list:
    """Return names of string/object columns whose name contains 'date'."""
    cat_cols = df.select_dtypes(include=["category", "object"]).columns.tolist()
    return [col for col in cat_cols if "date" in col.lower()]


def process_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Create the binary target variable ``late`` from delivery date columns.

    A shipment is considered late when *Delivered to Client Date* is strictly
    after *Scheduled Delivery Date*.
    """
    df = df.copy()
    for col in _get_string_date_columns(df):
        df[col] = pd.to_datetime(df[col], errors="coerce")
    df["late"] = (
        (df["Delivered to Client Date"] - df["Scheduled Delivery Date"]).dt.days > 0
    ).astype(int)
    return df


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Convert date strings to datetime objects, drop duplicates and ID columns."""
    df = df.copy()
    for col in _get_string_date_columns(df):
        df[col] = pd.to_datetime(df[col], errors="coerce")
    df = df.drop_duplicates()
    df = df.drop(columns=["PQ #", "PO / SO #", "ASN/DN #"], errors="ignore")
    return df


def feature_engineer_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Derive temporal features from date columns, then drop the raw date columns."""
    df = df.copy()
    df["lead_time"] = (
        df["Scheduled Delivery Date"] - df["PO Sent to Vendor Date"]
    ).dt.days
    df["processing_time"] = (
        df["PO Sent to Vendor Date"] - df["PQ First Sent to Client Date"]
    ).dt.days
    df["scheduled_month"] = df["Scheduled Delivery Date"].dt.month
    df["scheduled_quarter"] = df["Scheduled Delivery Date"].dt.quarter
    df["scheduled_weekday"] = df["Scheduled Delivery Date"].dt.weekday
    date_cols = [col for col in df.columns if "date" in col.lower()]
    df = df.drop(columns=date_cols, errors="ignore")
    return df


# ---------------------------------------------------------------------------
# Pipeline construction
# ---------------------------------------------------------------------------


def build_preprocessor() -> ColumnTransformer:
    """Build the sklearn ColumnTransformer used during training and inference."""
    numeric_pipeline = Pipeline(
        steps=[
            ("to_numeric", FunctionTransformer(_to_numeric, validate=False)),
            ("imputer", SimpleImputer(strategy="mean")),
            ("log", FunctionTransformer(np.log1p, validate=False)),
            ("scaler", StandardScaler()),
        ]
    )

    categorical_pipeline = ColumnTransformer(
        transformers=[
            ("target", TargetEncoder(smooth="auto"), TARGET_ENCODE_COLS),
            ("onehot", OneHotEncoder(handle_unknown="ignore"), ONE_HOT_COLS),
        ]
    )

    return ColumnTransformer(
        transformers=[
            ("num", numeric_pipeline, NUMERIC_COLS),
            ("cat", categorical_pipeline, ONE_HOT_COLS + TARGET_ENCODE_COLS),
        ]
    )


def build_pipeline(random_state: int = 42) -> Pipeline:
    """Return the full sklearn Pipeline: clean → feature engineer → preprocess → classify."""
    preprocessor = build_preprocessor()
    return Pipeline(
        steps=[
            ("clean", FunctionTransformer(clean_data, validate=False)),
            ("feature_engineer", FunctionTransformer(feature_engineer_dates, validate=False)),
            ("preprocess", preprocessor),
            ("classifier", LogisticRegression(max_iter=1000, random_state=random_state)),
        ]
    )


# ---------------------------------------------------------------------------
# Training entry point
# ---------------------------------------------------------------------------


def train(
    data_path: str,
    model_save_path: str,
    test_size: float = 0.2,
    random_state: int = 42,
) -> dict:
    """Load data, train the pipeline, evaluate on a test split, and save the model.

    Parameters
    ----------
    data_path:
        Path to the raw supply-chain CSV file.
    model_save_path:
        Destination path for the serialised (joblib) pipeline.
    test_size:
        Fraction of the data to hold out for evaluation.
    random_state:
        Random seed for reproducibility.

    Returns
    -------
    dict
        Dictionary containing evaluation metrics (``roc_auc``).
    """
    logger.info("Loading data from %s", data_path)
    df = import_data(data_path)

    logger.info("Creating target variable")
    df = process_dates(df)

    X = df.drop(columns=["late"])
    y = df["late"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state
    )
    logger.info(
        "Train size: %d  |  Test size: %d", len(X_train), len(X_test)
    )

    logger.info("Building pipeline")
    pipeline = build_pipeline(random_state=random_state)

    logger.info("Fitting pipeline")
    pipeline.fit(X_train, y_train)

    y_pred_prob = pipeline.predict_proba(X_test)[:, 1]
    auc = roc_auc_score(y_test, y_pred_prob)
    logger.info("ROC AUC Score: %.4f", auc)

    model_dir = os.path.dirname(os.path.abspath(model_save_path))
    os.makedirs(model_dir, exist_ok=True)
    joblib.dump(pipeline, model_save_path)
    logger.info("Model saved to %s", model_save_path)

    return {"roc_auc": auc}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    _config_path = Path("config/config.yaml")
    _config = load_config(_config_path)
    _system = _config["system"]
    _model = _config.get("model", {})
    train(
        data_path=_system["raw_dataset_path"],
        model_save_path=_system["model_save_path"],
        test_size=_model.get("test_size", 0.2),
        random_state=_model.get("random_state", 42),
    )
