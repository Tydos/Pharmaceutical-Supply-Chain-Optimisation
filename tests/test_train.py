"""Unit tests for src/train.py."""

import numpy as np
import pandas as pd
from sklearn.pipeline import Pipeline

from src.train import (
    NUMERIC_COLS,
    ONE_HOT_COLS,
    TARGET_ENCODE_COLS,
    build_pipeline,
    build_preprocessor,
    clean_data,
    feature_engineer_dates,
    process_dates,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

DATE_COLS = [
    "PQ First Sent to Client Date",
    "PO Sent to Vendor Date",
    "Scheduled Delivery Date",
    "Delivered to Client Date",
    "Delivery Recorded Date",
]


def _make_raw_df(n: int = 5) -> pd.DataFrame:
    """Build a minimal raw DataFrame that mirrors the supply-chain CSV schema."""
    base_date = pd.Timestamp("2023-01-01")
    rng = np.random.default_rng(0)

    data = {
        "ID": range(n),
        "PQ #": [f"PQ{i}" for i in range(n)],
        "PO / SO #": [f"PO{i}" for i in range(n)],
        "ASN/DN #": [f"ASN{i}" for i in range(n)],
        "Country": rng.choice(["USA", "Kenya", "Uganda"], n).tolist(),
        "Managed By": rng.choice(["PMO - US", "PMO - Kenya"], n).tolist(),
        "Fulfill Via": rng.choice(["Direct Drop Shipment", "From RDC"], n).tolist(),
        "Vendor INCO Term": rng.choice(["EXW", "CIP"], n).tolist(),
        "Shipment Mode": rng.choice(["Air", "Sea", "Truck"], n).tolist(),
        "Product Group": rng.choice(["ARV", "HRDT"], n).tolist(),
        "Sub Classification": rng.choice(["Adult", "Pediatric"], n).tolist(),
        "Vendor": [f"Vendor{i % 3}" for i in range(n)],
        "Item Description": [f"Item{i}" for i in range(n)],
        "Molecule/Test Type": [f"Mol{i % 2}" for i in range(n)],
        "Brand": [f"Brand{i % 2}" for i in range(n)],
        "Dosage": ["300mg", "200mg", "100mg", "50mg", "25mg"][:n],
        "Dosage Form": rng.choice(["Tablet", "Capsule"], n).tolist(),
        "Manufacturing Site": [f"Site{i % 2}" for i in range(n)],
        "Project Code": [f"P{i}" for i in range(n)],
        "First Line Designation": rng.choice(["Yes", "No"], n).tolist(),
        "Weight (Kilograms)": rng.uniform(10, 500, n).tolist(),
        "Freight Cost (USD)": rng.uniform(100, 5000, n).tolist(),
        "Line Item Insurance (USD)": rng.uniform(0, 100, n).tolist(),
        "Line Item Value": rng.uniform(1000, 50000, n).tolist(),
        "Pack Price": rng.uniform(1, 100, n).tolist(),
        "Unit Price": rng.uniform(0.1, 10, n).tolist(),
        # Date columns as strings (matching real CSV format)
        "PQ First Sent to Client Date": [
            (base_date - pd.Timedelta(days=90 + i)).strftime("%m/%d/%y") for i in range(n)
        ],
        "PO Sent to Vendor Date": [
            (base_date - pd.Timedelta(days=60 + i)).strftime("%m/%d/%y") for i in range(n)
        ],
        "Scheduled Delivery Date": [
            (base_date + pd.Timedelta(days=30 + i)).strftime("%m/%d/%y") for i in range(n)
        ],
        "Delivered to Client Date": [
            # alternating on-time / late
            (base_date + pd.Timedelta(days=30 + i + (5 if i % 2 == 0 else -5))).strftime(
                "%m/%d/%y"
            )
            for i in range(n)
        ],
        "Delivery Recorded Date": [
            (base_date + pd.Timedelta(days=32 + i)).strftime("%m/%d/%y") for i in range(n)
        ],
    }
    return pd.DataFrame(data)


# ---------------------------------------------------------------------------
# Tests for process_dates
# ---------------------------------------------------------------------------


class TestProcessDates:
    def test_creates_late_column(self):
        df = _make_raw_df()
        result = process_dates(df)
        assert "late" in result.columns

    def test_late_is_binary(self):
        df = _make_raw_df()
        result = process_dates(df)
        assert set(result["late"].unique()).issubset({0, 1})

    def test_does_not_mutate_input(self):
        df = _make_raw_df()
        original_cols = list(df.columns)
        process_dates(df)
        assert list(df.columns) == original_cols


# ---------------------------------------------------------------------------
# Tests for clean_data
# ---------------------------------------------------------------------------


class TestCleanData:
    def test_drops_id_columns(self):
        df = _make_raw_df()
        result = clean_data(df)
        for col in ["PQ #", "PO / SO #", "ASN/DN #"]:
            assert col not in result.columns

    def test_converts_date_strings_to_datetime(self):
        df = _make_raw_df()
        result = clean_data(df)
        for col in DATE_COLS:
            assert pd.api.types.is_datetime64_any_dtype(result[col]), (
                f"Expected datetime dtype for {col}, got {result[col].dtype}"
            )

    def test_drops_duplicates(self):
        df = _make_raw_df(n=3)
        df_duped = pd.concat([df, df], ignore_index=True)
        result = clean_data(df_duped)
        assert len(result) == len(df)

    def test_does_not_mutate_input(self):
        df = _make_raw_df()
        original_len = len(df)
        clean_data(df)
        assert len(df) == original_len


# ---------------------------------------------------------------------------
# Tests for feature_engineer_dates
# ---------------------------------------------------------------------------


class TestFeatureEngineerDates:
    def _cleaned_df(self, n: int = 5) -> pd.DataFrame:
        return clean_data(_make_raw_df(n))

    def test_creates_lead_time(self):
        df = self._cleaned_df()
        result = feature_engineer_dates(df)
        assert "lead_time" in result.columns

    def test_creates_processing_time(self):
        df = self._cleaned_df()
        result = feature_engineer_dates(df)
        assert "processing_time" in result.columns

    def test_creates_scheduled_date_features(self):
        df = self._cleaned_df()
        result = feature_engineer_dates(df)
        for col in ("scheduled_month", "scheduled_quarter", "scheduled_weekday"):
            assert col in result.columns, f"Missing column: {col}"

    def test_drops_date_columns(self):
        df = self._cleaned_df()
        result = feature_engineer_dates(df)
        for col in result.columns:
            assert "date" not in col.lower(), f"Date column not dropped: {col}"

    def test_does_not_mutate_input(self):
        df = self._cleaned_df()
        original_cols = list(df.columns)
        feature_engineer_dates(df)
        assert list(df.columns) == original_cols


# ---------------------------------------------------------------------------
# Tests for build_preprocessor
# ---------------------------------------------------------------------------


class TestBuildPreprocessor:
    def test_returns_column_transformer(self):
        from sklearn.compose import ColumnTransformer

        preprocessor = build_preprocessor()
        assert isinstance(preprocessor, ColumnTransformer)

    def test_has_numeric_and_cat_transformers(self):
        preprocessor = build_preprocessor()
        transformer_names = [name for name, _, _ in preprocessor.transformers]
        assert "num" in transformer_names
        assert "cat" in transformer_names


# ---------------------------------------------------------------------------
# Tests for build_pipeline
# ---------------------------------------------------------------------------


class TestBuildPipeline:
    def test_returns_pipeline(self):
        pipeline = build_pipeline()
        assert isinstance(pipeline, Pipeline)

    def test_pipeline_step_names(self):
        pipeline = build_pipeline()
        step_names = [name for name, _ in pipeline.steps]
        assert step_names == ["clean", "feature_engineer", "preprocess", "classifier"]

    def test_column_constants_non_empty(self):
        assert len(NUMERIC_COLS) > 0
        assert len(ONE_HOT_COLS) > 0
        assert len(TARGET_ENCODE_COLS) > 0
