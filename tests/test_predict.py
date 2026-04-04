"""Unit tests for src/predict.py."""

import numpy as np
import pandas as pd
from unittest.mock import MagicMock

from src.predict import predict, predict_proba


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_model(n: int = 4) -> MagicMock:
    """Return a mock sklearn Pipeline that produces deterministic outputs."""
    model = MagicMock()
    model.predict.return_value = np.array([0, 1, 0, 1][:n])
    model.predict_proba.return_value = np.column_stack(
        [
            np.array([0.8, 0.3, 0.7, 0.2][:n]),
            np.array([0.2, 0.7, 0.3, 0.8][:n]),
        ]
    )
    return model


def _make_input_df(n: int = 4) -> pd.DataFrame:
    return pd.DataFrame({"feature_a": range(n), "feature_b": range(n, 2 * n)})


# ---------------------------------------------------------------------------
# Tests for predict
# ---------------------------------------------------------------------------


class TestPredict:
    def test_returns_series(self):
        model = _make_mock_model()
        df = _make_input_df()
        result = predict(model, df)
        assert isinstance(result, pd.Series)

    def test_series_name(self):
        model = _make_mock_model()
        df = _make_input_df()
        result = predict(model, df)
        assert result.name == "late_predicted"

    def test_length_matches_input(self):
        n = 4
        model = _make_mock_model(n)
        df = _make_input_df(n)
        result = predict(model, df)
        assert len(result) == n

    def test_values_are_integer_like(self):
        model = _make_mock_model()
        df = _make_input_df()
        result = predict(model, df)
        assert set(result.unique()).issubset({0, 1})

    def test_calls_model_predict(self):
        model = _make_mock_model()
        df = _make_input_df()
        predict(model, df)
        model.predict.assert_called_once()


# ---------------------------------------------------------------------------
# Tests for predict_proba
# ---------------------------------------------------------------------------


class TestPredictProba:
    def test_returns_series(self):
        model = _make_mock_model()
        df = _make_input_df()
        result = predict_proba(model, df)
        assert isinstance(result, pd.Series)

    def test_series_name(self):
        model = _make_mock_model()
        df = _make_input_df()
        result = predict_proba(model, df)
        assert result.name == "late_probability"

    def test_length_matches_input(self):
        n = 4
        model = _make_mock_model(n)
        df = _make_input_df(n)
        result = predict_proba(model, df)
        assert len(result) == n

    def test_probabilities_between_0_and_1(self):
        model = _make_mock_model()
        df = _make_input_df()
        result = predict_proba(model, df)
        assert (result >= 0.0).all() and (result <= 1.0).all()

    def test_calls_model_predict_proba(self):
        model = _make_mock_model()
        df = _make_input_df()
        predict_proba(model, df)
        model.predict_proba.assert_called_once()
