"""Inference script – load a trained pipeline and generate predictions.

The ``predict`` and ``predict_proba`` functions accept the fitted pipeline
returned by ``src.train.build_pipeline`` (serialised with joblib) and a raw
DataFrame in the same format as the training data.
"""

import logging
from pathlib import Path

import joblib
import pandas as pd

from src.config import load_config
from src.import_data import import_data

logger = logging.getLogger(__name__)


def load_model(model_path: str):
    """Deserialise and return the fitted pipeline from *model_path*."""
    logger.info("Loading model from %s", model_path)
    return joblib.load(model_path)


def predict(model, df: pd.DataFrame) -> pd.Series:
    """Return predicted class labels (0 = on-time, 1 = late) for *df*.

    Parameters
    ----------
    model:
        A fitted sklearn Pipeline as produced by ``src.train.build_pipeline``.
    df:
        Raw input DataFrame (same schema as training data, without ``late``).

    Returns
    -------
    pd.Series
        Integer predictions named ``late_predicted``.
    """
    logger.info("Running predictions on %d records", len(df))
    predictions = model.predict(df)
    return pd.Series(predictions, name="late_predicted")


def predict_proba(model, df: pd.DataFrame) -> pd.Series:
    """Return the probability that each shipment in *df* is late.

    Parameters
    ----------
    model:
        A fitted sklearn Pipeline as produced by ``src.train.build_pipeline``.
    df:
        Raw input DataFrame (same schema as training data, without ``late``).

    Returns
    -------
    pd.Series
        Float probabilities (0–1) named ``late_probability``.
    """
    logger.info("Running probability predictions on %d records", len(df))
    probabilities = model.predict_proba(df)[:, 1]
    return pd.Series(probabilities, name="late_probability")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    _config_path = Path("config/config.yaml")
    _config = load_config(_config_path)
    _system = _config["system"]

    _model = load_model(_system["model_save_path"])
    _df = import_data(_system["raw_dataset_path"])

    _predictions = predict(_model, _df)
    _proba = predict_proba(_model, _df)

    logger.info("Sample predictions:\n%s", _predictions.head().to_string())
    logger.info("Sample probabilities:\n%s", _proba.head().to_string())
