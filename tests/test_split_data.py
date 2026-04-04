import pytest
import pandas as pd
import os
from src.split_data import split_data


def make_processed_csv(tmp_path, rows=50):
    """Create a minimal processed CSV file for split_data tests."""
    data = pd.DataFrame({
        "Feature1": range(rows),
        "Feature2": [float(i) * 0.5 for i in range(rows)],
        "Shipment Mode": [i % 4 for i in range(rows)],
    })
    csv_path = str(tmp_path / "processed.csv")
    data.to_csv(csv_path, index=False)
    return csv_path


def test_split_data_returns_paths(tmp_path):
    csv_path = make_processed_csv(tmp_path)
    output_dir = str(tmp_path / "splits")
    paths = split_data(csv_path, output_dir=output_dir)
    assert "X_train" in paths
    assert "X_test" in paths
    assert "y_train" in paths
    assert "y_test" in paths


def test_split_data_creates_files(tmp_path):
    csv_path = make_processed_csv(tmp_path)
    output_dir = str(tmp_path / "splits")
    paths = split_data(csv_path, output_dir=output_dir)
    for key, path in paths.items():
        assert os.path.isfile(path), f"Missing output file: {path}"


def test_split_data_sizes(tmp_path):
    rows = 100
    csv_path = make_processed_csv(tmp_path, rows=rows)
    output_dir = str(tmp_path / "splits")
    paths = split_data(csv_path, output_dir=output_dir, test_size=0.2)
    X_train = pd.read_csv(paths["X_train"])
    X_test = pd.read_csv(paths["X_test"])
    assert len(X_train) + len(X_test) == rows


def test_split_data_balance_classes(tmp_path):
    csv_path = make_processed_csv(tmp_path, rows=80)
    output_dir = str(tmp_path / "splits")
    paths = split_data(csv_path, output_dir=output_dir, balance_classes=True)
    y_train = pd.read_csv(paths["y_train"])
    counts = y_train["Shipment Mode"].value_counts()
    assert counts.max() == counts.min()


def test_split_data_default_output_dir(tmp_path):
    """split_data should create a directory under the system temp folder when output_dir is omitted."""
    csv_path = make_processed_csv(tmp_path)
    paths = split_data(csv_path)
    for key, path in paths.items():
        assert os.path.isfile(path), f"Missing output file: {path}"


def test_split_data_missing_file():
    with pytest.raises(Exception):
        split_data("/nonexistent/path/processed.csv")
