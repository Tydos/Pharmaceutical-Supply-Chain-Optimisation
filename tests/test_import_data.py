import pytest
import pandas as pd
from src.import_data import import_data


def test_import_data_valid_csv(tmp_path):
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("col1,col2\n1,a\n2,b\n")
    df = import_data(str(csv_file))
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert list(df.columns) == ["col1", "col2"]


def test_import_data_missing_file():
    with pytest.raises(Exception):
        import_data("/nonexistent/path/data.csv")


def test_import_data_returns_dataframe(tmp_path):
    csv_file = tmp_path / "data.csv"
    csv_file.write_text("a,b,c\n1,2,3\n4,5,6\n7,8,9\n")
    df = import_data(str(csv_file))
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (3, 3)
