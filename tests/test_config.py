import pytest
from pathlib import Path
from src.config import load_config


def test_load_config_valid(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("system:\n  raw_dataset_path: './data/supply_chain.csv'\n")
    config = load_config(config_file)
    assert "system" in config
    assert config["system"]["raw_dataset_path"] == "./data/supply_chain.csv"


def test_load_config_missing_file():
    with pytest.raises(FileNotFoundError):
        load_config(Path("/nonexistent/path/config.yaml"))


def test_load_config_empty_file(tmp_path):
    config_file = tmp_path / "empty.yaml"
    config_file.write_text("")
    config = load_config(config_file)
    assert config is None
