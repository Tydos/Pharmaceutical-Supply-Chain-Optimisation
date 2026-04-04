import pytest
import pandas as pd
import os
from src.process_data import process_data


def make_sample_df():
    """Return a minimal DataFrame matching the supply chain schema."""
    return pd.DataFrame({
        "ID": [1, 2],
        "Project Code": ["P1", "P2"],
        "PQ #": ["PQ1", "PQ2"],
        "PO / SO #": ["PO1", "PO2"],
        "ASN/DN #": ["ASN1", "ASN2"],
        "Country": ["USA", "Canada"],
        "Managed By": ["PMO - US", "PMO - US"],
        "Fulfill Via": ["Direct Drop", "Direct Drop"],
        "Vendor INCO Term": ["EXW", "EXW"],
        "Shipment Mode": ["Air", "Ocean"],
        "PQ First Sent to Client Date": ["2-Jun-06", "14-Nov-06"],
        "PO Sent to Vendor Date": ["2-Jun-06", "14-Nov-06"],
        "Scheduled Delivery Date": ["2-Jun-06", "14-Nov-06"],
        "Delivered to Client Date": ["2-Jun-06", "14-Nov-06"],
        "Delivery Recorded Date": ["2-Jun-06", "14-Nov-06"],
        "Product Group": ["HRDT", "ARV"],
        "Sub Classification": ["HIV test", "Pediatric"],
        "Vendor": ["Vendor A", "Vendor B"],
        "Item Description": ["Item A", "Item B"],
        "Molecule/Test Type": ["Type A", "Type B"],
        "Brand": ["Brand A", "Brand B"],
        "Dosage": ["10mg", "20mg"],
        "Dosage Form": ["Tablet", "Oral solution"],
        "Unit of Measure (Per Pack)": [30, 240],
        "Line Item Quantity": [19, 1000],
        "Line Item Value": [551, 6200],
        "Pack Price": [29, 6.2],
        "Unit Price": [0.97, 0.03],
        "Manufacturing Site": ["Site A", "Site B"],
        "First Line Designation": ["Yes", "No"],
        "Weight (Kilograms)": [13.0, 358.0],
        "Freight Cost (USD)": [780.34, 4521.5],
        "Line Item Insurance (USD)": [5.0, None],
    })


def test_process_data_returns_file_path():
    df = make_sample_df()
    result = process_data(df)
    assert isinstance(result, str)
    assert os.path.isfile(result)


def test_process_data_output_is_csv():
    df = make_sample_df()
    result = process_data(df)
    output = pd.read_csv(result)
    assert isinstance(output, pd.DataFrame)
    assert len(output) > 0


def test_process_data_drops_columns():
    df = make_sample_df()
    result = process_data(df)
    output = pd.read_csv(result)
    dropped = [
        "PO Sent to Vendor Date", "Molecule/Test Type", "Sub Classification",
        "Brand", "Manufacturing Site", "Item Description", "Project Code",
        "Country", "ID", "PQ #", "PO / SO #", "ASN/DN #", "Managed By",
        "Fulfill Via", "Vendor INCO Term", "PQ First Sent to Client Date",
        "Product Group", "Vendor",
    ]
    for col in dropped:
        assert col not in output.columns


def test_process_data_fills_insurance_nulls():
    df = make_sample_df()
    result = process_data(df)
    output = pd.read_csv(result)
    assert output["Line Item Insurance (USD)"].isna().sum() == 0


def test_process_data_encodes_shipment_mode():
    df = make_sample_df()
    result = process_data(df)
    output = pd.read_csv(result)
    assert output["Shipment Mode"].dtype in [int, "int64", "int32", float, "float64"]


def test_process_data_invalid_input():
    with pytest.raises(Exception):
        process_data(pd.DataFrame())
