import pandas as pd
import logging

def import_data(file_path):
    logging.info(f"Reading CSV from: {file_path}")

    try:
        data = pd.read_csv(file_path, encoding="latin1")
        logging.info("Data imported successfully")
        return data

    except Exception as e:
        logging.error(f"Failed to import data: {e}")
        raise  
