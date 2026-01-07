import pandas as pd
import logging
import numpy as np
import sklearn
from sklearn.calibration import LabelEncoder

def process_data(x):
    logging.info("Starting data processing...")

    try:
        # Drop unnecessary columns
        x=x.drop(columns=['PO Sent to Vendor Date','Molecule/Test Type','Sub Classification','Brand','Manufacturing Site','Item Description','Project Code','Country','ID','PQ #','PO / SO #','ASN/DN #','Managed By','Fulfill Via','Vendor INCO Term','PQ First Sent to Client Date','Product Group','Vendor'])

        # Handle 'Line Item Insurance (USD)' column
        x['Line Item Insurance (USD)']=x['Line Item Insurance (USD)'].fillna(x['Line Item Insurance (USD)'].median())
        x['Line Item Insurance (USD)'].values[x['Line Item Insurance (USD)'].values>1000]=x['Line Item Insurance (USD)'].median()
        x['Line Item Insurance (USD)'].values[x['Line Item Insurance (USD)'].values>200]=x['Line Item Insurance (USD)'].mean()
       
        # Convert all cells to numeric where possible, coerce errors to NaN
        x['Freight Cost (USD)'] = x['Freight Cost (USD)'].apply(pd.to_numeric, errors='coerce')

        # Drop rows where all values are NaN (i.e., non-numeric)
        x = x[x['Freight Cost (USD)'].notna()]

        x['Shipment Mode']=x['Shipment Mode'].fillna(x['Shipment Mode'].mode()[0])
        x['Dosage']=x['Dosage'].fillna(x['Dosage'].mode()[0])
        x['Dosage'] = x['Dosage'].str.extract('(\d+)', expand=False).astype(int)        
        x['Weight (Kilograms)'] = x['Weight (Kilograms)'].apply(pd.to_numeric, errors='coerce')
        x = x[x['Weight (Kilograms)'].notna()]  

        for column in ['Scheduled Delivery Date', 'Delivered to Client Date', 'Delivery Recorded Date']:
            x[column] = pd.to_datetime(x[column])
            x[column + ' Year'] = x[column].apply(lambda x: x.year)
            x[column + ' Month'] = x[column].apply(lambda x: x.month)
            x[column + ' Day'] = x[column].apply(lambda x: x.day)
            x = x.drop(column, axis=1)

        le=LabelEncoder()
        x['Shipment Mode']=le.fit_transform(x['Shipment Mode'])
        # air=0,ocean=2,air charter=1,truck=3
        x['First Line Designation']=le.fit_transform(x['First Line Designation'])
        x['Dosage Form']=le.fit_transform(x['Dosage Form'])    

        import tempfile, os
        temp_dir = tempfile.gettempdir()
        temp_file = os.path.join(temp_dir, "processed_data.csv")
        x.to_csv(temp_file, index=False)

        logging.info(f"Data processing completed successfully. Saved to {temp_file}")
        return temp_file

    except Exception as e:
        logging.error(f"Data processing failed: {e}")
        raise