from sklearn.model_selection import train_test_split
from sklearn.utils import resample
from collections import Counter
import logging
import pandas as pd
import os
def split_data(processed_file_path, output_dir="/tmp/ml_splits", test_size=0.2, random_state=42, balance_classes=False):
    logging.info("Starting data splitting...")

    try:
        os.makedirs(output_dir, exist_ok=True)
        data = pd.read_csv(processed_file_path)

        X_train, X_test, y_train, y_test = train_test_split(
            data.drop(columns=['Shipment Mode']),
            data['Shipment Mode'],
            test_size=test_size,
            random_state=random_state,
            stratify=data['Shipment Mode']
        )

        logging.info(f"Training set class distribution before balancing: {Counter(y_train)}")

        if balance_classes:
            # Combine X and y for easy resampling
            train_data = X_train.copy()
            train_data['Shipment Mode'] = y_train

            # Separate classes
            classes = train_data['Shipment Mode'].unique()
            max_count = train_data['Shipment Mode'].value_counts().max()

            balanced_frames = []
            for cls in classes:
                cls_data = train_data[train_data['Shipment Mode'] == cls]
                # Upsample to max_count
                cls_upsampled = resample(
                    cls_data,
                    replace=True,  # sample with replacement
                    n_samples=max_count,
                    random_state=random_state
                )
                balanced_frames.append(cls_upsampled)

            balanced_train_data = pd.concat(balanced_frames)
            X_train = balanced_train_data.drop(columns=['Shipment Mode'])
            y_train = balanced_train_data['Shipment Mode']

            logging.info(f"Training set class distribution after balancing: {Counter(y_train)}")

        paths = {
        "X_train": os.path.join(output_dir, "X_train.csv"),
        "X_test": os.path.join(output_dir, "X_test.csv"),
        "y_train": os.path.join(output_dir, "y_train.csv"),
        "y_test": os.path.join(output_dir, "y_test.csv")
        }

        X_train.to_csv(paths["X_train"], index=False)
        X_test.to_csv(paths["X_test"], index=False)
        y_train.to_csv(paths["y_train"], index=False)
        y_test.to_csv(paths["y_test"], index=False)

        logging.info("Data splitting, balancing, and saving completed successfully")
        return paths

    except Exception as e:
        logging.error(f"Data splitting failed: {e}")
        raise
