import yaml
from pathlib import Path
from src.config import load_config
import logging
logging.basicConfig(level=logging.DEBUG)

if __name__=="__main__":
    config_path = Path('config/config.yaml')
    config = load_config(config_path)
    system = config['system']
    logging.debug(f"Loaded configuration: {config}")