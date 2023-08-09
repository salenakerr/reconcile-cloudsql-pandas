# config_loader.py
import yaml

def load_config(file_path):
    with open(file_path, "r") as config_file:
        config = yaml.safe_load(config_file)
    return config

# Load configurations from db_config.yaml and config.yaml into module-level variables
config = load_config("configs/config.yaml")
