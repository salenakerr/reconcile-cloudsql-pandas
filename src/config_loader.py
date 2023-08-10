# config_loader.py
import yaml
from google.cloud import secretmanager_v1

def load_config(file_path):
    with open(file_path, "r") as config_file:
        config = yaml.safe_load(config_file)
    return config

def get_google_secret(secret_ref):
    # Authenticate the Google Cloud Service Account
    client = secretmanager_v1.SecretManagerServiceClient().from_service_account_json(
        config['default_sa_path']
    )

    # Retrieve the password from Secret Manager
    secret_response = client.access_secret_version(name=secret_ref)
    password = secret_response.payload.data.decode("UTF-8")

    return password

# Load configurations from db_config.yaml and config.yaml into module-level variables
config = load_config("/configs/config.yaml")
