# upload_to_azurite.py
import os
import argparse
from azure.storage.blob import BlobServiceClient

DEFAULT_CONN = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
    "K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
)

def upload(file_path, container_name="datasets", blob_name=None, conn_str=DEFAULT_CONN):
    if blob_name is None:
        blob_name = os.path.basename(file_path)
    bsc = BlobServiceClient.from_connection_string(conn_str)
    container_client = bsc.get_container_client(container_name)
    try:
        container_client.create_container()
    except Exception:
        pass
    blob_client = container_client.get_blob_client(blob_name)
    with open(file_path, "rb") as f:
        blob_client.upload_blob(f, overwrite=True)
    print(f"✅ Uploaded {file_path} to container '{container_name}' as blob '{blob_name}'")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--file", "-f", required=True, help="Path to All_Diets.csv")
    args = p.parse_args()
    upload(args.file)
