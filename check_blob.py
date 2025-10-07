from azure.storage.blob import BlobServiceClient
import os, sys

CONN = os.getenv("AZURITE_CONN") or (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
    "K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
)

def main():
    try:
        bsc = BlobServiceClient.from_connection_string(CONN)
        print('Connected to Azurite.')
        print('Containers:')
        for c in bsc.list_containers():
            print(' -', c['name'])
        # try to list blobs in our expected container
        container_name = 'datasets'
        try:
            container_client = bsc.get_container_client(container_name)
            print(f"Blobs in container '{container_name}':")
            found = False
            for blob in container_client.list_blobs():
                print('   *', blob.name)
                found = True
            if not found:
                print('   (no blobs found)')
        except Exception as e:
            print(f"Could not list blobs in '{container_name}':", e)
    except Exception as e:
        print('Connection to Azurite failed:', e)
        sys.exit(2)

if __name__ == '__main__':
    main()
