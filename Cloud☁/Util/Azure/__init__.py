# Pass in a container name and get names of all blobs in the container.
import os
from dotenv import load_dotenv

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

load_dotenv()

EndpointsProtocol = os.getenv("DefaultEndpointsProtocol")
AccountName = os.getenv("AccountName")
AccountKey = os.getenv("AccountKey")
EndpointSuffix= os.getenv("EndpointSuffix")

ConnectionString = ("DefaultEndpointsProtocol="+
                    EndpointsProtocol + ";" +
                    "AccountName="+ 
                    AccountName + ";" +
                    "AccountKey=" +
                    AccountKey + ";" +
                    "EndpointSuffix="+
                    EndpointSuffix
                    ) 

# """The below code prints all the blobs in the container(bucket)"""
blob_service_client = BlobServiceClient.from_connection_string(ConnectionString)

def blobs(container_name):
    """
    Returns blob list from azure blob storage container.

    Usage : 
    ```
    for blob in blobs("container_name"):
        print("\\t" + blob.name)
    ```
    """
    container_client=blob_service_client.get_container_client(container_name)
    blob_list = container_client.list_blobs()
    return blob_list


def getData(container_name, blob_name):
    """
    Returns the content of the blob in container.
    """
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    return blob_client.download_blob().readall()
