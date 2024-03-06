from google.cloud import storage
from google.cloud import pubsub_v1
import functions_framework

# CHANGE THIS VARIABLES
GCP_project_name = "YOUR-GCP-PROJECT-ID"
SHUFFLER_OUTPUT_BUCKET = "BUCKET-NAME" 

# Define cloud storage client and bucket name
storage_client = storage.Client()
bucket_name = "CONFIDENTIAL"

# Define pubsub client and pubsub details
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(GCP_project_name, "book-subscription")

@functions_framework.http
def read_and_publish_files(request):
    """
    Read the content from the
    bucket provided and create
    a new message for each file
    to a dedicated pub/sub system.
    Message will contain the filename
    from the bucket.
    """

    # Get files in bucket
    files = storage_client.list_blobs(bucket_name)

    # Reset content of bucket where output from previous run is stored
    reset_bucket_content()
    
    # Publish message for each file
    for each in files:
        # Define message and encode it
        message = each.name
        encoded_msg = message.encode("utf-8")

        # Publish message
        publisher.publish(
            topic_path, encoded_msg, origin="python-sample", username="gcp"
        )
    
    return '\n {} Script completed... [200] {} \n'.format(10*"*",10*"*")

def reset_bucket_content():
    """
    This function will reset the 
    content of the cloud storage
    bucket.
    """

    # Get all current files in bucket
    files = storage_client.list_blobs(SHUFFLER_OUTPUT_BUCKET)

    # Delete each file in bucket
    bucket = storage_client.bucket(SHUFFLER_OUTPUT_BUCKET)
    for each in files:
        file_ref = bucket.blob(each.name)
        file_ref.delete()
