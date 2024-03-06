from google.cloud import storage
import ast

# CHANGE THIS VARIABLES
SHUFFLER_OUTPUT_BUCKET = "shuffler-output-YOUR-PROJECT-ID"
ANAGRAMS_OUTPUT_BUCKET = "anagrams-YOUR-PROJECT-ID"

# Define cloud storage client and bucket name
storage_client = storage.Client()

def read_all_shufflers_from_bucket():
    """
    This function will read all shufflers
    from the bucket, convert them back to
    dictionary types from string, and will
    append them to a global list.
    """

    shufflers = []

    # Store all files contained in the bucket
    files = storage_client.list_blobs(SHUFFLER_OUTPUT_BUCKET)

    # Read each file...
    bucket = storage_client.bucket(SHUFFLER_OUTPUT_BUCKET)
    for each in files:
        # Download and save its content
        file_ref = bucket.blob(each.name)
        file_content_raw = file_ref.download_as_string().decode('utf-8')

        # Convert string back to dictionary
        file_content_dict = ast.literal_eval(file_content_raw)

        # Append to global list
        shufflers.append(file_content_dict)

    return shufflers


def store_reducer_output(anagrams):
    """
    This function will store the
    reducer's output (all valid 
    anagrams) into a bucket.
    """

    # Upload the anagrams to the bucket
    bucket = storage_client.bucket(ANAGRAMS_OUTPUT_BUCKET)
    file_ref = bucket.blob("anagrams")
    file_ref.upload_from_string(str(anagrams).encode('utf-8'))


def check_shuffler_files_count():
    """
    This function will return the count
    of items in the shuffler-output bucket.
    """

    # Get files in bucket and check their length
    files = storage_client.list_blobs(SHUFFLER_OUTPUT_BUCKET)
    file_count = 0
    for each in files:
        file_count += 1
    return file_count
