from google.cloud import storage

# CHANGE THIS VARIABLES
ANAGRAMS_OUTPUT_BUCKET = "anagrams-YOUR-PROJECT-ID"

# Define cloud storage client
storage_client = storage.Client()

def check_reducer_semaphore():
    """
    This function will check if other
    reducer instances are currently running
    i.e., is the semaphore is locked and if so
    it will return False, and if it is unlocked -
    True. If the file is non-existing it will
    return True and will lock the semaphore.
    """
    
    bucket = storage_client.bucket(ANAGRAMS_OUTPUT_BUCKET)
    file_ref = bucket.blob('semaphore')
    try:
        # Check status of reducer semaphore
        file_content = file_ref.download_as_string().decode('utf-8')
        if file_content == "locked":
            return False
        else:
            file_ref.upload_from_string("locked".encode('utf-8'))
            return True
    except:
        # Semaphore.txt does not exist
        file_ref.upload_from_string("locked".encode('utf-8'))
        return True


def unlock_reducer_semaphore():
    """
    This function will set the semaphore
    to unlocked.
    """
    
    bucket = storage_client.bucket(ANAGRAMS_OUTPUT_BUCKET)
    file_ref = bucket.blob('semaphore')
    file_ref.upload_from_string("unlocked".encode('utf-8'))
