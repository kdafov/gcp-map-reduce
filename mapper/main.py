from google.cloud import storage
from google.cloud import pubsub_v1
import base64
import re
import string

# CHANGE THIS VARIABLES
GCP_project_ID = "ENTER-PROJECT-ID"
SHUFFLER_OUTPUT_BUCKET = "BUCKET-NAME" 

# Define cloud storage client and bucket name
storage_client = storage.Client()
bucket_name = "CONFIDENTIAL"

# Define pubsub client and pubsub details
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(GCP_project_ID, "reducer-subscription")

def get_pubsub_message(event, context):
    """
    Generate shuffler output for each 
    message received which contains
    the file name from the bucket.
    """

    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    process_file(pubsub_message)

def process_file(file_name):
    """
    The function will get the content of
    the file and will clean it
    before processing it. It will then
    call the mapper and shuffler functions
    to produce a summarised dictionary with
    unique keys and list of values. The shuffler
    output will be uploaded to the bucket and
    a message will be send to the reducer
    pub sub.
    """

    content = get_clean_file_content(file_name)
    mapperOutput = mapper(content)
    shufflerOutput = shuffler(mapperOutput)
    upload_shuffler_output_to_bucket(file_name.split('.')[0],shufflerOutput)
    send_update_to_reducer()

    print("FILE {} PROCESSING COMPLETED [M:{}, S:{}, O:{}]".format(file_name,len(mapperOutput),len(shufflerOutput),"gs://" + SHUFFLER_OUTPUT_BUCKET + "/" + file_name))    

def get_clean_file_content(file_name):
    """
    This function will get the
    file content from the bucket
    and will clear the whitespace,
    punctuation, remove duplicates,
    and perform other clearing of data.
    It will then brake the text into
    words, store them in list and return.
    All items will be in lowercase.
    """

    # Get raw file content
    bucket = storage_client.bucket(bucket_name)
    file_ref = bucket.blob(file_name)
    file_content = file_ref.download_as_string()
    try:
        decoded_file_content = file_content.decode('utf-8')
    except:
        decoded_file_content = file_content.decode('latin-1')

    # Remove whitespace
    no_ws_content = re.sub("\s+", " ", decoded_file_content.lower())

    # Remove dashes
    no_dashes_content = no_ws_content.replace('-', ' ')

    # Remove punctuation (incl. quotes)
    no_punct_content = no_dashes_content.translate(str.maketrans('', '', string.punctuation)) 

    # Remove numbers
    no_numbers_content = no_punct_content.translate(str.maketrans('', '', string.digits))

    # Remove roman numbers
    no_roman_nums_content = re.sub(r"\b(?=[mdclxvii])m{0,4}(cm|cd|d?c{0,3})(xc|xl|l?x{0,3})([ii]x|[ii]v|v?[ii]{0,3})\b\.?", "", no_numbers_content)

    # Split into list with words
    list_of_content = [word for word in no_roman_nums_content.split(' ') if word.strip() != '']

    # Remove duplicate words
    list_no_dupl = list(dict.fromkeys(list_of_content))

    # Remove stopwords
    stopwords = ['tis', 'twas', 'a', 'able', 'about', 'across', 'after', 'aint', 'all', 'almost', 'also', 'am', 'among', 'an', 'and', 'any', 'are', 'arent', 'as', 'at', 'be', 'because', 'been', 'but', 'by', 'can', 'cant', 'cannot', 'could', 'couldve', 'couldnt', 'dear', 'did', 'didnt', 'do', 'does', 'doesnt', 'dont', 'either', 'else', 'ever', 'every', 'for', 'from', 'get', 'got', 'had', 'has', 'hasnt', 'have', 'he', 'hed', 'hell', 'hes', 'her', 'hers', 'him', 'his', 'how', 'howd', 'howll', 'hows', 'however', 'i', 'id', 'ill', 'im', 'ive', 'if', 'in', 'into', 'is', 'isnt', 'it', 'its', 'its', 'just', 'least', 'let', 'like', 'likely', 'may', 'me', 'might', 'mightve', 'mightnt', 'most', 'must', 'mustve', 'mustnt', 'my', 'neither', 'no', 'nor', 'not', 'of', 'off', 'often', 'on', 'only', 'or', 'other', 'our', 'own', 'rather', 'said', 'say', 'says', 'shant', 'she', 'shed', 'shell', 'shes', 'should', 'shouldve', 'shouldnt', 'since', 'so', 'some', 'than', 'that', 'thatll', 'thats', 'the', 'their', 'them', 'then', 'there', 'theres', 'these', 'they', 'theyd', 'theyll', 'theyre', 'theyve', 'this', 'tis', 'to', 'too', 'twas', 'us', 'wants', 'was', 'wasnt', 'we', 'wed', 'well', 'were', 'were', 'werent', 'what', 'whatd', 'whats', 'when', 'when', 'whend', 'whenll', 'whens', 'where', 'whered', 'wherell', 'wheres', 'which', 'while', 'who', 'whod', 'wholl', 'whos', 'whom', 'why', 'whyd', 'whyll', 'whys', 'will', 'with', 'wont', 'would', 'wouldve', 'wouldnt', 'yet', 'you', 'youd', 'youll', 'youre', 'youve', 'your']
    no_stopwords_list = [word for word in list_no_dupl if word not in stopwords]

    # Remove words that are less than 2 characters i.e., [a, b, c, d, e, f, ...]
    no_short_words_list = [word for word in no_stopwords_list if len(word) > 1]

    # Returns
    return no_short_words_list

def mapper(words):
    """
    This mapper function will convert
    a list of words into a list of
    key-value pairs where the key will
    be the letters of the word in
    alphabetical order.
    """

    # Produce key-value pairs for each word in words array
    return [
        { ''.join(sorted(list(word))) : word } for word in words
    ]

def shuffler(mapper):
    """
    This shuffler function will get
    the list of key-value pairs
    generated by the mapper and will
    group them by keys producing
    a dictionary with key-list pairs.
    """

    # Produce dictionary containing unique key-list pairs
    return {
        key: [d.get(key) for d in mapper if key in d] 
        for key in set().union(*mapper)  
    }

def upload_shuffler_output_to_bucket(file_name, shuffler_output):
    """
    This function will take the output from
    the shuffler for the given file name
    and will upload it as object to the
    cloud storage bucket.
    """

    # Upload the output from the shuffler to the bucket
    bucket = storage_client.bucket(SHUFFLER_OUTPUT_BUCKET)
    file_ref = bucket.blob(file_name)
    file_ref.upload_from_string(str(shuffler_output).encode('utf-8'))

def send_update_to_reducer():
    """
    This function will send a message
    to the pub/sub system to which the
    reducer is subsribed, so it can
    recompile its final output as 
    there is a new shuffler file added.
    """

    publisher.publish(
        topic_path, "new shuffler file!".encode('utf-8'), origin="python-sample", username="gcp"
    )
