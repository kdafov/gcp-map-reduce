from semaphore import check_reducer_semaphore, unlock_reducer_semaphore
from cloud_storage_operations import store_reducer_output, check_shuffler_files_count, read_all_shufflers_from_bucket

# Define a counter variable to store the number of runtimes
counter = 1

def reducer(event, context):
    """
    Upon receiving a message, meaning
    that the quantity of shuffler-outputs
    has changed, the reducer function will
    check if there is another reducer function
    currently updating the anagrams file; If yes,
    it will terminate, and if no - it will
    recompile and update the anagrams file.
    """

    if check_reducer_semaphore() == True:
        group_shufflers()


def group_shufflers():
    """
    This function will group the output
    from all shufflers into single
    dictionary. The values of each key
    will be checked if they contain more
    than 2 items (as per requirements) and
    if not will be removed. Returns list of
    anagrams more than two words each.
    """ 

    shufflers = read_all_shufflers_from_bucket()

    # Group all shuffler dictionaries
    grouped_shuffler = {
	    key: [','.join(dictionary.get(key)) for dictionary in shufflers if key in dictionary]
	    for key in set().union(*shufflers)
	}

    # Remove duplicate values from grouped shuffler
    no_dupl_shuffler = { key: set((','.join(val)).split(',')) for key,val in grouped_shuffler.items() }

    # Remove key-value pairs that have less than 2 values
    reduced_list = {
        key: value for key, value in no_dupl_shuffler.items() if len(','.join(value).split(',')) >= 2
    }

    # Convert reducer dictionary to list of anagrams
    anagrams = []

    for anagram_list in reduced_list.values():
        # Format anagrams such as ['ladys,sadly', '...']
        clean_list = ','.join(anagram_list).split(',')

        # Sort list of anagrams alphabetically
        alpha_list = sorted(clean_list)
        
        # Append in final list of anagrams
        if alpha_list not in anagrams:
            anagrams.append(alpha_list)
    
    # Sort anagram sets by length
    anagrams = sorted(anagrams, key=len)

    # Store anagrams to bucket
    store_reducer_output(anagrams)

    # Check if last shuffler has compiled
    file_count = check_shuffler_files_count()
    global counter
    if file_count == 100 and counter < 2:
        print('All shufflers compiled! Re-running reducer to verify anagrams...')
        counter += 1
        group_shufflers()
    else:
        # Unlock semaphore
        print('Shufflers [{}/100] compiled!'.format(file_count))
        unlock_reducer_semaphore()
    
    print("Found {} unique sets of anagrams. Output created at: gs://anagrams-PROJECT-ID/anagrams".format(len(anagrams)))

