from datetime import datetime
def custom_extra_template(file_name:str, branch_name: str) -> dict:
    """
    Generates a dictionary containing metadata about a file and branch, including the current timestamp.

    Args:
        file_name (str): The name of the file to be included in the metadata.
        branch_name (str): The name of the branch associated with the file.

    Returns:
        dict: A dictionary with the following keys:
            - "file_name": The provided file name.
            - "branch_name": The provided branch name.
            - "updated_at": The current timestamp in the format '%Y-%m-%dT%H:%M:%S'.
    """
    return {
        "file_name":    file_name,
        "branch_name":  branch_name,
        "updated_at":   datetime.now().strftime("%Y-%m-%dT%H:%M:%S") 
    }   
    
def get_extra_triggering_run(context) -> list:
    """
    Extracts the 'extra' metadata from the first dataset in each triggering event 
    and returns the 'extra' metadata list of all processed datasets.
    
    Returns:
        list: The 'extra' metadata from the first dataset of each triggering event.
              Returns an empty list if no triggering events are found.
    """
    triggering_dataset_events = context.get("triggering_dataset_events", {})
    
    result = []
    for _, dataset_list in triggering_dataset_events.items():
        if dataset_list:
            extra = dataset_list[0].extra  
            result.append(extra)
    return result