import json
from airflow.models import Variable

def get_active_processing_files(variable_name: str = None) -> list:
    """
    Retrieves a list of active processing files from an Airflow Variable.

    Args:
        variable_name (str): The name of the Airflow Variable that stores the list of active files.

    Returns:
        list: A list of active processing files. If the variable doesn't exist, an empty list is returned.
    """
    return Variable.get(
        variable_name, 
        deserialize_json=True, 
        default_var=[]
    )

def add_object_to_active_processing_files(variable_name: str = None, object_name: str = None):
    """
    Adds an object (file name) to the list of active processing files stored in an Airflow Variable.

    Args:
        variable_name (str): The name of the Airflow Variable that stores the list of active files.
        object_name (str): The object (file name) to be added to the list.

    Returns:
        None
    """
    active_processing_files = get_active_processing_files(variable_name)

    if object_name not in active_processing_files:
        active_processing_files.append(object_name)
        Variable.set(variable_name, json.dumps(active_processing_files))

def remove_object_from_active_processing_files(variable_name: str = None, object_name: str = None):
    """
    Removes an object (file name) from the list of active processing files stored in an Airflow Variable.

    Args:
        variable_name (str): The name of the Airflow Variable that stores the list of active files.
        object_name (str): The object (file name) to be removed from the list.

    Returns:
        None
    """
    active_processing_files = get_active_processing_files(variable_name)

    if object_name in active_processing_files:
        active_processing_files.remove(object_name)
        Variable.set(variable_name, json.dumps(active_processing_files))
