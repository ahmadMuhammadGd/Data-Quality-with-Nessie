from datetime import datetime
from airflow.datasets import Dataset
import json  
    
def get_extra_triggering_run(context)->list:
    """
    Extracts the 'extra' metadata from the first dataset in each triggering event 
    and returns the 'extra' metadata list of all processed datasets.
    
    Returns:
        list: The 'extra' metadata from the first dataset of each triggering event.
              Returns an empty list if no triggering events are found.
    """
    triggering_dataset_events = context["triggering_dataset_events"]
    result = []
    for _, dataset_list in triggering_dataset_events.items():
        if dataset_list:
            result.append(dataset_list[0].extra)
    return result


from typing import Union
def update_outlet ( dataset: Dataset, 
                    content: Union[str, dict, int, float, list, bool] = None,
                    context: dict = None
    ):
    if context is None:
        context = {}
    context["outlet_events"][dataset].extra = content 
    