from airflow.datasets import Dataset

def __airflow_dataset(dataset_name:str) -> tuple[Dataset, Dataset]:
    return (
        Dataset(f'INFO://{dataset_name}'), 
        Dataset(f'SUCCESS://{dataset_name}'), 
        Dataset(f'FAIL://{dataset_name}')
    ) 
INFO_FOUND_CSV,_,_ = __airflow_dataset('csv_found')
INFO_INGESTION_DATASET, SUCCESS_INGESTION_DATASET, FAIL_INGESTION_DATASET                   = __airflow_dataset('ingestion')
_, SUCCESS_INGESTION_DATASET, FAIL_INGESTION_DATASET                                        = __airflow_dataset('ingestion')
_, SUCCESS_CLEANING_DATASET, FAIL_CLEANING_DATASET                                          = __airflow_dataset('cleansing')
_, SUCCESS_DBT_TRANSFORM_DATASET, FAIL_DBT_TRANSFORM_DATASET                                = __airflow_dataset('dbt_transform')
_, SUCCESS_PUBLISH_DATASET, FAIL_PUBLISH_DATASET                                            = __airflow_dataset('publish')
_, SUCCESS_ERROR_HANDLING_DATASET, FAIL_ERROR_HANDLING_DATASET                              = __airflow_dataset('error_handeling')