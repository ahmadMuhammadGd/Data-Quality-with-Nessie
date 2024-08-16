# Data-Quality-with-Nessie

This project demonstrates a data quality pipeline using Nessie for managing data versioning and validation.

- **Dataset**: For more information about the dataset used in this project, [click here](https://www.kaggle.com/datasets/arpit2712/amazonsalesreport?resource=download).
- **Batch Creation**: Batches were created using `./utils/sampler.py`, which samples and prepares the data for processing.
  - `sampled_data_1` contains clean data, while `sampled_data_2` contains corrupted data.
- **Log Files**: To follow the pipeline process, check the log files in this order: `init_job`, `stage_1`, `stage_2`, and `insert_job`. Each log file records specific stages of the pipeline execution.
- **Bash Script**: The bash script is designed to process and analyze ONLY the corrupted data CSV file to identify data quality issues.
- **Data Corruption Module**: You can find the data corruption module, which is used to intentionally corrupt datasets for testing purposes, at [this GitHub repository](https://github.com/ahmadMuhammadGd/Pandas-Data-Frame-Corrupter-For-Data-Pipeline-Tests). Note that it's still under development and may not be fully stable.

## How to Run

To run the project, simply execute `sh start.sh`. Ensure that Docker is installed and properly configured on your system.
