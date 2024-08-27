from pyspark.sql import SparkSession
from soda.scan import Scan
import os

def check(
    spark_session: SparkSession,
    scan_definition_name: str,
    data_source_name: str,
    check_path_yaml: str,
    log_file_path: str = None,  # Optional log file path
    report_file_path: str = None,  # Optional report file path
    stop_on_fail: bool = False  # Option to stop execution on failure
) -> str:
    """
    Execute a Soda scan on a Spark DataFrame with enhanced features.

    :param spark_session: The SparkSession to use for the scan.
    :param scan_definition_name: The name of the scan definition.
    :param data_source_name: The name of the data source.
    :param check_path_yaml: The path to the YAML file containing Soda checks.
    :param log_file_path: Optional; path to save the scan logs.
    :param report_file_path: Optional; path to save the scan report.
    :param stop_on_fail: Optional; if True, raise an exception on scan failure.
    :return: None
    """
    try:
        scan = Scan()
        scan.set_scan_definition_name(scan_definition_name)
        scan.set_data_source_name(data_source_name)
        scan.add_spark_session(spark_session, data_source_name=data_source_name)
        scan.add_sodacl_yaml_files(check_path_yaml)

        scan.execute()

        logs = scan.get_logs_text()
        
        if log_file_path:
            os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
            with open(log_file_path, 'w') as log_file:
                log_file.write(logs)

        if report_file_path:
            os.makedirs(os.path.dirname(report_file_path), exist_ok=True)
            report = scan.get_scan_results_as_html()
            with open(report_file_path, 'w') as report_file:
                report_file.write(report)

        if stop_on_fail and scan.has_check_fails():
            raise RuntimeError("Soda scan failed. Check logs for details.")

        return logs
    
    except Exception as e:
        print(f"An error occurred during the Soda scan: {e}")
        if stop_on_fail:
            raise
