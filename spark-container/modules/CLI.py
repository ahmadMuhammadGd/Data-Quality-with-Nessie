import getopt
import sys
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)

def cli():
    options = "h:o:t:b:"
    long_options = ['help', 'object-path=', 'timestamp=', 'nessie-branch=']

    argument_list = sys.argv[1:]
    try:
        arguments, values = getopt.getopt(argument_list, options, long_options)
        object_path = None 
        timestamp = None
        nessie_branch = None

        for arg, val in arguments:
            if arg in ("-h", "--help"):
                print("Usage: python path/to/sparkjob --object-path <path> [options]")
                print("Options:")
                print("  -h, --help            Show this help message")
                print("  -o, --object-path <path>   Path to the object without s3a//")
                print("  -t, --timestamp <timestamp> Timestamp when the ingestion process started")
                print("  -b, --nessie-branch <branch> Nessie Branch to ingest in")
                sys.exit()
            elif arg in ("-o", "--object-path"):
                object_path = val
            elif arg in ("-t", "--timestamp"):
                timestamp = val
            elif arg in ("-b", "--nessie-branch"):  # Corrected typo
                nessie_branch = val

        if object_path is None:
            logging.error("Error: --object-path (-o) is required")
            sys.exit(2)

        if timestamp is None:
            logging.error("Error: --timestamp (-t) is required")
            sys.exit(2)

        if nessie_branch is None:
            logging.error("Error: --nessie-branch (-b) is required")
            sys.exit(2)

    except getopt.GetoptError as err:
        logging.error(f"Error: {err}")
        logging.info("Use -h or --help for usage information.")
        sys.exit(2)

    logging.info(f"Object Path: {object_path}, Timestamp: {timestamp}, Nessie Branch: {nessie_branch}")
    return (object_path, timestamp, nessie_branch)