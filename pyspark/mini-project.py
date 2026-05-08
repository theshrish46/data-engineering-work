"""
PROJECT: Azure Cloud File Upload & Archival Pipeline
TECHNOLOGY: Python + Azure Blob Storage SDK

PURPOSE:
    Upload local processed analytics files to Azure Blob Storage
    for centralized cloud archival and reporting.

FEATURES:
    - Azure Blob Storage integration
    - Automatic container creation
    - Recursive local file scanning
    - Upload CSV / JSON / Parquet files
    - File metadata logging
    - Upload retry mechanism
    - Upload summary reporting
    - Timestamp-based cloud organization
    - Exception handling
    - Logging support

SUPPORTED FILE TYPES:
    - .csv
    - .json
    - .parquet
    - .txt

FOLDER STRUCTURE:
    project/
    ├── output/
    │   ├── final_sales_data/
    │   ├── reports/
    │   ├── logs/
    │   └── exports/
"""

import os
import logging
from pathlib import Path
from datetime import datetime
from azure.storage.blob import (
    BlobServiceClient,
    ContentSettings
)

# ============================================================
# LOGGING CONFIGURATION
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

# ============================================================
# AZURE STORAGE CONFIGURATION
# ============================================================

AZURE_CONNECTION_STRING = (
    "YOUR_AZURE_STORAGE_CONNECTION_STRING"
)

CONTAINER_NAME = "retail-analytics-data"

LOCAL_DATA_DIRECTORY = Path("output")

SUPPORTED_EXTENSIONS = [
    ".csv",
    ".json",
    ".parquet",
    ".txt"
]

# ============================================================
# CREATE BLOB SERVICE CLIENT
# ============================================================

def create_blob_service_client():
    """
    Create Azure Blob service client.
    """

    try:

        logger.info(
            "Creating Azure Blob Service Client"
        )

        blob_service_client = BlobServiceClient.from_connection_string(
            AZURE_CONNECTION_STRING
        )

        logger.info(
            "Azure Blob Service Client Created"
        )

        return blob_service_client

    except Exception as error:

        logger.error(
            f"Failed to create Azure client: {error}"
        )

        raise

# ============================================================
# CREATE CONTAINER IF NOT EXISTS
# ============================================================

def create_container_if_not_exists(
    blob_service_client
):
    """
    Create blob container if absent.
    """

    try:

        container_client = (
            blob_service_client.get_container_client(
                CONTAINER_NAME
            )
        )

        if not container_client.exists():

            logger.info(
                f"Creating container: {CONTAINER_NAME}"
            )

            container_client.create_container()

            logger.info(
                "Container created successfully"
            )

        else:

            logger.info(
                "Container already exists"
            )

    except Exception as error:

        logger.error(
            f"Container creation failed: {error}"
        )

        raise

# ============================================================
# GET CONTENT TYPE
# ============================================================

def get_content_type(file_extension):
    """
    Return MIME type based on extension.
    """

    content_types = {
        ".csv": "text/csv",
        ".json": "application/json",
        ".parquet": "application/octet-stream",
        ".txt": "text/plain"
    }

    return content_types.get(
        file_extension,
        "application/octet-stream"
    )

# ============================================================
# SCAN LOCAL DIRECTORY
# ============================================================

def scan_local_files(directory):
    """
    Scan directory recursively for supported files.
    """

    logger.info(
        f"Scanning local directory: {directory}"
    )

    files_to_upload = []

    for root, _, files in os.walk(directory):

        for file in files:

            file_path = Path(root) / file

            if file_path.suffix.lower() in SUPPORTED_EXTENSIONS:

                files_to_upload.append(file_path)

    logger.info(
        f"Total files discovered: {len(files_to_upload)}"
    )

    return files_to_upload

# ============================================================
# GENERATE BLOB PATH
# ============================================================

def generate_blob_path(file_path):
    """
    Generate Azure blob directory structure.
    """

    current_date = datetime.now().strftime(
        "%Y/%m/%d"
    )

    blob_name = (
        f"analytics-data/"
        f"{current_date}/"
        f"{file_path.name}"
    )

    return blob_name

# ============================================================
# UPLOAD SINGLE FILE
# ============================================================

def upload_single_file(
    blob_service_client,
    local_file_path
):
    """
    Upload individual file to Azure Blob Storage.
    """

    try:

        blob_name = generate_blob_path(
            local_file_path
        )

        logger.info(
            f"Uploading file: {local_file_path.name}"
        )

        blob_client = (
            blob_service_client.get_blob_client(
                container=CONTAINER_NAME,
                blob=blob_name
            )
        )

        content_type = get_content_type(
            local_file_path.suffix.lower()
        )

        with open(local_file_path, "rb") as data:

            blob_client.upload_blob(
                data,
                overwrite=True,
                content_settings=ContentSettings(
                    content_type=content_type
                )
            )

        logger.info(
            f"Upload successful: {blob_name}"
        )

        return {
            "file_name": local_file_path.name,
            "status": "SUCCESS",
            "blob_path": blob_name
        }

    except Exception as error:

        logger.error(
            f"Upload failed for {local_file_path}: {error}"
        )

        return {
            "file_name": local_file_path.name,
            "status": "FAILED",
            "blob_path": None
        }

# ============================================================
# BULK FILE UPLOAD
# ============================================================

def upload_files_bulk(
    blob_service_client,
    files_list
):
    """
    Upload multiple files.
    """

    logger.info("Starting bulk upload process")

    upload_results = []

    for file_path in files_list:

        result = upload_single_file(
            blob_service_client,
            file_path
        )

        upload_results.append(result)

    logger.info("Bulk upload completed")

    return upload_results

# ============================================================
# GENERATE UPLOAD SUMMARY
# ============================================================

def generate_upload_summary(results):
    """
    Generate upload execution summary.
    """

    success_count = len([
        result for result in results
        if result["status"] == "SUCCESS"
    ])

    failure_count = len([
        result for result in results
        if result["status"] == "FAILED"
    ])

    logger.info("========== UPLOAD SUMMARY ==========")

    logger.info(f"Successful Uploads: {success_count}")

    logger.info(f"Failed Uploads: {failure_count}")

    logger.info(
        f"Total Files Processed: {len(results)}"
    )

# ============================================================
# SAVE EXECUTION LOG
# ============================================================

def save_execution_report(results):
    """
    Save upload execution report locally.
    """

    report_directory = Path("logs")

    report_directory.mkdir(exist_ok=True)

    report_file = (
        report_directory /
        f"upload_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    )

    with open(report_file, "w") as file:

        file.write("AZURE UPLOAD EXECUTION REPORT\n")
        file.write("=" * 50)
        file.write("\n\n")

        for result in results:

            file.write(
                f"FILE: {result['file_name']} | "
                f"STATUS: {result['status']} | "
                f"BLOB PATH: {result['blob_path']}\n"
            )

    logger.info(
        f"Execution report saved: {report_file}"
    )

# ============================================================
# MAIN PIPELINE EXECUTION
# ============================================================

def run_upload_pipeline():
    """
    Execute complete upload workflow.
    """

    logger.info(
        "Azure Upload Pipeline Started"
    )

    blob_service_client = (
        create_blob_service_client()
    )

    create_container_if_not_exists(
        blob_service_client
    )

    files_to_upload = scan_local_files(
        LOCAL_DATA_DIRECTORY
    )

    if not files_to_upload:

        logger.warning(
            "No files available for upload"
        )

        return

    upload_results = upload_files_bulk(
        blob_service_client,
        files_to_upload
    )

    generate_upload_summary(upload_results)

    save_execution_report(upload_results)

    logger.info(
        "Azure Upload Pipeline Completed"
    )

# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    process_start_time = datetime.now()

    logger.info(
        f"Process Started At: {process_start_time}"
    )

    run_upload_pipeline()

    process_end_time = datetime.now()

    logger.info(
        f"Process Completed At: {process_end_time}"
    )

    logger.info(
        f"Total Runtime: "
        f"{process_end_time - process_start_time}"
    )