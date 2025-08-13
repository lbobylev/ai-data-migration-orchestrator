from app_types import DataMigration, MyState
from file_utils import download_file, read_excel
from logger import get_logger

logger = get_logger()


def file_download_node(state: MyState) -> MyState:
    data_migration = state.get("task") or {}
    if isinstance(data_migration, DataMigration):
        if data_migration.data_source == "attachment_file":
            file_url = data_migration.file_url or ""
            if file_url.endswith(".xlsx"):
                logger.info(f"Downloading file from URL: {file_url}")
                try:
                    file_path = download_file(file_url)
                    logger.info(f"File downloaded to: {file_path}")
                    logger.info(f"Reading Excel file: {file_path}")
                    try:
                        data_migration.data = read_excel(file_path)
                        logger.info(
                            f"Extracted {len(data_migration.data)} records from Excel file."
                        )

                        return {
                            **state,
                            "task": data_migration,
                            "status": "file_loaded",
                        }

                    except Exception as e:
                        logger.error(f"Failed to read Excel file: {e}")
                except Exception as e:
                    logger.error(f"Failed to download file: {e}")
            else:
                logger.error("File URL is not an Excel file.")
        else:
            logger.error("Data source is not 'attachment_file'.")
    else:
        logger.error("No valid DataMigration task found in state.")

    return {**state, "status": "file_load_failed"}
