from app_types import MyState
from file_utils import download_file, read_excel
from logger import get_logger

logger = get_logger()


def file_download_node(state: MyState) -> MyState:
    data_source = state.get("data_source")
    if data_source == "attachment_file":
        file_url = state.get("file_url") or ""
        if file_url.endswith(".xlsx"):
            logger.info(f"Downloading file from URL: {file_url}")
            try:
                file_path = download_file(file_url)
                logger.info(f"File downloaded to: {file_path}")
                logger.info(f"Reading Excel file: {file_path}")
                try:
                    data = read_excel(file_path)
                    logger.info(f"Extracted {len(data)} records from Excel file.")

                    return {
                        "status": "file_loaded",
                        "data": data,
                    }

                except Exception as e:
                    logger.error(f"Failed to read Excel file: {e}")
            except Exception as e:
                logger.error(f"Failed to download file: {e}")
        else:
            logger.error("File URL is not an Excel file.")
    else:
        logger.error("Data source is not 'attachment_file'.")

    return {**state, "status": "file_load_failed"}
