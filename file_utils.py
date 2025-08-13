import os
import requests
import questionary
import pandas as pd
from typing import Dict, Optional, Protocol, Union, List
from urllib.parse import urlparse

from logger import get_logger

logger = get_logger()

def select_file(start_path="/Users/leonid/Downloads") -> str | None:
    """Select a file from the specified directory. Use this when the user
    mentions an attached file, a file to pick or a spreadsheet to update."""
    files = [f for f in os.listdir(start_path) if os.path.isfile(os.path.join(start_path, f))]
    files = [f for f in files if f.endswith(('.xlsx', '.xls', '.csv'))]
    
    if not files:
        print("No files found in", start_path)
        return None

    if (len(files) == 1):
        print("Only one file found:", files[0])
        return os.path.join(start_path, files[0])
    
    file_name = questionary.select(
        "Select file:",
        choices=files
    ).ask()
    
    if file_name:
        return os.path.join(start_path, file_name)
    return None

def read_excel(file_path: str, sheet: Optional[Union[int, str]] = 0) -> List[Dict[str, str]]:
    """
    Convert an Excel file to a list of dictionaries.

    Parameters:
        file_path: Path to the .xlsx/.xls file.
        sheet: Sheet selector (int or str) for the desired sheet.
               Default is 0 (first sheet).

    Returns:
        A list of dictionaries representing the rows in the Excel sheet.
    """
    # Read the specified sheet from the Excel file
    df = pd.read_excel(file_path, sheet_name=sheet)

    # Convert DataFrame to a list of dictionaries
    data_list = df.to_dict(orient='records')

    return data_list

class DownloadFile(Protocol):
    def __call__(self, url: str, dest: Optional[str] = None) -> str:
        ...

def download_file(url, dest=None) -> str:
    # if dest and os.path.exists(dest):
    #     logger.info(f"File {dest} already exists, skipping download.")
    #     return dest
    cookie_str = os.getenv("GITHUB_COOKIE", "")
    cookies = dict(item.split("=", 1) for item in cookie_str.split("; "))
    if dest is None:
        dest = os.path.basename(urlparse(url).path) or "download.bin"

    response = requests.get(url, cookies=cookies, stream=True)
    response.raise_for_status()
    with open(dest, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    return dest
