import os
from langchain_core.tools import tool
import questionary
import pandas as pd
from tabulate import tabulate
import json
from typing import Optional, Union, List
import numpy as np

@tool
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

@tool
def read_excel(file_path: str, sheet: Optional[Union[int, str]] = 0) -> List[dict]:
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
