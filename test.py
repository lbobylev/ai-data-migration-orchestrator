import json

from langchain_openai import ChatOpenAI


from nodes.data_migration_classification_node import (
    make_data_migration_classification_node,
)
from nodes.task_classification_node import make_task_classification_node
from file_utils import read_excel, select_file
from utils import get_logger

smart_llm = ChatOpenAI(model="gpt-4o", temperature=0.0)
fast_llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.0)
logger = get_logger()

user_input = """
We’d need to change the mapping of the base materials listed in the attached file.

Expected result: substitute the “OLD Base Material KEYE Key” (column G in the attached file) with “NEW Base Material KEYE Key” (column K in the attached file).
We expect that all components that have one or more of the base materials mentioned in the attached file will be consequently updated.
"""


def dump(x):
    print(json.dumps(x, indent=4, ensure_ascii=False))


def load_data():
    file_path = select_file.invoke({})
    data = read_excel.invoke({"file_path": file_path})
    return data


def run_task_classification_node():
    return make_task_classification_node(logger, fast_llm)(
        {
            "user_prompt": user_input,
            "user_input": user_input,
            "status": "other",
            "task_data": None,
        }
    )


def run_data_migration_classification_node():
    prompt = """
Create new Eyewears. Update Base Matarials listed in the attached file.
Also some Acetates should be deleted.
"""
    return make_data_migration_classification_node(logger, smart_llm)(
        {
            "user_prompt": user_input,
            "user_input": user_input,
            "status": "data_migration_detected",
            "task_data": None,
        }
    )
