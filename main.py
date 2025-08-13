from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langgraph.graph import END, START, StateGraph

from app_types import (
    DataMigration,
    GithubIssue,
    MyState,
)
from confirm import set_confirm_disable
from db import mongo, start_port_forward, stop_port_forward
from github_utils import get_issue
from logger import GraphLogger, get_logger
from nodes.bug_classification_node import make_bug_classification_node
from nodes.data_extraction_node import make_data_extraction_node
from nodes.data_migration_classification_node import (
    make_data_migration_classification_node,
)
from nodes.file_download_node import file_download_node
from nodes.operation_plan_fanout_node import make_operation_plan_fanout_node
from nodes.operation_plan_init_node import make_operation_plan_init_node
from nodes.operation_worker_node import make_operation_worker_node
from nodes.operations_detection_node import make_operation_detection_node
from nodes.task_classification_node import make_task_classification_node
from nodes.user_input_processing_node import make_user_input_processing_node
from nodes.operation_gather_node import operation_gather_node
from nodes.supplier_library_entry_deprecation_node import (
    make_supplier_library_entry_deprecation_node,
)

load_dotenv()
graphLogger = GraphLogger()
logger = get_logger()
fast_llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.0)
smart_llm = ChatOpenAI(model="gpt-4o", temperature=0.0)

set_confirm_disable(True)


def route_by_status(state: MyState) -> str:
    match state.get("status"):
        case "data_migration_detected":
            return "data_migration_classification_node"
        case "bug_detected":
            return "bug_classification_node"
        case "data_migration_classified":
            return "operation_detection_node"
        case "operations_detected":
            return "operation_plan_init_node"
    logger.error("Task is not recognized, routing to END.")
    return END


def route_by_data_source(state: MyState) -> str:
    task = state.get("task")
    if isinstance(task, DataMigration):
        if task.data_source == "attachment_file":
            return "file_download_node"
        elif task.data_source == "user_request":
            return "data_extraction_node"
    logger.error("Data source is not recognized, routing to END.")
    return END


operation_plan_fanout_node = make_operation_plan_fanout_node(smart_llm)


graph = StateGraph(MyState)
graph.add_node("user_input_processing_node", make_user_input_processing_node(fast_llm))
graph.add_node("task_classification_node", make_task_classification_node(fast_llm))
graph.add_node("bug_classification_node", make_bug_classification_node(fast_llm))
graph.add_node(
    "data_migration_classification_node",
    make_data_migration_classification_node(fast_llm),
)
graph.add_node("operation_detection_node", make_operation_detection_node(smart_llm))
graph.add_node("operation_plan_init_node", make_operation_plan_init_node(fast_llm))
graph.add_node(
    "supplier_library_entry_derprecation_node",
    make_supplier_library_entry_deprecation_node(fast_llm),
)
graph.add_node(
    "operation_plan_fanout_node",
    operation_plan_fanout_node,
)
graph.add_node(
    "operation_worker_node",
    make_operation_worker_node(fast_llm),
)
graph.add_node(
    "operation_gather_node",
    operation_gather_node,
)
graph.add_node("file_download_node", file_download_node)
graph.add_node("data_extraction_node", make_data_extraction_node(fast_llm))
graph.add_edge(START, "user_input_processing_node")
graph.add_edge("user_input_processing_node", "task_classification_node")
graph.add_conditional_edges(
    "task_classification_node",
    route_by_status,
    ["data_migration_classification_node", "bug_classification_node", END],
)
graph.add_edge("bug_classification_node", END)
graph.add_edge("data_migration_classification_node", "operation_detection_node")
graph.add_conditional_edges(
    "operation_detection_node",
    route_by_data_source,
    ["file_download_node", "data_extraction_node", END],
)

graph.add_edge("file_download_node", "operation_plan_init_node")
graph.add_edge("data_extraction_node", "operation_plan_init_node")
graph.add_conditional_edges("operation_plan_init_node", operation_plan_fanout_node)

graph.add_edge("operation_plan_fanout_node", "operation_worker_node")
graph.add_edge("operation_worker_node", "operation_gather_node")

graph.add_edge("operation_plan_fanout_node", "supplier_library_entry_derprecation_node")
graph.add_edge("supplier_library_entry_derprecation_node", "operation_gather_node")


def route_by_completion(state: MyState) -> str:
    operations = state.get("operations")
    if operations is None:
        return END
    total = operations.get("total") or 0
    done = operations.get("done") or 0
    if done >= total and total > 0:
        return END
    return "operation_gather_node"


graph.add_conditional_edges(
    "operation_gather_node",
    route_by_completion,
)
app = graph.compile()

one_deprecation = """
  Email Thread: VIRTUS AMS: Review Supplier Library for Mirage
  acquisition\nDate of report: 21.08.2025\nReporter: Kering Supply
  Chain\n\nReference environment: PROD Env\n**Bug description and current
  behavior:**\n\nWe need to:\na. Modify the \u201cSupplier Name\u201d of
  \u201cIT01527350126\u201d from \u201cMirage spa\u201d to \u201cMirage DO NOT
  USE\u201d\nb. Add the new supplier \u201cIT04092700121\u201d \u2013
  \u201cMIRAGE SRL\u201d\n\n| 0                                              |
  1                                         | 2                 | 3                     |
  4                            | 5                            | 6                      |
  7                  | 8                | 9                     | 10               |
  \n|:-----------------------------------------------|:------------------------------------------|
  :------------------|:----------------------|:-----------------------------|:-----------------------------|
  :-----------------------|:-------------------|:-----------------|:----------------------|
  :-----------------|\n| TO DO                                          |
  Supplier VAT Number / Registration Number | SAP Supplier Code | Supplier
  Country Code | Supplier Country Description | Supplier Name                |
  Semi Finished Supplier | Supplier Type      | Supplier Status  | Catalogue
  Uploaded By | Visibility Rules |\n| a. review the \"Supplier Name\" of
  IT01527350126 | IT01527350126                             | 100239            |
  IT                    | Italy                        | Mirage spa Mirage DO
  NOT USE | No                     | Frame Manufacturer | Not Active in BC |
  nan                   | No               |\n| b. Add a new supplier                          |
  IT04092700121                             | 107681            | IT                    |
  Italy                        | MIRAGE SRL                   | No                     |
  Frame Manufacturer | Not Active in BC | nan                   | No               |
  \n\n**Expected result:** update the supplier library ad detailed above.
  \n\n**Notes and/or comments:**\nThe supplier \u2018Mirage spa\u2019 has been
  acquired by a third party. As a result, the company name and VAT number have
  been changed respectively to \u2018Mirage SRL\u2019 and
  \u2018IT04092700121\u2019.\n\nWe therefore had to create a new master data
  record in SAP for \u2018Mirage SRL\u2019 and disable the previous code for
  \u2018Mirage spa\u2019.
  """

many_deprecations = """
  **Email Thread:** VIRTUS AMS: Review Supplier Library for LUMINA and ASTRA acquisitions  
**Date of report:** 04.09.2025  
**Reporter:** Kering Supply Chain  

**Reference environment:** PROD Env  

**Bug description and current behavior:**  

We need to:  
a. Modify the “Supplier Name” of “IT02133440987” from “Lumina s.p.a.” to “Lumina DO NOT USE”  
b. Add the new supplier “IT05876230411” – “LUMINA SRL”  

c. Modify the “Supplier Name” of “IT03398470122” from “Astra spa” to “Astra DO NOT USE”  
d. Add the new supplier “IT07845320991” – “ASTRA SRL”  

| 0   | 1 (Supplier VAT Number / Registration Number) | 2 (SAP Supplier Code) | 3 (Supplier Country Code) | 4 (Supplier Country Description) | 5 (Supplier Name) | 6 (Semi Finished Supplier) | 7 (Supplier Type) | 8 (Supplier Status) | 9 (Catalogue Uploaded By) | 10 (Visibility Rules) |  
|:---|:----------------------------------------------|:----------------------|:--------------------------|:--------------------------------|:------------------|:----------------------------|:------------------|:--------------------|:--------------------------|:---------------------|  
| a. review the "Supplier Name" of IT02133440987 | IT02133440987 | 109823 | IT | Italy | Lumina s.p.a. Lumina DO NOT USE | No | Frame Manufacturer | Not Active in BC | nan | No |  
| b. Add a new supplier | IT05876230411 | 112456 | IT | Italy | LUMINA SRL | No | Frame Manufacturer | Not Active in BC | nan | No |  
| c. review the "Supplier Name" of IT03398470122 | IT03398470122 | 110542 | IT | Italy | Astra spa Astra DO NOT USE | No | Frame Manufacturer | Not Active in BC | nan | No |  
| d. Add a new supplier | IT07845320991 | 114877 | IT | Italy | ASTRA SRL | No | Frame Manufacturer | Not Active in BC | nan | No |  

**Expected result:** update the supplier library as detailed above.  

**Notes and/or comments:**  
The suppliers *Lumina s.p.a.* and *Astra spa* have both been acquired by third parties. As a result, their company names and VAT numbers have changed respectively to *LUMINA SRL / IT05876230411* and *ASTRA SRL / IT07845320991*.  

We therefore had to create new master data records in SAP for *LUMINA SRL* and *ASTRA SRL*, and disable the previous codes for *Lumina s.p.a.* and *Astra spa*.  
"""

task_685 = {
    "number": 685,
    "title": "Update backend library Supplier",
    "body": """
Can you please add to the \u201cSupplier\u201d Library in all
  PROD/PREPROD env the supplier listed in the table below?\nDue date is the
  end of this week\n\nSupplier VAT Number / Registration Number | SAP Supplier
  Code | Supplier Country | Supplier Name | Semi Finished Supplier | Supplier
  Type Code | Catalogue Uploaded By | Note\n-- | -- | -- | -- | -- | -- | -- | --
  \n9144190007789468XY | \u00a0 | CN | Dongguan Shangpin Glass Products Co.,
  LTD | Yes | Component/Raw Material Supplier | None |
  \u00a0\n913502006120103000 | \u00a0 | CN | Xiamen Torch Special Metal
  Material Co., LTD | No | Component/Raw Material Supplier | None |
  \u00a0\n91440300MA5GLJP574 | \u00a0 | CN | Shenzhen Yushengxin Metal
  Material Co., LTD | No | Component/Raw Material Supplier | None |
  \u00a0\n913303042544926000 | \u00a0 | CN | Wenzhou Hengdeli Metal Materials
  Co., LTD | No | Component/Raw Material Supplier | None |
  \u00a0\n91440300MA5DPCFN0N | \u00a0 | CN | Shenzhen Pinxiang Yulong
  Precision Screw Co., LTD | No | Component/Raw Material Supplier | None |
  \u00a0\n91441900MA53B6PA6K | \u00a0 | CN | Dongguan Changsheng New Material
  Co., LTD | No | Component/Raw Material Supplier | None |
  \u00a0\n91440101MA59R4J03R | \u00a0 | CN | Guangzhou Lerun Composite
  Material Technology Co., LTD | No | Component/Raw Material Supplier | None |
  \u00a0\n91440300MA5FA5CG5W | \u00a0 | CN | Shenzhen Xinhe Xing Glass Co.,
  LTD | No | Component/Raw Material Supplier | None |
  \u00a0\n91441900MA4UHFRN5F | \u00a0 | CN | Dongguan Langfeng Glass Products
  Co., LTD | Yes | Component/Raw Material Supplier | None |
  \u00a0\n91440300359106426U | \u00a0 | CN | Shenzhen Jinaike Metal Material
  Technology Co., Ltd | Yes | Component/Raw Material Supplier | None |
  \u00a0\n91330302MA2AT8PM91 | \u00a0 | CN | Wenzhou Hengli Glasses Co., LTD |
  Yes | Component/Raw Material Supplier | None | \u00a0\n91440300MA5FLEAQ9L |
  \u00a0 | CN | Shenzhen Xingjing Feng Glass Co., LTD | Yes | Component/Raw
  Material Supplier | None | \u00a0\n91331021692399736W | \u00a0 | CN | Yuhuan
  Lula Glasses Co., LTD | Yes | Component/Raw Material Supplier | None |
  \u00a0\n91350200751619373G | \u00a0 | CN | Xiamen Jiyou New Material Co.,
  LTD | Yes | Component/Raw Material Supplier | None |
  \u00a0\n91310115607368434E | \u00a0 | CN | Toray International Trade (China)
  Co., Ltd | Yes | Component/Raw Material Supplier | None |
""",
}


if __name__ == "__main__":
    logger.info("Starting the application...")
    issue_num = 679  # 676 # 607
    #issue = get_issue(issue_num)
    issue = GithubIssue(**task_685)
    app.invoke(
        {"issue": issue, "status": "other"},
        config={"callbacks": [graphLogger], "recursion_limit": 30},
    )
