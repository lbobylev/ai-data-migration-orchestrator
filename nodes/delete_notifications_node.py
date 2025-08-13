from langchain_openai import ChatOpenAI
from app_types import MyState
from logger import get_logger
from db import start_port_forward, stop_port_forward, mongo, get_client

logger = get_logger(__name__)

llm = ChatOpenAI(model="gpt-4o-mini", temperature=0, top_p=1)


def delete_notifications_node(state: MyState) -> MyState:
    logger.info("Starting delete notifications node.")

    envs = state.get("environments")


    for env in envs or []:
        logger.info(f"Processing environment: {env}")
        namespaces = ["shared"] if env == "dev" else ["shared", "kering"]
        for ns in namespaces:
            try:
                start_port_forward(env, ns)
                client = get_client()
                dbs = client.list_database_names() 
                for db_name in dbs:
                    try:
                        try:
                            notifications = mongo().db(db_name).collection("notification")
                        except Exception as e:
                            # logger.warning(f"Skipping db {db_name}, cannot access notifications collection: {e}")
                            continue
                        # logger.info(f"Deleting notifications in db {db_name}...")
                        cnt = notifications.delete_many({})
                        logger.info(f"Deleted {cnt} notifications in db {db_name}.")
                    except Exception as e:
                        logger.error(f"Failed to delete notifications in db {db_name}: {e}")
                        continue
            finally:
                stop_port_forward()
                

    logger.info("Finished delete notifications node.")

    return state
