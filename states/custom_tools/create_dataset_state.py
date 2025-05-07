from sedarapi.dataset import Dataset
from ..agent_graph_state import BaseState

class CreateDatasetState(BaseState):
    filename: str
    datasource_definition: dict
    results: Dataset

def get_initial_create_dataset_state(user_query: str, filename: str) -> CreateDatasetState:
    return {
        "user_query": user_query,
        "messages": [],
        "filename": filename,
        "datasource_definition": {},
        "results": None
    }