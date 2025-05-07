from sedarapi.dataset import Dataset
from sedarapi.ontology import Ontology
from sedarapi.workspace import Workspace
from ..agent_graph_state import BaseState

class SemanticLabelingState(BaseState):
    current_dataset: Dataset
    current_dataset_index: int
    ontology: Ontology
    current_workspace: Workspace
    last_assigned_labels: dict[str, str]

def get_initial_semantic_labeling_state(ontology: Ontology, workspace: Workspace) -> SemanticLabelingState:
    return {
        'current_dataset': None,
        'current_dataset_index': 0,
        'ontology': ontology,
        'current_workspace': workspace,
        'last_assigned_labels': {}
    }