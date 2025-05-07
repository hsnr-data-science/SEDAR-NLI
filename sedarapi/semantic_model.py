from __future__ import annotations

from .commons import Commons
from .dataset import Dataset
from .attribute import Attribute
from .ontology import Annotation
from .semantic_mapping import SemanticMapping

from cache.cacheable import cacheable, exclude_from_cacheable

@cacheable
class SemanticModel:
    """
    This class provides methods to create and manage semantic models (OBDA) in the workspace.
    These models are created and modified using PLASMA.

    Attributes:
        connection (Commons): An instance of the Commons class representing the connection to the SEDAR API.
        workspace (str): The ID of the workspace where the semantic model is located.
        id (str): The ID of the semantic model.
        logger (Logger): The logger instance to log messages.
        content (dict): The JSON content of the ontology details.
        name (str): Name of the modeling
        description (str): Description of the modeling
        plasma_id (str): The ID of the model in PLASMA
        dataset_ids (list): List of dataset IDs used in the model
    """

    def __init__(self, connection: Commons, workspace_id: str, model_id: str):
        self.connection = connection
        self.workspace = workspace_id
        self.id = model_id
        self.logger = self.connection.logger
        self.content = self._get_modeling_json(self.workspace, self.id)

        # Extract some members from the "content" attribute
        self.name = self.content.get("name", "")
        self.description = self.content.get("description", "")
        self.plasma_id = self.content.get("plasma_id", "")
        self.dataset_ids = self.content.get("dataset_ids", [])
        # ...

    @exclude_from_cacheable
    def add_semantic_label_to_attribute(self, dataset: Dataset, attribute: Attribute, annotation: Annotation):
        """
        Adds a semantic label to an attribute in the dataset of the semantic model.

        Args:
            dataset (Dataset): The dataset where the attribute is located.
            attribute (Attribute): The attribute to annotate.
            annotation (Annotation): The annotation to add to the attribute.

        Returns:
            bool: True if the semantic label was added successfully, False otherwise.
        """
        self._add_semantic_label_to_attribute(dataset.id, attribute.name, annotation.string)
        return True
    
    def convert_into_mapping(self) -> SemanticMapping:
        """
        Converts the semantic model into a semantic mapping.
        The mapping can then be used for OBDA. We can execute SPARQL queries on the mapping.

        Returns:
            SemanticMapping: An instance of the SemanticMapping class representing the converted mapping.
        """
        response = self._convert_modeling_into_mapping()
        mapping_id = response.get("mapping", {}).get("id", "")
        return SemanticMapping(self.connection, self.workspace, mapping_id)
  
    def _get_modeling_json(self, workspace_id, modeling_id):
        resource_path = f"/api/v1/workspaces/{workspace_id}/modeling/plasma/{modeling_id}"

        response = self.connection._get_resource(resource_path)
        if response is None:
            raise Exception("Failed to fetch Modeling. Set the logger level to \"Error\" or below to get more detailed information.")
        
        return response
    
    def _add_semantic_label_to_attribute(self, dataset_id, attribute_name, annotation_uri):
        resource_path = f"/api/v1/workspaces/{self.workspace}/modeling/plasma/{self.id}/label"
        payload = {
            "dataset_id": dataset_id,
            "attribute_name": attribute_name,
            "annotation": annotation_uri
        }

        response = self.connection._post_resource(resource_path, payload)
        if response is None:
            raise Exception("Failed to add semantic label to attribute. Set the logger level to \"Error\" or below to get more detailed information.")

        self.logger.info(f"Semantic label added to attribute: {attribute_name} in dataset: {dataset_id}") 
        
        return response

    def _convert_modeling_into_mapping(self):
        resource_path = f"/api/v1/workspaces/{self.workspace}/modeling/serializer"
        payload = {
            "modeling_id": self.id,
            "plasma_id": self.plasma_id
        }

        response = self.connection._post_resource(resource_path, payload)
        if response is None:
             raise Exception("Failed to convert Modeling into Mapping. Set the logger level to \"Error\" or below to get more detailed information.")

        self.logger.info(f"Modeling converted into Mapping: {self.id}") 
        return response