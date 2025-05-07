from __future__ import annotations

from .commons import Commons
from .ontology import Annotation

from cache.cacheable import cacheable

@cacheable
class Entity:
    """
    Represents an Entity of a Dataset in the SEDAR API. This class provides methods to interact with the entity.

    Attributes:
        connection (Commons): The connection object for the SEDAR API.
        workspace (str): The ID of the workspace containing the entity.
        dataset (str): The ID of the dataset containing the entity.
        id (str): The ID of the entity.
        logger (logging.Logger): The logger of the SedarAPI.
        content (dict): The content of the entity details
        internal_name (str): The internal name of the entity.
        name (str): The name of the entity.
        description (str): The description of the entity.
        count_of_rows (int): The count of rows in the entity.

    """


    def __init__(self, connection: Commons, workspace_id: str, dataset_id: str, entity_id: str):
        self.connection = connection
        self.workspace = workspace_id
        self.dataset = dataset_id
        self.id = entity_id
        self.logger = self.connection.logger
        self.content = self._get_entity_json(self.workspace, self.dataset, self.id)
        
        # extract some members from content
        self.internal_name = self.content["internalname"]
        self.name = self.content["displayName"]
        self.description = self.content["description"]
        self.count_of_rows = self.content["countOfRows"]

    def update(self, name: str, description: str, ) -> Entity:
        """
        Updates the details of the specified Entity of the Dataset.

        Args:
            name (str, optional): The new name of the entity of the dataset.
            description (str, optional): A description for the entity of the dataset.

        Returns:
            Entity: An instance of the Entity class representing the updated dataset. 
            The content of the dataset details can be accessed using the `.content` attribute or the class members.

        Raises:
            Exception: If there's an error during the update process.

        Description:
            This method updates the details of a specific dataset by sending a PUT request to the 
            '/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/entitities/{entity_id}' endpoint. 
            The entity parameters can be updated using the provided parameters.

        Notes:
            - The method requires appropriate permissions to update a dataset.
            - Parameters not provided will retain their original values.

        Example:
            ```python
            entity = dataset_instance.get_all_entities()[0]
            try:
                updated_entity = entity.update(name="New Entity Title", description="Updated Description")
                print(updated_entity.name, updated_entity.description)
            except Exception as e:
                print(e)
            ``` 
        """
        return Entity(self.connection, self.workspace, self.dataset, self._update_entity(self.workspace, self.dataset, self.id, name, description)["id"])
    

    def add_annotation(self, annotation: Annotation, description: str = None, key: str = None) -> dict:
        """
        Adds an annotation to the specified Entity of the Dataset.

        Args:
            annotation (Annotation): An instance of the Annotation class to be added to the entity.
            description (str, optional): A description for the annotation.
            key (str, optional): A key for the annotation.

        Returns:
            dict: The details of the newly added annotation.

        Raises:
            Exception: If there's an error during the annotation addition process.

        Description:
            This method adds an annotation to a specific entity by sending a PATCH request to the 
            '/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/entities/{entity_id}' endpoint

        Notes:
            - The method requires appropriate permissions to add an annotation to an entity.

        Example:
        ```python
        entity = dataset_instance.get_all_entities()[0]
        annotation = ontology_instance.get_all_annotations()[0]
        try:
            new_annotation = entity.add_annotation(annotation, description="This is a test annotation")
            print(new_annotation)
        except Exception as e:
            print(e)
        ```
        """
        return self._create_entity_annotation(self.workspace, self.dataset, self.id, description, key, annotation.string, annotation.graph_id)
    
    def remove_annotation(self, annotation_id: str) -> bool:
        """
        Removes an annotation from the specified Entity of the Dataset.

        Args:
            annotation_id (str): The ID of the annotation to be removed from the entity.

        Returns:
            bool: True if the annotation was successfully removed.

        Raises:
            Exception: If there's an error during the annotation removal process.

        Description:
            This method removes an annotation from a specific entity by sending a PATCH request to the 
            '/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/entities/{entity_id}' endpoint

        Notes:
            - The method requires appropriate permissions to remove an annotation from an entity.

        Example:
        ```python
        entity = dataset_instance.get_all_entities()[0]
        annotation = ontology_instance.get_all_annotations()[0]
        try:
            new_annotation = entity.add_annotation(annotation, description="This is a test annotation")
            print(new_annotation)
            was_removed = entity.remove_annotation(new_annotation["annotation_id"])
            print(was_removed)
        except Exception as e:
            print(e)
        ```
        """
        return self._remove_entity_annotation(self.workspace, self.dataset, self.id, annotation_id)

    def _get_entity_json(self, workspace_id, dataset_id, entity_id):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/entities/{entity_id}"

        response = self.connection._get_resource(resource_path)
        if response is None:
            raise Exception(f"Failed to fetch the Entity '{entity_id}' for Dataset '{dataset_id}'. Set the logger level to \"Error\" or below to get more detailed information.")

        return response
    
    def _update_entity(self, workspace_id, dataset_id, entity_id, name, description):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/entities/{entity_id}"
        payload =  {
            "name":None, 
            "description":None, 
        }
        
        # Get the original Entity
        dataset = self._get_entity_json(workspace_id, dataset_id, entity_id)

        # Reinstate the old values from the original Dataset
        for key in payload:
            payload[key] = dataset.get(key)

        # If a new value for a parameter is given to this method, assign it
        if name is not None:
            payload["name"] = name
        if description is not None:
            payload["description"] = description

        response = self.connection._put_resource(resource_path, payload)
        if response is None:
            raise Exception(f"The Entity '{entity_id}' for Dataset '{dataset_id}' could not be updated. Set the logger level to \"Error\" or below to get more detailed information.")

        self.logger.info(f"The Entity '{entity_id}' for Dataset was updated successfully.")
        return response
    
    def _create_entity_annotation(self, workspace_id, dataset_id, entity_id, description, key, annotation, ontology_id):
        resource_path=f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/entities/{entity_id}"
        payload = {
            "description": description,
            "key": key,
            "annotation_id": None, # This is not the ID of the annotation to be added, but the id of an entity-annotation itself. used to remove existing annotations from entities
            "annotation": annotation,
            "ontology_id": ontology_id
        }

        response = self.connection._patch_resource(resource_path, payload)
        if response is None:
            raise Exception(f"The Entity Annotation '{annotation}' for the Entity '{entity_id}' could not be added. Set the logger level to \"Error\" or below to get more detailed information.")

        self.logger.info(f"The Entity Annotation '{annotation}' for the Entity '{entity_id}' was added successfully.")
        return response

    def _remove_entity_annotation(self, workspace_id, dataset_id, entity_id, annotation_id):
        resource_path=f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/entities/{entity_id}"
        payload = {
            "description": None,
            "key": None,
            "annotation_id": annotation_id,
            "annotation": None,
            "ontology_id": None
        }

        response = self.connection._patch_resource(resource_path, payload)
        if response is None:
            raise Exception(f"The Entity Annotation '{annotation_id}' for the Entity '{entity_id}' could not be removed. Set the logger level to \"Error\" or below to get more detailed information.")

        self.logger.info(f"The Entity Annotation '{annotation_id}' for the Entity '{entity_id}' was removed successfully.")
        return True