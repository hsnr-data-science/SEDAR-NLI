from __future__ import annotations

from .commons import Commons
from .ontology import Ontology, Annotation

from cache.cacheable import cacheable

@cacheable
class Attribute:
    """
    Represents an attribute/column/property of a dataset in the workspace. Contains methods to manage the attribute.

    Attributes:
        connection (Commons): The connection object to the SEDAR API.
        workspace (str): The ID of the workspace containing the dataset.
        dataset (str): The ID of the dataset containing the attribute.
        id (str): The ID of the attribute.
        name (str): The name of the attribute.
        data_type (str): The data type of the attribute.
        is_pk (bool): Indicates if the attribute is a primary key.
        is_fk (bool): Indicates if the attribute is a foreign key.
        content (dict): The JSON content of the attribute.
    """

    def __init__(self, connection: Commons, workspace_id: str, dataset_id: str, attribute_id: str):
        self.connection = connection
        self.workspace = workspace_id
        self.dataset = dataset_id
        self.id = attribute_id
        self.logger = self.connection.logger
        self.content = self._get_schema_attribute_json(self.workspace, self.dataset, self.id)

        # Extract some members from the "content" attribute
        self.name = self.content["name"]
        self.data_type = self.content["dataType"]
        self.is_pk = self.content["isPk"]
        self.is_fk = self.content["isFk"]
        # ...

    def update(self, description:str = None, datatype:str = None, is_pk: bool = None, is_fk: bool = None, contains_PII: bool = None) -> Attribute:
        """
        Updates the properties of the attribute of a dataset.

        Args:
            description (str, optional): Description of the attribute.
            datatype (str, optional): Data type of the attribute.
            is_pk (bool, optional): Indicates if the attribute is a primary key.
            is_fk (bool, optional): Indicates if the attribute is a foreign key.
            contains_PII (bool, optional): Indicates if the attribute contains personally identifiable information.

        Returns:
            Attribute: A new instance of the Attribute class, reflecting the updated properties.

        Raises:
            Exception: If the attribute update operation fails.

        Description:
            This method allows users to modify properties of an attribute in the dataset. 
            The provided parameters will replace the existing values for the attribute. If a parameter is not provided, the existing value will remain unchanged.

        Note:
            - Ensure that the provided datatype is valid and consistent with the actual data in the dataset.
        
        Example:
        ```python
        attribute = dataset.get_all_attributes()[0]
        updated_attribute = attribute.update(description="Updated description")
        print(updated_attribute.description)
        ```
        """
        return Attribute(self.connection, self.workspace, self.dataset, self._update_schema_attribute(self.workspace, self.dataset, self.id, description, datatype, is_pk, is_fk, contains_PII)["id"])

    def delete(self) -> bool:
        """
        Deletes the current attribute of a dataset.

        Returns:
            bool: True if the attribute deletion was successful.

        Raises:
            Exception: If the attribute deletion operation fails.

        Description:
            This method deletes the specified attribute from the dataset.
        
        Note:
            - Deleting an attribute is a permanent operation and cannot be undone. Ensure that you have backups or have considered the implications before proceeding.

        Example:
        ```python
        attribute = dataset.get_all_attributes()[0]
        success = attribute.delete()
        if success:
            print("Attribute deleted successfully.")
        else:
            print("Failed to delete the attribute.")
        ```
        """
        return self._delete_notebook(self.dataset.workspace.id,self.dataset.id, self.id)
    
    def annotate(self, ontology: Ontology, annotation: Annotation) -> dict:
        """
        Annotates the attribute with the entity or annotation from the specified ontology.

        Args:
            ontology (Ontology): Ontology for the annotation.
            annotation (Annotation): Annotation for the attribute.

        Returns:
            Dict: A dictionary with the content of the server response representing the annotation. It contains the ID of the annotation, the instance, the description, the key and the ontology.

        Raises:
            Exception: If the annotation operation fails.

        Example:
        ```python
        ontology = workspace.get_all_ontologies()[0]
        annotations = default_ontology.get_all_annotations()
        for annotation in annotations:
            if annotation.title == "Person":
                anon = annotation
        attribute = dataset.get_all_attributes()[0]
        annotation = attribute.annotate(ontology, anon)
        ```
        """
        if ontology.graph_id != annotation.graph_id:
            raise Exception(f"The passed Annotation {annotation.title} does not belong to the passed Ontology '{ontology.title}'. Please pass an Annotation that belongs to the passed Ontology.")

        return self._annotate_attribute(self.workspace, self.dataset, self.id, ontology.id, annotation.string)


    def create_foreign_key_construct(self, ontology: "Ontology", annotation: "Annotation", fk_dataset: "Dataset" = None, fk_attribute: "Attribute" = None, set_pk: bool = False) -> dict:
        """
        Creates an annotation or foreign key construct for the specified attribute of the dataset.

        Args:
            ontology (Ontology): Ontology for the fk construct.
            annotation (Annotation): Annotation for the fk construct.
            fk_dataset (Dataset, optional): Dataset for the fk construct.
            fk_attribute (Attribute, optional): Specific Attribute from the fk_dataset for the fk construct.
            set_pk (bool, optional): Define weather the pk should be set or not. Defaults to False.

        Returns:
            Dict: A dictionary with the content of the server response representing the fk construct. It contains the ID of the fk construct, the instance, the description, the key and the ontology.

        Notes:
            - If no fk_dataset and no fk_attribute is given, there will just be created an annotation for the attribute
            - If you pass a fk_dataset you also need to pass a fk_attribute

        Raises:
            Exception: If the creation of the fk construct fails.

        Example:
        ```python
        ontology = workspace.get_all_ontologies()[0]
        annotations = default_ontology.get_all_annotations()
        for annotation in annotations:
            if annotation.title == "Person":
                anon = annotation
        attribute = dataset.get_all_attributes()[0]
        fk_attribute = dataset2.get_all_attributes()[0]
        attribute.create_foreign_key_construct(ontology, anon, dataset2, fk_attribute)
        ```
        """
        # If no Foreign Key Dataset is given, just annotate the attribute
        if fk_dataset is None:
            response = self._manage_foreign_key_for_attribute(self.workspace, self.dataset, self.id, "", annotation.string, ontology.id, None, None, set_pk)
        
        # If we get a Foreign Dataset we create the fk_construct.
        if fk_dataset is not None and fk_attribute is not None:
            response = self._manage_foreign_key_for_attribute(self.workspace, self.dataset, self.id, "", annotation.string, ontology.id, fk_dataset.id, fk_attribute.id, set_pk)
        
        # If we get only one of fk_dataset and fk_attribute 
        else:
            self.logger.error(f"If a fk_dataset is passed to create_foreign_key_construct, also the correpsonding fk_attribute needs to be passed.")
        
        return response
    
    def remove_foreign_key_construct(self, annotation_id: str) -> bool:
        """
        Deletes an annotation or foreign key construct for the specified attribute of the dataset.

        Args:
            annotation_id (str): The ID of the attribute annotation that is to be deleted

        Returns:
            bool: True if the fk construct has successfully been removed.

        Raises:
            Exception: If the creation of the fk construct fails.
        
        Example:
        ```python
        fk_construct = create_foreign_key_construct(ontology, anon, dataset2, fk_attribute)
        id = fk_construct["id"]
        attribute.remove_foreign_key_construct(id)
        """
        response = self._manage_foreign_key_for_attribute(self.workspace, self.dataset, self.id, annotation_id, None, None, None, None, False)
        return True
    
    def _get_all_schema_attributes_json(self, workspace_id, dataset_id):
        # There is no serverside implementation for a "get_all"-Call for Attributes
        # Till then, we just extract the attributes from the answear of the "get_dataset" call
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}"
        
        response = self.connection._get_resource(resource_path)
        if response is None:
            raise Exception("Dataset Attributes could not be retrieved. Set the logger level to \"Error\" or below to get more detailed information.")
        
        # Right now, we always go for the first "entities" entry. If there exist multiple
        # ones, a correct way to select the desired one would need to be implemented.
        attributes = response["schema"]["entities"][0]["attributes"]

        return attributes
    
    def _get_schema_attribute_json(self, workspace_id, dataset_id, attribute_id):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/attributes/{attribute_id}"
        
        response = self.connection._get_resource(resource_path)
        if response is None:
            raise Exception(f"Dataset Attribute '{attribute_id}' could not be retrieved. Set the logger level to \"Error\" or below to get more detailed information.")
        
        return response

    def _update_schema_attribute(self, workspace_id, dataset_id, attribute_id, description, datatype, is_pk, is_fk, contains_PII):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/attributes/{attribute_id}"
        # Define the accepted parameters by the api here.
        payload =  {
            "description": None,
            "datatype": None,
            "is_pk": None,
            "is_fk": None,
            "contains_PII": None
        }

        # A mapping is needed, since the API-Parameters differ from the parameter names of the attribute itself
        mapping = {
            "description": "description",
            "datatype": "dataType",
            "is_pk": "isPk",
            "is_fk": "isFk",
            "contains_PII": "containsPII"
        }
        
        # Get the original Attribute
        attribute = self._get_schema_attribute_json(workspace_id,dataset_id,attribute_id)

        # Reinstate the old values from the original Dataset
        for payload_key, attribute_key in mapping.items():
            payload[payload_key] = attribute.get(attribute_key, payload[payload_key])

        # If a new value for a parameter is given to this method, assign it
        if description is not None:
            payload["description"] = description
        if datatype is not None:
            payload["datatype"] = datatype
        if is_pk is not None:
            payload["is_pk"] = is_pk
        if is_fk is not None:
            payload["is_fk"] = is_fk
        if contains_PII is not None:
            payload["contains_PII"] = contains_PII

        response = self.connection._put_resource(resource_path, payload)
        if response is None:
            raise Exception(f"The Schema Attribute '{attribute_id}' could not be updated. Set the logger level to \"Error\" or below to get more detailed information.")

        self.logger.info(f"The Schema Attribute '{attribute_id}' was updated successfully.")
        return response

    def _manage_foreign_key_for_attribute(self, workspace_id, dataset_id, attribute_id, annotation_id, annotation, ontology_id, id_of_fk_dataset, id_of_fk_attribute, set_pk):
        resource_path=f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/attributes/{attribute_id}"
        payload = {
            "annotation_id": annotation_id,
            "annotation": annotation,
            "ontology_id": ontology_id,
            "id_of_fk_dataset": id_of_fk_dataset,
            "id_of_fk_attribute": id_of_fk_attribute,
            "set_pk": set_pk
        }

        response = self.connection._patch_resource(resource_path, payload)
        if response is None:
            raise Exception(f"Could not update the foreign key construct for Dataset '{dataset_id}' . Set the logger level to \"Error\" or below to get more detailed information.")

        self.logger.info(f"The foreign key construct for Dataset '{dataset_id}' has been updated successfully.")
        return response

    def _annotate_attribute(self, workspace_id, dataset_id, attribute_id, ontology_id, annotation):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/attributes/{attribute_id}"
        payload = {
            "annotation_id": "",
            "annotation": annotation,
            "ontology_id": ontology_id,
            "id_of_fk_dataset": None,
            "id_of_fk_attribute": None,
            "set_pk": False
        }

        response = self.connection._patch_resource(resource_path, payload)
        if response is None:
            raise Exception(f"Could not annotate the attribute '{attribute_id}' of Dataset '{dataset_id}'. Set the logger level to \"Error\" or below to get more detailed information.")
        
        self.logger.info(f"The attribute '{attribute_id}' of Dataset '{dataset_id}' has been annotated successfully.")
        return response
