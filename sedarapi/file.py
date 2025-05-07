from __future__ import annotations

from .commons import Commons
from .ontology import Annotation

from cache.cacheable import cacheable

# TODO: At the moment this class is not used since the get_all_files method in the dataset class needs to be fixed first
@cacheable
class File:
    """
    Represents a file in a dataset. Contains methods to update the file details and download the file.

    Attributes:
        connection (Commons): An instance of the Commons class.
        workspace (str): The workspace ID.
        dataset (str): The dataset ID.
        id (str): The file ID.
        logger (Logger): The logger instance.
        name (str): The name of the file.
        description (str): The description of the file.
        size (int): The size of the file in bytes.
        content (dict): The content of the file details.
    """

    def __init__(self, connection: Commons, workspace_id: str, dataset_id: str, file_id: str):
        self.connection = connection
        self.workspace = workspace_id
        self.dataset = dataset_id
        self.id = file_id
        self.logger = self.connection.logger
        self.content = self._get_file_json(self.workspace, self.dataset, self.id)
        
        # extract some members from content
        self.name = self.content["filename"]
        self.description = self.content["description"]
        self.size = self.content["sizeInBytes"]

    def update(self, description: str) -> File:
        """
        Updates the details of the specified File of the Dataset.

        Args:
            description (str, optional): A description for the file of the dataset.

        Returns:
            file: An instance of the file class representing the updated dataset. 
            The content of the dataset details can be accessed using the `.content` attribute or the class members.

        Raises:
            Exception: If there's an error during the update process.

        Description:
            This method updates the details of a specific dataset by sending a PUT request to the 
            '/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/entitities/{file_id}' endpoint. 
            The file parameters can be updated using the provided parameters.

        Notes:
            - The method requires appropriate permissions to update a dataset.
            - Parameters not provided will retain their original values.

        Example:
            ```python
            file = dataset_instance.get_all_entities()[0]
            try:
                updated_file = file.update(name="New file Title", description="Updated Description")
                print(updated_file.name, updated_file.description)
            except Exception as e:
                print(e)
            ``` 
        """
        return File(self.connection, self.workspace, self.dataset, self._update_file(self.workspace, self.dataset, self.id, description)["id"])

    def add_annotation(self, annotation: Annotation, description: str = None, key: str = None) -> dict:
        return self._create_file_annotation(self.workspace, self.dataset, self.id, description, key, annotation.string, annotation.graph_id)
    
    def remove_annotation(self, annotation_id: str) -> bool:
        return self._remove_file_annotation(self.workspace, self.dataset, self.id, annotation_id)


    def download(self, file_path: str = "./") -> bool:
        """
        Downloads the file and saves it to the specified file path.

        Args:
            file_path (str): The path where the ontology should be saved. By default the file is saved in the current directory. 

        Returns:
            bool: `True` if the file was successfully downloaded and saved.

        Raises:
            Exception: If there's a failure in downloading or saving the file.

        Description:
            This method downloads the file from the server using the API endpoint `/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/files/{file_id}` 
            and saves it to the specified file path.

        Example:
            ```python
            ontology_instance = workspace.get_all_ontologies()[0]
            try:
                success = ontology_instance.download("/path/to/save/")
                if success:
                    print("Ontology downloaded and saved successfully!")
                else:
                    print("Failed to download and save ontology.")
            except Exception as e:
                print(f"Error: {e}")
            ```

        """
        return self._download_file(self.workspace, self.dataset, self.id, self.name, file_path)

    def _get_file_json(self, workspace_id, dataset_id, file_id):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/files/{file_id}"
        payload = {
            "is_download": False # If false, the file information as json is returned. If true, the file itself.
        }

        response = self.connection._get_resource(resource_path, payload)
        if response is None:
            raise Exception(f"Failed to fetch the File '{file_id}' for Dataset '{dataset_id}'. Set the logger level to \"Error\" or below to get more detailed information.")

        return response
    
    def _update_file(self, workspace_id, dataset_id, file_id, description):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/files/{file_id}"
        payload =  {
            "description":None, 
        }
        
        # Get the original file
        dataset = self._get_file_json(workspace_id, dataset_id, file_id)

        # Reinstate the old values from the original Dataset
        for key in payload:
            payload[key] = dataset.get(key)

        # If a new value for a parameter is given to this method, assign it
        if description is not None:
            payload["description"] = description

        response = self.connection._put_resource(resource_path, payload)
        if response is None:
            raise Exception(f"The File '{file_id}' for Dataset '{dataset_id}' could not be updated. Set the logger level to \"Error\" or below to get more detailed information.")

        self.logger.info(f"The File '{file_id}' for Dataset was updated successfully.")
        return response
    
    def _create_file_annotation(self, workspace_id, dataset_id, file_id, description, key, annotation, ontology_id):
        resource_path=f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/files/{file_id}"
        payload = {
            "description": description,
            "key": key,
            "annotation_id": None, # This is not the ID of the annotation to be added, but the id of an file-annotation itself. used to remove existing annotations from entities
            "annotation": annotation,
            "ontology_id": ontology_id
        }

        response = self.connection._patch_resource(resource_path, payload)
        if response is None:
            raise Exception(f"The File Annotation '{annotation}' for the file '{file_id}' could not be added. Set the logger level to \"Error\" or below to get more detailed information.")

        self.logger.info(f"The File Annotation '{annotation}' for the file '{file_id}' was added successfully.")
        return response

    def _remove_file_annotation(self, workspace_id, dataset_id, file_id, annotation_id):
        resource_path=f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/files/{file_id}"
        payload = {
            "description": None,
            "key": None,
            "annotation_id": annotation_id,
            "annotation": None,
            "ontology_id": None
        }

        response = self.connection._patch_resource(resource_path, payload)
        if response is None:
            raise Exception(f"The file Annotation '{annotation_id}' for the file '{file_id}' could not be removed. Set the logger level to \"Error\" or below to get more detailed information.")

        self.logger.info(f"The file Annotation '{annotation_id}' for the file '{file_id}' was removed successfully.")
        return True
    
    def _download_file(self, workspace_id, dataset_id, file_id, file_name, file_path):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/files/{file_id}"

        response = self.connection._get_resource(resource_path)
        if response is None:
            raise Exception(f"Failed to download the file '{file_id}'. Set the logger level to \"Error\" or below to get more detailed information.")

        # Append the file-name itself to the path
        file_path += file_name

        # Open the file and write the contents into the specified file.
        with open(file_path, 'wb') as f:
            f.write(response)

        return True