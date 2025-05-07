from __future__ import annotations
import os
import json
from typing import Union

from .commons import Commons
from .tag import Tag
from .notebook import Notebook
from .user import User
from .ontology import Ontology
from .ontology import Annotation
from .attribute import Attribute
from .entity import Entity
from .file import File
from .cleaning import DatasetCleaning

from cache.cacheable import cacheable

@cacheable
class Dataset:
    """
    A class to represent a dataset in SEDAR.
    This class provides methods to interact with the dataset, such as updating details, publishing, deleting, and more.

    Attributes:
        connection (Commons): An instance of the Commons class representing the connection to the SEDAR API.
        workspace (str): The ID of the workspace to which the dataset belongs.
        id (str): The ID of the dataset.
        logger (Logger): An instance of the Logger class for logging messages.
        title (str): The title of the dataset.
        description (str): A description of the dataset.
        is_public (bool): A boolean indicating whether the dataset is public or not.
        is_favorite (bool): A boolean indicating whether the dataset is marked as a favorite or not.
        author (str): The author of the dataset.
        longitude (str): The longitude of the dataset.
        latitude (str): The latitude of the dataset.
        license (str): The license details of the dataset.
        language (str): The language of the dataset.
        size_of_files (int): The size of the files associated with the dataset.
        columns (list[str]): A list of column names in the dataset.
        entity_count (int): The number of entities in the dataset.
        rows_count (int): The number of rows in the dataset.
    """
    def __init__(self, connection: Commons, workspace_id: str, dataset_id: str):
        self.connection = connection
        self.workspace = workspace_id
        self.id = dataset_id
        self.logger = self.connection.logger
        self.content = self._get_dataset_json(self.workspace, self.id)

        # Extract some members from the "content" attribute
        self.title = self.content["title"]
        self.description = self.content["description"]
        self.is_public = self.content["isPublic"]
        self.is_favorite = self.content["isFavorite"]
        self.author = self.content["author"]
        self.longitude = self.content["longitude"]
        self.latitude = self.content["latitude"]
        self.license = self.content["license"]
        self.language = self.content["language"]
        self.size_of_files = None
        self.columns = None
        self.entity_count = None
        self.rows_count = None
        self._extract_schema_info()
    
    def update(self, 
           title: str = None,
           description: str = None,
           author: str = None,
           longitude: str = None,
           latitude: str = None,
           range_start: str = None,
           range_end: str = None,
           license: str = None,
           language: str = None) -> Dataset:
        """
        Updates the details of the specified dataset.

        Args:
            title (str, optional): The new title of the dataset.
            description (str, optional): A description for the dataset.
            author (str, optional): The author of the dataset.
            longitude (str, optional): Longitude for geolocation data.
            latitude (str, optional): Latitude for geolocation data.
            range_start (str, optional): Start range for the dataset.
            range_end (str, optional): End range for the dataset.
            license (str, optional): License details for the dataset.
            language (str, optional): Language of the dataset.

        Returns:
            Dataset: An instance of the Dataset class representing the updated dataset. 
            The content of the dataset details can be accessed using the `.content` attribute.

        Raises:
            Exception: If there's an error during the update process.

        Description:
            This method updates the details of a specific dataset by sending a PUT request to the 
            '/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}' endpoint. 
            The dataset parameters can be updated using the provided parameters.

        Notes:
            - The method requires appropriate permissions to update a dataset.
            - Parameters not provided will retain their original values.

        Example:
            ```python
            dataset = workspace.get_dataset("dataset_id")
            try:
                updated_dataset = dataset.update(title="New Dataset Title", description="Updated Description")
                print(updated_dataset.content["title"], updated_dataset.content["description"])
            except Exception as e:
                print(e)
            ``` 
        """
        # raise error if all params are None
        if all(value is None for value in [title, description, author, longitude, latitude, range_start, range_end, license, language]):
            raise Exception("At least one parameter must be provided for the update operation.")
        
        return Dataset(self.connection, self.workspace, 
                       self._update_dataset(self.workspace, self.id, title, description, author, longitude, 
                                            latitude, range_start, range_end, license, language)["id"])
    
    def update_datasource(self, datasource_definition: dict, file_path: str) -> Dataset:
        """
        Updates the datasource of the specified dataset.

        Args:
            datasource_definition: The path to the datasource definition file or 
                                    a dictionary containing the datasource definition.
            file_path: The path to the datasource file

        Returns:
            Dataset: An instance of the Dataset class representing the updated dataset with the new datasource. 
            The content of the dataset details can be accessed using the `.content` attribute.

        Raises:
            Exception: If there's an error during the update process.

        Description:
            This method updates the datasource of a specific dataset by sending a PUT request to the 
            '/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/update-datasource' endpoint. 
            The dataset's datasource can be updated using the provided definition and file path.

        Notes:
            - The method requires appropriate permissions to update a dataset's datasource.
            - The new datasource will automatically be ingested
            - All opened files will be closed after the update process.

        Example:
            ```python
            dataset = workspace.get_dataset(dataset_id)
            try:
                updated_dataset = dataset.update_datasource("path_to_definition.json", path_to_datasource_file)
                print(updated_dataset.content["title"], updated_dataset.content["description"])
            except Exception as e:
                print(e)
            ```
        """
        self._update_datasource(self.workspace, self.id, datasource_definition, file_path)
        # Update the content of our dataset to avoid inconsistencies
        self.content = self._get_dataset_json(self.workspace, self.id)
        return self

    def publish(self, index:bool=False, with_thread:bool=True, profile:bool=False) -> bool:
        """
        Publishes the current dataset. This can only be done after ingesting the dataset.
        To publish a dataset, some tag has to be added to the dataset first.

        Args:
            index (bool, optional): If set to True, the dataset will be indexed. Defaults to False.
            with_thread (bool, optional): If set to True, the publishing will be threaded. Defaults to True.
            profile (bool, optional): If set to True, the dataset profiling will be started for the dataset. Defaults to False.

        Returns:
            bool: True if the dataset was successfully published

        Raises:
            Exception: If there's an error during the publish process.

        Description:
            This method publishes a dataset by sending a PATCH request to the '/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}' endpoint. 
            Optional parameters allow for indexing, threaded publishing, and dataset profiling.

        Notes:
            - Ensure that the dataset is in a state that can be published before calling this method.
            - The method requires appropriate permissions to publish a dataset.

        Example:
        ```python
        dataset = workspace.get_all_datasets()[0]
        try:
            is_published = dataset.publish()
            if is_published is True:
                print("Dataset published successfully.")
        except Exception as e:
            print(e)
        ```
        """
        return self._publish_dataset(self.workspace, self.id, index, with_thread, profile)

    def delete(self) -> bool:
        """
        Deletes the current dataset.

        Returns:
            bool: True if the dataset was successfully deleted

        Raises:
            Exception: If there's an error during the deletion process.

        Description:
            This method deletes a specific dataset by sending a DELETE request to the '/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}' endpoint.

        Notes:
            - Ensure that you have the required permissions to delete the dataset.
            - Once deleted, the dataset cannot be recovered.

        Example:
        ```python
        dataset = workspace.get_all_datasets()[0]
        try:
            success = dataset.delete()
            if success:
                print("Dataset deleted successfully.")
        except Exception as e:
            print(e)
        ```

        """
        return self._delete_dataset(self.workspace, self.id)
    
    def ingest(self) -> dict:
        """
        Initiates the ingestion process for the current dataset. Ingestion can only happen after a dataset has been created and before publishing a dataset.

        Returns:
            dict: A dictionary containing details about the ingestion process.

        Raises:
            Exception: If there's an error with starting the ingestion process

        Description:
            This method triggers the ingestion process for a specific dataset by sending a 
            GET request to the '/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/run-ingestion' endpoint. 

        Notes:
            - Ensure that you have the required permissions to start the ingestion process.
            - The ingestion process might take some time to complete, depending on the size and complexity of the dataset. 

        Example:
        ```python
        dataset = workspace.get_all_datasets()[0]
        try:
            ingestion_info = dataset.ingest()
            print(ingestion_info["currentRevision"])
        except Exception as e:
            print(e)
        ```
        """
        return self._ingest_dataset(self.workspace,self.id)
    
    def start_profiling(self, dataset_version:str="CURRENT REVISION") -> bool:
        """
        Initiates the profiling process for a specific version of the current dataset.

        Args:
            dataset_version (str, optional): The version of the dataset to be profiled. If set to "CURRENT REVISION", the current revision of the dataset will be profiled. 
            Defaults to "CURRENT REVISION".

        Returns:
            bool: True if the profiling process was started successfully, False otherwise.

        Raises:
            Exception: If there's an error with starting the profiling process

        Description:
            This method triggers the profiling process for a specific version of the dataset by sending a GET request to the '/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/profiling' endpoint. 

        Notes:
            - Ensure that you have the required permissions to start the profiling process.
            - The profiling process might take some time to complete, depending on the size and complexity of the dataset.

        Example:
        ```python
        dataset = workspace.get_all_datasets()[0]
        try:
            success = dataset.start_profiling(dataset_version="1")
            if success:
                print("Profiling started successfully.")
            else:
                print("Failed to start profiling.")
        except Exception as e:
            print(e)
        ```
        """
        # If the user does not specify a revision, the currentRevision is profiled.
        if dataset_version == "CURRENT REVISION":
            dataset_version = self.content["datasource"]["currentRevision"]
        return self._start_dataset_profiling(self.workspace,self.id,dataset_version)

    def get_preview_json(self) -> str:
        """
        Retrieves the preview of the dataset in JSON format.
        This can be very useful to get a quick overview of the dataset.

        Returns:
            str: The preview of the dataset in JSON format.

        Raises:
            Exception: If there's an error while fetching the dataset preview.

        Description:
            This method fetches the preview of the dataset in JSON format by sending a GET request to the '/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/preview' endpoint.

        Notes:
            - This endpoint may take some time to respond, depending on the size of the dataset.

        Example:
        ```python
        dataset = workspace.get_all_datasets()[0]
        try:
            preview = dataset.get_preview_json()
            print(preview)
        except Exception as e:
            print(e)
        ```
        """
        return self._get_dataset_preview_json(self.workspace, self.id)

    def get_tags(self) -> list[Tag]:
        """
        Retrieves all tags associated with the current dataset.

        Returns:
            list[Tag]: A list of Tag objects representing all tags associated with the dataset.
            The content of each tag can be accessed using the `.content` attribute.
            The content could for example look like this: {"id": "abc", "annotation"}
            {'id': '9ff3050a50074e46a64d0364ef930061', 'instance': '<http://www.w3.org/ns/dcat#Dataset>', 'description': None, 'key': None, 'ontology': {'title': 'DCAT3'}}
        Raises:
            Exception: If there's an error while fetching the tags attached to the dataset.

        Description:
            This method fetches all tags associated with the dataset by sending a GET request to the '/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/tags' endpoint. Each tag is represented as an instance of the Tag class.

        Notes:
            - Ensure that you have the required permissions to view tags of the dataset.
            - The returned Tag objects contain various details about the tags, including associated annotations and links.

        Example:
        ```python
        dataset = workspace.get_all_datasets()[0]
        try:
            tags = dataset.get_tags()
            for tag in tags:
                print(tag.content["title"])
        except Exception as e:
            print(e)
        ```

        """
        tags_info = self._get_all_tags(self.workspace, self.id)
        return [Tag(self.connection, self.workspace, self.id, tag_info["id"]) for tag_info in tags_info]
    
    def add_tag(self, ontology: Ontology, annotation: Annotation) -> Tag:
        """
        Adds a tag to the dataset. At least one tag should be added to a dataset before publishing it.
        Search ontologies inside a workspace using the ontology_annotation_search method.

        Args:
            ontology (Ontology): An instance of the Ontology class.
            annotation (str): The annotation string associated with the tag.

        Returns:
            Tag: An instance of the Tag class representing the newly added tag.
            The content of the tag can be accessed using the `.content` attribute.

        Raises:
            Exception: If there's an error during the tag addition process
        
        Description:
            This method adds a new tag to the dataset by sending a POST request to the '/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/tags' endpoint.

        Example:
        ```python
        dataset = workspace.get_all_datasets()[0]
        ontology = workspace.get_all_ontologies()[0]
        annotation = workspace.ontology_annotation_search("Test")[0]
        try:
            new_tag = dataset.add_tag(ontology, annotation)
            print(new_tag.content)
        except Exception as e:
            print(e)
        ```
        """
        # Check if the passed annotation is a part of the passed ontology.
        if ontology.graph_id != annotation.graph_id:
            raise Exception(f"The passed Annotation {annotation.title} does not belong to the passed Ontology '{ontology.title}'. Please pass an Annotation that belongs to the passed Ontology.")

        return Tag(self.connection, self.workspace, self.id, self._add_tag(self.workspace, self.id, annotation.string, ontology.id)["id"])
    
    def get_notebooks(self) -> list[Notebook]:
        """
        Retrieves all notebooks associated with the dataset. These notebooks often represent ML experiments or runs.

        Args:
            None

        Returns:
            list[Notebook]: A list of Notebook instances representing the notebooks associated with the dataset.
            The content of each Notebook can be accessed using the `.content` attribute.

        Raises:
            Exception: If there's an error during the notebook retrieval process.

        Description:
            This method fetches all notebooks associated with a specific dataset. It sends a GET request to the '/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/notebooks' endpoint to retrieve the data.

        Example:
        ```python
        dataset = workspace.get_all_datasets()[0]
        try:
            notebooks = dataset.get_notebooks()
            for notebook in notebooks:
                print(notebook.content)
        except Exception as e:
            print(e)
        """
        notebooks_info = self._get_all_notebooks(self.workspace, self.id)
        return [Notebook(self.connection, self.workspace, self.id, notebook_info["id"]) for notebook_info in notebooks_info]
    
    def add_notebook(self, title: str, description: str, type: str = "JUPYTER", is_public: bool = True, version: str = "LATEST") -> Notebook:
        """
        Adds a notebook to the dataset.

        Args:
            title (str): The title of the notebook.
            description (str): A description of the notebook.
            type (str): The type of the notebook (e.g., JUPYTER).
            is_public (bool): Specifies if the notebook is public or not.
            version (str): The revision of the dataset that the notebook is attached to.

        Returns:
            Notebook: An instance of the Notebook class representing the newly added notebook.
            The content of the Notebook can be accessed using the `.content` attribute.

        Raises:
            Exception: If there's an error during the notebook addition process.

        Description:
            This method adds a new notebook to the dataset by sending a POST request to the '/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/notebooks' endpoint.

        Example:
        ```python
        dataset = workspace.get_all_datasets()[0]
        try:
            new_notebook = dataset.add_notebook(title="Sample Notebook", description="Sample Description")
            print(new_notebook.content)
        except Exception as e:
            print(e)
        ```
    """
        return Notebook(self.connection, self.workspace, self.id, self._add_notebook(self.workspace, self.id, title, description, type, is_public, version, self.connection.jupyter_token)["id"])
    
    def get_cleaner(self, dataset_version:str="CURRENT REVISION"):
        """
        Gets an instance of a dataset cleaner. 

        Args:
            dataset_version (str, optional): The dataset version to perform the cleaning operatons on. 
                Defaults to the current revision of the datasource

        Returns:
            DatasetCleaner: An instance of the class to perform deequ dataset cleaning.

        Raises:
            Exception: None

        Example:
        ```python
        dataset = workspace.get_all_datasets()[0]
        cleaner = dataset.get_cleaner()
        ```
        """
        if dataset_version == "CURRENT REVISION":
            dataset_version = self.content["datasource"]["currentRevision"]

        return DatasetCleaning(self.connection, self.workspace, self.id, dataset_version)
    
    def get_all_attributes(self) -> list[Attribute]:
        """
        Retrieves all attributes (or columns, or properties) associated with the current dataset.

        Returns:
            list[Attribute]: A list of Attribute objects representing all attributes/columns/properties associated with the dataset.
            The content of each tag can be accessed using the `.content` attribute or it's members.

        Raises:
            Exception: If there's an error while fetching the attributes attached to the dataset.

        Description:
            This method fetches all attributes associated with the dataset by sending a GET request to the 
            '/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/attributes' endpoint. Each attribute is represented as an instance of the Attribute class.

        Notes:
            - Ensure that you have the required permissions to view the dataset
            - The returned Attribute objects contain various details about the attributes, including their type, name and corresponding annotations.

        Example:
        ```python
        dataset = workspace.get_all_datasets()[0]
        try:
            attributes = dataset.get_all_attributes()
            for attribute in attributes:
                print(attribute.name)
        except Exception as e:
            print(e)
        ```

        """
        attributes_info = self._get_all_schema_attributes_json(self.workspace, self.id)
        return [Attribute(self.connection, self.workspace, self.id, attribute_info["id"]) for attribute_info in attributes_info]
    
    def get_all_entities(self) -> list[Entity]:
        """
        Retrieves all entities associated with the current dataset.

        Returns:
            list[Entity]: A list of Entity objects representing all entities associated with the dataset.
            The content of each tag can be accessed using the `.content` entity or it's members.

        Raises:
            Exception: If there's an error while fetching the entities attached to the dataset.

        Description:
            This method fetches all entities associated with the dataset by sending a GET request to the 
            '/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/entities' endpoint. Each entity is represented as an instance of the Entity class.

        Notes:
            - Ensure that you have the required permissions to view the dataset
            - The returned Entity objects contain various details about the entities, including their name and corresponding annotations.

        Example:
        ```python
        dataset = workspace.get_all_datasets()[0]
        try:
            entities = dataset.get_all_attributes()
            for entity in entities:
                print(entity.name)
        except Exception as e:
            print(e)
        ```

        """
        entities_info = self._get_all_schema_entities_json(self.workspace, self.id)
        return [Entity(self.connection, self.workspace, self.id, entity_info["id"]) for entity_info in entities_info]

    def get_all_files(self) -> list[File]:
        """
        TODO: this does not work, currently only uses workspaces/{id}/datasets/{id}" endpoint which does not return ["schema"]["files"] keys anymore.
        TODO: Have to use a different endpoint to get the files
        Retrieves all files associated with the current unstructured dataset

        Returns:
            list[File]: A list of File objects representing all files associated with the dataset.
            The content of each tag can be accessed using the `.content` file or it's members.

        Raises:
            Exception: If there's an error while fetching the files attached to the dataset.

        Description:
            This method fetches all files associated with the dataset by sending a GET request to the 
            '/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/files' endpoint. Each file is represented as an instance of the File class.

        Notes:
            - This method will only work with unstructured datasets.
            - Ensure that you have the required permissions to view the dataset

        Example:
        ```python
        dataset = workspace.get_all_datasets()[0]
        try:
            files = dataset.get_all_attributes()
            for file in files:
                print(file.name)
        except Exception as e:
            print(e)
        ```

        """
        files_info = self._get_all_schema_files_json(self.workspace, self.id)
        return [File(self.connection, self.workspace, self.id, file_info["id"]) for file_info in files_info]
    
    def get_logs(self) -> list[str]:
        """
            Retrieves the logs associated with the dataset.

            Args:
                None

            Returns:
                list[str]: A list of formatted logs for the dataset. Each log is represented as a string detailing the changes, creation date, user, and description.

            Raises:
                Exception: If there's an error during the logs retrieval process.

            Description:
                This method fetches the logs associated with the dataset by sending a GET request to the '/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/logs' endpoint. 
                Each log entry provides details about the changes made, the date of the change, the user responsible for the change, and a description.

            Example:
            ```python
            dataset = workspace.get_all_datasets()[0]
            try:
                logs = dataset.get_logs()
                for log in logs:
                    print(log)
            except Exception as e:
                print(e)
            ```
        """
        return self._get_dataset_logs(self.workspace, self.id)
    
    def update_continuation_timer(self, timer: list[str]) -> bool:
        """
            Updates the continuation timer for the dataset.

            Args:
                timer (list[str]): A list of strings, each representing a duration (in milliseconds) to set for the continuation timer.

            Returns:
                bool: `True` if the continuation timer was successfully updated, `False` otherwise.

            Raises:
                Exception: If there's an error during the timer update process.

            Description:
                This method sets a continuation timer for the dataset by sending a PUT request to the '/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/timer' endpoint. 
                Each string in the timer list indicates a duration after which the dataset continuation should occur.

            Example:
            ```python
            dataset = workspace.get_all_datasets()[0]
            try:
                success = dataset.update_continuation_timer(["1000", "1500"])
                if success:
                    print("Timer updated successfully!")
                else:
                    print("Failed to update timer.")
            except Exception as e:
                print(e)
            ```
        """
        return self._update_continuation_timer(self.workspace,self.id,timer)
    
    def set_status_public(self) -> bool:
        """
            Sets the dataset's status to "public".

            Returns:
                bool: `True` if the dataset's status was successfully changed to "public", otherwise `False`.

            Raises:
                Exception: If there's an error during the status update process.

            Description:
                This method updates the visibility status of the dataset by sending a PUT request to the '/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/status' endpoint. 
                When the dataset is set to public, the API will respond with an empty answer. If the status was already public, no changes will be made.

            Example:
            ```python
            dataset = workspace.get_all_datasets()[0]
            try:
                success = dataset.set_status_public()
                if success:
                    print("Dataset status updated to public!")
                else:
                    print("Failed to update dataset status.")
            except Exception as e:
                print(e)
            ```
        """
        # For further explanation of the implementation, see the "_toggle_dataset_status"-Method of the main class
        if len(self._toggle_dataset_status(self.workspace,self.id)) != 0:
            self._toggle_dataset_status(self.workspace,self.id)
        return True
    
    def set_status_private(self) -> bool:
        """
            Sets the dataset's status to "private".

            Returns:
                bool: `True` if the dataset's status was successfully changed to "private", otherwise `False`.

            Raises:
                Exception: If there's an error during the status update process.

            Description:
                This method updates the visibility status of the dataset by sending a PUT request to the '/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/status' endpoint. 
                When the dataset is set to private, the API will respond with the current user. If the status was already private, no changes will be made.

            Example:
            ```python
            dataset = workspace.get_all_datasets()[0]
            try:
                success = dataset.set_status_private()
                if success:
                    print("Dataset status updated to private!")
                else:
                    print("Failed to update dataset status.")
            except Exception as e:
                print(e)
            ```
        """
        # For further explanation of the implementation, see the "_toggle_dataset_status"-Method of the main class
        if len(self._toggle_dataset_status(self.workspace, self.id)) == 0:
            self._toggle_dataset_status(self.workspace, self.id)
        return True
    
    def add_user_permission(self, user: User, can_read: bool = True, can_write: bool = True, can_delete: bool = True) -> dict:
        """
        Grants specific permissions to the specified user for this dataset.

        Args:
            user (User): An instance of the User class.
            can_read (bool, optional): Permission to read the dataset. Defaults to True.
            can_write (bool, optional): Permission to write or modify the dataset. Defaults to True.
            can_delete (bool, optional): Permission to delete the dataset. Defaults to True.

        Returns:
            dict: A dictionary containing the updated permissions for the user.

        Raises:
            Exception: If there's an error during the permission update process.

        Description:
            This method grants the specified permissions to the provided user for this dataset by sending a PUT request to the '/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/users' endpoint.

        Example:
        ```python
        dataset = workspace.get_all_datasets()[0]
        user = sedar.get_user("some_user")
        try:
            updated_permissions = dataset.add_user_permission(user, can_read=True, can_write=False, can_delete=False)
            print(updated_permissions)
        except Exception as e:
            print(e)
        ```
        """
        return self._edit_dataset_user_permissions(self.workspace, self.id, user.id, can_read, can_write, can_delete, add=True)
    
    def edit_user_permission(self, user: User, can_read: bool = None, can_write: bool = None, can_delete: bool = None) -> dict:
        """
        Modifies the permissions of a user for this dataset.

        Args:
            user (User): An instance of the User class.
            can_read (bool, optional): Permission to read the dataset. If not specified, the permission remains unchanged.
            can_write (bool, optional): Permission to write or modify the dataset. If not specified, the permission remains unchanged.
            can_delete (bool, optional): Permission to delete the dataset. If not specified, the permission remains unchanged.

        Returns:
            dict: A dictionary containing the updated permissions for the user.

        Raises:
            Exception: If there's an error during the permission modification process.

        Description:
            This method modifies the permissions of the provided user for this dataset by sending a PUT request to the '/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/users' endpoint.

        Example:
        ```python
        dataset = workspace.get_all_datasets()[0]
        user = sedar.get_user("some_user")
        try:
            updated_permissions = dataset.edit_user_permission(user, can_read=True)
            print(updated_permissions)
        except Exception as e:
            print(e)
        ```
        """
        return self._edit_dataset_user_permissions(self.workspace, self.id, user.id, can_read, can_write, can_delete)

    def remove_user_permission(self, user : User) -> dict:
        """
        Removes all permissions of a user for this dataset.

        Args:
            user (User): An instance of the User class.

        Returns:
            dict: A dictionary containing the removed user.

        Raises:
            Exception: If there's an error during the permission removal process.

        Description:
            This method removes all permissions of the provided user for this dataset by sending a PUT request to the '/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/users' endpoint.

        Example:
        ```python
        dataset = workspace.get_all_datasets()[0]
        user = sedar.get_user("some_user")
        try:
            removal_status = dataset.remove_user_permission(user)
            print(removal_status)
        except Exception as e:
            print(e)
        ```
        """
        return self._edit_dataset_user_permissions(self.workspace, self.id, user.id, add=False)
    
    def create_index_data(self) -> bool:
        """
        Creates an index for the dataset.

        Returns:
            bool: True if the index was successfully created.

        Raises:
            Exception: If there's an error during the indexing process.

        Description:
            This method creates an index for the dataset by sending a PUT request to the '/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/index' endpoint.

        Example:
        ```python
        dataset = workspace.get_all_datasets()[0]
        try:
            indexing_status = dataset.create_index_data()
            print(f"Indexing status: {indexing_status}")
        except Exception as e:
            print(e)
        ```
        """
        return self._create_index_dataset(self.workspace, self.id)
    
    def delete_index_data(self) -> bool:
        """
        Deletes the index data for the dataset.

        Returns:
            bool: True if the index was successfully deleted.

        Raises:
            Exception: If there's an error during the index deletion process.

        Description:
            This method deletes the index for the dataset by sending a DELETE request to the '/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/index' endpoint.

        Example:
        ```python
        dataset = workspace.get_all_datasets()[0]
        try:
            deletion_status = dataset.delete_index_data()
            print(f"Index deletion status: {deletion_status}")
        except Exception as e:
            print(e)
        ```
        """
        return self._delete_index_dataset(self.workspace, self.id)
    
    def add_as_favorite(self) -> bool:
        """
        Adds the dataset as a favorite for the authenticated user.

        Returns:
            bool: True if the dataset was successfully added as a favorite, False if the dataset was already a favorite.

        Raises:
            Exception: If there's an error during the process.

        Description:
            This method marks the dataset as a favorite by sending a PATCH request to the '/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/favorite' endpoint.
            If the dataset is already marked as a favorite, a warning is logged, and the method returns False.

        Example:
        ```python
        dataset = workspace.get_all_datasets()[0]
        try:
            is_favorite = dataset.add_as_favorite()
            print(f"Add as favorite status: {is_favorite}")
        except Exception as e:
            print(e)
        ```
        """
        return self._toggle_favorite_dataset(self.workspace,self.id,True)

    def remove_as_favorite(self):
        """
        Removes the dataset from favorites of the authenticated user.

        Returns:
            bool: True if the dataset was successfully removed from favorites, False if the dataset was already not a favorite.

        Raises:
            Exception: If there's an error during the process.

        Description:
            This method unmarks the dataset as a favorite by sending a PATCH request to the '/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/favorite' endpoint.
            If the dataset is not marked as a favorite, a warning is logged, and the method returns False.

        Example:
        ```python
        dataset = workspace.get_all_datasets()[0]
        try:
            is_not_favorite = dataset.remove_as_favorite()
            print(f"Remove as favorite status: {is_not_favorite}")
        except Exception as e:
            print(e)
        ```
        """
        return self._toggle_favorite_dataset(self.workspace, self.id, False)
    
    def get_lineage(self) -> dict:
        """
        Fetches the lineage of the dataset.

        Returns:
            dict: A dictionary containing the lineage details of the dataset. It includes the dataset's name, attributes (like name and ID), and any child datasets in its lineage.

        Raises:
            Exception: If there's an error while fetching the dataset lineage.

        Description:
            This method retrieves the lineage of the dataset by sending a GET request to the '/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/lineage' endpoint. The lineage provides a hierarchical view of the dataset's origin and the datasets that it may have given rise to. This can be helpful for understanding the flow of data and the relationships between different datasets.

        Example:
        ```python
        dataset = workspace.get_all_datasets()[0]
        try:
            lineage_info = dataset.get_lineage()
            print(lineage_info)
        except Exception as e:
            print(e)
        ```
        """
        return self._get_dataset_lineage(self.workspace,self.id)
    
    def get_linked_datasets(self) -> list[Dataset]:
        """
        Fetches the datasets linked to the current dataset based on recommendations.

        Returns:
            list[Dataset]: A list of Dataset objects that are linked to the current dataset.
            The content of each Dataset can be accessed using it's `.content` attribute.

        Raises:
            Exception: If there's an error while fetching the linked datasets.

        Description:
            This method retrieves datasets that are linked or related to the current dataset by sending a GET request to the '/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/recommendations' endpoint. The linked datasets are retrieved based on certain recommendations, and each dataset contains information such as its creation date, a description of any custom links, its ID, visibility (public/private), language, and other metadata.

        Example:
        ```python
        dataset = workspace.get_all_datasets()[0]
        try:
            linked_datasets = dataset.get_linked_datasets()
            for linked_dataset in linked_datasets:
                print(linked_dataset.content)
        except Exception as e:
            print(e)
        ```
        """
        datasets_info = self._get_linked_datasets_json(self.workspace, self.id)
        return [Dataset(self.connection, self.workspace, dataset_info["id"]) for dataset_info in datasets_info]
    
    def create_dataset_link(self, linked_dataset: Dataset, description: str) -> dict:
        """
        Creates a link between the current dataset and another dataset.

        Args:
            linked_dataset (Dataset): An instance of the Dataset class representing the dataset to be linked.
            description (str): A brief description of the link.

        Returns:
            dict: A dictionary containing the details of the linked dataset.

        Raises:
            Exception: If there's an error while creating the dataset link.

        Description:
            This method creates a link between the current dataset and another specified dataset by sending a POST request to the '/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/recommendations' endpoint. Each link contains information such as creation date, custom link description, visibility (public/private), language, and other metadata of the linked dataset.

        Example:
        ```python
        dataset1 = workspace.get_all_datasets()[0]
        dataset2 = workspace.get_all_datasets()[1]
        try:
            linked_dataset_info = dataset1.create_dataset_link(dataset2, "Sample Link Description")
            print(linked_dataset_info)
        except Exception as e:
            print(e)
        ```
        """
        return self._create_dataset_link(self.workspace,self.id,linked_dataset.id, description)
    
    def delete_dataset_link(self, linked_dataset: Dataset) -> bool:
        """
        Deletes a link between the current dataset and another dataset.

        Args:
            linked_dataset (Dataset): An instance of the Dataset class representing the linked dataset to be removed.

        Returns:
            bool: True if the dataset link was successfully deleted.

        Raises:
            Exception: If there's an error while deleting the dataset link.

        Description:
            This method deletes a link between the current dataset and another specified dataset by sending a DELETE request to the '/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/recommendations' endpoint. The link is identified using the ID of the linked dataset.

        Example:
        ```python
        dataset1 = workspace.get_all_datasets()[0]
        dataset2 = workspace.get_all_datasets()[1]
        try:
            success = dataset1.delete_dataset_link(dataset2)
            if success:
                print("Link was successfully deleted.")
        except Exception as e:
            print(e)
        ```
        """
        return self._delete_dataset_link(self.workspace,self.id,linked_dataset.id)
    
    def update_dataset_link(self, linked_dataset: Dataset, description: str = None):
        """
        Updates the description of a link between the current dataset and another dataset.

        Args:
            linked_dataset (Dataset): An instance of the Dataset class representing the linked dataset to be updated.
            description (str, optional): The new description for the link. If not provided, the link's description remains unchanged.

        Returns:
            dict: A dictionary containing details of the updated dataset link, including its creation date, description, ID, visibility status, language, last update date, owner details, schema information, tags, and title.

        Raises:
            Exception: If there's an error while updating the dataset link.

        Description:
            This method updates the description of a link between the current dataset and another specified dataset by sending a PUT request to the '/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/recommendations' endpoint. The link is identified using the ID of the linked dataset.

        Example:
        ```python
        dataset1 = workspace.get_all_datasets()[0]
        dataset2 = workspace.get_all_datasets()[1]
        try:
            updated_link_info = dataset1.update_dataset_link(dataset2, "Updated description for link.")
            print(updated_link_info)
        except Exception as e:
            print(e)
        ```
        """
        return self._update_dataset_link(self.workspace, self.id, linked_dataset.id, description)
    
    def get_linked_dataset_info(self, linked_dataset: Dataset):
        """
        Retrieves information about a dataset linked to the current dataset.

        Args:
            linked_dataset (Dataset): An instance of the Dataset class representing the linked dataset whose information is to be retrieved.

        Returns:
            list[str]: A list containing formatted string(s) with details about the linked dataset. If the dataset is not found, the list will be empty.

        Raises:
            Exception: If there's an error while fetching the linked dataset information.

        Description:
            This method fetches detailed information about a dataset that is linked to the current dataset. It sends a GET request to the '/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/recommendations' endpoint. The information is then formatted into a human-readable string format for easy consumption.

        Example:
        ```python
        dataset1 = workspace.get_all_datasets()[0]
        dataset2 = workspace.get_all_datasets()[1]
        try:
            link_info = dataset1.get_linked_dataset_info(dataset2)
            for entry in link_info:
                print(entry)
        except Exception as e:
            print(e)
        ```
        """
        return self._get_dataset_link_info(self.workspace, self.id, linked_dataset.id)
    
    def compare_deltas(self, revision_a: str, revision_b: str):
        """
        Compares the dataset deltas between two revisions. Lists the differences between the two revisions.
        An entry 'cd_deleted' = True indicates that the column was deleted in the newer revision. Otherwise, it was added or modified.

        Args:
            revision_a (str): The number/identifier for the first revision to compare. This should be the newer revision number.
            revision_b (str, optional): The number/identifier for the second revision to compare. This should be the older revision number.

        Returns:
            dict: A dictionary containing two keys:
                - "header": A list of column names in the dataset.
                - "body": A list of dictionaries, where each dictionary represents a row in the dataset and indicates the changes between the two revisions.

        Raises:
            Exception: If there's an error during the delta comparison process.

        Description:
            This method compares the dataset deltas between two specified revisions by sending a GET request to the '/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/deltas' endpoint.
            It's important to note that this method might take some time to complete.

        Example:
        ```python
        dataset = workspace.get_all_datasets()[0]
        try:
            comparison_result = dataset.compare_deltas("2", "0")
            print(comparison_result)
        except Exception as e:
            print(e)
        ```
        """
        return self._compare_dataset_deltas(self.workspace, self.id, revision_a, revision_b)

    def get_revisions(self) -> list[dict]:
        """
        Retrieves all revisions or versions of the dataset.

        Args:
            None

        Returns:
            list[dict]: A list of dictionaries, where each dictionary contains details about a specific revision of the dataset.

        Raises:
            Exception: If there's an error during the revision retrieval process.

        Description:
            This method extracts all revisions from the dataset content.

        Example:
        ```python
        dataset = workspace.get_all_datasets()[0]
        try:
            revisions = dataset.get_revisions()
            for revision in revisions:
                print(revision)
        except Exception as e:
            print(e)
        """
        return self.content["datasource"]["revisions"]

    def query_sourcedata(self, query: str):
        """
        TODO: not tested
        Executes a query on the source data of the dataset.

        Args:
            query (str): The SQL query string to be executed on the dataset. The dataset can be referenced in the query by its ID.

        Returns:
            dict: A dictionary containing two keys:
                - "header": A list of column names in the dataset.
                - "body": A list of dictionaries, where each dictionary represents a row in the dataset that matches the query.

        Raises:
            Exception: If there's an error during the query execution.

        Description:
            This method allows users to execute SQL-like queries on the source data of the dataset. The result of the query is returned as a dictionary containing the headers (column names) and the body (matching rows).

        Example:
        ```python
        dataset = workspace.get_all_datasets()[0]
        query_string = f"SELECT * FROM {dataset.id} WHERE Firstname = 'Rachel'"
        try:
            query_result = dataset.query_sourcedata(query_string)
            print(query_result)
        except Exception as e:
            print(e)
        ```
        """
        return self._query_dataset_sourcedata(self.workspace, self.id, query)
    
    def _get_dataset_json(self, workspace_id, dataset_id):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}"

        response = self.connection._get_resource(resource_path)
        if response is None:
            raise Exception(f"Failed to fetch Dataset '{dataset_id}'. Set the logger level to \"Error\" or below to get more detailed information.")

        return response
    
    def _update_dataset(self, workspace_id: str, dataset_id: str, title: str, description: str, author: str, longitude, 
                        latitude, range_start, range_end, license, language):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}"
        payload =  {"title":None, 
                    "description":None, 
                    "author":None, 
                    "longitude":None, 
                    "latitude":None, 
                    "range_start":None, 
                    "range_end":None, 
                    "license":None, 
                    "language":None
                    }
        
        # Get the original Dataset
        dataset = self._get_dataset_json(workspace_id,dataset_id)

        # Reinstate the old values from the original Dataset
        for key in payload:
            payload[key] = dataset.get(key)

        # If a new value for a parameter is given to this method, assign it
        if title is not None:
            payload["title"] = title
        if description is not None:
            payload["description"] = description
        if author is not None:
            payload["author"] = author
        if longitude is not None:
            payload["longitude"] = longitude
        if latitude is not None:
            payload["latitude"] = latitude
        if range_start is not None:
            payload["range_start"] = range_start
        if range_end is not None:
            payload["range_end"] = range_end
        if license is not None:
            payload["license"] = license
        if language is not None:
            payload["language"] = language

        response = self.connection._put_resource(resource_path, payload)
        if response is None:
            raise Exception("The Dataset could not be updated. Set the logger level to \"Error\" or below to get more detailed information.")

        self.logger.info("The Dataset was updated successfully.")
        return response
    
    def _update_datasource(self, workspace_id: str, dataset_id: str, datasource_definition: dict, file_path: str):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/update-datasource"

        # Check if the datasource definition is a file path. 
        if isinstance(datasource_definition, str):
            # If it is a valid path, open the file and convert it to json
            if os.path.exists(datasource_definition):
                with open(datasource_definition, "r") as f:
                    datasource_definition = json.load(f)
            else:
                self.logger.error(f"Datasource definition file not found: {datasource_definition}")
                return None

        # Create the payload with the datasource definition as a json-object and the title of the dataset
        payload = {
            "datasource_definition": json.dumps(datasource_definition)
        }

        file_name = os.path.basename(file_path)
        
        if os.path.exists(file_path):
            files = {self.connection._remove_file_extension(file_name): (file_name, open(file_path, 'rb'), "application/vnd.ms-excel")}
        else:
            self.logger.error(f"Datasource File not found: {file_paths}")
            return None

        response = self.connection._put_resource(resource_path, data=payload, files=files)

        # Close all opened files
        for file_tuple in files.values():
            file_obj = file_tuple[1]  # Get the file object from the tuple
            file_obj.close()

        if response is None:
            raise Exception(f"The Datasource for Dataset '{dataset_id}' could not be updated. Set the logger level to \"Error\" or below to get more detailed information.")

        self.logger.info(f"The Datasource for '{dataset_id}' was updated successfully. Starting ingestion of the new version...")
        return self._ingest_dataset(workspace_id, dataset_id)
    
    def _publish_dataset(self, workspace_id, dataset_id, index=False, with_thread=True, profile=False):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}"
        payload = {
            "index": index,
            "with_thrad": with_thread,
            "profile":profile
        }

        response = self.connection._patch_resource(resource_path,payload)
        if response is None:
            raise Exception(f"The Dataset '{dataset_id}' could not be published. Set the logger level to \"Error\" or below to get more detailed information.")

        self.logger.info(f"The Dataset '{dataset_id}' was published successfully.")
        return True

    def _delete_dataset(self, workspace_id, dataset_id):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}"

        response = self.connection._delete_resource(resource_path)
        if response is None:
            raise Exception(f"The Dataset '{dataset_id}' could not be deleted. Set the logger level to \"Error\" or below to get more detailed information.")
        
        self.logger.info(f"The Dataset '{dataset_id}' was deleted successfully.")
        return True
    
    def _ingest_dataset(self, workspace_id, dataset_id):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/run-ingestion"

        response = self.connection._get_resource(resource_path)
        if response is None:
            raise Exception(f"Failed to ingest Dataset '{dataset_id}'. Set the logger level to \"Error\" or below to get more detailed information.")
        
        self.logger.info(f"The ingestion of the Dataset '{dataset_id}' was started successfully. Please note that the ingestion is not finished yet and can take a while.")
        return response

    def _get_dataset_logs(self, workspace_id, dataset_id):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/logs"
        payload = {
            "is_download":False
        }

        response = self.connection._get_resource(resource_path, payload)
        if response is None:
            raise Exception(f"Failed to fetch Dataset logs for '{dataset_id}'. Set the logger level to \"Error\" or below to get more detailed information.")

        # Initialize an empty list to store the formatted logs
        formatted_response = []
        
        # Loop through each log entry in the logs
        for log in response:
            # Extract relevant information
            changes = log['changes']
            created_on = log['createdOn']
            description = log['description']['en']  # Assuming English description
            user_info = log['user']
            version = log['version']
            
            # Create a formatted string for this log entry
            formatted_log = f"Version: {version}, Created On: {created_on}, User: {user_info['username']}\n"
            formatted_log += f"Description: {description}\n"
            
            # Add details about each change
            for change in changes:
                formatted_log += f"\tChanged '{change['key']}' from '{change['from']}' to '{change['to']}'\n"
            
            # Add this formatted log to the list
            formatted_response.append(formatted_log)
        
        self.logger.info(f"The Dataset logs for '{dataset_id}' were retrieved successfully.")
        return formatted_response
    
    def _update_continuation_timer(self, workspace_id, dataset_id, timer):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/timer"
        payload = {
            "timer": timer
        }

        response = self.connection._put_resource(resource_path,payload)
        if response is None:
            raise Exception(f"The continuation timer for Dataset '{dataset_id}' could not be updated. Set the logger level to \"Error\" or below to get more detailed information.")

        self.logger.info(f"The continuation timer for Dataset '{dataset_id}' was updated successfully.")
        return True
    
    def _toggle_dataset_status(self, workspace_id, dataset_id):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/status"

        response = self.connection._put_resource(resource_path)
        if response is None:
            raise Exception(f"The status for Dataset '{dataset_id}' could not be updated. Set the logger level to \"Error\" or below to get more detailed information.")
        
        # If the status was set to "public" the API will respond with an empty answear
        # If the status was set to "private" the API will respond with the current user
        # To check if a dataset was made "public" or "private", just check the length of the server response
 
        self.logger.info(f"The status for Dataset '{dataset_id}' has been updated successfully.")
        return response
    
    def _edit_dataset_user_permissions(self, workspace_id, dataset_id, user_id, can_read=None, can_write=None, can_delete=None, add=None):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/users"
        payload =  {
            "email": user_id,
        }

        # Append optional parameters
        if can_read is not None:
            payload["can_read"] = can_read
        if can_write is not None:
            payload["can_write"] = can_write
        if can_delete is not None:
            payload["can_delete"] = can_delete
        if add is not None:
            payload["add"] = add

        response = self.connection._put_resource(resource_path,payload)
        if response is None:
            raise Exception(f"The User permissions for Dataset '{dataset_id}' could not be updated. Set the logger level to \"Error\" or below to get more detailed information.")

        self.logger.info(f"The User permissions for Dataset '{dataset_id}' have been updated successfully.")
        return response
    
    def _create_index_dataset(self, workspace_id, dataset_id):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/index"

        response = self.connection._put_resource(resource_path)
        if response is None:
            raise Exception(f"The Dataset '{dataset_id}' could not be indexed. Set the logger level to \"Error\" or below to get more detailed information.")
    
        self.logger.info(f"The Dataset '{dataset_id}' was succesfully Indexed.")
        return True

    def _delete_index_dataset(self, workspace_id, dataset_id):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/index"

        response = self.connection._delete_resource(resource_path)
        if response is None:
            raise Exception(f"Failed to delete the Index for Dataset '{dataset_id}'. Set the logger level to \"Error\" or below to get more detailed information.")
    
        self.logger.info(f"The Index Data for Dataset '{dataset_id}' has succesfully been deleted.")
        return True
    
    def _get_favorite_datasets_json(self, workspace_id):
        resource_path = f"/api/v1/workspaces/{workspace_id}/favorites"

        response = self.connection._get_resource(resource_path)

        if response is None:
            raise Exception(f"Failed to fetch favorite Datasets. Set the logger level to \"Error\" or below to get more detailed information.")
        
        self.logger.info(f"Favorite Datasets have been fetched successfully.")
        return response

    def _toggle_favorite_dataset(self, workspace_id, dataset_id, add=True):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/favorite"

        # Fetch all current favorised Datasets
        favorite_datasets = self._get_favorite_datasets_json(workspace_id)
        is_fav = False

        # Check if the Dataset is currently a favorite
        for dataset in favorite_datasets:
            if dataset["id"] == dataset_id:
                is_fav = True

        # Only execute the API-Call to toggle the favorite-status, if the wanted favorite-state is not already present.
        if add ^ is_fav:
            response = self.connection._patch_resource(resource_path)
        
            if response is None:
                raise Exception(f"The Favorite Status for Dataset '{dataset_id}' could not be changed. Set the logger level to \"Error\" or below to get more detailed information.")
    
            self.logger.info(f"The Favorite Status for Dataset '{dataset_id}' has succesfully been updated.")
            return True
        
        # If the favorite-state was already present (and no API-Call was made)
        if add == True:
            self.logger.warning(f"The dataset '{dataset_id}' already is a favorite. No change has been made.")

        if add == False:
            self.logger.warning(f"The dataset '{dataset_id}' already is not a favorite. No change has been made.")
    
        return False

    def _get_dataset_lineage(self, workspace_id, dataset_id):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/lineage"

        response = self.connection._get_resource(resource_path)
        if response is None:
            raise Exception(f"The lineage for Dataset '{dataset_id}' could not be fetched. Set the logger level to \"Error\" or below to get more detailed information.")

        self.logger.info(f"The lineage for Dataset '{dataset_id}' has succesfully been fetched.")
        return response
    
    def _get_linked_datasets_json(self, workspace_id, dataset_id):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/recommendations"

        response = self.connection._get_resource(resource_path)
        if response is None:
            raise Exception(f"The linked Datasets for Dataset '{dataset_id} could not be fetched. Set the logger level to \"Error\" or below to get more detailed information.")
    
        self.logger.info(f"The linked Datasets for Dataset '{dataset_id}' has succesfully been fetched.")
        return response
    
    def _get_dataset_link_info(self, workspace_id, dataset_id, dataset2_id):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/recommendations"

        response = self.connection._get_resource(resource_path)
        if response is None:
            raise Exception(f"Failed to fetch Dataset link info between Dataset '{dataset_id}' and '{dataset2_id}'. Set the logger level to \"Error\" or below to get more detailed information.")

        # Initialize an empty list to store the formatted logs
        formatted_response = []
        
        # Search for the correct dataset within the API-Response and construct an output string.
        for dataset in response:
            if dataset["id"] == dataset2_id:
                # Extract relevant information
                created_on = dataset['createdOn']
                custom_link_desc = dataset['customLinkDescription']
                is_public = dataset['isPublic']
                last_updated_on = dataset['lastUpdatedOn']
                owner_info = dataset['owner']
                schema_info = dataset['schema']
                tags = dataset['tags']
                title = dataset['title']
                
                # Create a formatted string for this dataset entry
                formatted_log = f"Created On: {created_on}, Last Updated On: {last_updated_on}, Owner: {owner_info['username']}\n"
                formatted_log += f"Title: {title}, Custom Link Description: {custom_link_desc}, Is Public: {is_public}\n"
                formatted_log += f"Schema ID: {schema_info['id']}, Schema Type: {schema_info['type']}\n"
                
                # Add details about tags if available
                if tags:
                    formatted_log += "Tags: " + ", ".join(tags) + "\n"
                else:
                    formatted_log += "Tags: N/A\n"
                
                # Add this formatted log to the list
                formatted_response.append(formatted_log)
        
        self.logger.info(f"Successfully retrieved detailed link information for Dataset '{dataset_id}' and '{dataset2_id}'")
        return formatted_response
    
    def _create_dataset_link(self, workspace_id, dataset_id, dataset2_id, description):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/recommendations"
        payload = {
            "id_of_linked_dataset":dataset2_id,
            "description":description
        }

        response = self.connection._post_resource(resource_path, payload)
        if response is None:
            raise Exception(f"The dataset link between Dataset '{dataset_id}' and '{dataset2_id}' could not be created. Set the logger level to \"Error\" or below to get more detailed information.")
    
        self.logger.info(f"The dataset link between Dataset '{dataset_id}' and '{dataset2_id}' was created succesfully.")
        return response
    
    def _update_dataset_link(self, workspace_id, dataset_id, dataset2_id, description):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/recommendations"
        payload = {
            "id_of_linked_dataset":dataset2_id,
            "description": description
        }

        response = self.connection._put_resource(resource_path, payload)
        if response is None:
            raise Exception(f"The dataset link between Dataset '{dataset_id}' and '{dataset2_id}' could not be updated. Set the logger level to \"Error\" or below to get more detailed information.")
    
        self.logger.info(f"The dataset link between Dataset '{dataset_id}' and '{dataset2_id}' has been updated succesfully.")
        return response
    
    def _delete_dataset_link(self, workspace_id, dataset_id, dataset2_id):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/recommendations"
        payload = {
            "id_of_linked_dataset":dataset2_id,
        }

        response = self.connection._delete_resource(resource_path, payload)
        if response is None:
            raise Exception(f"The dataset link between Dataset '{dataset_id}' and '{dataset2_id}' could not be deleted. Set the logger level to \"Error\" or below to get more detailed information.")
    
        self.logger.info(f"The dataset link between Dataset '{dataset_id}' and '{dataset2_id}' was deleted succesfully.")
        return True

    def _start_dataset_profiling(self, workspace_id, dataset_id, dataset_version):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/profiling"
        payload = {
            "version": dataset_version
        }
        response = self.connection._get_resource(resource_path, payload)
        if response is None:
            raise Exception(f"The profiling for Dataset '{dataset_id}' could not be started. Set the logger level to \"Error\" or below to get more detailed information.")
    
        self.logger.info(f"The profiling for '{dataset_id}' has been started successfully.")
        return True
    
    def _compare_dataset_deltas(self, workspace_id, dataset_id, version_a, version_b):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/deltas"
        payload = {
            "session_id": self.connection.session_id,
            "version": version_a,
            "version_to_compare": version_b
        }
        response = self.connection._get_resource(resource_path, payload)
        if response is None:
            raise Exception(f"The Dataset deltas '{version_a}' and '{version_b}' for Dataset '{dataset_id}' could not be compared. Set the logger level to \"Error\" or below to get more detailed information.")
    
        self.logger.info(f"Comparison between deltas '{version_a}' and '{version_b}' for Dataset '{dataset_id}' successful.")
        return response

    def _query_dataset_sourcedata(self, workspace_id, dataset_id, query):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/query"
        payload = {
            "session_id": self.connection.session_id,
            "query": query
        }
        print("modified!")
        response = self.connection._post_resource(resource_path, payload)
        if response is None:
            raise Exception(f"The query '{query}' for Dataset '{dataset_id}' could not be executed. Set the logger level to \"Error\" or below to get more detailed information.")
    
        self.logger.info(f"The query '{query}' for Dataset '{dataset_id}' has been executed successfully.")
        return response

    def _get_dataset_preview_json(self, workspace_id, dataset_id):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/preview"

        payload = {
            "session_id": self.connection.session_id,
            "flattened": False
        }

        response = self.connection._get_resource(resource_path, payload)
        if response is None:
            raise Exception(f"Failed to fetch Dataset preview for '{dataset_id}'. Set the logger level to \"Error\" or below to get more detailed information.")
        
        self.logger.info(f"Dataset preview for '{dataset_id}' has been fetched successfully.")
        return response


    def _get_all_tags(self, workspace_id, dataset_id) -> list[Tag]:
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/tags"
        
        response = self.connection._get_resource(resource_path)
        if response is None:
            raise Exception(f"The Tags for Dataset '{dataset_id}' could not be retrieved. Set the logger level to \"Error\" or below to get more detailed information.")

        self.logger.info(f"The Tags for Dataset '{dataset_id}' have been retrieved successfully.")
        return response
    
    def _get_tag_json(self, workspace_id, dataset_id, tag_id) -> list[Tag]:
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/tags/{tag_id}"
        
        response = self.connection._get_resource(resource_path)
        if response is None:
            raise Exception("Tag could not be retrieved. Set the logger level to \"Error\" or below to get more detailed information.")

        self.logger.info(f"The Tag '{tag_id}' for Dataset '{dataset_id}' have been retrieved successfully.")
        return response

    def _add_tag(self, workspace_id, dataset_id, annotation, ontology_id):
        resource_path=f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/tags"
        payload = {
            "annotation": annotation,
            "ontology_id": ontology_id
        }

        response = self.connection._post_resource(resource_path, payload)
        if response is None:
            raise Exception(f"The Tag '{annotation.title}' for Dataset '{dataset_id}' could not be added. Set the logger level to \"Error\" or below to get more detailed information.")

        self.logger.info(f"The Tag '{annotation.title}' for Dataset '{dataset_id}' was added successfully.")
        return response
    
    def _get_all_notebooks(self, workspace_id, dataset_id):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/notebooks"
        
        response = self.connection._get_resource(resource_path)
        if response is None:
            raise Exception(f"The Notebooks for Dataset '{dataset_id}'  could not be retrieved. Set the logger level to \"Error\" or below to get more detailed information.")

        self.logger.info(f"The Notebooks for Dataset '{dataset_id}' have been retrieved successfully.")
        return response
    
    def _add_notebook(self, workspace_id, dataset_id, title, description, type, is_public, version):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/notebooks"
        payload = {
            "title": title,
            "description": description,
            "type": type,
            "is_public": is_public,
            "version": version
        }

        response = self.connection._post_resource(resource_path, payload)
        if response is None:
            raise Exception(f"The Notebook '{title}' for Dataset '{dataset_id}' could not be added. Set the logger level to \"Error\" or below to get more detailed information.")

        self.logger.info(f"The Notebook '{title}' for Dataset '{dataset_id}' was added successfully.")
        return response    
    
    def _get_all_schema_attributes_json(self, workspace_id, dataset_id):
        # There is no serverside implementation for a "get_all"-Call for Attributes
        # Till then, we just extract the attributes from the answear of the "get_dataset" call
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}"
        
        response = self.connection._get_resource(resource_path)
        if response is None:
            raise Exception(f"The Attributes for Dataset '{dataset_id}' could not be retrieved. Set the logger level to \"Error\" or below to get more detailed information.")
        
        # Right now, we always go for the first "entities" entry. If there exist multiple
        # ones, a correct way to select the desired one would need to be implemented.
        attributes = response["schema"]["entities"][0]["attributes"]

        self.logger.info(f"The Attributes for Dataset '{dataset_id}' have been retrieved successfully.")
        return attributes
    
    def _get_all_schema_entities_json(self, workspace_id, dataset_id):
        # There is no serverside implementation for a "get_all"-Call for Entities
        # Till then, we just extract the attributes from the answear of the "get_dataset" call
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}"
        
        response = self.connection._get_resource(resource_path)
        if response is None:
            raise Exception(f"The Entities for Dataset '{dataset_id}' could not be retrieved. Set the logger level to \"Error\" or below to get more detailed information.")
        
        # Extract the Entities
        entities = response["schema"]["entities"]

        self.logger.info(f"The Entities for Dataset '{dataset_id}' have been retrieved successfully.")
        return entities
    
    def _get_all_schema_files_json(self, workspace_id, dataset_id):
        # There is no serverside implementation for a "get_all"-Call for Entities
        # Till then, we just extract the attributes from the answear of the "get_dataset" call
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}"
        
        response = self.connection._get_resource(resource_path)
        if response is None:
            raise Exception(f"The Entities for Dataset '{dataset_id}' could not be retrieved. Set the logger level to \"Error\" or below to get more detailed information.")
        
        # Extract the Entities
        files = response["schema"]["files"]

        self.logger.info(f"The Files for Dataset '{dataset_id}' have been retrieved successfully.")
        return files
    

    def _extract_schema_info(self):
        def get_columns(attributes, prefix):
            for element in attributes:
                if element.get("isObject"):
                    get_columns(element["attributes"], prefix + element["name"] + ".")
                elif element.get("isArrayOfObjects"):
                    array_prefix = prefix + element["name"] + ("[0]." if element["attributes"] else "[0]")
                    get_columns(element["attributes"], array_prefix)
                else:
                    self.columns.append(prefix + element["name"])

        if self.content:
            if "schema" in self.content and self.content["schema"].get("type", None) == "UNSTRUCTURED":
                self.size_of_files = None
                for file in self.content["schema"]["files"]:
                    self.size_of_files += file["sizeInBytes"]
            elif "schema" in self.content:
                self.columns = []
                self.rows_count = 0
                self.entity_count = 0
                if "entities" in self.content["schema"]:
                    self.entity_count = len(self.content["schema"]["entities"])
                    self.rows_count = 0

                    for entity in self.content["schema"]["entities"]:
                        get_columns(entity["attributes"], "")
                        self.rows_count += entity["countOfRows"]
                    
