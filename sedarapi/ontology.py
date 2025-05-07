from __future__ import annotations

from .commons import Commons

from cache.cacheable import cacheable, exclude_from_cacheable

@cacheable
class Ontology:
    """
    Represents an ontology in the workspace. This class provides methods to interact with the ontology, such as updating, deleting, and downloading the ontology.

    Attributes:
        connection (Commons): An instance of the Commons class representing the connection to the SEDAR API.
        workspace (str): The ID of the workspace where the ontology is located.
        id (str): The ID of the ontology.
        logger (Logger): The logger instance to log messages.
        graph_id (str): The ID of the graph associated with the ontology.
        content (dict): The JSON content of the ontology details.
        title (str): The title of the ontology.
        description (str): The description of the ontology.
        filename (str): The filename of the ontology.

    """

    def __init__(self, connection: Commons, workspace_id: str, ontology_id: str):
        self.connection = connection
        self.workspace = workspace_id
        self.id = ontology_id
        self.logger = self.connection.logger
        self.graph_id = self._extract_graph_id(self.workspace, self.id)
        self.content = self._get_ontology_json(self.workspace, self.id)

        # Extract some members from the "content" attribute
        self.title = self.content["title"]
        self.description = self.content["description"]
        self.filename = self.content["filename"]
        # ...

    def update(self, title: str = None, description: str = None) -> Ontology:
        """
        Updates an existing ontology in the current workspace.

        Args:
            title (str, optional): The new title for the ontology.
            description (str, optional): The new description for the ontology.

        Returns:
            Ontology: An instance of the Ontology class representing the updated ontology. 
            The content of the ontology details can be accessed using the `.content` attribute.

        Raises:
            Exception: If there's a failure in updating the ontology.

        Description:
            This method updates an existing ontology by sending a PUT request to the appropriate API endpoint.

        Example:
            try:
                updated_ontology = ontology_instance.update(title="New Title", description="New Description")
                print(updated_ontology.content['title'], updated_ontology.content['description'])
            except Exception as e:
                print(e)
        """
        return Ontology(self.connection, self.workspace, self._update_ontology(self.workspace, self.id, title, description)["id"])

    def delete(self) -> bool:
        """
        Deletes the current ontology from the workspace.

        Returns:
            bool: `True` if the ontology was successfully deleted, otherwise an exception is raised.

        Raises:
            Exception: If there's a failure in deleting the ontology.

        Description:
            This method deletes an existing ontology from the workspace by sending a DELETE request to the appropriate API endpoint `/api/v1/workspaces/{workspace_id}/ontologies/{ontology_id}`.

        Note:
            Deleting an ontology is irreversible. Ensure that you have proper backups or are sure about the deletion before executing this method.

        Example:
        ```python
        ontology_instance = workspace.get_all_ontologies()[0]
        try:
            success = ontology_instance.delete()
            if success:
                print("Ontology successfully deleted!")
            else:
                print("Failed to delete ontology.")
        except Exception as e:
            print(f"Error: {e}")
        ```
        """
        return self._delete_ontology(self.workspace, self.id)
    
    def get_iri(self) -> str:
        """
        >> This method is likely to be removed in a further version <<
        Retrieves the Internationalized Resource Identifier (IRI) for the ontology. 
        Since this method returns html, there will only be limited use.

        Returns:
            str: The IRI of the ontology.

        Raises:
            Exception: If there's a failure in fetching the IRI.

        Description:
            This method fetches the IRI for the current ontology by sending a GET request to the appropriate API endpoint `/api/v1/workspaces/{workspace_id}/ontologies/iri/{graph_id}`.

        Note:
            The IRI provides a globally unique identifier for the ontology, ensuring its distinctiveness in the Semantic Web.

        Example:
        ```python
        ontology_instance = Ontology(connection, "workspace_id", "ontology_id")
        try:
            iri = ontology_instance.get_iri()
            print(f"The IRI of the ontology is: {iri}")
        except Exception as e:
            print(f"Error: {e}")
        ```

        """
        return self._get_iri(self.workspace, self.graph_id)
    
    def execute_construct_query(self, querystring: str) -> dict:
        """
        Executes a construct query on the ontology.

        Args:
            querystring (str): The query string to be executed.

        Returns:
            dict: The result of the construct query.

        Raises:
            Exception: If the construct query fails.

        Description:
            This method sends a construct query request to the appropriate API endpoint `/api/v1/workspaces/{workspace_id}/ontologies/construct` 
            and returns the result. The provided `querystring` defines the specifics of the construct query.

        Example:
            ontology_instance = workspace.get_all_ontologies()[0]
            result = ontology_instance.execute_construct_query("your_query_here")
            print(result)
        """
        return self._construct(self.workspace, querystring)
    
    def download(self, file_path: str = "./") -> bool:
        """
        Downloads the ontology and saves it to the specified file path.

        Args:
            file_path (str): The path where the ontology should be saved. By default the file is saved in the current directory. 

        Returns:
            bool: `True` if the ontology was successfully downloaded and saved.

        Raises:
            Exception: If there's a failure in downloading or saving the ontology.

        Description:
            This method downloads the ontology from the server using the API endpoint `/api/v1/workspaces/{workspace_id}/ontologies/{graph_id}/download` 
            and saves it to the specified file.

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
        return self._download_ontology(self.workspace, self.graph_id, file_path, self.filename)

    @exclude_from_cacheable
    def get_all_annotations(self) -> list[Annotation]:
        """
        Retrieves all annotations associated with the current ontology in the workspace.
        To find a specific annotation, prefer the ontology_annotation_search method inside the workspace.

        Returns:
            list[Annotation]: A list of Annotation objects representing all annotations of the current ontology.

        Raises:
            Exception: If there's a failure in fetching the annotations.

        Description:
            This method fetches all existing annotations from the workspace by sending a GET request to the API endpoint `/api/v1/workspaces/{workspace_id}/ontologies/completion`. 
            It then filters the annotations to include only those associated with the current ontology (using the ontology's title for matching).

        Note:
            - The method assumes that the `graphName` attribute of an annotation corresponds to the `title` attribute of an ontology for matching purposes. If this assumption is not valid, the filtering logic may need to be adjusted.

        Example:
            ```python
            ontology_instance = workspace.get_all_ontologies()[0]
            try:
                annotations = ontology_instance.get_all_annotations()
                for annotation in annotations:
                    print(annotation.title)
            except Exception as e:
                print(f"Error: {e}")
            ```

        """
        # This call should return all existing annotations of that workstream.
        all_annotations = self._ontology_completion_search(self.workspace, search_term="", ontology=self)
        
        # Now we further filter for those ones, that are part of the current ontology
        filtered_annotations = []

        for annotation in all_annotations:
            if annotation["graphName"] == self.content["title"]:
                filtered_annotations.append(annotation)
        
        return [Annotation(self.connection, self.id, annotation_info) for annotation_info in filtered_annotations]
    
    def get_all_classes(self) -> list[Annotation]:
        """
        Retrieves all classes associated with the current ontology in the workspace.

        Returns:
            list[Annotation]: A list of Annotation objects representing all classes of the current ontology.

        Raises:
            Exception: If there's a failure in fetching the classes.

        Description:
            This method fetches all existing classes from the workspace by sending a GET request to the API endpoint `/api/v1/workspaces/{workspace_id}/ontologies/{ontology_id}/classes`. 
            It then filters the classes to include only those associated with the current ontology.

        Example:
            ```python
            ontology_instance = workspace.get_all_ontologies()[0]
            try:
                classes = ontology_instance.get_all_classes()
                for class_ in classes
                    print(class_.title)
            except Exception as e:
                print(f"Error: {e}")
            ```
        """
        response = self._get_all_classes(self.workspace, self)
        return [Annotation(self.connection, self.workspace, class_) for class_ in response]

    
    def _get_ontology_json(self, workspace_id, ontology_id):
        resource_path = f"/api/v1/workspaces/{workspace_id}/ontologies/{ontology_id}" 
    
        response = self.connection._get_resource(resource_path)
        if response is None:
            raise Exception("Failed to fetch Ontology. Set the logger level to \"Error\" or below to get more detailed information.")
    
        return response
    
    def _extract_graph_id(self, workspace_id, ontology_id):
        ontology = self._get_ontology_json(workspace_id,ontology_id)
        graph_url = ontology["graphname"]
        parts = graph_url.split("/")
        graph_id = parts[-1].rstrip('>')  # Entfernt das ">"-Zeichen vom Ende
        return graph_id
    
    def _get_iri(self, workspace_id, graph_id):
        resource_path = f"/api/v1/workspaces/{workspace_id}/ontologies/iri/{graph_id}"
    
        response = self.connection._get_resource(resource_path)
        if response is None:
            raise Exception("Failed to fetch the IRI. Set the logger level to \"Error\" or below to get more detailed information.")
    
        return response
    
    def _construct(self, workspace_id, querystring):
        resource_path = f"/api/v1/workspaces/{workspace_id}/ontologies/construct"
        payload = {
            "querystring": querystring
        }
        response = self.connection._get_resource(resource_path, payload)
        if response is None:
            raise Exception("Construct query failed. Set the logger level to \"Error\" or below to get more detailed information.")
    
        return response
    
    def _download_ontology(self, workspace_id, graph_id, file_path, file_name):
        resource_path = f"/api/v1/workspaces/{workspace_id}/ontologies/{graph_id}/download"

        response = self.connection._get_resource(resource_path)
        if response is None:
            raise Exception("Failed to download the ontology. Set the logger level to \"Error\" or below to get more detailed information.")

        # Append the file name to the path
        file_path += file_name

        # Open the file and write the contents into the specified file.
        with open(file_path, 'wb') as f:
            f.write(response)

        return True

    def _update_ontology(self, workspace_id, ontology_id, title, description):
        resource_path = f"/api/v1/workspaces/{workspace_id}/ontologies/{ontology_id}"
        payload =  {
            "title":None, 
            "description":None
        }
        
        # Get the original Ontology
        ontology = self._get_ontology_json(workspace_id,ontology_id)

        # Reinstate the old values from the original Ontology
        for key in payload:
            payload[key] = ontology.get(key)

        # If a new value for a parameter is given to this method, assign it
        if title is not None:
            payload["title"] = title
        if description is not None:
            payload["description"] = description

        response = self.connection._put_resource(resource_path, payload)
        if response is None:
            raise Exception("The Ontology could not be updated. Set the logger level to \"Error\" or below to get more detailed information.")

        self.logger.info("The Ontology was updated successfully.")
        return response
    
    def _delete_ontology(self, workspace_id, ontology_id):
        resource_path = f"/api/v1/workspaces/{workspace_id}/ontologies/{ontology_id}"
        
        # Get the original Ontology
        ontology = self._get_ontology_json(workspace_id,ontology_id)

        response = self.connection._delete_resource(resource_path)
        if response is None:
            raise Exception("The Ontology could not be deleted. Set the logger level to \"Error\" or below to get more detailed information.")
        
        self.logger.info("The Ontology was deleted successfully.")
        return True
    
    def _ontology_completion_search(self, workspace_id, search_term: str, ontology: Ontology = None):
        resource_path = f"/api/v1/workspaces/{workspace_id}/ontologies/completion"

        payload = {
            "search_term": search_term,
            "ontology_id": ontology.id if ontology else None
        }

        response = self.connection._get_resource(resource_path, payload)
        if response is None:
            raise Exception("The Ontology completion search could not be executed. Set the logger level to \"Error\" or below to get more detailed information.")

        self.logger.info("Ontology completion search was executed successfully.")
        return response
    
    def _get_all_classes(self, workspace_id, ontology: Ontology):
        resource_path = f"/api/v1/workspaces/{workspace_id}/ontologies/{ontology.id}/classes"
        
        response = self.connection._get_resource(resource_path)
        if response is None:
            raise Exception("Failed to fetch classes. Set the logger level to \"Error\" or below to get more detailed information.")
        
        return response
    
@cacheable
class Annotation:
    """
    Get an Annotation by executing the "ontology_annotation_search"-call on a workspace-instance.
    """
    def __init__(self, connection: Commons, workspace_id: str, content):
        self.connection = connection
        self.workspace = workspace_id
        self.logger = self.connection.logger
        self.content = content
        
        # extract members from content
        self.string = content["value"]
        self.description = content["description"]
        self.graph_name = content["graphName"]
        self.graph_id = content["graph"]
        self.title = content["text"]
