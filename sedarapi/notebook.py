from __future__ import annotations

from .commons import Commons

from cache.cacheable import cacheable

@cacheable
class Notebook:
    """
    Represents a notebook in the system. This class provides methods to update and delete a notebook and other functionalities.

    Attributes:
        connection (Commons): An instance of the Commons class.
        workspace (str): The workspace ID.
        dataset (str): The dataset ID.
        id (str): The notebook ID.
        link (str): The link to the notebook.
        logger (Logger): The logger instance.
        content (dict): The content of the notebook details.
        title (str): The title of the notebook.
        description (str): The description of the notebook.
        type (str): The type of the notebook.

    """

    def __init__(self, connection: Commons, workspace_id: str, dataset_id: str, notebook_id: str) -> Notebook:
        self.connection = connection
        self.workspace = workspace_id
        self.dataset = dataset_id
        self.id = notebook_id
        self.logger = self.connection.logger
        self.content = self._get_notebook_json(self.workspace, self.dataset, self.id)

        # Extract some members from the "content" attribute
        self.title = self.content["title"]
        self.description = self.content["description"]
        self.link = self.content["link"]
        self.type = self.content["type"]
        # ...

    def update(self, title: str = None, description: str = None, is_public: bool = None, version: str = None) -> Notebook:
        """
        Updates a notebook in the system.

        Args:
            title (str, optional): The new title for the notebook.
            description (str, optional): The new description for the notebook. Default is None.
            is_public (bool, optional): Make the notebook either public or private
            version (str, optional): Select the revision of the attached dataset.

        Returns:
            Notebook: An instance of the Notebook class representing the updated notebook.
            The content of the notebook details can be accessed using the `.content` attribute

        Raises:
            Exception: If there's a failure in updating the notebook.

        Example:
            updated_notebook = notebook_instance.update(title="Postman-Notebook_EDITED", description="descr_EDITED")
            print(updated_notebook['title'])
        """
        return Notebook(self.connection, self.workspace, self.dataset, self._update_notebook(self.workspace, self.dataset, self.id, title, description, is_public, version)["id"])

    def delete(self) -> bool:
        """
        Deletes a notebook from the system.

        Args:
            None

        Returns:
            bool: True if the notebook was successfully deleted.

        Raises:
            Exception: If there's a failure in deleting the notebook.

        Notes:
            - Once deleted, the notebook cannot be recovered.

        Example:
            was_deleted = notebook_instance.delete()
            if was_deleted:
                print("Notebook was successfully deleted.")
            else:
                print("Failed to delete the notebook.")
        """
        return self._delete_notebook(self.workspace, self.dataset, self.id)
    
    def add_to_hdfs(self):   # Untested #
        """
        Adds a notebook to the Hadoop Distributed File System (HDFS).

        Args:
            None
    
        Returns:
            bool: True if the notebook was successfully added to the HDFS.  

        Raises:
            Exception: If there's a failure in adding the notebook to HDFS or if there are issues with the Docker container.

        Description:
            This method first creates a temporary directory to store the notebook. It then retrieves the notebook from a Docker container and subsequently uploads it to the Hadoop Distributed File System (HDFS).

        Notes:
            - The method cleans up (deletes) the temporary directory after the notebook is added to HDFS.

        Example:
            try:
                notebook_instance.addNotebookToHDFS()
            except Exception as e:
                print(f"Failed to add the notebook to HDFS: {e}")
        """
        return self._add_notebook_to_hdfs(self.workspace, self.dataset,self.id,self.connection.user)
    
    def copy_from_hdfs_to_container(self):   # Untested #
        """
        ## UNTESTED ##
        Adds a notebook to the local container. 

        Args:
            None
    
        Returns:
            bool: True if the notebook was successfully copied.

        Raises:
            Exception: If there's a failure in adding the notebook to the container or if there are issues with the Docker container.

        Description:
            This method first creates a temporary directory to store the notebook. It then retrieves the notebook from a Docker container and subsequently uploads it to the Hadoop Distributed File System (HDFS).

        Notes:
            - The method cleans up (deletes) the temporary directory after the notebook is added to HDFS.

        Example:
            try:
                notebook_instance.copy_from_hdfs_to_container()
            except Exception as e:
                print(f"Failed to copy the notebook to the container: {e}")
        """
        return self._copy_from_hdfs_to_container(self.workspace, self.dataset,self.id,self.connection.user)
    
    def get_code(self) -> str:
        """
        Retrieves the code of the notebook.

        Args:
            None

        Returns:
            str: The code of the notebook.

        Raises:
            Exception: If there's a failure in retrieving the notebook code.

        Example:
            code = notebook_instance.get_code()
            print(code)
        """
        return self._get_notebook_code(self.workspace, self.id)
    
    def _get_notebook_json(self, workspace_id, dataset_id, notebook_id):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/notebooks/{notebook_id}"
        
        response = self.connection._get_resource(resource_path)
        if response is None:
            raise Exception("Notebook could not be retrieved. Set the logger level to \"Error\" or below to get more detailed information.")

        return response
    
    def _get_notebook_code(self, workspace_id, notebook_id):
        resource_path = f"/api/v1/workspaces/{workspace_id}/notebooks/{notebook_id}/code"
        payload = {
            "user_token": self.connection.jupyter_token
        }

        response = self.connection._get_resource(resource_path, payload)
    
        if response is None:
            raise Exception("Notebook code could not be retrieved. Set the logger level to \"Error\" or below to get more detailed information.")
        
        return response.get("code", "")

    
    def _update_notebook(self, workspace_id, dataset_id, notebook_id, title, description, is_public, version):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/notebooks/{notebook_id}"
        payload =  {  
            "title": None,
            "description": None,
            "is_public": None,
            "version": None
        }
        
        # Get the original Notebook
        notebook = self._get_notebook_json(workspace_id,dataset_id,notebook_id)

        # Reinstate the old values from the original Notebook
        for key in payload:
            payload[key] = notebook.get(key)

        # If a new value for a parameter is given to this method, assign it
        if title is not None:
            payload["title"] = title
        if description is not None:
            payload["description"] = description
        if is_public is not None:
            payload["is_public"] = is_public
        if version is not None:
            payload["version"] = version

        response = self.connection._put_resource(resource_path, payload)
        if response is None:
            raise Exception("The Notebook could not be updated. Set the logger level to \"Error\" or below to get more detailed information.")

        self.logger.info("The Notebook was updated successfully.")
        return response

    def _delete_notebook(self, workspace_id, dataset_id, notebook_id):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/notebooks/{notebook_id}"

        response = self.connection._delete_resource(resource_path)
        if response is None:
            raise Exception("Notebook could not be deleted. Set the logger level to \"Error\" or below to get more detailed information.")
        
        self.logger.info("The Notebook was deleted successfully.")

        return True
    
    def _add_notebook_to_hdfs(self, workspace_id,dataset_id, item_id, user):

        resource_path = f"/api/v1/jupyterhub/addNotebookToHDFS"
        payload = {
            "workspace_id": workspace_id,
            "session_id": self.connection.session_id,
            "dataset_id": dataset_id,
            "item_id": item_id,
            "username": user
        }

        response = self.connection._post_resource(resource_path, payload)
        if response is None:
            raise Exception("Failed to copy the notebook to the HDFS. Set the logger level to \"Error\" or below to get more detailed information.")
        
        self.logger.info("The Notebook was copied successfully to the HDFS.")
        return True

    def _copy_from_hdfs_to_container(self, workspace_id,dataset_id, item_id, user):
        resource_path = f"/api/v1/jupyterhub/copyNbFromHDFStoContainer"
        payload = {
            "workspace_id": workspace_id,
            "session_id": self.connection.session_id,
            "dataset_id": dataset_id,
            "item_id": item_id,
            "username": user
        }

        response = self.connection._post_resource(resource_path, payload)
        if response is None:
            raise Exception("Failed to copy the notebook to the Jupyterhub container. Set the logger level to \"Error\" or below to get more detailed information.")
        
        self.logger.info("The Notebook was copied successfully to the Jupyterhub container.")
        return True