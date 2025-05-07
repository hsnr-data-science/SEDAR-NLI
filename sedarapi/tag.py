from .commons import Commons

from cache.cacheable import cacheable

@cacheable
class Tag:
    """
    A class to represent a Tag in a dataset.

    Attributes:
        connection (Commons): The connection object to the workspace.
        workspace (str): The workspace ID.
        dataset (str): The dataset ID.
        id (str): The tag ID.
        logger (Logger): The logger object.
        content (dict): The JSON content of the tag.

    """

    def __init__(self, connection: Commons, workspace_id: str, dataset_id: str, tag_id: str):
        self.connection = connection
        self.workspace = workspace_id
        self.dataset = dataset_id
        self.id = tag_id
        self.logger = self.connection.logger
        self.content = self._get_tag_json(self.workspace, self.dataset, self.id)
    
    def delete(self):
        """
        Deletes the current tag from the associated dataset.

        Returns:
            bool: `True` if the tag was successfully deleted.

        Raises:
            Exception: If there's a failure in deleting the tag.

        Description:
            This method deletes an existing tag from the associated dataset in the workspace by sending a DELETE request to the appropriate API endpoint `/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/tags/{tag_id}`.

        Note:
            Deleting a tag is irreversible. Ensure that you have proper backups or are sure about the deletion before executing this method.

        Example:
            ```python
            tag_instance = dataset.get_all_tags()[0]
            try:
                success = tag_instance.delete()
                if success:
                    print("Tag successfully deleted!")
                else:
                    print("Failed to delete tag.")
            except Exception as e:
                print(f"Error: {e}")
            ```
        """
        return self._delete_tag(self.workspace,self.dataset, self.id)
    
    def _get_tag_json(self, workspace_id, dataset_id, tag_id):
        if dataset_id:
            resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/tags/{tag_id}"
        else:
            resource_path = f"/api/v1/workspaces/{workspace_id}/tags/{tag_id}"
        
        response = self.connection._get_resource(resource_path)
        if response is None:
            raise Exception("Tag could not be retrieved. Set the logger level to \"Error\" or below to get more detailed information.")

        return response
    
    def _delete_tag(self, workspace_id, dataset_id, tag_id):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/tags/{tag_id}"

        response = self.connection._delete_resource(resource_path)
        if response is None:
            raise Exception("The Tag could not be deleted. Set the logger level to \"Error\" or below to get more detailed information.")
        
        self.logger.info("The Tag was deleted successfully.")

        return True
