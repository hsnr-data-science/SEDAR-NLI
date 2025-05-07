from __future__ import annotations

from .commons import Commons

from cache.cacheable import cacheable, exclude_from_cacheable

@cacheable
class SemanticMapping:
    """
    This class provides methods to create and manage semantic mappings (OBDA).
    These mappings are created and modified using PLASMA.
    A mapping can be created from a semantic modeling.
    A mapping can then be used to perform OBDA and execute SPARQL queries.
    """

    def __init__(self, connection: Commons, workspace_id: str, mapping_id: str):
        self.connection = connection
        self.workspace = workspace_id
        self.id = mapping_id
        self.logger = self.connection.logger
        self.content = self._get_mapping_json(self.workspace, self.id)

        # Extract some members from the "content" attribute
        self.name = self.content.get("name", "")
        self.description = self.content.get("description", "")
        self.mappings_file = self.content.get("mappings_file", "")
        # ...

    @exclude_from_cacheable
    def execute_obda_query(self, query: str) -> dict:
        """
        Executes a SPARQL query to perform OBDA on the semantic mapping.

        Args:
            query (str): The SPARQL query to execute.

        Returns:
            dict: The result of the query.
        """
        return self._execute_obda_query(query)
  
    def _get_mapping_json(self, workspace_id, mapping_id):
        resource_path = f"/api/v1/workspaces/{workspace_id}/modeling/serializer/{mapping_id}"

        response = self.connection._get_resource(resource_path)
        if response is None:
            raise Exception("Failed to fetch Modeling. Set the logger level to \"Error\" or below to get more detailed information.")
        
        return response

    def _execute_obda_query(self, query):
        resource_path = f"/api/v1/workspaces/{self.workspace}/obda/squerall"
        payload = {
            "mapping_id": self.id,
            "query_string": query
        }

        response = self.connection._post_resource(resource_path, payload)
        if response is None:
            raise Exception("Failed to execute OBDA query. Set the logger level to \"Error\" or below to get more detailed information.")
        
        return response