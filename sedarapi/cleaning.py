from __future__ import annotations
from typing import Any
import json
import os

from .commons import Commons

from cache.cacheable import cacheable

@cacheable
class DatasetCleaning:
    """
    The `DatasetCleaning` class provides methods to interact with the cleaning and preprocessing functionalities of a dataset.

    Attributes:
        connection (Commons): The connection object used to communicate with the SEDAR API.
        workspace (str): The ID of the workspace containing the dataset.
        dataset (str): The ID of the dataset to be cleaned.
        dataset_version (str): The version of the dataset to be cleaned.
        logger (Logger): The logger object used to log messages.
        constraints (list): A list of constraints to be applied to the dataset.
        filters (list): A list of filters to be applied to the dataset.
    """

    def __init__(self, connection: Commons, workspace_id: str, dataset_id: str, dataset_version: str):
        self.connection = connection
        self.workspace = workspace_id
        self.dataset = dataset_id
        self.dataset_version = dataset_version
        self.logger = self.connection.logger
        self.constraints = []
        self.filters = []

    def get_constraint_suggestions(self) -> list[ConstraintSuggestion]:
        """
        Retrieves constraint suggestions for the current dataset.

        Returns:
            list[ConstraintSuggestions]: A list of constraint suggestions.
                Each suggestion provides details such as the constraint name, column name, current value,
                description, suggesting rule, rule description, and the code for applying the constraint.
                The properties can be reached via the class members (name, column, current_value, description, suggesting_rule, rule_description, code).

        Raises:
            Exception: If there's an error while fetching the constraint suggestions.

        Description:
            This method fetches constraint suggestions for a specific dataset by sending a 
            GET request to the '/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/cleaning/suggest' endpoint.

            The constraints are generated based on rules applied to the dataset, including but not limited to:
            - Completeness checks
            - Uniqueness checks
            - Non-negative number checks
            - Retaining type checks

        Notes:
            - Ensure that you have the required permissions to fetch the constraint suggestions.
            - Constraint suggestions are generated based on rules applied by the `ConstraintSuggestionRunner` class.

        Example:
        ```python
        cleaning = dataset_instance.get_cleaner()
        try:
            suggestions = cleaning_instance.get_constraint_suggestions()
            for suggestion in suggestions:
                print(suggestion.name, suggestion.code)
        except Exception as e:
            print(e)
        ```
        """
        constraints_info = self._get_dataset_cleaning_suggestions_json(self.workspace, self.dataset, self.dataset_version)
        return [ConstraintSuggestion(constraint_info) for constraint_info in constraints_info]
    
    def get_dataset_validations(self) -> dict:
        """
        Fetches the validation results for the constraints applied to the dataset.

        Returns:
            dict: A dictionary containing details about the constraints requested and their validation results.

        Raises:
            Exception: If there's an error retrieving the validation results.

        Description:
            This method fetches the validation results of the constraints that were applied to the dataset. The results 
            provide insights into which constraints passed and which failed, allowing the user to understand the quality 
            of their data in the context of the specified constraints. The method communicates with the API endpoint 
            `/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/cleaning/verify`.

        Notes:
            - The validation results are based on the constraints that were previously set and checked against the dataset.
            - Each validation result will provide a status (e.g., "PASS" or "FAIL") and other relevant details.

        Example:
        ```python
        cleaning = dataset_instance.get_cleaner()
        try:
            validation_results = cleaning.get_dataset_validations()
            for detail in validation_results["response"]["details"]:
                print(f"Constraint: {detail['constraint']}, Status: {detail['status']}")
        except Exception as e:
            print(e)
        ```
        """
        return self._get_dataset_validated_constraints_json(self.workspace, self.dataset, self.dataset_version)
    
    def delete_dataset_validations(self) -> bool: # seems to be unfunctional, check serverside implementation # 
        """
        Deletes the validation results of the dataset for the specified version.

        Returns:
            bool: True if the applied constraints of the dataset were successfully deleted.

        Raises:
            Exception: If there's an error deleting the validation results.

        Description:
            This method deletes the validation results of the dataset for a specific version. It is useful in scenarios 
            where the user might want to remove the results for any reason, such as erroneous validation, or to free up 
            space. The method communicates with the API endpoint `/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/cleaning/verify`.

        Notes:
            - Make sure you have the necessary permissions to delete the validation results.
            - Once deleted, the validation results cannot be recovered.

        Example:
        ```python
        dataset_instance = workspace.get_all_datasets()[0]
        cleaning = dataset_instance.get_cleaner()
        try:
            cleaning.delete_dataset_validations()
            print("Validation results deleted successfully.")
        except Exception as e:
            print(e)
        ```
        """
        return self._delete_dataset_validated_constraints(self.workspace, self.dataset, self.dataset_version)

    def validate_local_constraints(self) -> dict:
        """
        Validates the dataset against the local constraints.

        Returns:
            dict: A dictionary containing summary and detailed results of the validation process.

        Raises:
            Exception: If there's an error with the validation process.

        Description:
            This method triggers the validation process for a dataset against locally defined constraints 
            by sending a POST request to the '/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/cleaning/verify' endpoint. 
            The validation results provide an overall summary (e.g., whether the validation passed or failed) 
            and details about each constraint validation.

        Notes:
            - Make sure the dataset has been properly cleaned and preprocessed before validating against constraints.
            - This method only works with constraints that have been locally defined.

        Example:
        ```python
        cleaning = dataset_instance.get_cleaner()
        try:
            validation_results = cleaning.validate_local_constraints()
            print(validation_results["summary"])
            for detail in validation_results["details"]:
                print(detail)
        except Exception as e:
            print(e)
        ```
        """
        return self._validate_dataset_cleaning_constraints(self.workspace, self.dataset, self.dataset_version, self.constraints)

    def get_filter_suggestions(self, add_to_local_filters=True) -> list[dict]:
        """
        Retrieves suggestions for filters based on the local constraints.

        Returns:
            dict: A dictionary containing filter suggestions for the dataset.

        Raises:
            Exception: If there's an error with retrieving the filter suggestions.

        Description:
            This method fetches filter suggestions based on the locally stored constraints 
            by sending a GET request to the '/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/cleaning/filters' endpoint. 
            Each filter suggestion provides a recommended type, the associated column, 
            and the filter expression that can be applied to the dataset.

        Notes:
            - These suggestions are generated based on the previously generated constraints within this class instance.
            - Always review filter suggestions before applying them to ensure they are appropriate for the dataset's context.

        Example:
        ```python
        cleaning = dataset_instance.get_cleaner()
        
        try:
            cleaner.add_is_complete_constraint("Identifier")
            filter_suggestions = cleaning.get_filter_suggestions()
            for suggestion in filter_suggestions:
                print(f"Column: {suggestion['column']}, Filter: {suggestion['filter_expression']}")
        except Exception as e:
            print(e)
        ```
        """
        filters = self._get_dataset_filter_suggestions_json(self.workspace, self.dataset, self.dataset_version, self.constraints)

        if add_to_local_filters is True:
            for filter_ in filters:
                self.filters.append(filter_)
    
        return filters
    
    def execute_local_filters(self, datasource_definition: Any) -> bool:
        """
        Executes the locally stored filters and creates a new version of the dataset using the provided datasource definition.

        Args:
            datasource_definition (dict): Definition for the new dataset version, including details such as name, source files, and more.

        Returns:
            bool: True if the filter was successfully executed.

        Raises:
            Exception: If there's an error during the filter execution or dataset version creation.

        Description:
            This method applies the filters that have been added to the cleaner instance and then utilizes 
            the provided datasource definition to create a new version of the dataset. The process involves:
            1. Sending a PUT request to the '/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/cleaning/filters' endpoint.
            2. Applying each filter on the dataset.
            3. Using the provided datasource definition to create a new dataset version.

        Notes:
            - Ensure that the datasource definition is valid and correctly describes the new dataset version.
            - Applying filters will modify the dataset, so be cautious and ensure that all filters are accurate.

        Example:
        ```python
        cleaning = dataset_instance.get_cleaner()
        cleaning.add_is_complete_constraint("Identifier")
        }
        try:
            cleaning.execute_local_filters(datasource_definition)
            print("Filters executed and new dataset version created.")
        except Exception as e:
            print(e)
        ```
        """
        return self._execute_dataset_filters(self.workspace, self.dataset, self.dataset_version, self.filters, datasource_definition)

    def get_local_constraints(self) -> list[dict]:
        """
        Retrieves the list of constraints stored locally in the cleaning instance.
        
        Returns:
            list[dict]: A list of dictionaries, each representing a constraint.
        """
        return self.constraints
    
    def delete_local_constraints(self) -> bool:
        """
        Removes all constraints from the list of local constraints.
        
        Returns:
            Bool: True, if the constraints were deleted.
        """
        self.constraints = []
        return True
    
    def get_local_filters(self) -> list[dict]:
        """
        Retrieves the list of filters stored locally in the cleaning instance.
        
        Returns:
            list[dict]: A list of dictionaries, each representing a filter.
        """
        return self.filters
    
    def delete_local_filters(self) -> bool:
        """
        Removes all filters from the list of local filters.
        
        Returns:
            bool: True if the filters were successfully removed.
        """
        self.filters = []
        return True

    def add_is_complete_constraint(self, column: str) -> DatasetCleaning:
        """
        Adds an 'isComplete' constraint to the local constraints list.

        Args:
            column (str): Name of the column to which the constraint should be applied.

        Returns:
            DatasetCleaning: Returns the instance of the `DatasetCleaning` class to allow for method chaining.
        """
        constraint = {
            "type": "isComplete",
            "params": {
                "column": column
            }
        }
        self.constraints.append(constraint)
        return self

    def add_is_unique_constraint(self, column: str) -> DatasetCleaning:
        """
        Adds an 'isUnique' constraint to the local constraints list.

        Args:
            column (str): Name of the column to which the constraint should be applied.

        Returns:
            DatasetCleaning: Returns the instance of the `DatasetCleaning` class to allow for method chaining.
        """
        constraint = {
            "type": "isUnique",
            "params": {
                "column": column
            }
        }
        self.constraints.append(constraint)
        return self

    def add_is_non_negative_constraint(self, column: str) -> DatasetCleaning:
        """
        Adds an 'isNonNegative' constraint to the local constraints list.

        Args:
            column (str): Name of the column to which the constraint should be applied.

        Returns:
            DatasetCleaning: Returns the instance of the `DatasetCleaning` class
        """
        constraint = {
            "type": "isNonNegative",
            "params": {
                "column": column
            }
        }
        self.constraints.append(constraint)
        return self

    def add_has_size_constraint(self, operator: str, value: int) -> DatasetCleaning:
        """
        Adds a 'hasSize' constraint to the local constraints list.

        Args:
            operator (str): The comparison operator to use for the constraint (e.g., ">=", "<=").
            value (int): The size value to compare against.

        Returns:
            DatasetCleaning: Returns the instance of the `DatasetCleaning` class to allow for method chaining.
        """
        constraint = {
            "type": "hasSize",
            "params": {
                "operator": operator,
                "value": value
            }
        }
        self.constraints.append(constraint)
        return self

    def add_is_positive_constraint(self, column: str) -> DatasetCleaning:
        """
        Adds an 'isPositive' constraint to the local constraints list.

        Args:
            column (str): Name of the column to which the constraint should be applied.

        Returns:
            DatasetCleaning: Returns the instance of the `DatasetCleaning` class to allow for method chaining.
        """
        constraint = {
            "type": "isPositive",
            "params": {
                "column": column
            }
        }
        self.constraints.append(constraint)
        return self

    def add_contains_credit_card_number_constraint(self, column: str) -> DatasetCleaning:
        """
        Adds a 'containsCreditCardNumber' constraint to the local constraints list.

        Args:
            column (str): Name of the column to which the constraint should be applied.

        Returns:
            DatasetCleaning: Returns the instance of the `DatasetCleaning` class to allow for method chaining.
        """
        constraint = {
            "type": "containsCreditCardNumber",
            "params": {
                "column": column
            }
        }
        self.constraints.append(constraint)
        return self

    def add_contains_email_constraint(self, column: str) -> DatasetCleaning:
        """
        Adds a 'containsEmail' constraint to the local constraints list.

        Args:
            column (str): Name of the column to which the constraint should be applied.

        Returns:
            DatasetCleaning: Returns the instance of the `DatasetCleaning` class to allow for method chaining.
        """
        constraint = {
            "type": "containsEmail",
            "params": {
                "column": column
            }
        }
        self.constraints.append(constraint)
        return self

    def add_contains_social_security_number_constraint(self, column: str) -> DatasetCleaning:
        """
        Adds a 'containsSocialSecurityNumber' constraint to the local constraints list.

        Args:
            column (str): Name of the column to which the constraint should be applied.

        Returns:
            DatasetCleaning: Returns the instance of the `DatasetCleaning` class to allow for method chaining.
        """
        constraint = {
            "type": "containsSocialSecurityNumber",
            "params": {
                "column": column
            }
        }
        self.constraints.append(constraint)
        return self

    def add_contains_url_constraint(self, column: str) -> DatasetCleaning:
        """
        Adds a 'containsURL' constraint to the local constraints list.

        Args:
            column (str): Name of the column to which the constraint should be applied.

        Returns:
            DatasetCleaning: Returns the instance of the `DatasetCleaning` class.
        """
        constraint = {
            "type": "containsURL",
            "params": {
                "column": column
            }
        }
        self.constraints.append(constraint)
        return self
    
    def add_has_completeness_constraint(self, operator: str, value: int) -> DatasetCleaning:
        """
        Adds a 'hasCompleteness' constraint to the local constraints list.

        Args:
            operator (str): The comparison operator to use for the constraint (e.g., ">=", "<=").
            value (int): The size value to compare against.

        Returns:
            DatasetCleaning: Returns the instance of the `DatasetCleaning` class to allow for method chaining.
        """
        constraint = {
            "type": "hasCompleteness",
            "params": {
                "operator": operator,
                "value": value
            }
        }
        self.constraints.append(constraint)
        return self
    
    def add_has_entropy_constraint(self, operator: str, value: int) -> DatasetCleaning:
        """
        Adds a 'hasEntropy' constraint to the local constraints list.

        Args:
            operator (str): The comparison operator to use for the constraint (e.g., ">=", "<=").
            value (int): The size value to compare against.

        Returns:
            DatasetCleaning: Returns the instance of the `DatasetCleaning` class to allow for method chaining.
        """
        constraint = {
            "type": "hasEntropy",
            "params": {
                "operator": operator,
                "value": value
            }
        }
        self.constraints.append(constraint)
        return self
    
    def add_has_min_length_constraint(self, operator: str, value: int) -> DatasetCleaning:
        """
        Adds a 'hasMinLength' constraint to the local constraints list.

        Args:
            operator (str): The comparison operator to use for the constraint (e.g., ">=", "<=").
            value (int): The size value to compare against.

        Returns:
            DatasetCleaning: Returns the instance of the `DatasetCleaning` class to allow for method chaining.
        """
        constraint = {
            "type": "hasMinLength",
            "params": {
                "operator": operator,
                "value": value
            }
        }
        self.constraints.append(constraint)
        return self
    
    def add_has_max_length_constraint(self, operator: str, value: int) -> DatasetCleaning:
        """
        Adds a 'hasMaxLength' constraint to the local constraints list.

        Args:
            operator (str): The comparison operator to use for the constraint (e.g., ">=", "<=").
            value (int): The size value to compare against.

        Returns:
            DatasetCleaning: Returns the instance of the `DatasetCleaning` class to allow for method chaining.
        """
        constraint = {
            "type": "hasMaxLength",
            "params": {
                "operator": operator,
                "value": value
            }
        }
        self.constraints.append(constraint)
        return self
    
    def add_has_min_constraint(self, operator: str, value: int) -> DatasetCleaning:
        """
        Adds a 'hasMin' constraint to the local constraints list.

        Args:
            operator (str): The comparison operator to use for the constraint (e.g., ">=", "<=").
            value (int): The size value to compare against.

        Returns:
            DatasetCleaning: Returns the instance of the `DatasetCleaning` class to allow for method chaining.
        """
        constraint = {
            "type": "hasMin",
            "params": {
                "operator": operator,
                "value": value
            }
        }
        self.constraints.append(constraint)
        return self
    
    def add_has_max_constraint(self, operator: str, value: int) -> DatasetCleaning:
        """
        Adds a 'hasMax' constraint to the local constraints list.

        Args:
            operator (str): The comparison operator to use for the constraint (e.g., ">=", "<=").
            value (int): The size value to compare against.

        Returns:
            DatasetCleaning: Returns the instance of the `DatasetCleaning` class to allow for method chaining.
        """
        constraint = {
            "type": "hasMax",
            "params": {
                "operator": operator,
                "value": value
            }
        }
        self.constraints.append(constraint)
        return self
    
    def add_has_mean_constraint(self, operator: str, value: int) -> DatasetCleaning:
        """
        Adds a 'hasMean' constraint to the local constraints list.

        Args:
            operator (str): The comparison operator to use for the constraint (e.g., ">=", "<=").
            value (int): The size value to compare against.

        Returns:
            DatasetCleaning: Returns the instance of the `DatasetCleaning` class to allow for method chaining.
        """
        constraint = {
            "type": "hasMean",
            "params": {
                "operator": operator,
                "value": value
            }
        }
        self.constraints.append(constraint)
        return self
    
    def add_has_sum_constraint(self, operator: str, value: int) -> DatasetCleaning:
        """
        Adds a 'hasSum' constraint to the local constraints list.

        Args:
            operator (str): The comparison operator to use for the constraint (e.g., ">=", "<=").
            value (int): The size value to compare against.

        Returns:
            DatasetCleaning: Returns the instance of the `DatasetCleaning` class to allow for method chaining.
        """
        constraint = {
            "type": "hasSum",
            "params": {
                "operator": operator,
                "value": value
            }
        }
        self.constraints.append(constraint)
        return self
    
    def add_has_standard_deviation_constraint(self, operator: str, value: int) -> DatasetCleaning:
        """
        Adds a 'hasStandardDeviation' constraint to the local constraints list.

        Args:
            operator (str): The comparison operator to use for the constraint (e.g., ">=", "<=").
            value (int): The size value to compare against.

        Returns:
            DatasetCleaning: Returns the instance of the `DatasetCleaning` class to allow for method chaining.
        """
        constraint = {
            "type": "hasEntropy",
            "params": {
                "operator": operator,
                "value": value
            }
        }
        self.constraints.append(constraint)
        return self
    
    def add_has_approx_count_disctinct_constraint(self, operator: str, value: int) -> DatasetCleaning:
        """
        Adds a 'hasApproxCountDistinct' constraint to the local constraints list.

        Args:
            operator (str): The comparison operator to use for the constraint (e.g., ">=", "<=").
            value (int): The size value to compare against.

        Returns:
            DatasetCleaning: Returns the instance of the `DatasetCleaning` class to allow for method chaining.
        """
        constraint = {
            "type": "hasApproxCountDistinct",
            "params": {
                "operator": operator,
                "value": value
            }
        }
        self.constraints.append(constraint)
        return self
    
    def add_has_data_type_constraint(self, operator: str, value: int) -> DatasetCleaning:
        """
        Adds a 'hasDataType' constraint to the local constraints list.

        Args:
            operator (str): The comparison operator to use for the constraint (e.g., ">=", "<=").
            value (string): The data type to check (e.g. "Null", "Fractional", "Integral", "Boolean", "String", "Numeric")

        Returns:
            DatasetCleaning: Returns the instance of the `DatasetCleaning` class to allow for method chaining.
        """
        constraint = {
            "type": "hasDataType",
            "params": {
                "operator": operator,
                "value": value
            }
        }
        self.constraints.append(constraint)
        return self
    
    def add_is_contained_in_constraint(self, operator: str, value: int) -> DatasetCleaning:
        """
        Adds a 'isContainedIn' constraint to the local constraints list.

        Args:
            operator (str): The comparison operator to use for the constraint (e.g., ">=", "<=").
            value (string): Comma separated values (e.g. "a,b,c")

        Returns:
            DatasetCleaning: Returns the instance of the `DatasetCleaning` class to allow for method chaining.
        """
        constraint = {
            "type": "isContainedIn",
            "params": {
                "operator": operator,
                "value": value
            }
        }
        self.constraints.append(constraint)
        return self


    # ... more Constraints could be implemented here

    def add_custom_constraint(self, constraint_type: str, params: dict) -> DatasetCleaning:
        """
        Adds a custom constraint to the local constraints list.

        Args:
            constraint_type (str): The type of the custom constraint.
            params (dict): A dictionary of parameters required for the custom constraint.

        Returns:
            DatasetCleaning: Returns the instance of the `DatasetCleaning` class to allow for method chaining.

        Raises:
            ValueError: If the input format for the custom constraint is invalid.
        """
        # Check if the constraint type and parameters are formatted correctly
        if not isinstance(constraint_type, str) or not isinstance(params, dict):
            raise ValueError("Ungültiges Format für den benutzerdefinierten Constraint.")
        
        # create the constraint
        constraint = {
            "type": constraint_type,
            "params": params
        }
        
        # add it to the local constraints
        self.constraints.append(constraint)
        return self
    
    def add_raw_constraint(self, constraint: dict) -> DatasetCleaning:
        """
        Adds a raw constraint to the local constraints list.

        Args:
            constraint (dict): A dictionary representing the constraint.

        Returns:
            DatasetCleaning: Returns the instance of the `DatasetCleaning` class to allow for method chaining.

        Raises:
            ValueError: If the input format for the raw constraint is invalid.
        """
        # Validate the format of the constraint
        if not isinstance(constraint, dict):
            raise ValueError("Invalid format for the raw constraint.")
        
        # Check if the dictionary contains the required keys
        if "type" not in constraint or "params" not in constraint:
            raise ValueError("Constraint must have 'type' and 'params' keys.")

        # Check if 'params' is a dictionary and contains the required 'column' key
        if not isinstance(constraint["params"], dict):
            raise ValueError("'params' must be a dictionary.")
        
        if "column" not in constraint["params"]:
            raise ValueError("'params' must contain a 'column' key.")

        # Add the constraint to the local constraints
        self.constraints.append(constraint)
        return self
    
    def add_custom_filter(self, filter_type: str, column: str, filter_expression: str) -> DatasetCleaning:
        """
        Adds a custom filter to the local filters list.

        Args:
            filter_type (str): The type of the custom filter.
            column (str): The column to which the filter should be applied.
            filter_expression (str): The expression that defines the filter.

        Returns:
            DatasetCleaning: Returns the current instance of the DatasetCleaning class.

        Raises:
            ValueError: If the input format for the custom filter is invalid.
        """
        # check the filter syntax
        if not all(isinstance(item, str) for item in [filter_type, column, filter_expression]):
            raise ValueError("Ungültiges Format für den benutzerdefinierten Filter.")
        
        # additional syntax checking ...
        
        # create the filter
        filter_ = {
            "type": filter_type,
            "column": column,
            "filter_expression": filter_expression
        }
        
        # Zappend the filter to the locally stored filters
        self.filters.append(filter_)
        return self
    
    def _get_dataset_cleaning_suggestions_json(self, workspace_id, dataset_id, version):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/cleaning/suggest"
        payload = {
            "version": version
        }
        
        response = self.connection._get_resource(resource_path, payload)
        if response is None:
            raise Exception(f"The Cleaning suggestions for Dataset '{dataset_id}' could not be retrieved. Set the logger level to \"Error\" or below to get more detailed information.")

        self.logger.info(f"The Cleaning suggestions for Dataset '{dataset_id}' have been retrieved successfully.")
        return response["constraint_suggestions"]
    
    def _validate_dataset_cleaning_constraints(self, workspace_id, dataset_id, version, constraints):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/cleaning/verify"
        
        # Creation of the payload with the help of the handed constraints
        payload = {
            "version": version,
            "constraints": constraints
        }
    
        response = self.connection._post_resource(resource_path, payload)
        if response is None:
            raise Exception(f"The Constraints for Dataset '{dataset_id}' could not be veryfied. Set the logger level to \"Error\" or below to get more detailed information.")

        self.logger.info(f"The Constraints for Dataset '{dataset_id}' were veryfied successfully.")
        return response    
    
    def _get_dataset_validated_constraints_json(self, workspace_id, dataset_id, version):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/cleaning/verify"
        payload = {
            "version": version
        }
        
        response = self.connection._get_resource(resource_path, payload)
        if response is None:
            raise Exception(f"The validated Constraints for for Dataset '{dataset_id}' could not be retrieved. Set the logger level to \"Error\" or below to get more detailed information.")

        self.logger.info(f"The validated Constraints for for Dataset '{dataset_id}' have been retrieved successfully.")
        return response
    
    def _delete_dataset_validated_constraints(self, workspace_id, dataset_id, version):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/cleaning/verify"
        payload = {
            "version": version
        }
        
        response = self.connection._delete_resource(resource_path, payload)
        if response is None:
            raise Exception(f"The validated Constraints for for Dataset '{dataset_id}' could not be deleted. Set the logger level to \"Error\" or below to get more detailed information.")

        self.logger.info(f"The validated Constraints for Dataset '{dataset_id}' have been deleted successfully.")
        return True
    
    def _get_dataset_filter_suggestions_json(self, workspace_id, dataset_id, version, constraints):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/cleaning/filters"
        payload = {
            "version": version,
            "constraints": constraints
        }
        
        response = self.connection._post_resource(resource_path, payload)
        if response is None:
            raise Exception(f"The Filter suggestions for Dataset '{dataset_id}' could not be retrieved. Set the logger level to \"Error\" or below to get more detailed information.")

        self.logger.info(f"The Filter suggestions for Dataset '{dataset_id}' have been retrieved successfully.")
        return response["filters"]
    
    def _execute_dataset_filters(self, workspace_id, dataset_id, version, filters, datasource_definition):
        resource_path = f"/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/cleaning/filters"


        # Check if the datasource definition is a file path. 
        if isinstance(datasource_definition, str):
            # If it is a valid path, open the file and convert it to json
            if os.path.exists(datasource_definition):
                with open(datasource_definition, "r") as f:
                    datasource_definition = json.load(f)
            else:
                self.logger.error(f"File not found: {datasource_definition}")
                return None
        
        # Creation of the payload with the help of the handed constraints
        payload = {
            "datasourcedefinition": json.dumps(datasource_definition),
            "version": version,
            "filters": filters
        }
    
        response = self.connection._put_resource(resource_path, payload)
        if response is None:
            raise Exception(f"The Filters for Dataset '{dataset_id}' could not be executed. Set the logger level to \"Error\" or below to get more detailed information.")

        self.logger.info(f"The Filters for Dataset '{dataset_id}' were executed successfully.")
        return True


@cacheable
class ConstraintSuggestion:
    """ 
    The only purpose of this class is to increase the easy of use with the '/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/cleaning/suggest' call
    Please only created instances via the 'get_cleaning_suggestions()' method of an cleaner's instance.
    """
    def __init__(self, constraint_json):
        self.content = constraint_json

        # Extract some members from the "content" attribute
        self.name = self.content["constraint_name"]
        self.column = self.content["column_name"]
        self.current_value = self.content["current_value"]
        self.description = self.content["description"]
        self.suggesting_rule = self.content["suggesting_rule"]
        self.rule_description = self.content["rule_description"]
        self.code = self.content["code_for_constraint"]