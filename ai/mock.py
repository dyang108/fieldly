import logging
import copy
from typing import List, Dict, Any

from .base import SchemaGenerator

logger = logging.getLogger(__name__)


class MockSchemaGenerator(SchemaGenerator):
    """Mock schema generator for testing and fallback"""
    
    def generate_schema(self, conversation: List[Dict[str, str]]) -> Dict[str, Any]:
        """
        Generate a mock schema based on keywords in the conversation
        
        Args:
            conversation: List of conversation messages
            
        Returns:
            Dict with schema info
        """
        logger.info("Using mock schema generation")
        
        # Extract the last user message to determine schema type
        last_user_message = None
        for msg in reversed(conversation):
            if msg["role"] == "user":
                last_user_message = msg["content"].lower()
                break
        
        # Generate a simple schema based on keywords in the message
        schema = {
            "type": "object",
            "properties": {
                "id": {
                    "type": "integer",
                    "description": "Unique identifier"
                },
                "name": {
                    "type": "string",
                    "description": "Name field"
                },
                "created_at": {
                    "type": "string",
                    "format": "date-time",
                    "description": "Creation timestamp"
                }
            },
            "required": ["id", "name"]
        }
        
        suggested_name = "new_schema"
        
        # Add some fields based on common keywords
        if last_user_message:
            if "financial" in last_user_message or "finance" in last_user_message:
                schema["properties"]["amount"] = {
                    "type": "number",
                    "description": "Financial amount"
                }
                schema["properties"]["currency"] = {
                    "type": "string",
                    "description": "Currency code"
                }
                schema["required"].append("amount")
                suggested_name = "financial_data"
                
            elif "user" in last_user_message or "profile" in last_user_message:
                schema["properties"]["email"] = {
                    "type": "string",
                    "format": "email",
                    "description": "User email address"
                }
                schema["properties"]["age"] = {
                    "type": "integer",
                    "description": "User age"
                }
                suggested_name = "user_profile"
                
            elif "product" in last_user_message or "item" in last_user_message:
                schema["properties"]["price"] = {
                    "type": "number",
                    "description": "Product price"
                }
                schema["properties"]["description"] = {
                    "type": "string",
                    "description": "Product description"
                }
                schema["properties"]["inventory"] = {
                    "type": "integer",
                    "description": "Available inventory"
                }
                schema["required"].extend(["price", "description"])
                suggested_name = "product"
        
        return {
            "message": "Here is a generated schema based on your description. You can edit it in the schema editor.",
            "schema": schema,
            "suggested_name": suggested_name
        }

    def update_schema(self, conversation: List[Dict[str, str]], current_schema: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update an existing schema based on conversation
        
        Args:
            conversation: List of conversation messages
            current_schema: The current schema to be updated
            
        Returns:
            Dict with updated schema info
        """
        logger.info("Using mock schema update")
        
        # Make a deep copy of the current schema to avoid modifying the original
        updated_schema = copy.deepcopy(current_schema)
        
        # Extract the last user message to determine modifications
        last_user_message = None
        for msg in reversed(conversation):
            if msg["role"] == "user":
                last_user_message = msg["content"].lower()
                break
        
        if not last_user_message:
            return {
                "message": "No update instructions found in the conversation.",
                "schema": updated_schema
            }
        
        # Analyze the message for modification instructions
        if "add field" in last_user_message or "add property" in last_user_message:
            # Example: Add a new field
            if "address" in last_user_message:
                updated_schema.setdefault("properties", {})["address"] = {
                    "type": "string",
                    "description": "User address"
                }
                return {
                    "message": "Added address field to the schema.",
                    "schema": updated_schema
                }
            elif "email" in last_user_message:
                updated_schema.setdefault("properties", {})["email"] = {
                    "type": "string",
                    "format": "email",
                    "description": "Email address"
                }
                return {
                    "message": "Added email field to the schema.",
                    "schema": updated_schema
                }
            elif "date" in last_user_message or "timestamp" in last_user_message:
                updated_schema.setdefault("properties", {})["date"] = {
                    "type": "string",
                    "format": "date-time",
                    "description": "Date or timestamp"
                }
                return {
                    "message": "Added date field to the schema.",
                    "schema": updated_schema
                }
        elif "remove field" in last_user_message or "delete property" in last_user_message:
            # Example: Remove a field
            for field in ["name", "id", "email", "address", "date", "created_at"]:
                if field in last_user_message and field in updated_schema.get("properties", {}):
                    del updated_schema["properties"][field]
                    # Also remove from required if present
                    if "required" in updated_schema and field in updated_schema["required"]:
                        updated_schema["required"].remove(field)
                    return {
                        "message": f"Removed {field} field from the schema.",
                        "schema": updated_schema
                    }
        elif "make required" in last_user_message or "set required" in last_user_message:
            # Example: Make a field required
            for field in updated_schema.get("properties", {}):
                if field in last_user_message:
                    updated_schema.setdefault("required", [])
                    if field not in updated_schema["required"]:
                        updated_schema["required"].append(field)
                    return {
                        "message": f"Made {field} a required field.",
                        "schema": updated_schema
                    }
        elif "optional" in last_user_message or "not required" in last_user_message:
            # Example: Make a field optional
            for field in updated_schema.get("properties", {}):
                if field in last_user_message and "required" in updated_schema and field in updated_schema["required"]:
                    updated_schema["required"].remove(field)
                    return {
                        "message": f"Made {field} an optional field.",
                        "schema": updated_schema
                    }
        
        # Default response if no specific modification was made
        return {
            "message": "I've analyzed your request but didn't make any changes to the schema.",
            "schema": updated_schema
        } 