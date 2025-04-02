import logging
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