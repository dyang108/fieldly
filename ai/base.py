from abc import ABC, abstractmethod
from typing import List, Dict, Any


class SchemaGenerator(ABC):
    """Abstract interface for schema generation models"""
    
    @abstractmethod
    def generate_schema(self, conversation: List[Dict[str, str]]) -> Dict[str, Any]:
        """
        Generate a schema from conversation history
        
        Args:
            conversation: List of conversation messages, each with 'role' and 'content'
                role can be 'user', 'assistant', or 'system'
            
        Returns:
            Dict with schema info including schema, message, suggested_name
        """
        pass
    
    def prepare_conversation(self, conversation: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """
        Prepare conversation by adding system message if needed
        
        Args:
            conversation: Original conversation history
            
        Returns:
            Prepared conversation history
        """
        messages = conversation.copy()
        
        # Add a system message if not present
        if not any(msg["role"] == "system" for msg in messages):
            messages.insert(0, {
                "role": "system",
                "content": """You are a helpful assistant that generates JSON schemas based on natural language descriptions. 
                When asked to create a schema:
                1. Analyze the user's requirements carefully
                2. Generate a comprehensive JSON schema that captures all the fields mentioned
                3. Include appropriate data types, descriptions, and constraints
                4. Return your response as valid JSON
                5. Structure your response with a 'message' field containing your explanation
                   and a 'schema' field containing the JSON schema object
                6. Include a 'suggested_name' field with a good name for this schema
                """
            })
        
        # If the conversation doesn't end with a specific request for a schema, add it
        if not messages[-1]["content"].lower().strip().endswith("schema"):
            messages.append({
                "role": "user",
                "content": "Based on our conversation, please generate a complete JSON schema."
            })
            
        return messages 