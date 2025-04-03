"""
Constants and configuration values used throughout the application.
"""

# Model configuration
DEFAULT_LLM_PROVIDER = "deepseek"  # Options: "deepseek", "openai", "anthropic", "ollama", etc.

# Provider-specific configuration
PROVIDER_CONFIGS = {
    "deepseek": {
        "local": {
            "model": "deepseek-r1:14b",
            "api_url": "http://localhost:11434/api/chat",
        },
        "api": {
            "model": "deepseek-chat",
            "api_url": "https://api.deepseek.com/v1/chat/completions",
        }
    },
    "openai": {
        "api": {
            "model": "gpt-4-turbo-preview",
            "api_url": "https://api.openai.com/v1/chat/completions",
        }
    },
    "anthropic": {
        "api": {
            "model": "claude-3-opus-20240229",
            "api_url": "https://api.anthropic.com/v1/messages",
        }
    },
    "ollama": {
        "local": {
            "model": "llama3",
            "api_url": "http://localhost:11434/api/chat",
        }
    }
}

# Default temperature settings
DEFAULT_TEMPERATURE = 0.3  # Lower for more deterministic extraction 