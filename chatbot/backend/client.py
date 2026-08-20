import os
import re
import time
import math
from google import genai
from google.genai.errors import ClientError
from dotenv import load_dotenv

# Load environment variables relative to the script location
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(dotenv_path=env_path, override=True)

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.5-flash-lite")

# Initialize the Gemini GenAI Client lazily to prevent startup crashes if key is not set
_client = None

def get_gemini_client():
    global _client
    if _client is None:
        # Check both the loaded env variable and the direct environment
        key = GEMINI_API_KEY or os.getenv("GEMINI_API_KEY")
        if key:
            key = str(key).strip()
            
        if not key or key.lower() in ("none", "null", "undefined", ""):
            raise ValueError(
                "GEMINI_API_KEY environment variable is missing or invalid. "
                "Please add a valid Gemini API key to your Render Environment Variables."
            )
        _client = genai.Client(api_key=key)
    return _client

def generate_llm(system_prompt: str, user_message: str, model: str = None) -> str:
    """Helper to query the Gemini API with system instructions, including robust rate limit retries."""
    if not model:
        model = GEMINI_MODEL
    config = {"system_instruction": system_prompt}
    
    client = get_gemini_client()
    
    max_retries = 8
    backoff = 3.0
    
    for attempt in range(max_retries):
        try:
            response = client.models.generate_content(
                model=model,
                contents=user_message,
                config=config,
            )
            return response.text.strip()
        except ClientError as e:
            # Handle rate limits (429 RESOURCE_EXHAUSTED) gracefully with smart waiting
            if e.code == 429 or "429" in str(e):
                if attempt < max_retries - 1:
                    # Attempt to parse the API recommended wait time (e.g., "Please retry in 11.37s.")
                    match = re.search(r"Please retry in (\d+\.?\d*)s", str(e))
                    if match:
                        sleep_time = float(match.group(1)) + 1.5
                    else:
                        sleep_time = backoff * (2 ** attempt)
                    
                    # Ensure sleep time is reasonable and not negative
                    sleep_time = max(1.0, sleep_time)
                    print(f"[Rate Limit] 429 Resource Exhausted. Waiting {sleep_time:.2f}s before retry (Attempt {attempt+1}/{max_retries})...")
                    time.sleep(sleep_time)
                    continue
            raise e
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(2.0)
                continue
            raise e
