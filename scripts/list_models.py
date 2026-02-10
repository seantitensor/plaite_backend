import os

from dotenv import load_dotenv
from google import genai

# Load .env
load_dotenv()

api_key = os.getenv("GOOGLE_API_KEY")
if not api_key:
    print("Error: GOOGLE_API_KEY not set")
    exit(1)

client = genai.Client(api_key=api_key)

print("Available models:\n")
models = client.models.list()

for model in models:
    print(f"Name: {model.name}")
    print(f"Description: {model.description}")
    print()
