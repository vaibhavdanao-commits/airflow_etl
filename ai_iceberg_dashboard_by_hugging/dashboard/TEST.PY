
import os
from pathlib import Path
from dotenv import load_dotenv
from openai import OpenAI

HERE = Path(__file__).resolve().parent
load_dotenv(HERE / ".env", override=True)

token = os.getenv("HF_TOKEN", "").strip()
if not token:
    raise RuntimeError("HF_TOKEN is missing.")

client = OpenAI(
    base_url=os.getenv(
        "HF_BASE_URL",
        "https://router.huggingface.co/v1",
    ),
    api_key=token,
    timeout=120,
)

try:
    response = client.chat.completions.create(
        model=os.getenv(
            "HF_MODEL",
            "Qwen/Qwen2.5-7B-Instruct",
        ),
        messages=[
            {
                "role": "user",
                "content": (
                    "Reply with exactly: "
                    "Hugging Face API connection successful"
                ),
            }
        ],
        max_tokens=50,
        temperature=0,
    )

    print("SUCCESS: Hugging Face API is responding.")
    print("Response:", response.choices[0].message.content)

except Exception as exc:
    print("Hugging Face API test failed.")
    print(type(exc).__name__, str(exc))
