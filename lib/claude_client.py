import base64
import time
from pathlib import Path

import anthropic


def extract_from_pdf(
    pdf_path: str | Path,
    prompt: str,
    model: str = "claude-sonnet-4-6",
) -> str:
    """Send a PDF to Claude and return the raw text response."""
    pdf_bytes = Path(pdf_path).read_bytes()
    b64_pdf = base64.standard_b64encode(pdf_bytes).decode("utf-8")

    client = anthropic.Anthropic()

    message_content = [
        {
            "type": "document",
            "source": {
                "type": "base64",
                "media_type": "application/pdf",
                "data": b64_pdf,
            },
        },
        {
            "type": "text",
            "text": prompt,
        },
    ]

    delays = [5, 15, 45]
    for attempt, delay in enumerate(delays + [None]):
        try:
            response = client.messages.create(
                model=model,
                max_tokens=4096,
                messages=[{"role": "user", "content": message_content}],
            )
            return response.content[0].text
        except anthropic.RateLimitError:
            if delay is None:
                raise
            print(f"  Rate limited. Retrying in {delay}s (attempt {attempt + 1}/3)...")
            time.sleep(delay)
