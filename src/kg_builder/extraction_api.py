"""Optional LLM paper extractor interface (assignment scaffold).

Disabled unless the caller explicitly opts in. Not executed or tested.
Returns a raw model response; downstream JSON parsing/schema validation remains TODO.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from urllib.request import Request, urlopen


@dataclass(frozen=True)
class PaperInput:
    paper_id: str
    title: str
    abstract: str
    full_text: str = ""


class DisabledExtractorError(RuntimeError):
    pass


class OpenAICompatibleExtractor:
    """OpenAI-compatible Chat Completions API; no calls on construction."""

    def __init__(self, *, enabled: bool = False, base_url: str = "", model: str = ""):
        self.enabled = enabled
        self.base_url = base_url.rstrip("/")
        self.model = model

    def extract(self, paper: PaperInput) -> str:
        if not self.enabled:
            raise DisabledExtractorError("LLM API extraction is disabled by default")
        token = os.environ.get("LLM_API_KEY")
        if not token or not self.base_url or not self.model:
            raise ValueError("LLM_API_KEY, base_url, and model are required")
        instruction = (
            "Extract research paper information as a JSON object: "
            "paper_id, title, subdomains, contributions, methods, "
            "research_problems, benchmarks, evaluations, findings, "
            "and evidence_locators. Distinguish reported claims from verified facts. "
            "Never invent evidence. Return JSON only."
        )
        payload = json.dumps({
            "model": self.model,
            "messages": [
                {"role": "system", "content": instruction},
                {"role": "user", "content": json.dumps(paper.__dict__)},
            ],
        }).encode("utf-8")
        req = Request(
            self.base_url + "/chat/completions",
            data=payload,
            headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"},
            method="POST",
        )
        with urlopen(req, timeout=120) as response:
            data = json.load(response)
        return data["choices"][0]["message"]["content"]
