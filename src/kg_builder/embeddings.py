"""Local CPU and remote Voyage embeddings for research problem descriptions.

Construction/import is offline. Encoders and tokenizers are loaded (and potentially
downloaded) only on the first nonempty encode call. Callers persist model_id
with vectors and must not compare vectors from different representations.
"""
from __future__ import annotations

from importlib.metadata import version
import math
import os
from pathlib import Path
import re

import numpy as np


class EmbeddingModel:
    def __init__(self, model_name: str = "BAAI/bge-small-en-v1.5", cache_dir: str | Path | None = None,
                 threads: int = 2):
        if not isinstance(model_name, str) or not model_name.strip():
            raise ValueError("model_name must be nonempty")
        if not isinstance(threads, int) or isinstance(threads, bool) or threads < 1:
            raise ValueError("threads must be a positive integer")
        self.model_name = model_name
        self.cache_dir = str(cache_dir) if cache_dir is not None else None
        self.threads = threads
        self._model = None
        self._dimension: int | None = None

    @property
    def model_id(self) -> str:
        # A change in input representation or normalization requires a new ID.
        return f"fastembed-{version('fastembed')}:{self.model_name}:research-problem-text-v1:l2-v1"

    def encode(self, texts: list[str]) -> np.ndarray:
        if not isinstance(texts, list) or any(not isinstance(t, str) for t in texts):
            raise TypeError("texts must be a list of strings")
        if not texts:
            return np.empty((0, self._dimension or 0), dtype=np.float32)
        if self._model is None:
            from fastembed import TextEmbedding

            self._model = TextEmbedding(model_name=self.model_name, cache_dir=self.cache_dir,
                                        threads=self.threads)
        # FastEmbed yields one vector per text; batch_size bounds encoder memory.
        array = np.asarray(list(self._model.embed(texts, batch_size=32)), dtype=np.float32)
        if array.ndim != 2 or array.shape[0] != len(texts) or not array.shape[1]:
            raise ValueError("encoder returned an invalid shape")
        if self._dimension is not None and self._dimension != array.shape[1]:
            raise ValueError("encoder dimension changed")
        if not np.all(np.isfinite(array)):
            raise ValueError("encoder returned nonfinite values")
        norms = np.linalg.norm(array, axis=1)
        if not np.all(np.isfinite(norms)) or np.any(norms <= 0):
            raise ValueError("encoder returned a zero vector")
        array /= norms[:, None]
        self._dimension = array.shape[1]
        return array


class VoyageEmbeddingModel:
    """Lazy official Voyage client; document and query roles have distinct identities.

    The SDK owns tokenization, retries and response ordering. Vectors are checked
    against its float/unit-length contract, never repaired or silently truncated.
    """

    def __init__(self, model_name: str = "voyage-4-large", dimension: int = 1024,
                 batch_size: int = 32, timeout: float = 30, max_retries: int = 2):
        if model_name != "voyage-4-large":
            raise ValueError("unsupported Voyage model")
        if type(dimension) is not int or dimension not in (256, 512, 1024, 2048):
            raise ValueError("unsupported Voyage dimension")
        if type(batch_size) is not int or not 1 <= batch_size <= 1000:
            raise ValueError("batch_size must be between 1 and 1000")
        if (isinstance(timeout, bool) or not isinstance(timeout, (int, float))
                or not math.isfinite(timeout) or not 0 < timeout <= 300):
            raise ValueError("timeout must be finite and between 0 and 300 seconds")
        if type(max_retries) is not int or not 0 <= max_retries <= 5:
            raise ValueError("max_retries must be between 0 and 5")
        self.model_name = model_name
        self.dimension = dimension
        self.batch_size = batch_size
        self.timeout = timeout
        self.max_retries = max_retries
        self._client = None
        self._sdk_requests = 0
        self._total_tokens = 0
        self._usage_complete = True

    @property
    def usage(self) -> dict[str, int | None]:
        """Observed SDK calls and provider-reported tokens, without request data.

        SDK retries can make multiple HTTP attempts; those are not counted here.
        Missing usage or a failed call makes the cumulative token count unknown.
        """
        return {"sdk_requests": self._sdk_requests,
                "total_tokens": (self._total_tokens
                                 if self._sdk_requests and self._usage_complete else None)}

    def _identity(self, role: str) -> str:
        return (f"voyageai:{self.model_name}:dimension={self.dimension}:input={role}:"
                "output=float:research-problem-text-v1:unit-l2-v1")

    @property
    def model_id(self) -> str:
        return self._identity("document")

    @property
    def query_model_id(self) -> str:
        return self._identity("query")

    def _get_client(self):
        if self._client is None:
            api_key = os.environ.get("VOYAGE_API_KEY")
            if not api_key or not api_key.strip():
                raise RuntimeError("VOYAGE_API_KEY is required for Voyage encoding")
            try:
                from voyageai import Client
            except ImportError:
                raise RuntimeError("install the voyage extra to encode with Voyage") from None
            try:
                self._client = Client(api_key=api_key, timeout=self.timeout,
                                      max_retries=self.max_retries)
            except Exception:
                raise RuntimeError("Voyage client initialization failed") from None
        return self._client

    def encode(self, texts: list[str]) -> np.ndarray:
        return self._encode(texts, "document")

    def encode_query(self, texts: list[str]) -> np.ndarray:
        return self._encode(texts, "query")

    def _encode(self, texts: list[str], role: str) -> np.ndarray:
        if not isinstance(texts, list) or any(not isinstance(t, str) for t in texts):
            raise TypeError("texts must be a list of strings")
        if not texts:
            return np.empty((0, self.dimension), dtype=np.float32)
        client = self._get_client()
        # Use the official model tokenizer once per input. Check the complete
        # request before making any embedding call, including later batches.
        counts = []
        for start in range(0, len(texts), self.batch_size):
            chunk = texts[start:start + self.batch_size]
            try:
                tokenized = client.tokenize(chunk, model=self.model_name)
                lengths = [len(item) for item in tokenized]
            except Exception:
                raise RuntimeError("Voyage tokenization failed") from None
            if len(lengths) != len(chunk) or any(n > 32000 for n in lengths):
                raise ValueError("Voyage input exceeds the 32000 token limit or tokenizer count is invalid")
            counts.extend(lengths)
        batches = []
        start, tokens = 0, 0
        for index, count in enumerate(counts):
            if index > start and (index - start >= self.batch_size or tokens + count > 120000):
                batches.append(texts[start:index])
                start, tokens = index, 0
            # The documented request limit applies to input tokens counted by
            # the official model tokenizer. Provider role prompting is owned
            # by Voyage; truncation=False keeps server enforcement explicit.
            tokens += count
        batches.append(texts[start:])
        arrays = []
        for batch in batches:
            self._sdk_requests += 1
            try:
                result = client.embed(batch, model=self.model_name, input_type=role,
                                      output_dimension=self.dimension, output_dtype="float",
                                      truncation=False)
            except Exception:
                self._usage_complete = False
                # Provider exceptions can contain request details or credentials.
                raise RuntimeError("Voyage embedding request failed") from None
            reported_tokens = getattr(result, "total_tokens", None)
            if type(reported_tokens) is int and reported_tokens >= 0:
                self._total_tokens += reported_tokens
            else:
                self._usage_complete = False
            try:
                array = np.asarray(result.embeddings, dtype=np.float32)
            except (AttributeError, TypeError, ValueError, OverflowError):
                raise ValueError("Voyage returned invalid embeddings") from None
            if array.shape != (len(batch), self.dimension):
                raise ValueError("Voyage returned an invalid embedding shape")
            if not np.all(np.isfinite(array)):
                raise ValueError("Voyage returned nonfinite embeddings")
            norms = np.linalg.norm(array, axis=1)
            if not np.allclose(norms, 1, rtol=1e-4, atol=1e-5):
                raise ValueError("Voyage returned embeddings that are not unit length")
            arrays.append(array)
        return np.concatenate(arrays, axis=0)


def create_embedding_model(provider: str = "fastembed", model_name: str | None = None,
                           dimension: int = 1024, cache_dir: str | Path | None = None
                           ) -> EmbeddingModel | VoyageEmbeddingModel:
    """Construct an encoder offline; provider selection never falls back."""
    if provider == "fastembed":
        return EmbeddingModel("BAAI/bge-small-en-v1.5" if model_name is None else model_name,
                              cache_dir=cache_dir)
    if provider == "voyage":
        return VoyageEmbeddingModel("voyage-4-large" if model_name is None else model_name,
                                    dimension=dimension)
    raise ValueError("unsupported embedding provider")


def embedding_model_from_id(model_id: str, cache_dir: str | Path | None = None
                            ) -> EmbeddingModel | VoyageEmbeddingModel:
    """Restore only exact supported identities, before loading models or SDKs."""
    if not isinstance(model_id, str):
        raise ValueError("unsupported embedding model identity")
    voyage = re.fullmatch(
        r"voyageai:voyage-4-large:dimension=(256|512|1024|2048):input=document:"
        r"output=float:research-problem-text-v1:unit-l2-v1", model_id)
    if voyage:
        return VoyageEmbeddingModel(dimension=int(voyage.group(1)))
    fastembed = re.fullmatch(
        r"fastembed-[^:]+:([^:]+):research-problem-text-v1:l2-v1", model_id)
    if fastembed:
        model = EmbeddingModel(fastembed.group(1), cache_dir=cache_dir)
        if model.model_id == model_id:
            from fastembed import TextEmbedding

            if any(item["model"] == model.model_name for item in TextEmbedding.list_supported_models()):
                return model
    raise ValueError("unsupported embedding model identity")
