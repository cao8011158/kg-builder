"""Local CPU and remote Voyage embeddings for research problem descriptions.

Construction/import is offline. Encoders and tokenizers are loaded (and potentially
downloaded) only on the first nonempty encode call. Callers persist model_id
with vectors and must not compare vectors from different representations.
"""
from __future__ import annotations

from collections import deque
from concurrent.futures import ThreadPoolExecutor
from importlib.metadata import version
import math
import os
from pathlib import Path
import re
import threading
import time

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

    The SDK owns tokenization, retries and response ordering within each batch.
    Worker clients are thread-local; the SDK has no client close API. Executors
    join workers on success or failure. Vectors are checked
    against its float/unit-length contract, never repaired or silently truncated.
    """

    def __init__(self, model_name: str = "voyage-4-large", dimension: int = 1024,
                 batch_size: int = 32, timeout: float = 30, max_retries: int = 2,
                 max_concurrency: int = 8, requests_per_minute: int | None = 2000,
                 tokens_per_minute: int | None = 3000000):
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
        if type(max_concurrency) is not int or not 1 <= max_concurrency <= 64:
            raise ValueError("max_concurrency must be between 1 and 64")
        for name, budget in (("requests_per_minute", requests_per_minute),
                             ("tokens_per_minute", tokens_per_minute)):
            if budget is not None and (type(budget) is not int or budget < 1):
                raise ValueError(f"{name} must be a positive integer or None")
        if requests_per_minute is not None and requests_per_minute < max_retries + 1:
            raise ValueError("request budget must accommodate the SDK retry allowance")
        self.max_concurrency = max_concurrency
        self.requests_per_minute = requests_per_minute
        self.tokens_per_minute = tokens_per_minute
        self._local = threading.local()
        self._lock = threading.Lock()
        self._slots = threading.BoundedSemaphore(max_concurrency)
        self._reservations = deque()
        self._clock = time.monotonic
        self._sleep = time.sleep
        self._active = self._peak = 0
        self._active_since = None
        self._request_seconds = self._request_union_seconds = 0.0
        self._queue_seconds = self._encode_seconds = 0.0
        self._failures = self._reserved_attempts = self._reserved_tokens = 0
        self._unknown_usage_requests = 0
        self.model_name = model_name
        self.dimension = dimension
        self.batch_size = batch_size
        self.timeout = timeout
        self.max_retries = max_retries
        self._sdk_requests = 0
        self._total_tokens = 0
        self._usage_complete = True

    @property
    def usage(self) -> dict[str, int | float | None]:
        """Thread-safe cumulative logical-call metrics; HTTP retries are opaque.

        Budgets reserve the worst-case SDK retry allowance before each call.
        Configured defaults are local pacing settings, not account entitlements.
        Request union time counts overlapping calls once; cumulative request and
        encode times sum durations. Queue time includes concurrency/rate waiting.
        """
        with self._lock:
            union = self._request_union_seconds
            if self._active_since is not None:
                union += self._clock() - self._active_since
            return {"sdk_requests": self._sdk_requests,
                    "total_tokens": (self._total_tokens
                                     if self._sdk_requests and self._usage_complete else None),
                    "known_total_tokens": self._total_tokens,
                    "failed_requests": self._failures,
                    "unknown_usage_requests": self._unknown_usage_requests,
                    "http_attempts_upper_bound": self._reserved_attempts,
                    "reserved_input_tokens": self._reserved_tokens,
                    "active_requests": self._active, "peak_concurrency": self._peak,
                    "request_cumulative_seconds": self._request_seconds,
                    "request_wall_union_seconds": union,
                    "queue_seconds": self._queue_seconds,
                    "encode_cumulative_seconds": self._encode_seconds}

    def _reserve(self, tokens: int) -> None:
        # A rolling minute admits the complete retry allowance up front. SDK
        # retries stay inside the same concurrency slot and have no outer retry.
        attempts = self.max_retries + 1
        reserved_tokens = tokens * attempts
        if self.tokens_per_minute is not None and reserved_tokens > self.tokens_per_minute:
            raise ValueError("Voyage batch exceeds the configured token rate budget")
        while True:
            with self._lock:
                now = self._clock()
                while self._reservations and now - self._reservations[0][0] >= 60:
                    self._reservations.popleft()
                requests = sum(item[1] for item in self._reservations)
                total = sum(item[2] for item in self._reservations)
                if ((self.requests_per_minute is None or requests + attempts <= self.requests_per_minute)
                        and (self.tokens_per_minute is None or total + reserved_tokens <= self.tokens_per_minute)):
                    if self.requests_per_minute is not None or self.tokens_per_minute is not None:
                        self._reservations.append((now, attempts, reserved_tokens))
                    self._reserved_attempts += attempts
                    self._reserved_tokens += reserved_tokens
                    return
                delay = max(0.0, 60 - (now - self._reservations[0][0]))
            self._sleep(delay)

    @property
    def encode_window_size(self) -> int:
        """Bounded caller window that can fill the configured request workers."""
        return self.max_concurrency * self.batch_size

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
        if not hasattr(self._local, "client"):
            api_key = os.environ.get("VOYAGE_API_KEY")
            if not api_key or not api_key.strip():
                raise RuntimeError("VOYAGE_API_KEY is required for Voyage encoding")
            try:
                from voyageai import Client
            except ImportError:
                raise RuntimeError("install the voyage extra to encode with Voyage") from None
            try:
                self._local.client = Client(api_key=api_key, timeout=self.timeout,
                                      max_retries=self.max_retries)
            except Exception:
                raise RuntimeError("Voyage client initialization failed") from None
        return self._local.client

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
        token_limit = min(120000, self.tokens_per_minute // (self.max_retries + 1)
                          if self.tokens_per_minute is not None else 120000)
        if any(count > token_limit for count in counts):
            raise ValueError("Voyage input exceeds the configured token rate budget")
        start, tokens = 0, 0
        for index, count in enumerate(counts):
            if index > start and (index - start >= self.batch_size or tokens + count > token_limit):
                batches.append((texts[start:index], tokens))
                start, tokens = index, 0
            # The documented request limit applies to input tokens counted by
            # the official model tokenizer. Provider role prompting is owned
            # by Voyage; truncation=False keeps server enforcement explicit.
            tokens += count
        batches.append((texts[start:], tokens))
        started = self._clock()
        arrays = []
        pending = deque()
        try:
            # Never enqueue all batches in ThreadPoolExecutor's unbounded queue.
            # The instance semaphore also covers simultaneous document/query calls.
            with ThreadPoolExecutor(max_workers=self.max_concurrency) as executor:
                try:
                    for batch, tokens in batches:
                        queued = self._clock()
                        self._slots.acquire()
                        try:
                            future = executor.submit(self._request, batch, tokens, role, queued)
                        except BaseException:
                            self._slots.release()
                            raise
                        # Includes futures cancelled before their worker starts.
                        future.add_done_callback(lambda _: self._slots.release())
                        pending.append(future)
                        if len(pending) >= self.max_concurrency:
                            arrays.append(pending.popleft().result())
                    while pending:
                        arrays.append(pending.popleft().result())
                except BaseException:
                    for future in pending:
                        future.cancel()
                    raise
            return np.concatenate(arrays, axis=0)
        finally:
            with self._lock:
                self._encode_seconds += self._clock() - started

    def _request(self, batch, tokens, role, queued):
        self._reserve(tokens)
        client = self._get_client()
        started = self._clock()
        with self._lock:
            self._queue_seconds += started - queued
            self._sdk_requests += 1
            self._active += 1
            self._peak = max(self._peak, self._active)
            if self._active_since is None:
                self._active_since = started
        try:
            try:
                result = client.embed(batch, model=self.model_name, input_type=role,
                                      output_dimension=self.dimension, output_dtype="float",
                                      truncation=False)
            except Exception:
                with self._lock:
                    self._usage_complete = False
                    self._failures += 1
                    self._unknown_usage_requests += 1
                # Provider exceptions can contain request details or credentials.
                raise RuntimeError("Voyage embedding request failed") from None
            reported_tokens = getattr(result, "total_tokens", None)
            with self._lock:
                if type(reported_tokens) is int and reported_tokens >= 0:
                    self._total_tokens += reported_tokens
                else:
                    self._usage_complete = False
                    self._unknown_usage_requests += 1
            return self._validate_result(result, len(batch))
        finally:
            ended = self._clock()
            with self._lock:
                self._request_seconds += ended - started
                self._active -= 1
                if not self._active:
                    self._request_union_seconds += ended - self._active_since
                    self._active_since = None

    def _validate_result(self, result, count):
        try:
            array = np.asarray(result.embeddings, dtype=np.float32)
        except (AttributeError, TypeError, ValueError, OverflowError):
            raise ValueError("Voyage returned invalid embeddings") from None
        if array.shape != (count, self.dimension):
            raise ValueError("Voyage returned an invalid embedding shape")
        if not np.all(np.isfinite(array)):
            raise ValueError("Voyage returned nonfinite embeddings")
        norms = np.linalg.norm(array, axis=1)
        if not np.allclose(norms, 1, rtol=1e-4, atol=1e-5):
            raise ValueError("Voyage returned embeddings that are not unit length")
        return array




def create_embedding_model(provider: str = "fastembed", model_name: str | None = None,
                           dimension: int = 1024, cache_dir: str | Path | None = None,
                           max_concurrency: int = 8, requests_per_minute: int | None = 2000,
                           tokens_per_minute: int | None = 3000000
                           ) -> EmbeddingModel | VoyageEmbeddingModel:
    """Construct an encoder offline; provider selection never falls back."""
    if provider == "fastembed":
        return EmbeddingModel("BAAI/bge-small-en-v1.5" if model_name is None else model_name,
                              cache_dir=cache_dir)
    if provider == "voyage":
        return VoyageEmbeddingModel("voyage-4-large" if model_name is None else model_name,
                                    dimension=dimension, max_concurrency=max_concurrency,
                                    requests_per_minute=requests_per_minute,
                                    tokens_per_minute=tokens_per_minute)
    raise ValueError("unsupported embedding provider")


def embedding_model_from_id(model_id: str, cache_dir: str | Path | None = None, *,
                            max_concurrency: int = 8, requests_per_minute: int | None = 2000,
                            tokens_per_minute: int | None = 3000000
                            ) -> EmbeddingModel | VoyageEmbeddingModel:
    """Restore only exact supported identities, before loading models or SDKs."""
    if not isinstance(model_id, str):
        raise ValueError("unsupported embedding model identity")
    voyage = re.fullmatch(
        r"voyageai:voyage-4-large:dimension=(256|512|1024|2048):input=document:"
        r"output=float:research-problem-text-v1:unit-l2-v1", model_id)
    if voyage:
        return VoyageEmbeddingModel(dimension=int(voyage.group(1)), max_concurrency=max_concurrency,
                                    requests_per_minute=requests_per_minute,
                                    tokens_per_minute=tokens_per_minute)
    fastembed = re.fullmatch(
        r"fastembed-[^:]+:([^:]+):research-problem-text-v1:l2-v1", model_id)
    if fastembed:
        model = EmbeddingModel(fastembed.group(1), cache_dir=cache_dir)
        if model.model_id == model_id:
            from fastembed import TextEmbedding

            if any(item["model"] == model.model_name for item in TextEmbedding.list_supported_models()):
                return model
    raise ValueError("unsupported embedding model identity")
