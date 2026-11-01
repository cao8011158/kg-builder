# Jev semantic context and probability thresholds

This contract supersedes confidence-based business gates for NEW jobs. Historical decisions remain bound to their stored metric. The 2026-11-02 step authorizes code and synthetic offline checks only; a real API/sample graph replay requires later user authorization.

## Business decisions

Use the probability of the named option for decisions. Support/identity/scope positive and negative gates use >=0.7; member exclusion requires explicit not_belongs with P(not_belongs)>0.7. Exactly0.7 is retained for exclusion. Confidence is a native response statistic kept in raw audit, not an additional probability-mode gate. Completeness remains nonblocking diagnostic information. Missing, nonfinite or malformed probabilities are errors, never an excuse to reuse confidence as probability.

Old jobs without a metric retain confidence-mode semantics and their audit interpretation. New jobs pin probability-mode semantics; metadata, status and outcomes declare the metric so restore cannot silently change it. Existing numeric argument names can remain compatibility aliases, but documentation and new CLI options must describe the probability meaning explicitly.

## Model input versus local protocol

Model state contains only semantic evidence needed for the question. Database IDs, item keys, input/source hashes, paper version identifiers, extractor/schema versions and processing status remain in code/database/audit mapping. They are not objects for model truth checking. Use short opaque local references to connect questions and evidence; returned question IDs map back to the original local record without losing leases, hashes or provenance.

- Scope: title and source text.
- Item support: the source text and business claims in the item, including name/description, proposed/used roles, and benchmark version/subset where present. Do not ask the model to prove a generated key or source address from the paper.
- Completeness: source text and the four projected semantic arrays, without the extraction administrative envelope.
- Entity identity: object kind, name/description, and benchmark version/subset or existing direct identity evidence. The question compares objects, not consuming papers. Different usage contexts do not themselves prove distinct entities; unknown version means unknown, not a conflict.
- Group membership: group kind/name/definition and projected member semantics. ResearchArea means thematic membership; Method/Benchmark means the same actual object.

Do not apply keyword rules to rewrite or blank source descriptions. This step trims structured metadata and clarifies interpretation; the original approved payload remains immutable. Improving an already vague usage-only description would require separately grounded extraction, not fabricated identity facts.

## Per-request shared evidence

Share one source text among several item questions, one entity object among several pair questions, and one group definition among member questions within the same HTTP request. Different source text/version must not accidentally bind to a shared record. A query record A with20nearby records produces20questions over21objects, not210pair questions. Each question has an explicit local target; records with the same name are not automatically merged.

Batching is based on the actual projected serialized state and questions. Preserve24questions/request,40000state characters,60000UTF8body bytes and bounded concurrency. Rebuild references when splitting. Too-large semantic evidence fails explicitly instead of truncating. No global grouping LLM, fixed representative anchor, new blocking heuristic or changed candidate inventory is introduced.

## Research area granularity

Expose --problem-resolution and the matching constructor parameter. Persist effective resolution in a graph job and reuse it for validation and restore; reject an explicit incompatible override. Old missing values mean0.05. Keep a compatibility default rather than claim a new optimum without sample testing. The desired finer research areas are decided; which higher resolution gives useful stable subareas is left to the later authorized experiment. Vectors, k20, edge cutoff, seed and clustering algorithm remain unchanged in this step.

## Explicit references after live fault isolation

New probability jobs pin `context_format=explicit-v1`. Pair questions contain the projected left/right object facts and literal kind directly, while retaining the shared semantic state; this matches the live-tested successful input variant. Other questions use code-resolved literal source/group paths and explicit member facts, rather than asking Jev to evaluate nested registry lookups. Large shared evidence stays in state once. Budgets account for the final question text and rebuild references after splitting. Data containing path-shaped text is never rewritten.

Historical jobs without a saved format retain `registry-v1`; confidence mode retains its historical unprojected wire. Reopening must not silently mix formats. Native workflow/graph adapters pass their pinned format; injected callbacks retain existing signatures. Thresholds, schema, candidate inventories, graph contradiction handling and completeness policy are unchanged. The live21-request diagnosis supported fixing references, not forcing every request to have one anchor or clearing all shared state.
