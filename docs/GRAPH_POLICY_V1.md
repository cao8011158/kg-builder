# Graph policy: domain-v1

The immutable preparation snapshot remains the input. Approved source payloads and original paper/version links are not rewritten. Collection, scope filtering and extraction are not repeated when rebuilding a graph.

## Two different operations

Methods and benchmarks use entity resolution: within-kind HNSW candidate neighborhoods and deduplicated candidate pairs. New graph jobs accept Jev SAME at P(same)>=0.60 and DIFFERENT at P(different)>=0.70; the SAME setting is bound to each job. Existing jobs without the setting retain 0.70, including confidence-mode jobs. ResearchProblem identity remains at 0.70. Similarity does not establish identity. Accepted same links form provisional components. A pending comparison does not block the whole component.

Research problems remain distinct source-grounded ResearchProblem nodes. Their whole-snapshot similarity graph is partitioned by Leiden CPM. Its actual clusters, rather than exact-identity links, become candidate research areas. An LLM summarizes each cluster of at least two members into a ResearchArea name and definition. Singleton problems remain searchable without forcing a synthetic area. Area nodes are distinct from their original problem nodes and expose member paper provenance.

## Second review is member exclusion

Jev reviews individual members. For Method/Benchmark, the new `whole-group-v1` review shows the complete provisional component once in shared request state and asks whether each target matches its coherent dominant concrete identity; no member is designated as representative. For ResearchArea, review still uses the externally generated area name/definition and asks for thematic membership, not identical concrete research objectives. Historical `anchor-v1` member reviews keep their recorded interpretation.

A member is excluded only if Jev selects `not_belongs` AND P(not_belongs) is strictly greater than 0.7 for probability-mode jobs. Exactly 0.7, lower negative probability, `belongs`, and `insufficient` are retained. No extra group-wide approval gate is added. Technical errors are recorded and stop a stage for recovery; they are not fabricated semantic answers.

Exclusion removes the group relationship, not the source item. Excluded methods/benchmarks and unmatched items materialize as independent source-authored nodes. Every original research problem remains a distinct searchable node even if it has no area. Uncertain identity relations remain audited and do not hide approved content from search. Explicit `different` decisions inside a provisional `same` component remain in the pair audit and `identity_conflicts` diagnostic. They do not stop the graph: Jev's individual member review decides retention under the strict exclusion rule above. The job records `identity_conflict_policy=member_review_v1` when this stage runs.

## Persistence and evaluation

Each job pins its graph policy, input snapshot hash and configuration. New jobs use domain-v1; prior jobs retain their legacy behavior and are not rewritten. Candidate decisions, individual membership reviews and exclusions remain inspectable. Generation can use leased external workers or bounded API workers; this measured replay uses external generation and live Jev.

All original 15 source-retrieval queries can be repeated. Method/Benchmark identity reference cases still measure entity resolution. Old research-problem identity cases must not score research-area co-membership as entity equality: they are reported separately as inapplicable under the changed semantics. Area quality and original-problem coverage are separate results. Agent-authored proxy references are not human gold labels.

The previous unexplained missing-index-marker observation is recorded separately. It is not declared fixed by these semantic policy changes. Close/reopen and downloaded restore must verify the new graph; new issues are reported and discussed before further policy changes.


The 2026-11-02 code update separates semantic model inputs from local provenance and shares evidence within requests; see JEV_CONTEXT_CONTRACT.md. Research area resolution is explicitly configurable and pinned per job; old jobs default to0.05. Finer-grained sample evaluation is deferred until the user authorizes another graph test.
