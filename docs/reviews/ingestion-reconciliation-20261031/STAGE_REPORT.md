# Ingestion reconciliation — 2026-10-31

Status: development and candidate relevance screening in progress; no LLM API calls or scheduled jobs.

## Scope and authorization

User authorized the proposed four steps: ingestion/storage fixes, conservative identity and version reconciliation, relevance screening, merging the initial seed database; persist validated database and backup in Drive and push code plus main-repository submodule pointer.

Main baseline: `46cad1cf3da34e64ae22a4d35c592f4bcaaf6afb` (`main`). KG Builder baseline: `1e94d75b7c98ec83304639466c4dc5719d232a79` (initially detached; earlier uncommitted collection changes preserved). No AGENTS.md found in project tree. Implementation delegated to ingestion_fix (gpt-6-sol, medium); independent reviewer: root.

## Initial findings

Uploaded input: 362 papers, 363 versions, 371 source records, no extractions; 14 normalized-title duplicate candidate groups; 348 papers with an abstract and 14 without. Source identities: 179 OpenAlex, 152 arXiv, 40 Semantic Scholar; 31 papers only from Semantic Scholar. SQLite integrity and FK checks pass.

Correction to earlier interpretation: 210 null current_version_id pointers belong to unverified metadata snapshots; selecting those as confirmed current versions would violate version semantics. Preserve NULL until a confirmed source version exists, expose metadata selection separately.

Tests planned: identity normalization/conflict rollback, indexed localized matching with irrelevant data growth, version numeric ordering, metadata-only state, provenance/extraction preserving offline merges, idempotency and input hash preservation, integrity/FK checks and Drive roundtrip byte hashes. No paid/extraction API tests.

## Data screening preparation (before reconciliation)

Read all 362 titles. Used rule-assisted topic suggestions plus manual decisions on displayed abstract/excerpt boundary cases, not a full-text or exhaustive abstract validation. Preliminary input-record decisions: include 308, pending 38, exclude 16. Fourteen records without abstracts remain pending. Exclusions include non-language DQN applications, generic tabular RL chapters, non-language image/molecule generation, supplementary artifact and PREreview records. Pending includes generic RAG/security applications and unusual publication/quality boundaries. All decisions remain reversible; raw rows are not deleted. Exact-title groups are candidates, never automatic merge authority.

Seed fetched from exact Drive file ID `1hO1VlJN_NOL1mDTyTTxDnopf331-62Q7`, integrity OK, 20 papers and 20 Draft extractions, SHA256 `73ded7af230f68da8f741e64ddaacae35073f0f9b2017ba40d9d5f3399972410`. Original seed backed up in snapshots (Drive `1AXF6mXVp_irx_2-1G0G2O-YJzsd0FX3y`); uploaded candidate snapshot independently backed up (Drive `1PETqwcS9V8xYCaELoHgCNflyzjU7aC_c`). Source selected attachment stays unchanged.

## Development stage — local implementation complete, not committed

Implementation actor: ingestion_fix (gpt-6-sol, medium). Branch `work/ingestion-reconciliation-20261031`, base `1e94d75b7c98ec83304639466c4dc5719d232a79`. Preserved earlier working changes.

Changes: indexed source/arXiv/DOI+alias matching; atomic conflict rejection; arXiv DOI normalization; distinct unverified metadata snapshots; earliest observed dates/year; numeric confirmed version selection without downgrade; metadata-only current pointer remains NULL with separate display helper. Offline reconciliation CLI opens inputs read-only, writes a new DB, preserves named source/extraction/discovery/topic tables and original-row audit, remaps extraction crossrefs and retains redirects/prior audit. Semantic Scholar removed from default `all`, explicit adapter retained. No title-only automatic merge.

Developer-reported validation: `PYTHONPATH=src python -m unittest discover -s tests -q` passed 6 tests in 0.46 s; `git diff --check` passed. Root diagnostic during development observed a temporary expected-version-ID assertion mismatch while snapshot IDs were being changed; corrected before final developer run. This is not a formal review. Root real-data development trial: 365 papers,383 versions,391 sources,20 original drafts; all original sources audited, draft content/status preserved and input hashes unchanged. Final fixed-commit real run is still required.

Limits: reconciliation supports documented named tables, not arbitrary application tables; relevance reviews are applied after merge. Contradictory strong identities can abort instead of silently resolving ambiguity. No LLM or scheduler tests; no fresh live collection in this batch. Next: commit/push candidate, independent remote-SHA review, final real-data reconciliation, annotations and Drive roundtrip.

## Formal independent review — round 1/3: changes required

Fixed remote commit `3edf0cc2c94d1c4f00c7adc2593013e0ad89bd05` (tree `b361b62b9ffe76b73a2a1b0d1f160b27c64f460f`, exactly matches local candidate). CLI push lacked credentials, so authenticated GitHub connector created the identical tree; remote feature ref created and fetched. Root inspected complete diff and ran six-test regression (pass) and separate public-entrypoint adversarial checks (fail).

RKG-01, P2, confirmed blocking, owner=current ingestion reconciliation batch: `reconcile([already_reconciled_db, original_input])` raises `sqlite3.IntegrityError` for reconciliation_audit PK `(input_sha256,table_name,ordinal)`. Prior audit from first input is copied before second input original rows, whose INSERT is unconditional. Thus re-merging an already imported input is not idempotent. Source inputs are read-only and output transaction rolls back, so no input loss occurred. Fix: insert repeated identical audited rows idempotently, retain mapped canonical references, reject conflicting contents for the same provenance key; add a regression on output+original and repeated identical-byte inputs at distinct paths.

Other adversarial checks reached before failure passed: same-title unrelated works stay separate, malformed version rejection rolls back even after tentative SQL writes, contradictory single-record IDs rejected, source landing arXiv URL recognized, v2 cannot downgrade v10. Public upsert SQL instruction/statement work remains bounded after 1000 unrelated papers; final passing evidence still required. Next: bounded RKG-01 fix, same batch round2; no main ref update or database publication until fixed.

## RKG-01 repair stage — local fix complete, not committed

Implementation actor ingestion_fix, same batch and scope. `save_audit` now treats identical provenance rows idempotently, refreshes resolved canonical references, and rejects original-content changes for an existing provenance key. `save_redirect` handles repeated identical input hashes. Expanded regression covers output+original, identical-byte copies at distinct paths, provenance tampering rejection/output rollback, core count stability and unchanged input bytes. Developer `PYTHONPATH=src python -m unittest discover -s tests -q` and `git diff --check` both exit 0; raw log `logs/developer-fix1.log`. Next: fixed remote repair commit, formal independent round2/3 and real-data acceptance.
