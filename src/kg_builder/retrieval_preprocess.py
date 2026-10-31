"""Build a disposable, indexed retrieval sidecar from the read-only master store.

No network or model calls occur here. Scientific labels retain their source and
paper-version provenance; this module does not reconcile or merge concepts.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path


SCHEMA = """
PRAGMA foreign_keys=ON;
CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE papers (
 paper_id TEXT PRIMARY KEY, title TEXT NOT NULL, year INTEGER,
 first_published TEXT, doi TEXT, arxiv_id TEXT, current_version_id TEXT,
 abstract TEXT, abstract_version_id TEXT, authors_json TEXT NOT NULL,
 cited_by_count INTEGER, openalex_work_id TEXT,
 match_status TEXT NOT NULL, match_basis TEXT, arxiv_status TEXT NOT NULL,
 field_status_json TEXT NOT NULL, error TEXT
);
CREATE TABLE versions (
 version_id TEXT PRIMARY KEY, paper_id TEXT NOT NULL REFERENCES papers(paper_id),
 version_label TEXT, version_status TEXT NOT NULL, published_at TEXT,
 source_url TEXT, abstract TEXT, abstract_available INTEGER NOT NULL
);
CREATE INDEX idx_versions_paper ON versions(paper_id);
CREATE TABLE aliases (
 scheme TEXT NOT NULL, value TEXT NOT NULL, paper_id TEXT NOT NULL REFERENCES papers(paper_id),
 origin TEXT NOT NULL, PRIMARY KEY(scheme,value,paper_id,origin)
);
CREATE INDEX idx_aliases_paper ON aliases(paper_id);
CREATE TABLE sources (
 source_name TEXT NOT NULL, source_id TEXT NOT NULL,
 paper_id TEXT NOT NULL REFERENCES papers(paper_id), url TEXT,
 retrieved_at TEXT NOT NULL, payload_hash TEXT NOT NULL,
 PRIMARY KEY(source_name,source_id)
);
CREATE INDEX idx_sources_paper ON sources(paper_id);
CREATE TABLE labels (
 paper_id TEXT NOT NULL REFERENCES papers(paper_id), kind TEXT NOT NULL,
 label_id TEXT NOT NULL DEFAULT '', name TEXT NOT NULL, score REAL,
 origin TEXT NOT NULL, parent_json TEXT, source_id TEXT NOT NULL DEFAULT '',
 PRIMARY KEY(paper_id,kind,label_id,name,origin,source_id)
);
CREATE INDEX idx_labels_paper ON labels(paper_id);
CREATE INDEX idx_labels_reverse ON labels(kind,label_id,paper_id);
CREATE TABLE edges (
 paper_id TEXT NOT NULL REFERENCES papers(paper_id), relation TEXT NOT NULL,
 target_id TEXT NOT NULL, origin TEXT NOT NULL,
 PRIMARY KEY(paper_id,relation,target_id,origin)
);
CREATE INDEX idx_edges_paper ON edges(paper_id,relation);
CREATE INDEX idx_edges_target ON edges(target_id,relation,paper_id);
CREATE TABLE contributions (
 extraction_id TEXT NOT NULL, ordinal INTEGER NOT NULL,
 paper_id TEXT NOT NULL REFERENCES papers(paper_id), version_id TEXT NOT NULL REFERENCES versions(version_id),
 extractor_type TEXT NOT NULL, extractor_version TEXT NOT NULL,
 source_scope TEXT NOT NULL, review_status TEXT NOT NULL, description TEXT,
 payload_json TEXT NOT NULL, PRIMARY KEY(extraction_id,ordinal)
);
CREATE INDEX idx_contributions_paper ON contributions(paper_id);
CREATE TABLE tasks (
 paper_id TEXT NOT NULL REFERENCES papers(paper_id), task_kind TEXT NOT NULL,
 reason TEXT NOT NULL, version_id TEXT, PRIMARY KEY(paper_id,task_kind)
);
CREATE INDEX idx_tasks_kind ON tasks(task_kind,paper_id);
"""


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _load_enrichment(path):
    if path is None:
        return {"schema_version": "retrieval-enrichment/1", "works": []}, {}
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if data.get("schema_version") != "retrieval-enrichment/1" or not isinstance(data.get("works"), list):
        raise ValueError("Expected retrieval-enrichment/1 with a works array")
    by_id = {}
    for work in data["works"]:
        pid = work["paper_id"]
        if pid in by_id:
            raise ValueError(f"Duplicate enrichment paper_id: {pid}")
        if work.get("match_status", "unrequested") not in {"matched", "not_found", "error", "unrequested"}:
            raise ValueError(f"Invalid match_status: {pid}")
        if work.get("arxiv_status", "unrequested") not in {"matched", "not_found", "error", "unrequested"}:
            raise ValueError(f"Invalid arxiv_status: {pid}")
        if work.get("match_basis") not in (None, "existing_openalex_id", "doi", "arxiv_url", "title_authors"):
            raise ValueError(f"Invalid match_basis: {pid}")
        if work.get("match_status") == "matched" and not work.get("work_id"):
            raise ValueError(f"Matched work has no work_id: {pid}")
        by_id[pid] = work
    return data, by_id


def _labels(out, pid, kind, items, origin, source_id=""):
    for item in items or []:
        if isinstance(item, str):
            item = {"name": item}
        if not isinstance(item, dict) or not (item.get("name") or item.get("display_name") or item.get("category")):
            continue
        parent = {k: item[k] for k in ("subfield", "field", "domain") if k in item}
        out.execute("INSERT OR IGNORE INTO labels VALUES(?,?,?,?,?,?,?,?)",
                    (pid, kind, item.get("id") or "", item.get("name") or item.get("display_name") or item["category"], item.get("score"),
                     item.get("origin") or origin, _json(parent) if parent else None, source_id))


def _insert_paper(out, p, versions, source_rows, aliases, enrichment):
    pid = p["paper_id"]
    matched = enrichment.get("match_status") == "matched"
    sorted_versions = sorted(versions, key=lambda v: (v["version_status"] != "source_confirmed",
        -(int(v["version_label"][1:]) if v["version_label"] and v["version_label"][1:].isdigit() else 0), v["version_id"]))
    abstract_version = next((v for v in sorted_versions if v["abstract"]), None)
    authors = []
    for s in source_rows:
        payload = json.loads(s["payload_json"])
        norm = payload.get("normalized") or {}
        if not authors and norm.get("authors"):
            authors = norm["authors"]
    if matched and enrichment.get("authors") is not None:
        authors = enrichment["authors"]
    abstract = abstract_version["abstract"] if abstract_version else (enrichment.get("abstract") if matched else None)
    field_status = enrichment.get("field_status") or {}
    out.execute("INSERT INTO papers VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (pid,p["title"],p["year"],p["first_published"],p["doi"],p["arxiv_id"],
         p["current_version_id"],abstract,abstract_version["version_id"] if abstract_version else None,
         _json(authors),enrichment.get("cited_by_count") if matched else None,
         enrichment.get("work_id") if matched else None,enrichment.get("match_status","unrequested"),
         enrichment.get("match_basis"),enrichment.get("arxiv_status","unrequested"),
         _json(field_status),enrichment.get("error")))
    for s in source_rows:
        payload = json.loads(s["payload_json"])
        norm = payload.get("normalized") or {}
        raw = payload.get("raw") or {}
        if s["source_name"] == "arxiv":
            _labels(out,pid,"original_class",raw.get("categories"),"arxiv",s["source_id"])
            _labels(out,pid,"original_primary_class",[raw["primary_category"]] if raw.get("primary_category") else [],"arxiv",s["source_id"])
        if s["source_name"] == "semantic_scholar":
            _labels(out,pid,"original_class",raw.get("fieldsOfStudy"),"semantic_scholar",s["source_id"])
            _labels(out,pid,"original_class",raw.get("s2FieldsOfStudy"),"semantic_scholar",s["source_id"])
        # The stored candidate heuristics are useful retrieval labels, not authoritative ontology classes.
        _labels(out,pid,"candidate_topic",norm.get("topics"),s["source_name"],s["source_id"])
        out.execute("INSERT INTO sources VALUES(?,?,?,?,?,?)",(s["source_name"],s["source_id"],pid,
            s["url"],s["retrieved_at"],s["payload_hash"]))
        out.execute("INSERT OR IGNORE INTO aliases VALUES(?,?,?,?)",
                    (s["source_name"],s["source_id"],pid,"master_source_record"))
    for key, value in (("doi",p["doi"]),("arxiv",p["arxiv_id"])):
        if value: out.execute("INSERT OR IGNORE INTO aliases VALUES(?,?,?,?)",(key,value,pid,"master_paper"))
    for alias in aliases:
        out.execute("INSERT OR IGNORE INTO aliases VALUES(?,?,?,?)",("doi",alias["doi"],pid,"master_doi_alias"))
    if matched:
        out.execute("INSERT OR IGNORE INTO aliases VALUES(?,?,?,?)",("openalex",enrichment["work_id"],pid,"matched_work"))
        _labels(out,pid,"openalex_topic",enrichment.get("topics"),"openalex_topic",enrichment["work_id"])
        _labels(out,pid,"openalex_primary_topic",[enrichment["primary_topic"]] if enrichment.get("primary_topic") else [],
                "openalex_primary_topic",enrichment["work_id"])
        _labels(out,pid,"openalex_keyword",enrichment.get("keywords"),"openalex_keyword",enrichment["work_id"])
        for field, relation in (("referenced_works","references"),("related_works","related_work")):
            for target in enrichment.get(field) or []:
                if target:
                    out.execute("INSERT OR IGNORE INTO edges VALUES(?,?,?,?)",(pid,relation,target,"openalex"))
    _labels(out,pid,"original_class",enrichment.get("arxiv_categories"),"arxiv_enrichment")
    _labels(out,pid,"original_primary_class",[enrichment["arxiv_primary_category"]] if enrichment.get("arxiv_primary_category") else [],"arxiv_enrichment")
    return abstract_version


def prepare_index(master_path, output_path, enrichment_path=None):
    """One bounded corpus pass; publish a complete sidecar atomically on success."""
    master_path, output_path = Path(master_path).resolve(), Path(output_path).resolve()
    if master_path == output_path or (enrichment_path and Path(enrichment_path).resolve() == output_path):
        raise ValueError("Output must be separate from input files")
    data, enrichments = _load_enrichment(enrichment_path)
    # File digest is provenance for the input snapshot; independently verify an
    # expected digest when the collection manifest supplies one.
    digest=hashlib.sha256()
    with master_path.open("rb") as source:
        for chunk in iter(lambda:source.read(1024*1024),b""):
            digest.update(chunk)
    master_sha256=digest.hexdigest()
    for hash_field in ("source_backup_sha256", "master_file_sha256"):
        if data.get(hash_field) and data[hash_field] != master_sha256:
            raise ValueError(f"Enrichment {hash_field} does not match master input")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=".retrieval-",suffix=".sqlite3",dir=output_path.parent)
    os.close(fd)
    try:
        master = sqlite3.connect(master_path.as_uri()+"?mode=ro",uri=True)
        master.row_factory = sqlite3.Row
        out = sqlite3.connect(temp_name)
        try:
            out.executescript(SCHEMA)
            out.execute("BEGIN")
            master.execute("BEGIN")  # one consistent source snapshot for all corpus scans
            now = datetime.now(timezone.utc).isoformat(timespec="seconds")
            for key,value in {"schema_version":"retrieval-index/1", "prepared_at":now,
                "master_filename":master_path.name,"master_file_sha256":master_sha256,
                "enrichment_collected_at":data.get("collected_at"),
                "source_backup_sha256":data.get("source_backup_sha256"),
                "enrichment_sha256":hashlib.sha256(Path(enrichment_path).read_bytes()).hexdigest() if enrichment_path else None}.items():
                out.execute("INSERT INTO metadata VALUES(?,?)",(key,_json(value)))
            versions = {}
            for v in master.execute("SELECT * FROM paper_versions"):
                versions.setdefault(v["paper_id"],[]).append(v)
            source_rows = {}
            for s in master.execute("SELECT * FROM source_records"):
                source_rows.setdefault(s["paper_id"],[]).append(s)
            aliases = {}
            for a in master.execute("SELECT * FROM paper_doi_aliases"):
                aliases.setdefault(a["paper_id"],[]).append(a)
            for p in master.execute("SELECT * FROM papers"):
                pid=p["paper_id"]
                _insert_paper(out,p,versions.get(pid,[]),source_rows.get(pid,[]),aliases.get(pid,[]),enrichments.get(pid,{}))
            unknown = set(enrichments) - {p[0] for p in out.execute("SELECT paper_id FROM papers")}
            if unknown: raise ValueError(f"Enrichment contains unknown paper_id: {sorted(unknown)[:3]}")
            for vlist in versions.values():
                for v in vlist:
                    out.execute("INSERT INTO versions VALUES(?,?,?,?,?,?,?,?)",
                        (v["version_id"],v["paper_id"],v["version_label"],v["version_status"],
                         v["published_at"],v["source_url"],v["abstract"],v["abstract_available"]))
            for t in master.execute("SELECT * FROM paper_topics"):
                _labels(out,t["paper_id"],"candidate_topic",[t["topic"]],t["annotation_basis"])
            for e in master.execute("SELECT * FROM extraction_records"):
                payload=json.loads(e["payload_json"])
                for ordinal, contribution in enumerate(payload.get("contributions") or []):
                    out.execute("INSERT INTO contributions VALUES(?,?,?,?,?,?,?,?,?,?)",
                        (e["extraction_id"],ordinal,e["paper_id"],e["version_id"],e["extractor_type"],
                         e["extractor_version"],e["source_scope"],e["status"],
                         contribution.get("description"),_json(contribution)))
            for p in out.execute("SELECT paper_id,abstract,abstract_version_id,match_status,field_status_json FROM papers").fetchall():
                pid,abstract,version_id,match_status,status_raw=p
                field_status=json.loads(status_raw)
                have_contribution=out.execute("SELECT 1 FROM contributions WHERE paper_id=? LIMIT 1",(pid,)).fetchone()
                if not have_contribution:
                    reason="needs_text" if not abstract else "missing_contribution_summary"
                    out.execute("INSERT INTO tasks VALUES(?,?,?,?)",(pid,"contribution_summary",reason,version_id))
                have_labels=out.execute("SELECT 1 FROM labels WHERE paper_id=? AND kind IN ('openalex_topic','openalex_primary_topic') LIMIT 1",(pid,)).fetchone()
                if not have_labels:
                    enrichment=enrichments.get(pid,{})
                    if match_status == "error" or field_status.get("topics") == "error":
                        reason="metadata_error_defer"
                    elif match_status == "unrequested" or (match_status == "matched" and
                            "topics" not in enrichment and "primary_topic" not in enrichment and
                            field_status.get("topics") not in ("empty","confirmed_empty")):
                        reason="metadata_unrequested_defer"
                    else:
                        reason="confirmed_metadata_gap"
                    out.execute("INSERT INTO tasks VALUES(?,?,?,?)",(pid,"classification",reason,version_id))
            out.commit()
            if out.execute("PRAGMA foreign_key_check").fetchall(): raise ValueError("Sidecar foreign-key check failed")
            counts={t:out.execute("SELECT count(*) FROM "+t).fetchone()[0] for t in
                ("papers","versions","sources","labels","edges","contributions","tasks")}
            counts["contribution_papers"]=out.execute("SELECT count(DISTINCT paper_id) FROM contributions").fetchone()[0]
            counts["historical_extraction_records"]=master.execute("SELECT count(*) FROM extraction_records").fetchone()[0]
            counts["actionable_tasks"]=out.execute("SELECT count(*) FROM tasks WHERE reason IN ('missing_contribution_summary','confirmed_metadata_gap')").fetchone()[0]
        finally:
            out.close(); master.close()
        os.replace(temp_name,output_path)
        return counts
    finally:
        if os.path.exists(temp_name): os.unlink(temp_name)


def get_paper(index_path, paper_id):
    """Indexed point lookup; children are read only for the selected paper."""
    con=sqlite3.connect(Path(index_path).resolve().as_uri()+"?mode=ro",uri=True)
    con.row_factory=sqlite3.Row
    try:
        row=con.execute("SELECT * FROM papers WHERE paper_id=?",(paper_id,)).fetchone()
        if row is None: return None
        result=dict(row)
        result["authors"]=json.loads(result.pop("authors_json"))
        result["field_status"]=json.loads(result.pop("field_status_json"))
        for key,table,order in (("versions","versions","version_id"),("aliases","aliases","scheme,value"),
                                ("sources","sources","source_name,source_id"),("labels","labels","kind,name"),
                                ("edges","edges","relation,target_id"),("contributions","contributions","extraction_id,ordinal"),
                                ("tasks","tasks","task_kind")):
            result[key]=[dict(x) for x in con.execute(f"SELECT * FROM {table} WHERE paper_id=? ORDER BY {order}",(paper_id,))]
        for label in result["labels"]:
            label["parent"]=json.loads(label.pop("parent_json")) if label["parent_json"] else None
        for contribution in result["contributions"]:
            contribution["payload"]=json.loads(contribution.pop("payload_json"))
        return result
    finally: con.close()


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--master",required=True,type=Path)
    parser.add_argument("--output",required=True,type=Path)
    parser.add_argument("--enrichment",type=Path,help="Optional private retrieval-enrichment/1 JSON")
    args=parser.parse_args(argv)
    print(_json(prepare_index(args.master,args.output,args.enrichment)))


if __name__ == "__main__": main()
