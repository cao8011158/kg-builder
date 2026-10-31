import json
import hashlib
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
from kg_builder.retrieval_preprocess import prepare_index, get_paper


class RetrievalPreprocessTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)
        self.master=self.root/"master.sqlite3"
        self.index=self.root/"index.sqlite3"
        self.enrichment=self.root/"enrichment.json"
        self.db=sqlite3.connect(self.master)
        self.addCleanup(self.db.close)
        self.db.executescript("""
            CREATE TABLE papers(paper_id TEXT PRIMARY KEY,title TEXT,year INTEGER,first_published TEXT,
                doi TEXT,arxiv_id TEXT,current_version_id TEXT);
            CREATE TABLE paper_versions(version_id TEXT PRIMARY KEY,paper_id TEXT,version_label TEXT,
                version_status TEXT,published_at TEXT,source_url TEXT,abstract TEXT,abstract_available INTEGER);
            CREATE TABLE source_records(source_name TEXT,source_id TEXT,paper_id TEXT,url TEXT,
                retrieved_at TEXT,payload_hash TEXT,payload_json TEXT);
            CREATE TABLE paper_doi_aliases(doi TEXT,paper_id TEXT);
            CREATE TABLE paper_topics(paper_id TEXT,topic TEXT,annotation_basis TEXT);
            CREATE TABLE extraction_records(extraction_id TEXT,paper_id TEXT,version_id TEXT,extractor_type TEXT,
                extractor_version TEXT,source_scope TEXT,status TEXT,payload_json TEXT,extracted_at TEXT);
        """)
        self.pid,self.vid="arxiv:2501.00001","arxiv:2501.00001@v1"
        self.db.execute("INSERT INTO papers VALUES(?,?,?,?,?,?,?)",(self.pid,"Example",2025,"2025-01-01",None,"2501.00001",self.vid))
        self.db.execute("INSERT INTO paper_versions VALUES(?,?,?,?,?,?,?,?)",(self.vid,self.pid,"v1","source_confirmed",
                        "2025-01-01","https://arxiv.org/abs/2501.00001v1","Text",1))
        self.db.execute("INSERT INTO source_records VALUES(?,?,?,?,?,?,?)",("arxiv","2501.00001",self.pid,
            "https://arxiv.org/abs/2501.00001","2026-10-31","hash",
            json.dumps({"normalized":{"authors":["Ada"]},"raw":{"categories":["cs.AI"],"primary_category":"cs.AI"}})))
        self.db.execute("INSERT INTO source_records VALUES(?,?,?,?,?,?,?)",("semantic_scholar","S1",self.pid,
            None,"2026-10-31","hash2",json.dumps({"normalized":{},
                "raw":{"s2FieldsOfStudy":[{"category":"Computer Science","source":"s2-fos-model"}]}})))
        self.db.execute("INSERT INTO extraction_records VALUES(?,?,?,?,?,?,?,?,?)",
            ("e1",self.pid,self.vid,"manual","1","selected_sections","draft_requires_review",
             json.dumps({"contributions":[{"description":"A contribution", "key":"c1"}]}),"2026-01-01"))
        self.db.commit()

    def _write_enrichment(self, works):
        self.enrichment.write_text(json.dumps({"schema_version":"retrieval-enrichment/1",
            "collected_at":"2026-10-31T00:00:00Z", "source_backup_sha256":hashlib.sha256(self.master.read_bytes()).hexdigest(),
            "works":works}))

    def test_full_semantics_and_indexed_point_lookup(self):
        self._write_enrichment([{"paper_id":self.pid,"match_status":"matched", "match_basis":"arxiv_url",
            "work_id":"https://openalex.org/W1", "authors":[{"name":"Ada","author_id":"A1"}],
            "cited_by_count":0, "topics":[{"id":"T1","name":"AI","score":0.8,
                "subfield":{"id":"S1","display_name":"AI"}}],
            "primary_topic":{"id":"T1","name":"AI","score":0.8},
            "keywords":[{"id":"K1","name":"memory","score":0.4}],
            "referenced_works":["https://openalex.org/W2"],
            "related_works":["https://openalex.org/W3"],
            "arxiv_status":"matched", "arxiv_categories":["cs.AI"]}])
        before=self.master.read_bytes()
        counts=prepare_index(self.master,self.index,self.enrichment)
        self.assertEqual((counts["papers"],counts["contributions"],counts["contribution_papers"]),(1,1,1))
        self.assertEqual(self.master.read_bytes(),before)
        detail=get_paper(self.index,self.pid)
        self.assertEqual(detail["cited_by_count"],0)
        self.assertEqual(detail["abstract_version_id"],self.vid)
        self.assertEqual(detail["contributions"][0]["review_status"],"draft_requires_review")
        self.assertEqual(detail["contributions"][0]["version_id"],self.vid)
        self.assertEqual({e["relation"] for e in detail["edges"]},{"references","related_work"})
        self.assertEqual({l["origin"] for l in detail["labels"] if l["kind"]=="original_class"},
                         {"arxiv","arxiv_enrichment","semantic_scholar"})
        self.assertEqual(next(l["parent"]["subfield"]["id"] for l in detail["labels"] if l["kind"]=="openalex_topic"),"S1")
        self.assertEqual(detail["tasks"],[])
        con=sqlite3.connect(self.index)
        for table in ("papers","versions","sources","labels","edges","contributions","tasks"):
            plan=con.execute("EXPLAIN QUERY PLAN SELECT * FROM "+table+" WHERE paper_id=?",(self.pid,)).fetchall()
            self.assertTrue(any("SEARCH" in row[3] for row in plan),(table,plan))
        con.close()

    def test_missing_error_and_unrequested_are_distinct(self):
        other,error="openalex:W2","openalex:W3"
        self.db.execute("INSERT INTO papers VALUES(?,?,?,?,?,?,?)",(other,"Other",2025,None,None,None,None))
        self.db.execute("INSERT INTO papers VALUES(?,?,?,?,?,?,?)",(error,"Error",2025,None,None,None,None))
        self.db.execute("INSERT INTO paper_versions VALUES(?,?,?,?,?,?,?,?)",(other+"@snapshot",other,None,
            "unverified_metadata_snapshot",None,None,"Text",1))
        self.db.commit()
        self._write_enrichment([{"paper_id":other,"match_status":"not_found","arxiv_status":"not_found"},
            {"paper_id":error,"match_status":"error","error":"rate limit","arxiv_status":"unrequested"}])
        prepare_index(self.master,self.index,self.enrichment)
        self.assertIsNone(get_paper(self.index,other)["cited_by_count"])
        self.assertEqual(next(t["reason"] for t in get_paper(self.index,other)["tasks"] if t["task_kind"]=="classification"),
                         "confirmed_metadata_gap")
        self.assertEqual(next(t["reason"] for t in get_paper(self.index,error)["tasks"] if t["task_kind"]=="classification"),
                         "metadata_error_defer")
        self.assertEqual(next(t["reason"] for t in get_paper(self.index,error)["tasks"] if t["task_kind"]=="contribution_summary"),
                         "needs_text")
        self.assertIsNone(get_paper(self.index,"missing"))

    def test_unknown_id_rejects_without_replacing_sidecar(self):
        self._write_enrichment([])
        prepare_index(self.master,self.index,self.enrichment)
        before=self.index.read_bytes()
        self._write_enrichment([{"paper_id":"missing","match_status":"not_found"}])
        with self.assertRaisesRegex(ValueError,"unknown paper_id"):
            prepare_index(self.master,self.index,self.enrichment)
        self.assertEqual(self.index.read_bytes(),before)

    def test_digest_and_unrelated_growth(self):
        self._write_enrichment([])
        prepare_index(self.master,self.index,self.enrichment)
        baseline=get_paper(self.index,self.pid)
        self.db.executemany("INSERT INTO papers VALUES(?,?,?,?,?,?,?)",
            [(f"other:{i}","large irrelevant metadata "*100,2025,None,None,None,None) for i in range(200)])
        self.db.commit()
        with self.assertRaisesRegex(ValueError,"source_backup_sha256"):
            prepare_index(self.master,self.index,self.enrichment)
        self.assertEqual(get_paper(self.index,self.pid),baseline)
        self._write_enrichment([])
        prepare_index(self.master,self.index,self.enrichment)
        self.assertEqual(get_paper(self.index,self.pid),baseline)
        con=sqlite3.connect(self.index)
        plan=con.execute("EXPLAIN QUERY PLAN SELECT * FROM papers WHERE paper_id=?",(self.pid,)).fetchall()
        self.assertTrue(any("SEARCH papers USING INDEX sqlite_autoindex_papers_1" in r[3] for r in plan),plan)
        con.close()


if __name__=="__main__": unittest.main()
