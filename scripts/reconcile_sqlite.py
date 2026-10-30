"""Offline, transactional SQLite reconciliation. Inputs are opened read-only; no network/LLM."""
import argparse
from collections import defaultdict
import hashlib
import json
from pathlib import Path
import sqlite3
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from kg_builder.storage import connect, normalise_doi, arxiv_norm, identities

TABLES = ('papers', 'paper_versions', 'source_records', 'paper_topics',
          'discovery_hits', 'extraction_records', 'pipeline_runs')


def rows(db, table):
    return [dict(r) for r in db.execute(f'SELECT * FROM {table}')]


def reconcile(inputs, output, report):
    inputs = [Path(p).resolve() for p in inputs]
    output, report = Path(output).resolve(), Path(report).resolve()
    if (len(set(inputs)) != len(inputs) or output in inputs or report in inputs
            or output == report or output.exists() or report.exists()):
        raise ValueError('Inputs must be distinct; output and report must be new paths')
    dbs = []
    try:
        for path in inputs:
            db = sqlite3.connect(f'file:{path}?mode=ro', uri=True)
            db.row_factory = sqlite3.Row
            if db.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
                raise ValueError(f'Input failed integrity_check: {path}')
            if db.execute('PRAGMA foreign_key_check').fetchall():
                raise ValueError(f'Input failed foreign_key_check: {path}')
            dbs.append(db)
        datasets = [{t: rows(db,t) for t in TABLES} for db in dbs]
        papers = {(i,p['paper_id']):p for i,d in enumerate(datasets) for p in d['papers']}
        nodes = list(papers)
        parent = {n:n for n in nodes}
        members = {n:{n} for n in nodes}
        group_arxiv = {n:set() for n in nodes}
        arxiv = {}
        dois = defaultdict(set)
        sources = defaultdict(set)
        for i,d in enumerate(datasets):
            for p in d['papers']:
                n = (i,p['paper_id'])
                arxiv[n] = arxiv_norm(p['arxiv_id'])
                if p['doi'] and normalise_doi(p['doi']): dois[n].add(normalise_doi(p['doi']))
            for s in d['source_records']:
                n = (i,s['paper_id'])
                sources[n].add((s['source_name'],s['source_id']))
                try:
                    payload = json.loads(s['payload_json'])
                    normalized = payload.get('normalized') or {}
                    normalized['source_payload'] = payload.get('raw')
                    found_dois, found_arxiv = identities(normalized)
                    dois[n].update(found_dois)
                    if found_arxiv:
                        if arxiv[n] and arxiv[n] != found_arxiv:
                            raise ValueError(f'Conflicting arXiv IDs within {n}')
                        arxiv[n] = found_arxiv
                except (ValueError, TypeError, AttributeError) as exc:
                    raise ValueError(f'Invalid or contradictory source identity for {n}') from exc
            try:
                for a in rows(dbs[i], 'paper_doi_aliases'):
                    dois[(i,a['paper_id'])].add(a['doi'])
            except sqlite3.OperationalError:
                pass
        keys = defaultdict(list)
        for n in nodes:
            if arxiv[n]: group_arxiv[n].add(arxiv[n])
            if arxiv[n]: keys[('arxiv',arxiv[n])].append(n)
            for doi in dois[n]: keys[('doi',doi)].append(n)
            for source in sources[n]: keys[('source',source)].append(n)
        def find(n):
            while parent[n] != n:
                parent[n] = parent[parent[n]]
                n = parent[n]
            return n
        conflicts = []
        for key, owners in sorted(keys.items(), key=lambda x: str(x[0])):
            for n in owners[1:]:
                a,b = find(owners[0]),find(n)
                if a == b: continue
                distinct = group_arxiv[a] | group_arxiv[b]
                if len(distinct) > 1:
                    conflicts.append({'identity':repr(key),'papers':[list(x) for x in members[a] | members[b]]})
                    continue
                parent[b] = a
                members[a].update(members.pop(b))
                group_arxiv[a].update(group_arxiv.pop(b))
        groups = defaultdict(list)
        for n in nodes: groups[find(n)].append(n)
        mapping = {}
        used_ids = set()
        for group in sorted(groups.values(),key=min):
            winner = min(group)
            canonical = winner[1]
            if canonical in used_ids:
                canonical += ':' + hashlib.sha256(str(winner).encode()).hexdigest()[:12]
            used_ids.add(canonical)
            for n in group: mapping[n] = canonical
        output.parent.mkdir(parents=True, exist_ok=True)
        out = connect(output)
        out.execute('''CREATE TABLE reconciliation_audit (
            input_sha256 TEXT NOT NULL, table_name TEXT NOT NULL, ordinal INTEGER NOT NULL,
            original_json TEXT NOT NULL, mapped_paper_id TEXT, mapped_version_id TEXT,
            PRIMARY KEY(input_sha256,table_name,ordinal))''')
        out.execute('''CREATE TABLE paper_redirects (
            input_sha256 TEXT NOT NULL, original_paper_id TEXT NOT NULL,
            paper_id TEXT NOT NULL REFERENCES papers(paper_id),
            PRIMARY KEY(input_sha256,original_paper_id))''')
        hashes = [hashlib.sha256(p.read_bytes()).hexdigest() for p in inputs]
        version_map = {}
        def save_audit(input_hash, table, ordinal, original_json, paper_id, version_id):
            key = (input_hash,table,ordinal)
            previous = out.execute('''SELECT original_json FROM reconciliation_audit
                WHERE input_sha256=? AND table_name=? AND ordinal=?''',key).fetchone()
            if previous:
                if previous[0] != original_json:
                    raise ValueError(f'Conflicting provenance for audit key {key}')
                # The active merge may resolve more identities than an earlier run.
                out.execute('''UPDATE reconciliation_audit SET mapped_paper_id=?,mapped_version_id=?
                    WHERE input_sha256=? AND table_name=? AND ordinal=?''',
                    (paper_id,version_id,*key))
            else:
                out.execute('INSERT INTO reconciliation_audit VALUES(?,?,?,?,?,?)',
                            (*key,original_json,paper_id,version_id))

        def save_redirect(input_hash, old_id, new_id):
            previous = out.execute('''SELECT paper_id FROM paper_redirects
                WHERE input_sha256=? AND original_paper_id=?''',(input_hash,old_id)).fetchone()
            if previous:
                # A repeated source must point to the final canonical paper.
                out.execute('''UPDATE paper_redirects SET paper_id=? WHERE
                    input_sha256=? AND original_paper_id=?''',(new_id,input_hash,old_id))
            else:
                out.execute('INSERT INTO paper_redirects VALUES(?,?,?)',(input_hash,old_id,new_id))
        try:
            with out:
                for group in groups.values():
                    records = [papers[n] for n in group]
                    winner = mapping[group[0]]
                    primary = papers[min(group)]
                    first = min((p['first_published'] for p in records if p['first_published']),default=None)
                    a = next((arxiv[n] for n in group if arxiv[n]),None)
                    all_dois = set().union(*(dois[n] for n in group))
                    main_doi = normalise_doi(primary['doi']) or (min(all_dois) if all_dois else None)
                    out.execute('INSERT INTO papers VALUES(?,?,?,?,?,?,?,?,?)',
                        (winner,primary['title'],first,main_doi,a,
                         int(first[:4]) if first and first[:4].isdigit() else
                         min((p['year'] for p in records if p['year']),default=None),
                         None,min(p['created_at'] for p in records),max(p['updated_at'] for p in records)))
                    for doi in sorted(all_dois):
                        out.execute('INSERT OR IGNORE INTO paper_doi_aliases VALUES(?,?)',(doi,winner))
                    for i,pid in group:
                        save_redirect(hashes[i],pid,winner)
                for i,d in enumerate(datasets):
                    for v in d['paper_versions']:
                        target = mapping[(i,v['paper_id'])]
                        label,status = v['version_label'],v['version_status']
                        existing = None
                        if label is not None:
                            existing = out.execute('''SELECT version_id FROM paper_versions WHERE paper_id=? AND
                                version_label=? AND version_status=?''',(target,label,status)).fetchone()
                        else:
                            existing = out.execute('''SELECT version_id FROM paper_versions WHERE paper_id=? AND
                                version_label IS NULL AND version_status=? AND
                                COALESCE(abstract,'')=COALESCE(?,'') AND
                                COALESCE(source_url,'')=COALESCE(?,'') AND
                                COALESCE(content_hash,'')=COALESCE(?,'')''',
                                (target,status,v['abstract'],v['source_url'],v['content_hash'])).fetchone()
                        if existing:
                            vid = existing[0]
                            out.execute('''UPDATE paper_versions SET published_at=CASE
                                WHEN published_at IS NULL THEN ?
                                WHEN ? IS NULL THEN published_at
                                ELSE MIN(published_at,?) END WHERE version_id=?''',
                                (v['published_at'],v['published_at'],v['published_at'],vid))
                        else:
                            vid = v['version_id']
                            if out.execute('SELECT 1 FROM paper_versions WHERE version_id=?',(vid,)).fetchone():
                                vid = f"{target}@{label or 'unverified'}:{hashes[i][:12]}"
                            if out.execute('SELECT 1 FROM paper_versions WHERE version_id=?',(vid,)).fetchone():
                                vid += ':'+hashlib.sha256(v['version_id'].encode()).hexdigest()[:12]
                            out.execute('INSERT INTO paper_versions VALUES(?,?,?,?,?,?,?,?,?,?)',
                                (vid,target,label,status,v['published_at'],v['source_url'],v['content_hash'],
                                 v['abstract'],v['abstract_available'],v['created_at']))
                        version_map[(i,v['version_id'])] = vid
                for p in out.execute('SELECT paper_id FROM papers').fetchall():
                    versions = out.execute('''SELECT version_id FROM paper_versions WHERE paper_id=?
                        AND version_status='source_confirmed' AND version_label GLOB 'v[0-9]*'
                        ORDER BY CAST(SUBSTR(version_label,2) AS INTEGER) DESC LIMIT 1''',(p[0],)).fetchone()
                    if versions: out.execute('UPDATE papers SET current_version_id=? WHERE paper_id=?',(versions[0],p[0]))
                for i,d in enumerate(datasets):
                    for s in d['source_records']:
                        vals = (s['source_name'],s['source_id'],mapping[(i,s['paper_id'])],s['url'],
                                s['source_updated_at'],s['retrieved_at'],s['payload_json'],s['payload_hash'])
                        out.execute('INSERT OR IGNORE INTO source_records VALUES(?,?,?,?,?,?,?,?)',vals)
                    for t in d['paper_topics']:
                        out.execute('INSERT OR IGNORE INTO paper_topics VALUES(?,?,?)',
                                    (mapping[(i,t['paper_id'])],t['topic'],t['annotation_basis']))
                    for h in d['discovery_hits']:
                        out.execute('INSERT OR IGNORE INTO discovery_hits VALUES(?,?,?,?)',
                                    (mapping[(i,h['paper_id'])],h['source_name'],h['query'],h['discovered_at']))
                    for e in d['extraction_records']:
                        target = mapping[(i,e['paper_id'])]
                        vid = version_map[(i,e['version_id'])]
                        eid = e['extraction_id']
                        if out.execute('SELECT 1 FROM extraction_records WHERE extraction_id=?',(eid,)).fetchone():
                            eid += ':'+hashes[i][:12]
                        payload = json.loads(e['payload_json'])
                        if isinstance(payload,dict):
                            payload['paper_id'],payload['version_id'] = target,vid
                        vals=(eid,target,vid,e['extractor_type'],e['extractor_version'],e['source_scope'],
                              e['status'],json.dumps(payload,ensure_ascii=False),e['extracted_at'])
                        try: out.execute('INSERT INTO extraction_records VALUES(?,?,?,?,?,?,?,?,?)',vals)
                        except sqlite3.IntegrityError:
                            # Same logical extraction key: full original remains in reconciliation_audit.
                            pass
                    for run in d['pipeline_runs']:
                        vals = list(run.values())
                        out.execute('INSERT OR IGNORE INTO pipeline_runs VALUES(?,?,?,?,?,?,?,?)',vals)
                    for table in TABLES:
                        for index,row in enumerate(d[table]):
                            original_pid = row.get('paper_id')
                            original_vid = row.get('version_id')
                            save_audit(hashes[i],table,index,json.dumps(row,ensure_ascii=False,sort_keys=True),
                                       mapping.get((i,original_pid)) if original_pid else None,
                                       version_map.get((i,original_vid)) if original_vid else None)
                    # Keep the full history if an input was itself a prior reconciliation.
                    try:
                        for prior in rows(dbs[i],'reconciliation_audit'):
                            save_audit(prior['input_sha256'],prior['table_name'],prior['ordinal'],prior['original_json'],
                                       mapping.get((i,prior['mapped_paper_id']),prior['mapped_paper_id']),
                                       version_map.get((i,prior['mapped_version_id']),prior['mapped_version_id']))
                        for prior in rows(dbs[i],'paper_redirects'):
                            target = mapping.get((i,prior['paper_id']))
                            if target:
                                save_redirect(prior['input_sha256'],prior['original_paper_id'],target)
                    except sqlite3.OperationalError:
                        pass
                if out.execute('PRAGMA foreign_key_check').fetchall():
                    raise ValueError('Reconciled database fails foreign_key_check')
        except BaseException:
            out.close()
            output.unlink(missing_ok=True)
            raise
        counts = {t:out.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0] for t in TABLES}
        counts['reconciliation_audit'] = out.execute('SELECT COUNT(*) FROM reconciliation_audit').fetchone()[0]
        out.close()
        result = {'inputs':[{'path':str(path),'sha256':h} for path,h in zip(inputs,hashes)],
                  'output':str(output),'counts':counts,'conflicts':conflicts,
                  'paper_mapping':[{'input':str(inputs[i]),'original_paper_id':pid,'paper_id':target}
                                   for (i,pid),target in sorted(mapping.items())],
                  'version_mapping':[{'input':str(inputs[i]),'original_version_id':vid,'version_id':target}
                                     for (i,vid),target in sorted(version_map.items())]}
        report.parent.mkdir(parents=True,exist_ok=True)
        report.write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
        return result
    finally:
        for db in dbs: db.close()


if __name__ == '__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input',action='append',required=True,help='Repeat for each SQLite input; first input wins canonical IDs')
    parser.add_argument('--output',required=True,help='New SQLite output path')
    parser.add_argument('--report',required=True,help='New private JSON report path')
    args=parser.parse_args()
    print(json.dumps(reconcile(args.input,args.output,args.report)['counts'],sort_keys=True))
