"""Import assistant/human reviewed structured JSON without invoking any LLM API."""
import argparse,json,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from kg_builder.storage import connect,ingest_extraction
p=argparse.ArgumentParser()
p.add_argument('--db',default='data/research_knowledge.sqlite3')
p.add_argument('--input',required=True)
a=p.parse_args()
con=connect(a.db)
records=json.loads(Path(a.input).read_text(encoding='utf-8'))
for item in records: ingest_extraction(con,item)
print('Imported',len(records),'reviewed or draft extraction records (statuses preserved)')
con.close()
