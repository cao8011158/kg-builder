"""Bounded first-page API metadata collectors."""
from __future__ import annotations
import json
import os
import time
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from xml.etree import ElementTree as ET
from .storage import arxiv_norm

UA='research-kg-prototype/0.1 (academic research; contact via configured HTTP_USER_AGENT)'

def fetch_bytes(url, headers=None):
    hdr={'User-Agent':os.getenv('HTTP_USER_AGENT',UA),'Accept':'application/json'}
    hdr.update(headers or {})
    for retry in range(4):
        try:
            with urlopen(Request(url,headers=hdr),timeout=35) as r: return r.read()
        except HTTPError as e:
            if e.code not in (429,500,502,503,504) or retry==3: raise
            time.sleep(min(15,2**retry))
        except URLError:
            if retry==3: raise
            time.sleep(2**retry)
    raise RuntimeError('Unreachable')

def reconstruct_abstract(inv):
    if not inv: return ''
    positions={i:word for word, inds in inv.items() for i in inds}
    return ' '.join(positions.get(i,'') for i in range(max(positions)+1)).strip() if positions else ''

def openalex_search(query,limit=20,since='2025-01-01',until=None):
    filt='from_publication_date:'+since
    if until: filt+=',to_publication_date:'+until
    args={'search':query,'filter':filt,'per-page':min(limit,100),'select':
      'id,doi,title,publication_year,publication_date,authorships,abstract_inverted_index,ids,primary_location,updated_date'}
    if os.getenv('OPENALEX_API_KEY'):
        args['api_key'] = os.environ['OPENALEX_API_KEY']
    url='https://api.openalex.org/works?'+urlencode(args)
    obj=json.loads(fetch_bytes(url))
    for x in obj.get('results',[])[:limit]:
        aid=(x.get('ids') or {}).get('arxiv')
        yield {'title':x.get('title'), 'year':x.get('publication_year'),
          'published_at':x.get('publication_date'),'doi':x.get('doi'),
          'arxiv_id':arxiv_norm(aid) if aid else None,
          'authors':[a.get('author',{}).get('display_name') for a in x.get('authorships',[])],
          'abstract':reconstruct_abstract(x.get('abstract_inverted_index')),
          'url':x.get('id'), 'source_id':x.get('id'), 'source_payload':x}

def semantic_scholar_search(query,limit=20,year='2025-'):
    args={'query':query,'limit':min(limit,100),'year':year,
      'fields':'title,abstract,year,publicationDate,authors,externalIds,url,openAccessPdf,citationCount'}
    headers={}
    if os.getenv('SEMANTIC_SCHOLAR_API_KEY'):
        headers['x-api-key']=os.environ['SEMANTIC_SCHOLAR_API_KEY']
    obj=json.loads(fetch_bytes('https://api.semanticscholar.org/graph/v1/paper/search?'+urlencode(args),headers))
    for x in obj.get('data',[]):
        ids=x.get('externalIds') or {}
        yield {'title':x.get('title'),'year':x.get('year'),
          'published_at':x.get('publicationDate'),'doi':ids.get('DOI'),
          'arxiv_id':ids.get('ArXiv'),'authors':[a.get('name') for a in x.get('authors',[])],
          'abstract':x.get('abstract') or '', 'url':x.get('url'),
          'source_id':x.get('paperId'),'source_payload':x}

def arxiv_search(query,limit=20,since='2025-01-01'):
    # arXiv Atom API. The caller filters first-release dates so it can count rejected hits.
    # arXiv API expects field-tagged terms with explicit AND.
    expression = ' AND '.join('all:'+t for t in query.split())
    args={'search_query':expression,'start':0,'max_results':min(limit,100),
          'sortBy':'submittedDate','sortOrder':'descending'}
    xml=fetch_bytes('https://export.arxiv.org/api/query?'+urlencode(args),headers={'Accept':'application/atom+xml'})
    root=ET.fromstring(xml)
    ns={'a':'http://www.w3.org/2005/Atom', 'arxiv':'http://arxiv.org/schemas/atom'}
    for e in root.findall('a:entry',ns):
        url=e.findtext('a:id',default='',namespaces=ns)
        pub=e.findtext('a:published',default='',namespaces=ns)
        updated=e.findtext('a:updated',default='',namespaces=ns)
        title=' '.join(e.findtext('a:title',default='',namespaces=ns).split())
        abstract=' '.join(e.findtext('a:summary',default='',namespaces=ns).split())
        version=url.rsplit('v',1)[-1] if url.rsplit('v',1)[-1].isdigit() else None
        yield {'title':title,'year':int(pub[:4]) if pub[:4].isdigit() else None,
          'published_at':pub[:10] or None, 'version_published_at':updated or None,
          'arxiv_id':arxiv_norm(url), 'doi':e.findtext('arxiv:doi',default=None,namespaces=ns),
          'version_label': 'v'+version if version else None,
          'authors':[x.findtext('a:name',namespaces=ns) for x in e.findall('a:author',ns)],
          'abstract':abstract,'url':url,'source_id':url,
          'source_payload':{'atom_entry_xml':ET.tostring(e,encoding='unicode')}}
