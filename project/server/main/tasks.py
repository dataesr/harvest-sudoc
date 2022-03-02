import os
import requests
from bs4 import BeautifulSoup
import datetime
import json
import re

from project.server.main.logger import get_logger


AFFILIATION_MATCHER_SERVICE = os.getenv('AFFILIATION_MATCHER_SERVICE')
matcher_endpoint_url = f'{AFFILIATION_MATCHER_SERVICE}/enrich_filter'
FRENCH_ALPHA2 = ['fr', 'gp', 'gf', 'mq', 're', 'yt', 'pm', 'mf', 'bl', 'wf', 'tf', 'nc', 'pf']


logger = get_logger(__name__)


def set_doi(notice_json, soup, notice_id) -> str:
    doi = None
    try:
        for e in soup.find_all('datafield', {'tag': '017'}):
            identifier = e.find('subfield', {'code': 'a'}).text
            if identifier.startswith('10.'):
                doi = identifier.strip().lower()
    except:
        pass
    if doi:
        notice_json['id'] = f'doi{doi}'
        notice_json['doi'] = doi
    else:
        notice_json['id'] = f'sudoc{notice_id}'
    return notice_json


def set_genre(notice_json, soup):
    genres = {
        'k': 'map',
        'b': 'multimedia',
        'z': 'multimedia'
    }
    genre = soup.find('controlfield', {'tag': '008'})
    genre = genres.get(genre.text.lower()[0], 'book') if genre else 'other'
    notice_json['genre'] = genre
    return notice_json


def set_publication_date(notice_json, soup):
    attributes = [{ 'tag': '210', 'code': 'd' }, { 'tag': '100', 'code': 'a' }, { 'tag': '940', 'code': 'a' }, { 'tag': '033', 'code': 'd' }]
    for attribute in attributes:
        publication_date = None
        d = soup.find('datafield', {'tag': attribute.get('tag')})
        if d and d.find('subfield', {'code': attribute.get('code')}):
            publication_date_info = d.find('subfield', {'code': attribute.get('code')}).text.replace('impr. ','').replace('-', ' ').replace(',', '')
            for s in publication_date_info.split(' '):
                if len(s) == 4 and s.startswith(('1', '2')):
                    publication_date = f'{s}-01-01'
                    break
            if publication_date is None and len(publication_date_info) >= 8 and publication_date_info.startswith(('1', '2')):
                publication_date_info = publication_date_info[0:8]
                publication_date = f'{publication_date_info[0:4]}-{publication_date_info[4:6]}-{publication_date_info[6:8]}'
        if publication_date:
            notice_json['publication_date'] = datetime.datetime.strptime(publication_date, '%Y-%m-%d').isoformat()
            break
    return notice_json


def set_title(notice_json, soup):
    title, sub_title = '', ''
    d = soup.find('datafield', {'tag': '200'})  
    if d and d.find('subfield', {'code': 'a'}):
        title = d.find('subfield', {'code': 'a'}).text
    if d and d.find('subfield', {'code': 'e'}):
        sub_title = d.find('subfield', {'code': 'e'}).text
    if len(sub_title) > 0:
        title += f' : {sub_title}'
    title = title.strip()
    notice_json['title'] = title
    return notice_json


def set_authors(notice_json, soup):
    authors = []
    for d in soup.find_all('datafield', {'tag': re.compile(r'(700|701|702)')}):
        author = {'role': 'author'}
        idref, last_name, first_name = None, None, None
        idref = d.find('subfield', {'code': '3'})
        if idref:
            author['id'] = f'idref{idref.text}'
            notice_json['persons_identified'] = True
        last_name = d.find('subfield', {'code': 'a'})
        if last_name:
            author['last_name'] = last_name.text
        first_name = d.find('subfield', {'code': 'b'})
        if first_name:
            author['first_name'] = first_name.text
        full_name = f'{author.get("first_name", "")} {author.get("last_name", "")}'
        author['full_name'] = full_name.strip()
        if idref or last_name:
            authors.append(author)
    notice_json['authors'] = authors   
    return notice_json


def set_id_external(notice_json, soup, sudoc_id):
    ids_external = [{'id_type': 'sudoc', 'id_value': sudoc_id}]
    d = soup.find('datafield', {'tag': '010'})
    isbn = d.find('subfield', {'code': 'a'})
    if isbn:
        id_external = {'id_type': 'isbn', 'id_value': isbn.text}
        ids_external.append(id_external)
    d = soup.find('datafield', {'tag': '073'})
    ean = d.find('subfield', {'code': 'a'})
    if ean:
        id_external = {'id_type': 'ean', 'id_value': ean.text}
        ids_external.append(id_external)
    for d in soup.find_all('datafield', {'tag': '035'}):
        worldcat = d.find('subfield', {'code': 'a'})
        if worldcat and '(OCoLC)' in worldcat.text:
            id_external = {'id_type': 'worldcat', 'id_value': worldcat.text.replace('(OCoLC)', '').strip()}
            ids_external.append(id_external)
    notice_json['id_external'] = ids_external
    return notice_json


def set_thematics(notice_json, soup):
    thematics = []
    for d in soup.find_all('datafield', {'tag': '606'}):
        thematic = {}
        code = d.find('subfield', {'code': '3'})
        if code:
            thematic['code'] = code.text
        reference = d.find('subfield', {'code': '2'})
        if reference:
            thematic['reference'] = reference.text
        label = d.find('subfield', {'code': 'a'})
        if label:
            thematic['fr_label'] = label.text
            thematics.append(thematic)
    notice_json['thematics'] = thematics
    return notice_json


def set_summary(notice_json, soup):
    d = soup.find('datafield', {'tag': '330'})
    summary = d.find('subfield', {'code': 'a'})
    if summary:
        notice_json['summary'] = summary.text
    return notice_json


def set_source(notice_json, soup):
    notice_json['source'] = {}
    datafield = soup.find('datafield', {'tag': '210'})
    if datafield:
        publishers = []
        for subfield in datafield.find_all('subfield', {'code': 'c'}):
            publishers.append(subfield.text)
        publisher = ';'.join(publishers)
        if len(publisher) > 1:
            notice_json['source']['publisher'] = publisher
        issn = soup.find('datafield', {'tag': '461'}).find('subfield', {'code': 'x'})
        if issn:
            notice_json['source']['journal_issns'] = [issn.text]
        source_title = soup.find('datafield', {'tag': '461'}).find('subfield', {'code': 't'})
        if source_title:
            notice_json['source']['source_title'] = source_title.text
    return notice_json


def transform(soup, notice_id):
    notice_json = {
        'is_french': True,
        'persons_identified': True,
        'structures_identified': True,
        'data_sources': ['sudoc']
    }
    notice_json = set_doi(notice_json, soup, notice_id)
    notice_json = set_genre(notice_json, soup)
    notice_json = set_publication_date(notice_json, soup)
    notice_json = set_title(notice_json, soup)
    notice_json = set_authors(notice_json, soup)
    notice_json = set_id_external(notice_json, soup, notice_id)
    notice_json = set_thematics(notice_json, soup)
    notice_json = set_summary(notice_json, soup)
    notice_json = set_source(notice_json, soup)
    return notice_json


def create_task_parse():
    notice_id = '258064072'
    notice_url = f'https://www.sudoc.fr/{notice_id}.xml'
    notice = requests.get(url=notice_url).text
    notice_xml = BeautifulSoup(notice, 'lxml')
    notice_json = transform(notice_xml, notice_id)
    previous_result = '{"is_french": "True", "persons_identified": "True", "structures_identified": "True", "data_sources": ["sudoc"], "id": "doi10.1007/978-3-030-70179-6", "doi": "10.1007/978-3-030-70179-6", "genre": "book", "publication_date": "2021-10-20T00:00:00", "title": "COVID-19 and Similar Futures : Pandemic Geographies", "authors": [{"role": "author", "last_name": "Andrews", "first_name": "Gavin J", "full_name": "Gavin J Andrews"}, {"role": "author", "last_name": "Crooks", "first_name": "Valorie A", "full_name": "Valorie A Crooks"}, {"role": "author", "last_name": "Pearce", "first_name": "Jamie R", "full_name": "Jamie R Pearce"}, {"role": "author", "last_name": "Messina", "first_name": "Jane P", "full_name": "Jane P Messina"}], "id_external": [{"id_type": "sudoc", "id_value": "258064072"}, {"id_type": "isbn", "id_value": "978-3-030-70179-6"}], "thematics": [{"reference": "lc", "fr_label": "Geography"}, {"reference": "lc", "fr_label": "Human geography"}, {"reference": "lc", "fr_label": "Epidemiology"}, {"reference": "lc", "fr_label": "Public health"}, {"reference": "lc", "fr_label": "Immunology"}, {"reference": "lc", "fr_label": "Virology"}], "summary": "This volume provides a critical response to the COVID-19 pandemic showcasing the full range of issues and perspectives that the discipline of geography can expose and bring to the table, not only to this specific event, but to others like it that might occur in future. Comprised of almost 60 short (2500 word) easy to read chapters, the collection provides numerous theoretical, empirical and methodological entry points to understanding the ways in which space, place and other geographical phenomenon are implicated in the crisis. Although falling under a health geography book series, the book explores the centrality and importance of a full range of biological, material, social, cultural, economic, urban, rural and other geographies. Hence the book bridges fields of study and sub-disciplines that are often regarded as separate worlds, demonstrating the potential for future collaboration and cross-disciplinary inquiry. Indeed book articulates a diverse but ultimately fulsome and multiscalar geographical approach to the major health challenge of our time, bringing different types of scholarship together with common purpose. The intended audience ranges from senior undergraduate students and graduate students to professional academics in geography and a host of related disciplines. These scholars might be interested in COVID-19 specifically or in the book\'s broad disciplinary approach to infectious disease more generally. The book will also be helpful to policy-makers at various levels in formulating responses, and to general readers interested in learning about the COVID-19 crisis.", "source": {}}'
    previous_result = json.loads(previous_result)
    for key, value in previous_result.items():
        if value == 'True':
            previous_result[key] = True
    notice_json == previous_result
