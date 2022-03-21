import datetime
import re

from project.server.main.logger import get_logger

logger = get_logger(__name__)


def set_doi(notice_json: str, soup: object, notice_id: str) -> str:
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


def set_genre(notice_json: str, soup: object) -> str:
    genres = {
        'k': 'map',
        'b': 'multimedia',
        'z': 'multimedia'
    }
    genre = soup.find('controlfield', {'tag': '008'})
    genre = genres.get(genre.text.lower()[0], 'book') if genre else 'other'
    notice_json['genre'] = genre
    return notice_json


def set_publication_date(notice_json: str, soup: object) -> str:
    attributes = [{'tag': '210', 'code': 'd'}, {'tag': '100', 'code': 'a'}, {'tag': '940', 'code': 'a'},
                  {'tag': '033', 'code': 'd'}]
    for attribute in attributes:
        publication_date = None
        d = soup.find('datafield', {'tag': attribute.get('tag')})
        if d and d.find('subfield', {'code': attribute.get('code')}):
            publication_date_info = d.find('subfield', {'code': attribute.get('code')}).text.replace('impr. ', '').replace('-', ' ').replace(',', '')
            for s in publication_date_info.split(' '):
                if len(s) == 4 and s.startswith(('1', '2')):
                    publication_date = f'{s}-01-01'
                    break
            if publication_date is None and len(publication_date_info) >= 8 and publication_date_info.startswith(('1', '2')):
                publication_date_info = publication_date_info[0:8]
                publication_date = f'{publication_date_info[0:4]}-{publication_date_info[4:6]}-{publication_date_info[6:8]}'
        if publication_date:
            publication_date = publication_date.replace('X', '0').replace('.', '0').replace('?', '0')
            notice_json['publication_date'] = datetime.datetime.strptime(publication_date, '%Y-%m-%d').isoformat()
            break
    return notice_json


def set_title(notice_json: str, soup: object) -> str:
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


def set_authors(notice_json: str, soup: object) -> str:
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


def set_id_external(notice_json: str, soup: object, sudoc_id: str) -> str:
    ids_external = [{'id_type': 'sudoc', 'id_value': sudoc_id}]
    d = soup.find('datafield', {'tag': '010'})
    isbn = d.find('subfield', {'code': 'a'}) if d else None
    if isbn:
        id_external = {'id_type': 'isbn', 'id_value': isbn.text}
        ids_external.append(id_external)
    d = soup.find('datafield', {'tag': '073'})
    ean = d.find('subfield', {'code': 'a'}) if d else None
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


def set_thematics(notice_json: str, soup: object) -> str:
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


def set_summary(notice_json: str, soup: object) -> str:
    d = soup.find('datafield', {'tag': '330'})
    summary = d.find('subfield', {'code': 'a'}) if d else None
    if summary:
        notice_json['summary'] = summary.text
    return notice_json


def set_source(notice_json: str, soup: object) -> str:
    notice_json['source'] = {}
    datafield = soup.find('datafield', {'tag': '210'})
    if datafield:
        publishers = []
        for subfield in datafield.find_all('subfield', {'code': 'c'}):
            publishers.append(subfield.text)
        publisher = ';'.join(publishers)
        if len(publisher) > 1:
            notice_json['source']['publisher'] = publisher
        issn_parent = soup.find('datafield', {'tag': '461'})
        issn = issn_parent.find('subfield', {'code': 'x'}) if issn_parent else None
        if issn:
            notice_json['source']['journal_issns'] = [issn.text]
        source_field_parent = soup.find('datafield', {'tag': '461'})
        source_title = source_field_parent.find('subfield', {'code': 't'}) if source_field_parent else None
        if source_title:
            notice_json['source']['source_title'] = source_title.text
    return notice_json


def harvest(notice_id: str, soup: object) -> str:
    logger.debug(f'Harvest sudoc for notice id : {notice_id}')
    notice_json = {
        'sudoc_id': notice_id,
        'detected_countries': ['fr'],
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
