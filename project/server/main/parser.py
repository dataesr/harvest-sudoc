import datetime
import re

from project.server.main.logger import get_logger

logger = get_logger(__name__)

#https://abes.fr/wp-content/uploads/2020/04/format-d-echange-des-donnees-bibliographiques.pdf


def is_thesis(soup):
    thesis_elt = soup.find('datafield', {'tag': '029'})
    if thesis_elt:
        thesis_sub_elt = thesis_elt.find('subfield', {'code': 'b'})
        if thesis_sub_elt:
            logger.debug(f'thesis {thesis_sub_elt}')
            return True
    return False

def is_not_text(soup):
#https://documentation.abes.fr/sudoc/formats/unmb/DonneesCodees/Correspondance_008_UNM_USM.htm
    genre_elt = soup.find('controlfield', {'tag': '008'})
    if genre_elt:
        genre = genre_elt.get_text()
        if genre[0:1] in ['B', 'G', 'I', 'K', 'L', 'M', 'N', 'P', 'V', 'Z']:
            logger.debug(f'controlfield 008 {genre}')
            return True
    for f in ['110', '115', '116', '117', '120', '121',
              '123', '124', '125', '126', '127', '128',
              '129']:
        if soup.find('datafield', {'tag': f}):
            logger.debug(f'datafield {f}')
            return True
    return False

def is_re_edition(soup):
    edition_elt = soup.find('datafield', {'tag': '205'})
    if edition_elt and edition_elt.find('subfield', {'code': 'a'}):
        edition_txt = edition_elt.find('subfield', {'code': 'a'}).get_text().lower()
        if ('ed' in edition_txt) or ('éd' in edition_txt):
            logger.debug(f're-edition: {edition_txt}')
            return True
    return False

def filter_notice(soup):
    if is_re_edition(soup):
        return True
    if is_not_text(soup):
        return True
    if is_thesis(soup):
        return True
    return False

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
    #genre_elt = soup.find('controlfield', {'tag': '008'})
    notice_json['genre'] = 'book'
    return notice_json


def set_publication_date(notice_json: str, soup: object) -> str:
    try:
        publication_date = soup.find('datafield', {'tag': '100'}).find('subfield', {'code': 'a'}).text
        #publication_date = publication_date[:8]
        #publication_date = datetime.datetime.strptime(publication_date, '%Y%m%d').isoformat()
        publication_year = publication_date[9:13]
        publication_date = datetime.datetime.strptime(f'{publication_year}0101', '%Y%m%d').isoformat()
    except:
        logger.error('Error while retrieving publication date')
        publication_date = datetime.datetime(1900, 1, 1, 0, 0).isoformat()
    notice_json['publication_date'] = publication_date
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

def set_publisher(notice_json: str, soup: object) -> str:
    d = soup.find('datafield', {'tag': '214'})
    publisher = d.find('subfield', {'code': 'c'}) if d else None
    if publisher:
        notice_json['publisher'] = publisher.text
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


def parse(notice_id: str, soup: object) -> str:
    logger.debug(f'Parsing sudoc notice id : {notice_id}')
    notice_json = {
        'sudoc_id': notice_id,
        'detected_countries': ['fr'],
        'data_sources': ['sudoc']
    }
    notice_json = set_doi(notice_json, soup, notice_id)
    notice_json = set_genre(notice_json, soup)
    notice_json = set_publication_date(notice_json, soup)
    notice_json = set_title(notice_json, soup)
    notice_json = set_publisher(notice_json, soup)
    notice_json = set_authors(notice_json, soup)
    notice_json = set_id_external(notice_json, soup, notice_id)
    notice_json = set_thematics(notice_json, soup)
    notice_json = set_summary(notice_json, soup)
    notice_json = set_source(notice_json, soup)
    return notice_json
