from bs4 import BeautifulSoup
import datetime
import requests

from project.server.main.harvester import harvest
from project.server.main.logger import get_logger
from project.server.main.utils_swift import set_objects

logger = get_logger(__name__)


def is_thesis(soup: object) -> bool:
    parent = soup.find('datafield', {'tag': '328'})
    thesis = parent.find('subfield', {'code': 'b'}) if parent else None
    return thesis.text.lower() in ['thèse d\'exercice', 'thèse de doctorat'] if thesis else False


def get_sudoc_ids(id_ref: str) -> list:
    logger.debug(f'Get all sudoc ids for id_ref {id_ref}')
    sudoc_ids = []
    response = requests.get(f'https://www.idref.fr/services/biblio/{id_ref}.json').json()
    roles = response.get('sudoc', {}).get('result', {}).get('role', [])
    roles = roles if isinstance(roles, list) else [roles]
    for role in roles:
        docs = role.get('doc', [])
        docs = docs if isinstance(docs, list) else [docs]
        for doc in docs:
            sudoc_ids.append(doc.get('id'))
    sudoc_ids = list(filter(None, sudoc_ids))
    return list(set(sudoc_ids))


def create_task_harvest(id_refs: list) -> None:
    today = datetime.datetime.today().strftime('%Y%m%d')
    id_refs = id_refs if isinstance(id_refs, list) else [id_refs]
    sudoc_ids = []
    for id_ref in id_refs:
        sudoc_ids.append(get_sudoc_ids(id_ref=id_ref))
    chunk_size = 500
    chunks = [sudoc_ids[i:i+chunk_size] for i in range(0, len(sudoc_ids), chunk_size)]
    i = 0
    for chunk in chunks:
        notices_json = []
        notices_xml = []
        for sudoc_id in chunk:
            notice_url = f'https://www.sudoc.fr/{sudoc_id}.xml'
            notice_xml = requests.get(url=notice_url).text
            soup = BeautifulSoup(notice_xml, 'lxml')
            if not is_thesis(soup=soup):
                notice_json = harvest(sudoc_id, soup)
                notices_json.append(notice_json)
                notices_xml.append({
                    'id': sudoc_id,
                    'date': today,
                    'notice': notice_xml
                })
        set_objects(all_objects=notices_json, container='sudoc', path=f'parsed/sudoc_{today}_{i}.jsonl.gz')
        set_objects(all_objects=notices_xml, container='sudoc', path=f'raw/sudoc_{today}_{i}.jsonl.gz')
        i += 1
