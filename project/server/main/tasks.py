from bs4 import BeautifulSoup
import json
import os
import pymongo
import requests

from project.server.main.harvester import harvest
from project.server.main.logger import get_logger
from project.server.main.utils_swift import set_objects

logger = get_logger(__name__)

MONGO_HOST = 'mongodb://mongo:27017/'
MONGO_DB = 'harvest'
MONGO_COLLECTION = 'sudoc'


def is_thesis(soup: object) -> bool:
    parent = soup.find('datafield', {'tag': '328'})
    thesis = parent.find('subfield', {'code': 'b'}) if parent else None
    comment = parent.find('subfield', {'code': 'z'}) if parent else None
    return thesis and (thesis.text.lower() == 'thèse de doctorat') and (comment is None)


def get_sudoc_ids(idref):
    logger.debug(f'Get all sudoc ids for idref {idref}')
    sudoc_ids = []
    url = f'https://www.sudoc.fr/services/generic/?servicekey=qualinca_nbs_cache&ppn={idref}&format=application/xml'
    try:
        xml = requests.get(url).text
    except:
        logger.debug(f'erreur avec la requete {url}')
        return []
    soup = BeautifulSoup(xml, 'lxml')
    for n in soup.find_all('o_noticebiblio'):
        ppn = n.find('ppn')
        if ppn:
            sudoc_ids.append(ppn.text)
    return sudoc_ids


def get_sudoc_ids_old(idref: str) -> list:
    logger.debug(f'Get all sudoc ids for idref {idref}')
    sudoc_ids = []
    url = f'https://www.idref.fr/services/biblio/{idref}.json'
    try:
        response = requests.get(url).json()
    except:
        logger.debug(f'erreur avec la requete {url}')
        return []
    roles = response.get('sudoc', {}).get('result', {}).get('role', [])
    roles = roles if isinstance(roles, list) else [roles]
    for role in roles:
        docs = role.get('doc', [])
        docs = docs if isinstance(docs, list) else [docs]
        for doc in docs:
            sudoc_ids.append(doc.get('id'))
    sudoc_ids = list(filter(None, sudoc_ids))
    return list(set(sudoc_ids))


def create_task_harvest_notices(sudoc_ids: list, force_download: bool = False) -> None:
    logger.debug(f'Task harvest notices for sudoc_ids {sudoc_ids}')
    sudoc_ids = sudoc_ids if isinstance(sudoc_ids, list) else [sudoc_ids]
    sudoc_ids = list(set(sudoc_ids))
    mongo_client = pymongo.MongoClient(MONGO_HOST)
    mongo_db = mongo_client[MONGO_DB]
    mongo_collection = mongo_db[MONGO_COLLECTION]
    mongo_collection.create_index('sudoc_id')
    json_file = 'data_output.json'
    chunk_size = 500
    chunks = [sudoc_ids[i:i+chunk_size] for i in range(0, len(sudoc_ids), chunk_size)]
    for chunk in chunks:
        notices_json = []
        ids_already_harvested = [k.get('sudoc_id') for k in list(mongo_collection.find({'sudoc_id': {'$in': chunk}}))]
        for sudoc_id in chunk:
            if force_download or sudoc_id not in ids_already_harvested:
                notice_url = f'https://www.sudoc.fr/{sudoc_id}.xml'
                notice_xml = requests.get(url=notice_url).text
                soup = BeautifulSoup(notice_xml, 'lxml')
                set_objects(all_objects=notice_xml.encode('utf8'), container='sudoc', path=f'raw/{sudoc_id[-2:]}/{sudoc_id}.xml')
                if not is_thesis(soup=soup):
                    notice_json = harvest(sudoc_id, soup)
                    notices_json.append(notice_json)
                    json_content = json.dumps(notice_json, indent=4, ensure_ascii=False).encode('utf8')
                    set_objects(all_objects=json_content, container='sudoc', path=f'parsed/{sudoc_id[-2:]}/{sudoc_id}.json')
            else:
                logger.debug(f'This sudoc_id is already harvested {sudoc_id}')
        if notices_json:
            with open(json_file, 'w') as file:
                json.dump([{'sudoc_id': n['sudoc_id']} for n in notices_json], file)
            mongoimport = f'mongoimport --numInsertionWorkers 2 --uri {MONGO_HOST}{MONGO_DB} -c {MONGO_COLLECTION} --file {json_file} --jsonArray'
            os.system(mongoimport)
            os.remove(json_file)


def create_task_harvest(idrefs: list, force_download: bool = False) -> None:
    logger.debug(f'Task harvest for idrefs {idrefs}')
    idrefs = idrefs if isinstance(idrefs, list) else [idrefs]
    idrefs = list(set(idrefs))
    sudoc_ids = []
    for idref in idrefs:
        sudoc_ids += get_sudoc_ids(idref=idref)
    create_task_harvest_notices(sudoc_ids, force_download)
