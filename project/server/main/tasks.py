
import datetime
import requests

from project.server.main.harvester import harvest
from project.server.main.logger import get_logger
from project.server.main.utils_swift import set_objects

logger = get_logger(__name__)


def get_sudoc_ids(id_ref: str) -> list:
    logger.debug(f'Get all sudoc ids for person {id_ref}')
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


def create_task_harvest(id_ref: str) -> None:
    logger.debug(f'Create harvest task for id_ref : {id_ref}')
    today = datetime.datetime.today().strftime('%Y/%m/%d')
    sudoc_ids = get_sudoc_ids(id_ref=id_ref)
    chunk_size = 500
    chunks = [sudoc_ids[i:i+chunk_size] for i in range(0, len(sudoc_ids), chunk_size)]
    i = 0
    for chunk in chunks:
        notices_json = []
        notices_xml = []
        for notice_id in chunk:
            notice_url = f'https://www.sudoc.fr/{notice_id}.xml'
            notice_xml = requests.get(url=notice_url).text
            notice_json = harvest(notice_id, notice_xml)
            notices_json.append(notice_json)
            notices_xml.append({
                'id': notice_id,
                'date': today,
                'notice': notice_xml
            })
        set_objects(all_objects=notices_json, container='sudoc', path=f'parsed/sudoc_{today}_{i}.jsonl.gz')
        set_objects(all_objects=notices_xml, container='sudoc', path=f'raw/sudoc_{today}_{i}.jsonl.gz')
        i += 1
