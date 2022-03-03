
import datetime
import requests

from project.server.main.harvester import harvest
from project.server.main.logger import get_logger
from project.server.main.utils_swift import set_objects

logger = get_logger(__name__)


def create_task_harvest(notices_id: list) -> None:
    logger.debug(f'Create task harvest for sudoc ids : {notices_id}')
    today = datetime.datetime.today().strftime('%Y/%m/%d')
    notices_id = notices_id if isinstance(notices_id, list) else [notices_id]
    chunk_size = 500
    chunks = [notices_id[i:i+chunk_size] for i in range(0, len(notices_id), chunk_size)]
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
