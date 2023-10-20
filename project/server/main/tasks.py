from bs4 import BeautifulSoup
import json
import os
import pymongo
import requests

from project.server.main.parser import parse, filter_notice
from project.server.main.logger import get_logger
from project.server.main.utils_swift import upload_object, download_object, delete_object

logger = get_logger(__name__)

MONGO_HOST = 'mongodb://mongo:27017/'
MONGO_DB = 'harvest'
MONGO_COLLECTION = 'sudoc'


#def is_thesis(soup: object) -> bool:
#    parent = soup.find('datafield', {'tag': '328'})
#    thesis = parent.find('subfield', {'code': 'b'}) if parent else None
#    comment = parent.find('subfield', {'code': 'z'}) if parent else None
#    return thesis and (thesis.text.lower() == 'thèse de doctorat') and (comment is None)


def get_sudoc_ids(idref):
    logger.debug(f'Get all sudoc ids for idref {idref}')
    sudoc_ids = []
    #url = f'https://www.sudoc.fr/services/generic/?servicekey=qualinca_nbs_cache&ppn={idref}&format=application/xml'
    url = f'https://www.idref.fr/Proxy?https://data.idref.fr/sparql?default-graph-uri=&query=select+distinct%28%3Fdoc%29%2C+%3Fcitation%0D%0Awhere%0D%0A%7B%3Fdoc+%3Frel+%3Chttp%3A%2F%2Fwww.idref.fr%2F{idref}%2Fid%3E.%0D%0A%3Fdoc+a+%3Ftype+%3B+dcterms%3AbibliographicCitation+%3Fcitation.%0D%0AFILTER%28regex%28%3Fdoc%2C%27http%3A%2F%2Fwww.sudoc%27%29%29%0D%0AOPTIONAL+%7B%3Fdoc+dc%3Adate+%3FdatePub%7D.%0D%0A%0D%0A%7D%0D%0AORDER+by+desc%28%3FdatePub%29+&format=application%2Frdf%2Bxml&timeout=0&debug=on&run=+Run+Query+'
    try:
        xml = requests.get(url).text
    except:
        logger.debug(f'erreur avec la requete {url}')
        return []
    soup = BeautifulSoup(xml, 'lxml')
    #for n in soup.find_all('o_noticebiblio'):
    #    ppn = n.find('ppn')
    #    if ppn:
    #        sudoc_ids.append(ppn.text)
    for res in soup.find_all('res:value'):
        try:
            if 'sudoc.fr' in res.attrs['rdf:resource']:
                sudoc_ids.append(res.attrs['rdf:resource'].split('/')[3])
        except:
            pass
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


def create_task_harvest_notices(sudoc_ids: list, force_download: bool = False, force_parsing: bool = True) -> None:
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
            notice_xml = None
            if force_download or sudoc_id not in ids_already_harvested:
                notice_url = f'https://www.sudoc.fr/{sudoc_id}.xml'
                notice_xml = requests.get(url=notice_url).text
                current_file = open(f'{sudoc_id}.xml', 'w')
                current_file.write(notice_xml)
                current_file.close()
                upload_object('sudoc', f'{sudoc_id}.xml', f'raw/{sudoc_id[-2:]}/{sudoc_id}.xml') 
                notices_json.append({'sudoc_id': sudoc_id})
                #set_objects(all_objects=notice_xml.encode('utf8'), container='sudoc', path=f'raw/{sudoc_id[-2:]}/{sudoc_id}.xml')
            else:
                download_object('sudoc', f'raw/{sudoc_id[-2:]}/{sudoc_id}.xml', f'{sudoc_id}.xml')
            if (force_parsing or force_download) or sudoc_id not in ids_already_harvested:
                if notice_xml is None:
                    current_file = open(f'{sudoc_id}.xml', 'r')
                    notice_xml = current_file.read()
                    current_file.close()
                soup = BeautifulSoup(notice_xml, 'lxml')
                os.system(f'rm -rf {sudoc_id}.xml')
                if filter_notice(soup=soup):
                    # make sure notice not stored on object storage
                    try:
                        delete_object('sudoc', f'parsed/{sudoc_id[-2:]}/{sudoc_id}.json')
                    except:
                        pass
                else:
                    # we keep and parse
                    notice_json = parse(sudoc_id, soup)
                    out_file = open(f"{sudoc_id}.json", "w")
                    json.dump(notice_json, out_file, indent = 4, ensure_ascii=False)
                    out_file.close()
                    upload_object('sudoc', f'{sudoc_id}.json', f'parsed/{sudoc_id[-2:]}/{sudoc_id}.xml')
                    os.system(f'rm -rf {sudoc_id}.json')
                    #json_content = json.dump(notice_json, indent=4, ensure_ascii=False)
                    #set_objects(all_objects=json_content, container='sudoc', path=f'parsed/{sudoc_id[-2:]}/{sudoc_id}.json')
        if notices_json:
            with open(json_file, 'w') as file:
                json.dump([{'sudoc_id': n['sudoc_id']} for n in notices_json], file)
            mongoimport = f'mongoimport --numInsertionWorkers 2 --uri {MONGO_HOST}{MONGO_DB} -c {MONGO_COLLECTION} --file {json_file} --jsonArray'
            os.system(mongoimport)
            os.remove(json_file)


def create_task_harvest(idrefs: list, force_download: bool = False, force_parsing: bool = True) -> None:
    logger.debug(f'Task harvest for idrefs {idrefs}')
    idrefs = idrefs if isinstance(idrefs, list) else [idrefs]
    idrefs = list(set(idrefs))
    sudoc_ids = []
    for idref in idrefs:
        sudoc_ids += get_sudoc_ids(idref=idref)
    create_task_harvest_notices(sudoc_ids, force_download, force_parsing)
