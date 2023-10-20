import hashlib
import os
import pandas as pd
import swiftclient
from retry import retry
import subprocess

from io import BytesIO

from project.server.main.logger import get_logger

logger = get_logger(__name__)

user = f'{os.getenv("OS_TENANT_NAME")}:{os.getenv("OS_USERNAME")}'
key = os.getenv('OS_PASSWORD')
project_id = os.getenv('OS_TENANT_ID')
project_name = os.getenv('OS_PROJECT_NAME')

init_cmd = f"swift --os-auth-url https://auth.cloud.ovh.net/v3 --auth-version 3 \
      --key {key}\
      --user {user} \
      --os-user-domain-name Default \
      --os-project-domain-name Default \
      --os-project-id {project_id} \
      --os-project-name {project_name} \
      --os-region-name GRA"

conn = swiftclient.Connection(
    authurl='https://auth.cloud.ovh.net/v3',
    user=user,
    key=key,
    os_options={
            'user_domain_name': 'Default',
            'project_domain_name': 'Default',
            'project_id': project_id,
            'project_name': project_name,
            'region_name': 'GRA'},
    auth_version='3'
)


@retry(delay=2, tries=50)
def exists_in_storage(container, filename):
    try:
        conn.head_object(container, filename)
        return True
    except:
        return False


def get_hash(x):
    return hashlib.md5(x.encode('utf-8')).hexdigest()


def get_filename(doi):
    init = doi.split('/')[0]
    notice_id = f'doi{doi}'
    id_hash = get_hash(notice_id)
    filename = f'{init}/{id_hash}.json.gz'
    return filename


@retry(delay=2, tries=50)
def get_data_from_ovh(doi=None, filename=None, container='landing-page-html'):
    if doi:
        filename = get_filename(doi)
    if exists_in_storage(container, filename) is False:
        logger.debug("ERROR : missing file")
        return {}
    if filename is None:
        logger.debug("ERROR : missing file")
        return {}
    df_notice = pd.read_json(BytesIO(conn.get_object(container, filename)[1]), compression='gzip')
    return df_notice.to_dict(orient='records')[0]


@retry(delay=2, tries=50)
def get_objects(container, path):
    try:
        df = pd.read_json(BytesIO(conn.get_object(container, path)[1]), compression='gzip')
    except:
        df = pd.DataFrame([])
    return df.to_dict('records')


@retry(delay=2, tries=50)
def set_objects(all_objects, container, path):
    logger.debug(f'Setting object {container} {path}')
    conn.put_object(container, path, contents=all_objects, headers={})
    logger.debug('Done')
    return


@retry(delay=2, tries=50)
def delete_folder(cont_name, folder):
    cont = conn.get_container(cont_name)
    for n in [e['name'] for e in cont[1] if folder in e['name']]:
        print(n)
        conn.delete_object(cont_name, n)

@retry(delay=3, tries=50, backoff=2)
def upload_object(container: str, filename: str, destination: str) -> str:
    if destination is None:
        destination = filename.split('/')[-1]
    logger.debug(f'Uploading {filename} in {container} as {destination}')
    cmd = init_cmd + f' upload {container} {filename} --object-name {destination}' \
                     f' --segment-size 1048576000 --segment-threads 100'
    #os.system(cmd)
    r = subprocess.check_output(cmd, shell=True)
    return f'https://storage.gra.cloud.ovh.net/v1/AUTH_{project_id}/{container}/{destination}'


@retry(delay=3, tries=50, backoff=2)
def download_object(container: str, filename: str, out: str) -> None:
    logger.debug(f'Downloading {filename} from {container} to {out}')
    cmd = init_cmd + f' download {container} {filename} -o {out}'
    #os.system(cmd)
    r = subprocess.check_output(cmd, shell=True)

def delete_object(container: str, filename: str) -> None:
    logger.debug(f'Deleting {filename} from {container}')
    cmd = init_cmd + f' delete {container} {filename}'
    #os.system(cmd)
    r = subprocess.check_output(cmd, shell=True)
