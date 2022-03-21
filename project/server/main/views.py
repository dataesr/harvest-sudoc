import redis

from flask import Blueprint, current_app, jsonify, render_template, request
from rq import Connection, Queue

from project.server.main.logger import get_logger
from project.server.main.tasks import create_task_harvest, create_task_harvest_notices


main_blueprint = Blueprint('main', __name__,)
logger = get_logger(__name__)
REDIS_QUEUE = 'harvest-sudoc'


@main_blueprint.route('/', methods=['GET'])
def home():
    return render_template('main/home.html')


@main_blueprint.route('/harvest', methods=['POST'])
def run_task_harvest():
    args = request.get_json(force=True)
    logger.debug(args)
    id_refs = args.get('id_refs')
    force_download = args.get('force_download', False)
    if id_refs:
        with Connection(redis.from_url(current_app.config['REDIS_URL'])):
            q = Queue(REDIS_QUEUE, default_timeout=2160000)
            task = q.enqueue(create_task_harvest, id_refs, force_download)
        response_object = {
            'status': 'success',
            'data': {
                'task_id': task.get_id()
            }
        }
    else:
        logger.error('Missing "id_refs" argument in the "/harvest" request')
        response_object = {
            'status': 'error',
            'message': 'Missing "id_refs" argument'
        }
    return jsonify(response_object)


@main_blueprint.route('/harvest_notices', methods=['POST'])
def run_task_harvest_notices():
    args = request.get_json(force=True)
    logger.debug(args)
    sudoc_ids = args.get('sudoc_ids')
    force_download = args.get('force_download', False)
    if sudoc_ids:
        with Connection(redis.from_url(current_app.config['REDIS_URL'])):
            q = Queue(REDIS_QUEUE, default_timeout=21600)
            task = q.enqueue(create_task_harvest_notices, sudoc_ids, force_download)
        response_object = {
            'status': 'success',
            'data': {
                'task_id': task.get_id()
            }
        }
    else:
        logger.error('Missing "sudoc_ids" argument in the "/harvest_notices" request')
        response_object = {
            'status': 'error',
            'message': 'Missing "sudoc_ids" argument'
        }
    return jsonify(response_object)


@main_blueprint.route('/tasks/<task_id>', methods=['GET'])
def get_status(task_id):
    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
        q = Queue(REDIS_QUEUE)
        task = q.fetch_job(task_id)
    if task:
        response_object = {
            'status': 'success',
            'data': {
                'task_id': task.get_id(),
                'task_status': task.get_status(),
                'task_result': task.result,
            },
        }
    else:
        response_object = {'status': 'error'}
    return jsonify(response_object)
