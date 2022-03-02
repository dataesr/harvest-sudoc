import redis

from flask import Blueprint, current_app, jsonify, render_template, request
from rq import Connection, Queue

from project.server.main.logger import get_logger
from project.server.main.tasks import create_task_parse


main_blueprint = Blueprint('main', __name__,)
logger = get_logger(__name__)
REDIS_QUEUE = 'parser'


@main_blueprint.route('/', methods=['GET'])
def home():
    return render_template('main/home.html')


@main_blueprint.route('/parse', methods=['POST'])
def run_task_parse():
    args = request.get_json(force=True)
    logger.debug(args)
    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
        q = Queue(REDIS_QUEUE, default_timeout=21600)
        task = q.enqueue(create_task_parse, args)
    response_object = {
        'status': 'success',
        'data': {
            'task_id': task.get_id()
        }
    }
    return jsonify(response_object), 202


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
