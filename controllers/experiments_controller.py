from flask import request
from flask import Blueprint
import services.experiment_service as experiment_service


def construct_experiment_blueprint(es_client):
    blueprint = Blueprint('experiments', __name__)

    @blueprint.route('/experiment/create', methods=['POST'])
    def run_experiment():
        vector_file = request.files['file'].stream
        body = request.form
        message = experiment_service.create_experiment_index(body["name"], es_client,
                                                             vector_file, body["dims"], body["similarity"],
                                                             body["user"])
        response = {'message': message}
        return response

    @blueprint.route('/experiment/delete', methods=['POST'])
    def delete_experiment():
        body = request.json
        message = experiment_service.delete_experiment(body["name"], es_client, body["user"])
        response = {'message': message}
        return response

    @blueprint.route('/experiment/search', methods=['POST'])
    def search_experiment():
        body = request.json
        return experiment_service.search_experiment(body["name"], body["query"], body["query_vector"],
                                                    body["knn_weight"], body["k"], es_client)

    @blueprint.route('/experiment/<user>', methods=['GET'])
    def get_experiments(user):
        print(user)
        return experiment_service.get_experiments(es_client, user)

    return blueprint
