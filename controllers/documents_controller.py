import json
from flask import request
from flask import Blueprint
from elasticsearch import ConflictError
import services.documents_service as document_service


def construct_document_blueprint(es_client):
    blueprint = Blueprint('documents', __name__)

    @blueprint.route('/search', methods=['POST'])
    def search_documents():
        body = request.json
        query = body["query"]
        method = body["method"]
        field = body["field"]
        user = body["user"]
        if 'googleId' in user:
            user_id = user['googleId']
        elif 'id' in user:
            user_id = user['id']
        else:
            user_id = ''

        out_fields = []
        exclude_fields = ["abstract_vector"]

        # Logging user query
        document_service.store_search_logs(query, user_id, es_client)

        documents = document_service.search_documents(
            query, method, field, out_fields, exclude_fields, es_client)
        return json.dumps(documents, indent=4)

    @blueprint.route('/documents/<etd_id>', methods=['GET'])
    def get_etd(etd_id):
        # User ID is given by the frontend through a header value
        user_id = request.headers.get('User')
        document_service.store_view_logs(user_id, etd_id, es_client)
        documents = document_service.get_document(etd_id, es_client)
        return json.dumps(documents, indent=4)

    @blueprint.route('/autocomplete', methods=['POST'])
    def autocomplete():
        body = request.json
        query = body["query"]
        method = body["method"]
        field = body["field"]
        out_fields = [field]
        documents = document_service.get_suggestions(
            query, method, field, out_fields, es_client)
        return json.dumps(documents, indent=4)

    @blueprint.route('/index', methods=['POST'])
    def index():
        body = request.json
        try:
            document_service.create_document(es_client, body)
            return "Document created!", 201
        except ConflictError:
            return "Document already exists!", 400


    @blueprint.route('/logs/<user_id>', methods=['GET'])
    def logs(user_id):
        return json.dumps(document_service.get_logs(user_id, es_client), indent=4)

    return blueprint


