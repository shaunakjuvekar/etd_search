import json
from flask import request
from flask import Blueprint
import services.chapters_service as chapters_service


def construct_chapter_blueprint(es_client):
    blueprint = Blueprint('chapters', __name__)

    @blueprint.route('/chapters/search', methods=['POST'])
    def search_chapters():
        body = request.json
        query = body["query"]
        field = body["field"]
        out_fields = []
        exclude_fields = []
        chapters = chapters_service.search_chapters(
            query, field, out_fields, exclude_fields, es_client)
        return json.dumps(chapters)

    @blueprint.route('/chapters/<chapter_id>', methods=['GET'])
    def get_chapter(chapter_id):
        return json.dumps(chapters_service.get_chapter(chapter_id, es_client))

    return blueprint
