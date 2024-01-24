import os
from flask import Flask
from flask_cors import CORS
from flask_swagger_ui import get_swaggerui_blueprint
from controllers.documents_controller import construct_document_blueprint
from controllers.users_controller import construct_user_blueprint
from controllers.experiments_controller import construct_experiment_blueprint
from controllers.chapters_controller import construct_chapter_blueprint
from settings import elasticsearch_settings

app = Flask(__name__)
CORS(app)

elasticsearch_settings.init()
es_client = elasticsearch_settings.client

SWAGGER_URL = '/swagger'
API_URL = '/static/swagger.json'
SWAGGER_UI_BLUEPRINT = get_swaggerui_blueprint(
    SWAGGER_URL,
    API_URL,
    config={
        'app_name': "ETD Search Service"
    }
)
app.register_blueprint(SWAGGER_UI_BLUEPRINT, url_prefix=SWAGGER_URL)

app.register_blueprint(construct_user_blueprint(es_client), url_prefix='/api/v1')
app.register_blueprint(construct_document_blueprint(es_client), url_prefix='/api/v1')
app.register_blueprint(construct_experiment_blueprint(es_client), url_prefix='/api/v1')
app.register_blueprint(construct_chapter_blueprint(es_client), url_prefix='/api/v1')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv("PORT")))
