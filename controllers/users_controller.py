import json
from flask import request, Blueprint
from services.users_service import get_user, create_user, authenticate, DuplicateUser, PasswordMismatch, MissingUser, update_user, create_google_user


def construct_user_blueprint(es_client):
    blueprint = Blueprint('users', __name__)

    @blueprint.route('/signup', methods=['POST'])
    def signup():
        user = request.json

        try:
            create_user(es_client, user)
            return "Created user!", 201
        except DuplicateUser:
            return "User already exists!", 400

    @blueprint.route('/login', methods=['POST'])
    def login():
        user = request.json

        try:
            return json.dumps(authenticate(es_client, user))
        except (MissingUser, PasswordMismatch) as error:
            return "Username or password is incorrect!", 400

    @blueprint.route('/users/<user_id>', methods=['GET'])
    def fetch_profile(user_id):
        return json.dumps(get_user(es_client, user_id))

    @blueprint.route('/users/<user_id>', methods=['PUT'])
    def update_profile(user_id):
        user = request.json
        update_user(es_client, user_id, user)
        return "Updated user!", 200

    @blueprint.route('/google/login', methods=['POST'])
    def google_login():
        user = request.json

        try:
            create_google_user(es_client, user)
            return "Created Google user!", 201
        except DuplicateUser:
            return "User already exists!", 200

    return blueprint
