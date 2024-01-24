import uuid
import hashlib
import json


class Error(Exception):
    """Base class for other exceptions"""
    pass


class DuplicateUser(Error):
    """Username already exists!"""
    pass


class PasswordMismatch(Error):
    """Incorrect username or password"""
    pass


class MissingUser(Error):
    """User does not exist"""
    pass


def create_user(es_client, user):

    resp = es_client.search(index="users", query={
                            "bool": {"must": {"term": {"email": {"value": user["email"]}}}}})
    if resp['hits']['total']['value'] > 0:
        raise DuplicateUser

    user['password'] = hashlib.sha256(
        user['password'].encode('utf-8')).hexdigest()
    user["authType"] = "custom"
    es_client.create(id=uuid.uuid4(), index='users', document=json.dumps(user))


def get_user(es_client, user_id):
    document = es_client.get(index="users", id=user_id, source_includes=[
                             "org", "area", "topics"])
    return document['_source']


def update_user(es_client, user_id, user):
    es_client.update(index="users", id=user_id, doc=user)


def authenticate(es_client, user):
    resp = es_client.search(index="users", query={"bool": {"must": {"term": {
                            "email": {"value": user["email"]}}}}})

    if resp['hits']['total']['value'] == 0:
        raise MissingUser
    else:
        user_info = resp['hits']['hits'][0]['_source']
        password_hash = hashlib.sha256(
            user['password'].encode('utf-8')).hexdigest()
        password = user_info['password']
        if password == password_hash:
            del user_info['password']
            user_info['id'] = resp['hits']['hits'][0]['_id']
            return user_info
        else:
            raise PasswordMismatch


def create_google_user(es_client, user):
    resp = es_client.search(index="users", query={
                            "bool": {"must": {"term": {"email": {"value": user["email"]}}}}})
    if resp['hits']['total']['value'] > 0:
        raise DuplicateUser

    user['authType'] = "google"

    es_client.create(id=user["googleId"], index='users',
                     document=json.dumps(user))
