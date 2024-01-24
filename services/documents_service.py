import elasticsearch
import json
from helpers import embeddings
import os


dirname = os.path.dirname(os.path.realpath(__file__))

model = embeddings.Model('all-distilroberta-v1', 768, 128, "l2_norm")


def get_document(etd_id, es_client):
    try:
        return es_client.get(index='etds', id=etd_id)['_source']
    except elasticsearch.NotFoundError:
        return []


# def log_user_search_query(query, method, field, user: opt):
# elasticsearch insert

def search_documents(query, method, field, out_fields, exclude_fields, es_client):
    index = 'knn'
    if method == 'traditional':
        index = 'etds'
        if field == 'keyword':
            search_query = {
                "query": {
                    "query_string": {
                        "query": query
                    }
                }
            }
        else:
            search_query = {
                "query": {
                    "match": {
                        field: {
                            "query": query
                        }
                    }
                }
            }

        response = es_client.search(index=index, body=search_query,
                                    source=out_fields, source_excludes=exclude_fields, size=100)
    elif method == 'knn':
        if field != 'keyword':
            return None
        search_query = {
            "field": "abstract_vector",
            "query_vector": model.encode(query),
            "k": 100,
            "num_candidates": 100
        }
        response = es_client.search(
            index=index, knn=search_query, source=out_fields, size=100)
    elif method == 'combined':
        search_query = {
            "query": {
                "match": {
                    "abstract": {
                        "query": query,
                        "boost": 0.8
                    }
                }
            },
            "knn": {
                "field": "abstract_vector",
                "query_vector": model.encode(query),
                "k": 10,
                "num_candidates": 100,
                "boost": 0.1
            }
        }
        response = es_client.search(
            index=index, body=search_query, source=out_fields)
    else:
        return None

    all_hits = response['hits']['hits']

    documents = []
    for _, document in enumerate(all_hits):
        documents.append(document['_source'])

    return documents


def get_suggestions(query, method, field, out_fields, es_client):

    if method == 'traditional':
        if field == 'title' or field == 'author':
            search_query = {
                "_source": False,
                "fields": [f"{field}"],
                "query": {
                    "multi_match": {
                        "query": query,
                        "type": "bool_prefix",
                        "fuzziness": "AUTO",
                        "fields": [
                            f"{field}",
                            f"{field}._2gram",
                            f"{field}._3gram"
                        ]
                    }
                }
            }
            response = es_client.search(index="etds", body=search_query,
                                        source=out_fields, size=5)

            all_hits = response['hits']['hits']
            documents = []
            for _, document in enumerate(all_hits):
                documents.append(document['fields'][field][0])
            return documents

    return None


def extractChapter(body):

    chapters = []
    for object in body["objects"]:
        if object["type"] == "cleaned_text":
            chapter = {"categories": None,
                       "topics": None,
                       "summary": None,
                       "title": None,
                       "id": None}

            if object["classification"]:
                chapter["categories"] = [i["class_name"]
                                         for i in object["classification"]]

            if object["topics"] and object["topics"]["topic_term"]:
                chapter["topics"] = object["topics"]["topic_term"][1:-1]

            if object["summarization"] and object["summarization"]["summarisation_text"]:
                chapter["summary"] = object["summarization"]["summarisation_text"]

            chapter["id"] = object["id"]

            if object["local_path"]:
                chapter["title"] = "Chapter " + os.path.basename(
                    object["local_path"]).split(".")[0].split("_")[1]
            chapters.append(chapter)

    return chapters


def create_document(es_client, body):
    chapters = extractChapter(body)
    document = {**body['metadata'], "id": body["id"],
                "chapters": chapters}
    document["origin"] = "api"
    es_client.create(
        id=document["id"], index='etds', document=json.dumps(document))


# Logging which search result the user clicks on
def store_view_logs(user_id, etd_id, es_client):
    mappingObject = json.load(
        open(os.path.join(dirname, 'log_structure.json')))
    if not es_client.indices.exists(index="logs"):
        response = es_client.indices.create(
            index="logs",
            settings={},
            mappings=mappingObject
        )
        print('Mapping response for LOGS:', response)

    user_id = user_id if user_id != 'undefined' else 'default'

    try:
        data = es_client.get(id=user_id, index="logs")
        etd_ids = data['_source']['etds_read']
        etd_ids.append(etd_id)
        queries = data['_source']['queries']
    except elasticsearch.NotFoundError:
        etd_ids = [etd_id]
        queries = []

    etd_ids = list(set(etd_ids))
    try:
        es_client.index(
            index='logs',
            id=user_id,
            document={
                'user': user_id,
                'etds_read': etd_ids,
                'queries': queries
            })
    except Exception as e:
        print("Error storing document view logs: ", e)


# Logging the query the user provides
def store_search_logs(query, user_id, es_client):
    mappingObject = json.load(
        open(os.path.join(dirname, 'log_structure.json')))
    if not es_client.indices.exists(index="logs"):
        response = es_client.indices.create(
            index="logs",
            settings={},
            mappings=mappingObject
        )
        print('Mapping response for LOGS:', response)

    user_id = user_id if user_id != '' else 'default'

    try:
        data = es_client.get(id=user_id, index='logs')
        queries = data['_source']['queries']
        queries.append(query)
        etd_ids = data['_source']['etds_read']
    except elasticsearch.NotFoundError:
        queries = [query]
        etd_ids = []

    queries = list(set(queries))
    try:
        es_client.index(
            index='logs',
            id=user_id,
            document={
                'user': user_id,
                'queries': queries,
                'etds_read': etd_ids
            })
    except Exception as e:
        print("Error storing query logs: ", e)


def get_logs(user_id, es_client):
    try:
        return es_client.get(id=user_id, index='logs')['_source']
    except elasticsearch.NotFoundError:
        return []
