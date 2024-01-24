import pandas as pd
import json
from helpers import embeddings
from elasticsearch import helpers

index_prefix = 'experiment_'
default_experiments = ['experiment_all-distilroberta-v1_default', 'experiment_all-mpnet-base-v2_default',
                       'experiment_all-minilm-l12-v2_default', 'experiment_labse_default']


def create_experiment_index(name, es_client, file, dims, similarity, user='default', model_name='custom'):
    try:
        mapping = {
            "properties": {
                "vector": {
                    "type": "dense_vector",
                    "dims": dims,
                    "index": "true",
                    "similarity": similarity
                }
            }
        }

        index_name = f"{index_prefix}{name}"
        if not es_client.indices.exists(index=index_name):
            print("Index does not exist")
            es_client.indices.create(
                index=index_name,
                settings={},
                mappings=mapping
            )
        else:
            return f"Experiment {name}: Experiment already exists. Delete the existing or create another one."

        if not es_client.indices.exists(index='experiments'):
            es_client.indices.create(index='experiments')

        experiment_data = {
            "user": user,
            "name": name,
            "model": model_name
        }

        es_client.index(index='experiments', document=experiment_data, id=f"{user}_{name}")

        df = pd.read_csv(file)
        json_str = df.to_json(orient='records')

        json_records = json.loads(json_str)

        documents = []
        for row in json_records:
            row['vector'] = [float(x.strip(','))
                             for x in row['vector'][1:-1].split()]
            doc = {
                '_op_type': 'index',
                '_index': index_name,
                '_source': row
            }

            documents.append(doc)
        helpers.bulk(es_client, documents)
        return "Index Success!"
    except Exception as e:
        return f"Failed to index, check the input format of all the parameters. Error: {e}"


def delete_experiment(name, es_client, user='default'):
    if name in default_experiments:
        return f"Cannot delete default experiment"
    try:
        index_name = f"{index_prefix}{name}"
        if es_client.indices.exists(index=index_name):
            es_client.indices.delete(index=index_name)

        es_client.delete(index="experiments", id=f"{user}_{name}")
        return f"Experiment {name}: Deleted the experiment."
    except Exception as e:
        return f"Failed to delete the experiment {name}. Error: {e}"


def get_experiments(es_client, user='default'):
    try:
        query = {
            "query": {
                "terms": {"user.keyword": ["default", user]}
            }
        }

        response = es_client.search(index='experiments', body=query)
        all_hits = response['hits']['hits']

        documents = []
        for _, document in enumerate(all_hits):
            documents.append(document['_source'])

        return documents
    except Exception as e:
        print(e)
        return []


def search_experiment(name, query, query_vector, knn_weight, k, es_client):
    index_name = f"{index_prefix}{name}"
    if not es_client.indices.exists(index=index_name):
        return f"Experiment {name}: Index does not exist. Create an experiment first"

    if name in default_experiments:
        experiment = es_client.get(index='experiments', id=f"default_{name}")['_source']
        model = embeddings.Model(experiment['model'])
        vector = model.encode(query)
    else:
        vector = [float(x.strip(','))
                  for x in query_vector[1:-1].split()]

    search_query = {
        "query": {
            "match": {
                "abstract": {
                    "query": query,
                    "boost": 1 - float(knn_weight)
                }
            }
        },
        "knn": {
            "field": "vector",
            "query_vector": vector,
            "k": int(k),
            "num_candidates": 100,
            "boost": float(knn_weight)
        }
    }
    response = es_client.search(
        index=index_name, body=search_query, source=['title', 'id'], size=int(k))

    all_hits = response['hits']['hits']

    documents = []
    for _, document in enumerate(all_hits):
        result = document['_source']
        result['score'] = document['_score']
        documents.append(result)

    return documents
