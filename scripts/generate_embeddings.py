import pandas as pd
import numpy as np
import requests
import os
import sys
import csv
import json

dirname = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(dirname)
sys.path.append(parent)

from helpers import embeddings
from services import experiment_service
from settings import elasticsearch_settings

elasticsearch_settings.init()
es_client = elasticsearch_settings.client
columns = ['title', 'author', 'advisor', 'year', 'abstract', 'university',
           'degree', 'source', 'department', 'discipline', 'id', 'vector']


def convert_to_vector(documents, model, max_length):
    embedding_list = []
    for doc in documents:
        abstract = doc["metadata"]["abstract"]
        title = doc["metadata"]["title"]
        sentences = f"{abstract} {title}"
        if len(sentences) > max_length:
            out_vector = model.encode_plus(sentences)
        else:
            out_vector = model.encode(sentences)

        metadata = []
        for col in columns[:-2]:
            metadata.append(doc['metadata'][col])
        metadata.append(doc['id'])
        metadata.append(out_vector)
        embedding_list.append(metadata)

    return embedding_list


def create_embeddings_csv(start, end, limit, model, create_csv=False):
    if create_csv:
        vector_map = []
        """
        for i in range(start, end, limit):
            print(i)
            api_url = "https://team-1-flask.discovery.cs.vt.edu/v1/digitalobjects/all?start=" + str(
                i) + "&limit=" + str(
                limit)
            response = requests.get(api_url)
            if response.status_code == 200:
                print("Valid response...indexing data")
                response = response.json()
                etds = response['etds']
                vector_map = vector_map + convert_to_vector(etds, model, model.max_length)
            else:
                raise Exception("Improper response received")
        """
        directory = 'documents'
        for j, filename in enumerate(os.listdir(directory)):
            if j == 20:
                break
            print("Indexing response: " + str(j))
            f = open(os.path.join(directory, filename), 'r')
            data = json.load(f)
            etds = data['etds']
            vector_map = vector_map + convert_to_vector(etds, model, model.max_length)

        dataframe = pd.DataFrame(vector_map, columns=columns)
        dataframe.set_index('id')
        dataframe.to_csv(f"{model.model_name}.csv")

    f = open(f"{model.model_name}.csv")
    experiment_name = f"experiment_{model.model_name}_default".lower()
    result = experiment_service.create_experiment_index(experiment_name, es_client, f,
                                                        model.dims, model.similarity, 'default', model.model_name)
    print(result)


if __name__ == '__main__':
    with open("default_models.csv", 'r') as file:
        csvFile = csv.DictReader(file)
        for line in csvFile:
            print('Model for index:' + line['name'])
            local_model = embeddings.Model(line['name'], int(line['dims']), int(line['max_length']), line['similarity'])
            create_embeddings_csv(1, 10, 10, local_model, True)
