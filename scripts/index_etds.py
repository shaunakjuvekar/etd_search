#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 14 23:48:46 2022

@authors: shaunakjuvekar, manojprabhakar, ramaraja
"""
import json
import os
import argparse
import math
import sys
from elasticsearch import helpers
import requests
import pandas as pd

dirname = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(dirname)
sys.path.append(parent)

from settings import elasticsearch_settings

parser = argparse.ArgumentParser(
    prog='index_etds.py',
    description='Fetches ETD information and indexes them in Elasticsearch',
    epilog='Contact Team 2 for any questions at cs-5604_team-2-g@vt.edu')

parser.add_argument('-c', '--count',
                    default=1000, help='Count of documents to index')
parser.add_argument("-s", "--start",
                    default=1,
                    help="Offset for indexing")

args = parser.parse_args()

elasticsearch_settings.init()
es = elasticsearch_settings.client

mappingObject = json.load(open(os.path.join(dirname, 'template.json')))
knn_mappings = json.load(open(os.path.join(dirname, 'knn_index.json')))

df = pd.read_csv(open(os.path.join(dirname, 'all-distilroberta-v1.csv')))

if not es.indices.exists(index="etd"):
    response = es.indices.create(
        index="etd",
        settings={},
        mappings=mappingObject
    )
    print('Mapping response:', response)
else:
    print("Index: etd already exists")

if not es.indices.exists(index="knn"):
    response = es.indices.create(
        index="knn",
        settings={},
        mappings=knn_mappings
    )
    print('Knn Mapping response:', response)
else:
    print("Index: knn already exists")

if not es.indices.exists(index="experiments"):
    response = es.indices.create(
        index="experiments",
        settings={},
        mappings=mappingObject
    )
    print('Experiments Mapping response:', response)
else:
    print("Index: Experiments already exists")


def get_abstract_embeddings(etd_url):
    row = df[df['etd_url'] == etd_url]
    if len(row) == 0:
        return None
    return row.iloc[0]['vector']


def generate_docs(page):
    page = page
    per_page = 50
    url = "https://team-1-flask.discovery.cs.vt.edu/v2/digitalobjects/all?page=" + str(page) + "&per_page=" + str(
        per_page)
    response = requests.get(url)
    if response.status_code == 200:
        print(f"Indexing documents {page}")
        response = response.json()
        all_chapters = process_object(response)

        for i, obj in enumerate(response["etds"]):
            vector = get_abstract_embeddings(obj["metadata"]["source"])
            if vector is None:
                vector = [-100000] * 768
            else:
                vector = [float(x.strip(','))
                                   for x in vector[8:-2].split()]
            doc = {**obj['metadata'], "id": obj["id"], "chapters": all_chapters[i]}
            yield {
                "_index": "etd",
                "_id": (doc["id"]),
                "_source": doc,
            }
            doc["vector"] = vector
            yield {
                "_index": "knn",
                "_id": (doc["id"]),
                "_source": doc
            }

    else:
        response.raise_for_status()


def process_object(data):
    all_chapters = []
    for etd in data["etds"]:
        chapters = []
        for object in etd["objects"]:
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
        all_chapters.append(chapters)
    return all_chapters


def create_index_from_local(es):
    directory = 'documents'
    for j, filename in enumerate(os.listdir(directory)):
        print("Indexing response: " + str(j))
        f = open(os.path.join(directory, filename), 'r')
        data = json.load(f)
        all_chapters = process_object(data)
        for i, obj in enumerate(data["etds"]):
            doc = {**obj['metadata'], "id": obj["id"], "chapters": all_chapters[i]}
            yield {
                "_index": "etd",
                "_id": (doc["id"]),
                "_source": doc,
            }


fetch_count = 2 if math.ceil(int(args.count) / 50) <= 1 else math.ceil(
    int(args.count) / 50)

# To create etds index from local files
# helpers.bulk(es, create_index_from_local(es))

# To create etds index from team 1 api.
for i in range(args.start, fetch_count + 1):
    helpers.bulk(es, generate_docs(i))


result = es.count(index="etd")
print(f"Indexed {result} documents")
