#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tuesday Nov 20 22:50:23 2022

@authors: shaunakjuvekar, manojprabhakar, ramaraja
"""
import json
import os
import argparse
import math
import sys
from elasticsearch import helpers
import requests

dirname = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(dirname)
sys.path.append(parent)

from settings import elasticsearch_settings

parser = argparse.ArgumentParser(
    prog='index_chapters.py',
    description='Fetches ETD chapter information and indexes them in Elasticsearch',
    epilog='Contact Team 2 for any questions at cs-5604_team-2-g@vt.edu')

parser.add_argument('-c', '--count',
                    default=50, help='Count of documents to index(multiples of 50)')
parser.add_argument("-s", "--start",
                    default=1,
                    help="Offset for indexing")

args = parser.parse_args()

elasticsearch_settings.init()
es = elasticsearch_settings.client

mappingObject = json.load(open(os.path.join(dirname, 'chapter_template.json')))

if not es.indices.exists(index="chapters"):
    response = es.indices.create(
        index="chapters",
        settings={},
        mappings=mappingObject
    )
    print('Mapping response:', response)
else:
    print("Index: chapters already exists")


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
                           "id": None,
                           "etd_id": None,
                           "etd_title": None}

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
                if etd["metadata"] and etd["metadata"]["title"]:
                    chapter["etd_title"] = etd["metadata"]["title"]
                if etd["id"]:
                    chapter["etd_id"] = etd["id"]
                chapters.append(chapter)
        all_chapters.append(chapters)
    return all_chapters


def get_and_generate_chapters(page):
    page = page
    per_page = 50
    url = "https://team-1-flask.discovery.cs.vt.edu/v2/digitalobjects/all?page=" + str(page) + "&per_page=" + str(
        per_page)

    response = requests.get(url)
    if response.status_code == 200:
        print(f"Indexing documents {page}")
        print("Indexing....")
        response = response.json()
        all_chapters = process_object(response)
        for chapters in all_chapters:
            for chapter in chapters:
                yield {
                    "_index": "chapters",
                    "_id": chapter["id"],
                    "_source": chapter,
                }

    else:
        response.raise_for_status()


for page in range(1, 101):
    helpers.bulk(es, get_and_generate_chapters(page))

result = es.count(index="chapters")
print(f"Indexed {result} documents")

# print(res)
