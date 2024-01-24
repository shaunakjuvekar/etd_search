import os
from elasticsearch import Elasticsearch
from dotenv import load_dotenv


def init():
    global client
    load_dotenv()
    if os.getenv("ELASTIC_URL") == "localhost":
        client = Elasticsearch("http://localhost:9200")
    else:
        client = Elasticsearch(
            [
                {
                    'host': os.getenv('ELASTIC_URL'),
                    'port': int(os.getenv('ELASTIC_PORT')),
                    'scheme': os.getenv('ELASTIC_SCHEME'),

                }
            ],
            basic_auth=(os.getenv('ELASTIC_USERNAME'),
                        os.getenv('ELASTIC_PASSWORD')),
            timeout=30
        )

