from elasticsearch import Elasticsearch
from dotenv import load_dotenv
import os

load_dotenv()

elastic_name = os.getenv("elastic_name")
password = os.getenv("password")
ca_certs = os.getenv("ca_certs")
host = os.getenv("host")


elasticsearch = Elasticsearch(
    ["https://localhost:9200"],
    basic_auth=(elastic_name, password),
    ca_certs=ca_certs,
)

index_name = "tweets"


def index_settings_constructor():
    index_settings = {
        "mappings": {
            "properties": {
                "tweet_id": {"type": "keyword"},
                "text": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                },
                "hashtags": {"type": "keyword"},
                "created_at": {
                    "type": "date",
                    "format": "strict_date_optional_time||epoch_millis",
                },
                "geo_coordinates": {"type": "geo_point", "null_value": "0,0"},
                "user": {
                    "properties": {
                        "user_id": {"type": "keyword"},
                        "name": {
                            "type": "text",
                            "fields": {
                                "keyword": {"type": "keyword", "ignore_above": 256}
                            },
                        },
                        "screen_name": {"type": "keyword"},
                        "location": {"type": "text"},
                    }
                },
                "sentiments": {"type": "keyword"},
            }
        },
    }
    return index_settings


def index_exists(index_name):
    return elasticsearch.indices.exists(index=index_name)


if __name__ == "__main__":
    configurations = index_settings_constructor()

    if index_exists(index_name=index_name):
        elasticsearch.indices.delete(index=index_name)
        print(f"Index {index_name} already exist")
        exit(0)

    created_index = elasticsearch.indices.create(
        index=index_name, ignore=400, body=configurations
    )
    print(created_index)
    print(f"Index {index_name} created")
