from elasticsearch import Elasticsearch
import hashlib

# Настройки подключения к Elasticsearch
es = Elasticsearch(
    [{"host": "127.0.0.1", "port": 9200, "scheme": "http"}],
    basic_auth=("elastic", "nuMZ7JRTtJpQYSORhI=n")
)

# Удаление индекса, если он существует
if es.indices.exists(index="parsernews"):
    es.indices.delete(index="parsernews")

# Создание индекса с указанными полями
index_settings = {
    "mappings": {
        "properties": {
            "url": {
                "type": "text"
            },
            "text": {
                "type": "text"
            },
            "date": {
                "type": "text"
            },
            "author": {
                "type": "text"
            },
            "title": {
                "type": "text"
            },
            "hash": {
                "type": "text"
            }
        }
    }
}

es.indices.create(index="parsernews", **index_settings)