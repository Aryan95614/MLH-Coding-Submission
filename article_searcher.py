from dotenv import load_dotenv
import os
from elasticsearch import Elasticsearch
import typesense
import tempfile
import json
import requests

load_dotenv()

es_client = Elasticsearch(
    os.getenv('ES_URL'),
    api_key=os.getenv('ES_API_KEY')
)

# Retrieves the point-in-time ID.
headers = {
    "Content-Type": "application/json",
    "Authorization": f"ApiKey {os.getenv('ES_API_KEY')}"
}
response = requests.post(f"{os.getenv("ES_URL")}/newsarticles/_pit?keep_alive=1m", headers=headers)
pit_id = response.json()["id"]
print(f"pit_id = {pit_id}")

# Makes the first query to the articles index.
data = {
    "pit": {
        "id": pit_id,
        "keep_alive": "1m"
    },
    "size": 10000,
    "query": {
        "match_all": {}
    },
    "sort": [
        {"publishedAt": "desc"},
    ],
}
response = requests.post(f"{os.getenv("ES_URL")}/_search", headers=headers, json=data)

# Continues querying the articles index until there aren't any left.
hits = response.json()["hits"]["hits"]
articles = []
last_sort = []
while hits:
    for hit in hits:
        articles.append(hit)
        last_sort = hit["sort"]
    data = {
        "pit": {
            "id": pit_id,
            "keep_alive": "1m"
        },
        "size": 10000,
        "query": {
            "match_all": {}
        },
        "sort": [
            {"publishedAt": "desc"},
        ],
        "search_after": last_sort,
    }
    response = requests.post(f"{os.getenv("ES_URL")}/_search", headers=headers, json=data)
    hits = response.json()["hits"]["hits"]
print(f"len(articles) = {len(articles)}")

ts_client = typesense.Client({
  'nodes': [{
    'host': 'typesense-on-render-tk1y.onrender.com', # For Typesense Cloud use xxx.a1.typesense.net
    'port': '443',      # For Typesense Cloud use 443
    'protocol': 'https'   # For Typesense Cloud use https
  }],
  'api_key': 'xyz',
  'connection_timeout_seconds': 2
})
schema = {
    "name" : "news_articles",
    "fields": [
        # Metadata.
        {"name": "_index", "type": "string"},
        {"name": "_id", "type": "string"},
        {"name": "_score", "type": "auto"},
        {"name": "sort", "type": "auto"},

        # Actual article source data.
        {"name": "interest", "type": "string"},
        {"name": "source", "type": "string"},
        {"name": "author", "type": "string"},
        {"name": "title", "type": "string"},
        {"name": "description", "type": "string"},
        {"name": "url", "type": "string"},
        {"name": "urlToImage", "type": "string"},
        {"name": "color", "type": "string"},
        {"name": "publishedAt", "type": "string"}, # can't store as a datetime object in Typesense
        {"name": "content", "type": "string"},
        {"name": "briefs", "type": "string[]"},
        {"name": "comments", "type": "object[]", "optional": True},
        {"name": "upvotes", "type": "string[]"},
        {"name": "downvotes", "type": "string[]"},
    ],
    "enable_nested_fields": True,
}
try:
    ts_client.collections.create(schema)
except typesense.exceptions.ObjectAlreadyExists:
    print("schema already existed in the Typesense server")
    ts_client.collections["news_articles"].delete()
    ts_client.collections.create(schema)
except Exception as e:
    print(f"--------\nunexpected error creating schema:\n{e}\n--------")

# Loads the articles into the Typesense server.
for i in range(0, len(articles), 1000):
    segment = articles[i : min(len(articles), i + 1000)]
    with tempfile.NamedTemporaryFile(mode="w+t", delete=True) as file:
        for article in segment:
            doc = {}
            doc["_index"] = article["_index"]
            doc["_id"] = article["_id"]
            doc["_score"] = article["_score"]
            doc["sort"] = article["sort"]
            for field in article["_source"]:
                doc[field] = article["_source"][field]
            # print(f"article = {article}\n\n")
            # print(f"doc = {doc}\n\n")
            file.write(f"{json.dumps(doc)}\n")
        file.seek(0)
        content = file.read()
        response = ts_client.collections['news_articles'].documents.import_(content.encode("utf-8"))
        # print(f"response from importing = {response}")
    print(f"i = {i}, finished importing")

# Writes all documents that were successfully loaded into Typesense into a text file.
with open("imported_articles.txt", "w") as file:
    documents = ts_client.collections["news_articles"].documents.export()
    file.write(documents)

# Sample search requests.
search_parameters = {
  'q'         : 'art',
  'query_by'  : 'title',
#   'sort_by'   : '_updated_at:desc'
}
response = ts_client.collections['news_articles'].documents.search(search_parameters)
for i in range(len(response["hits"])):
    print(f"response['hits'][{i}] = {response["hits"][i]}\n")
print(f"search time in ms = {response["search_time_ms"]}")
