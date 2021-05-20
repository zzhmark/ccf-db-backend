import json

from elasticsearch import Elasticsearch
from flask import Flask, url_for, render_template, request
import psql

app = Flask(__name__)

es = Elasticsearch([{"host": "192.168.3.128", "port": 9200}])





# Mysql -> database -> table -> rows -> columns
# Elasticsearch -> index -> type -> documents -> fields

# body = {
#     "mappings": {
#         "type": {
#             "properties": {
#                 "title": {
#                     "type": "text",
#                     "analyzer": "ik_max_word"
#                 },
#                 "url": {
#                     "type": "text"
#                 },
#                 "action_type": {
#                     "type": "text"
#                 },
#                 "content": {
#                     "type": "text"
#                 }
#             }
#         }
#     }
# }
#


@app.route("/create_index", methods=["POST"])
def create_index():
    if request.method == "POST":
        # index_name数据库名称，类似database
        index_name = request.form.get("index_name")
        es.indices.create(index=index_name, ignore=400)
        return "create success"
    else:
        return "not post"


@app.route("/put_mapping", methods=['POST'])
def put_mapping():
    if request.method == "POST":
        # index_name数据库名称，类似database
        index_name = request.form.get("index_name")
        # 实例化数据库，body规定所存储数据的类型
        body = request.form.get("body")
        doc_type = request.form.get("doc_type")
        es.indices.put_mapping(index=index_name, body=body, doc_type=doc_type)
        return "put mapping success"
    else:
        return "not post"


@app.route("/add_index", methods=['POST'])
def add_index():
    if request.method == "POST":
        index_name = request.form.get("index_name")
        doc_type = request.form.get("doc_type")
        body = request.form.get("body")
        _id = request.form.get("_id")
        es.index(index=index_name, body=body, doc_type=doc_type, id=_id)
        return "index success"
    else:
        return "not post"


@app.route("/add_indexes", methods=['POST'])
def add_indexes():
    if request.method == "POST":
        index_name = request.form.get("index_name")
        doc_type = request.form.get("doc_type")
        bodies = request.form.get("bodies")
        es.bulk(index=index_name, doc_type=doc_type, body=bodies)
        return "bulk success"
    else:
        return "not post"


@app.route("/delete_index", methods=["POST"])
def delete_index():
    if request.method == "POST":
        # index_name数据库名称，类似database
        index_name = request.form.get("index_name")
        es.indices.delete(index=index_name)
        return "delete success"
    else:
        return "not post"


# match = {
#     "query": {
#         "match": {
#             "_id": "aSlZgGUBmJ2C8ZCSPVRO"
#         }
#     }
# }

@app.route("/search_index", methods=['POST'])
def search_index():
    if request.method == "POST":
        index_name = request.form.get("index_name")
        doc_type = request.form.get("doc_type")
        match = request.form.get("match")
        result = es.search(index=index_name, doc_type=doc_type, body=match)
        return json.dumps(result, indent=2, ensure_ascii=False)
    else:
        return "not post"

@app.route('/get_collection', methods=['GET'])
def get_collection():
    id = request.form.get('id')
    col_res = psql.query('select * from data_info.collection')

if __name__ == "__main__":
    app.run()
