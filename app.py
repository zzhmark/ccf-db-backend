import json

from elasticsearch import Elasticsearch
from flask import Flask, url_for, render_template, request
import psql
from flask_cors import CORS
# import pandas as pd
from datetime import date
from flask_pymongo import PyMongo
from bson.json_util import dumps
from bson.objectid import ObjectId
from bson import binary
from pymongo import MongoClient

#
# client = MongoClient('127.0.0.1', 27017) #连接mongodb
# db = client.photo #连接对应数据库
# image_collection = db.images
# data = requests.get(dic["photo_url"], timeout=10).content
# if not image_collection.find_one({"photo_url":dic["photo_url"]})
#     dic["imagecontent"] = binary.Binary(data)
#     image_collection.insert(dic)1234567891011


# init


app = Flask(__name__)
cors = CORS(app)

es = Elasticsearch([{"host": "127.0.0.1", "port": 9200}])

mongo_static = PyMongo(app, uri="mongodb://localhost:27017/ccf_static")
mongo_main = PyMongo(app, uri="mongodb://localhost:27017/ccf_main")


# es


@app.route("/create_index", methods=["GET"])
def create_index():
    if request.method == "GET":
        # index_name数据库名称，类似database
        index_name = request.args.get("index_name")
        if es.indices.exists(index_name):
            return "index already exists, rename it please"
        else:
            print("create index in es: ", index_name)
            result = es.indices.create(index=index_name, ignore=400)
            return result
    else:
        return "Please send GET request"


@app.route("/put_mapping", methods=['POST'])
def put_mapping():
    if request.method == "POST":
        data = json.loads(request.get_data(as_text=True))
        # index_name数据库名称，类似database
        index_name = data["index_name"]
        mappings = data['mappings']

        # print(index_name)
        # print(type(mappings))
        # print(mappings)
        # return "s"

        if not es.indices.exists(index_name):
            return "index not exists, create it first please"
        # 实例化数据库，body规定所存储数据的类型
        result = es.indices.put_mapping(index=index_name, body=mappings)
        return result
    else:
        return "Please send POST request"


@app.route("/add_document", methods=['POST'])
def add_document():
    if request.method == "POST":
        data = json.loads(request.get_data(as_text=True))
        # index_name数据库名称，类似database
        index_name = data["index_name"]
        doc = data['doc']
        # print(index_name)
        # print(doc)
        # return "s"
        result = es.index(index=index_name, body=doc)
        return result
    else:
        return "Please send POST request"


# @app.route("/add_documents", methods=['GET'])
# def add_documents():
#     df = pd.read_csv("./literatures.csv", sep=',', header=0)
#     values = json.loads(request.get_data(as_text=True))
#     df.fillna(value=values, inplace=True)
#     for i in df.index.to_list():
#         p = df.loc[i, :].to_dict()
#         id_ = p['PMID']
#         if id_ == '-1111':
#             es.index(index="literatures", body=p)
#         else:
#             es.index(index="literatures", body=p, id=id_)
#     return "success"


@app.route("/delete_index", methods=["DELETE"])
def delete_index():
    if request.method == "DELETE":
        # index_name数据库名称，类似database
        index_name = request.args.get("index_name")
        result = es.indices.delete(index=index_name)
        return result
    else:
        return "Please send POST request"


# match = {
#     "query": {
#         "match": {
#             "_id": "aSlZgGUBmJ2C8ZCSPVRO"
#         }
#     }
# }

@app.route("/search_index_basic", methods=['GET'])
def search_index_basic():
    index_name = request.args.get("index_name")
    content = request.args.get("content")
    query = {'query': {'multi_match': {'query': content, 'fields': ['title', 'abstract']}}}
    result = es.search(index=index_name, body=query)
    print(result['hits']['hits'])
    return json.dumps(result['hits']['hits'])


@app.route("/search_index", methods=['GET'])
def search_index():
    index_name = request.args.get("index_name")
    title = request.args.getlist("title")
    abstract = request.args.getlist("abstract")
    author = request.args.getlist("author")
    assay_type = request.args.getlist("assay_type")
    gt = request.args.get("gt")
    lt = request.args.get("lt")

    title_term = {'terms': {"title": title}}
    abstract_term = {'terms': {'abstract': abstract}}
    author_term = {'terms': {"author": author}}
    assay_type_term = {'terms': {'assay': assay_type}}

    # print('type gte', type(gte))
    if not lt:
        lt = date.today()
    if not gt:
        gt = date.min
    time_range = {'range': {'date': {'gt': gt, 'lt': lt}}}

    must_list = [time_range]
    if title:
        must_list.append(title_term)
    if abstract:
        # print("ab ",abstract)
        must_list.append(abstract_term)
    if author:
        must_list.append(author_term)
    if assay_type:
        must_list.append(assay_type_term)

    query = {'query': {'bool': {'must': must_list}}}
    # print(query)
    result = es.search(index=index_name, body=query)
    print(result['hits']['hits'])
    return json.dumps(result['hits']['hits'])


# mongo ccf static

@app.route('/get_draco', methods=['GET'])
def get_draco():
    try:
        id = int(request.args.get('id'))
        brain_model = mongo_static.db.brain_model.find({'id': id})
    except:
        name = request.args.get('id')
        brain_metadata = mongo_static.db.brain_metadata.find({'name': name})
        print(brain_metadata[0])
        id = brain_metadata[0]['id']
        brain_model = mongo_static.db.brain_model.find({'id': id})
    return dumps(brain_model[0]['draco'])


@app.route('/get_brain_metadata', methods=['GET'])
def get_brain_metadata():
    id = [int(i) for i in request.args.getlist('id')]
    brain_metadata = mongo_static.db.brain_metadata.find({'$or': [{'id': i} for i in id]})
    id_inv = dict(zip(id, range(len(id))))
    out = id.copy()
    for meta in brain_metadata:
        out[(id_inv[meta['id']])] = meta
    return dumps(out)


# mongo ccf main


@app.route('/get_recipe', methods=['GET'])
def get_recipe():
    oid = request.args.get('oid')
    res = mongo_main.db.recipe.find({"_id": ObjectId(oid)})
    return dumps(res[0])


@app.route('/get_dataframe', methods=['GET'])
def get_dataframe():
    oid = request.args.get('oid')
    res = mongo_main.db.dataframe.find({"_id": ObjectId(oid)})
    return dumps(res[0])


@app.route('/get_ingredient', methods=['GET'])
def get_ingredient():
    oid = request.args.get('oid')
    res = mongo_main.db.ingredient.find({"_id": ObjectId(oid)})
    return dumps(res[0])


@app.route('/get_reference', methods=['GET'])
def get_reference():
    oid = request.args.get('oid')
    res = mongo_main.db.reference.find({"_id": ObjectId(oid)})
    return dumps(res[0])


@app.route('/get_collection', methods=['GET'])
def get_collection():
    oid = request.args.get('oid')
    res = mongo_main.db.collection.find({"_id": ObjectId(oid)})
    return dumps(res[0])


if __name__ == "__main__":
    app.run(host='0.0.0.0')
