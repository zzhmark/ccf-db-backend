import json

from elasticsearch import Elasticsearch
from flask import Flask, url_for, render_template, request
import psql
from flask_cors import CORS
import pandas as pd
from datetime import date
from flask_pymongo import PyMongo
from bson.json_util import dumps

app = Flask(__name__)
cors = CORS(app)

es = Elasticsearch([{"host": "127.0.0.1", "port": 9200}])

mongo = PyMongo(app, uri="mongodb://localhost:27017/ccf_static")

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


@app.route("/add_documents", methods=['GET'])
def add_documents():
    df = pd.read_csv("./literatures.csv", sep=',', header=0)
    values = json.loads(request.get_data(as_text=True))
    df.fillna(value=values, inplace=True)
    for i in df.index.to_list():
        p = df.loc[i, :].to_dict()
        id_ = p['PMID']
        if id_ == '-1111':
            es.index(index="literatures", body=p)
        else:
            es.index(index="literatures", body=p, id=id_)
    return "success"


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


@app.route('/get_collection', methods=['GET'])
def get_collection():
    id = request.args.get('id')
    res = psql.query('select * from data_info.collection where collection_id=' + id)
    return json.dumps(res, indent=2, ensure_ascii=False, cls=psql.JsonCustomEncoder).replace("\\", "")
    # frame_id = col_res[0][1]
    # lit_id = col_res[0][2]
    # recipe_id = col_res[0][3]
    # frame_res = [psql.query('select * from dataframes.frame_' + str(i)) for i in frame_id]
    # lit_res = [psql.query('select * from data_info.literature where literature_id=' + str(i)) for i in lit_id]
    # recipe_res = [psql.query('select * from data_vis.recipe where recipe_id=' + str(i)) for i in recipe_id]
    # return json.dumps({'frames': frame_res, 'literatures': lit_res, 'recipes': recipe_res},
    #                   indent=2, ensure_ascii=False, cls=psql.JsonCustomEncoder)


@app.route('/get_literature', methods=['GET'])
def get_literature():
    id = request.args.get('id')
    res = psql.query('select * from data_info.literature where literature_id=' + id)
    return json.dumps(res, indent=2, ensure_ascii=False, cls=psql.JsonCustomEncoder).replace("\\", "")


@app.route('/get_frame', methods=['GET'])
def get_frame():
    id = request.args.get('id')
    res = psql.query('select * from dataframes.frame_' + id)
    return json.dumps(res, indent=2, ensure_ascii=False, cls=psql.JsonCustomEncoder).replace("\\", "")


@app.route('/get_frame_info', methods=['GET'])
def get_frame_info():
    id = request.args.get('id')
    res = psql.query('select * from data_info.frame where dataframe_id=' + id)
    return json.dumps(res, indent=2, ensure_ascii=False, cls=psql.JsonCustomEncoder).replace("\\", "")


@app.route('/get_recipe', methods=['GET'])
def get_recipe():
    id = request.args.get('id')
    res = psql.query('select * from data_vis.recipe where recipe_id=' + id)
    return json.dumps(res, indent=2, ensure_ascii=False, cls=psql.JsonCustomEncoder).replace("\\", "")
    # strategy_res = [psql.query('select * from data_vis.strategy where strategy_id=' + str(i)) for i in strategy_id]
    # frame_res = [psql.query('select * from dataframes.frame_' + str(i)) for i in frame_id]
    # return json.dumps({'frames': frame_res, 'strategies': strategy_res, 'title': recipe_res[0][3],
    #                    'description': recipe_res[0][4], 'mapping': recipe_res[0][5], 'filter': recipe_res[0][6]},
    #                   indent=2, ensure_ascii=False, cls=psql.JsonCustomEncoder)


@app.route('/get_unit', methods=['GET'])
def get_unit():
    id = request.args.get('id')
    res = psql.query('select * from data_vis.unit where unit_id=' + id)
    # strategy_id = unit_res['records'][0][1]
    # frame_id = unit_res['records'][0][2]
    # strategy_res = psql.query('select * from data_vis.strategy where strategy_id=' + str(strategy_id))
    # frame_res = psql.query('select * from dataframes.frame_' + str(frame_id))
    return json.dumps(res, indent=2, ensure_ascii=False, cls=psql.JsonCustomEncoder).replace("\\", "")

    # return json.dumps({'frame': frame_res, 'strategy': strategy_res, 'title': unit_res['records'][0][3],
    #                    'description': unit_res['records'][0][4], 'mapping': unit_res['records'][0][5], 'filter': unit_res['records'][0][6]},
    #                   indent=2, ensure_ascii=False, cls=psql.JsonCustomEncoder)


@app.route('/get_strategy', methods=['GET'])
def get_strategy():
    id = request.args.get('id')
    res = psql.query('select * from data_vis.strategy where strategy_id=' + id)
    return json.dumps(res, indent=2, ensure_ascii=False, cls=psql.JsonCustomEncoder).replace("\\", "")


# mongodb ccf static

@app.route('/get_draco', methods=['GET'])
def get_draco():
    id = int(request.args.get('id'))
    brain_model = mongo.db.brain_model.find({'id': id})
    return dumps(brain_model[0]['draco'])


@app.route('/get_brain_metadata', methods=['GET'])
def get_brain_metadata():
    id = [int(i) for i in request.args.getlist('id')]
    brain_model = mongo.db.brain_metadata.find({'$or': [{'id': i} for i in id]})
    id_inv = dict(zip(id, range(len(id))))
    out = id.copy()
    for meta in brain_model:
        out[(id_inv[meta['id']])] = meta
    return dumps(out)


if __name__ == "__main__":
    app.run(host='0.0.0.0')
