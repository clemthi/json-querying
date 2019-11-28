import json
import logging.config
import traceback

import pymongo
import requests

logger = logging.getLogger()
logging.config.fileConfig('logging.conf')


def load_json_from_url(api_url, api_param):
    loaded_data = {}

    header = {'Content-type': 'application/json'}
    resp = requests.get(api_url, params=api_param, headers=header)
    if resp:
        loaded_data = resp.json()
    else:
        logger.error('load_json_data query failed (%i) : %s ' % (resp.status_code, resp.text))

    return loaded_data


def save_json_data(mongo_conn_str, mongo_db, mongo_coll, json_data):
    client = pymongo.MongoClient(mongo_conn_str)
    db = client.get_database(mongo_db)

    if mongo_coll not in db.list_collection_names():
        db.create_collection(mongo_coll)
    coll = db.get_collection(mongo_coll)

    if coll.count() != 0:
        coll.drop()
    coll.insert_many(json_data)

    client.close()


def query_json_data(mongo_conn_str, mongo_db, mongo_coll):
    client = pymongo.MongoClient(mongo_conn_str)
    coll = client.get_database(mongo_db).get_collection(mongo_coll)

    query = {'status': {'$in': ["Error", "Done"]}}
    res = coll.find(query)
    print('query 1 result = %s' % str(res.count()))

    query = {'status': "Planned"}
    res = coll.find(query)
    print('query 2 result = %s' % str(res.count()))

    query = {'status': "Validated"}
    res = coll.find(query)
    print('query 3 result = %s' % str(res.count()))

    query = [{'$group': {'_id': '$status', 'count': {'$sum': 1}}}, {'$sort': {'count': -1}}]
    res = list(coll.aggregate(query))
    print('query 4 result = %s' % str(res))

    client.close()


def main():
    logger.info('*** APP START ***')
    try:
        # load config
        with open('app_config.json') as conf_file:
            config = json.load(conf_file)

        source_url = config['source_url']
        source_file = config['source_file']

        if len(source_url) > 0:
            param = {}  # set this if required
            json_data = load_json_from_url(config['source_url'], param)
        elif len(source_file) > 0:
            with open(source_file) as json_file:
                json_data = json.load(json_file)

        if len(json_data) == 0:
            logger.info('Empty json, exiting ...')
            exit(0)

        save_json_data(config['mongo']['conn_str'], config['mongo']['db'], config['mongo']['collection'], json_data)
        query_json_data(config['mongo']['conn_str'], config['mongo']['db'], config['mongo']['collection'])

    except Exception as e:
        logger.critical('Script failed: %s' % e)
        print(traceback.format_exc())

    logger.info('*** APP END ***')


if __name__ == '__main__':
    main()
