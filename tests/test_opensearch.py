import logging

import boto3
import pandas as pd


import awswrangler as wr


logging.getLogger("awswrangler").setLevel(logging.DEBUG)

# TODO: create test_infra for opensearch
OPENSEARCH_DOMAIN = 'search-es71-public-z63iyqxccc4ungar5vx45xwgfi.us-east-1.es.amazonaws.com'  # change to your domain
OPENSEARCH_DOMAIN_FGAC = 'search-os1-public-urixc6vui2il7oawwiox2e57n4.us-east-1.es.amazonaws.com'


def test_connection():
    client = wr.opensearch.connect(host=OPENSEARCH_DOMAIN)
    print(client.info())


# def test_fgac_connection():
#     client = wr.opensearch.connect(host=OPENSEARCH_DOMAIN_FGAC,
#                                    fgac_user='admin',
#                                    fgac_password='SECRET')
#     print(client.info())


def test_create_index():
    client = wr.opensearch.connect(host=OPENSEARCH_DOMAIN)
    response = wr.opensearch.create_index(
        client,
        index='test-index1',
        mappings={
            'properties': {
                'name': {'type': 'text'},
                'age': {'type': 'integer'}
            }
        },
        settings={
            'index': {
                'number_of_shards': 1,
                'number_of_replicas': 1
            }
        }
    )
    print(response)


def test_index_df():
    client = wr.opensearch.connect(host=OPENSEARCH_DOMAIN)
    response = wr.opensearch.index_df(client,
                                      df=pd.DataFrame([{'_id': '1', 'name': 'John'},
                                                       {'_id': '2', 'name': 'George'},
                                                       {'_id': '3', 'name': 'Julia'}
                                                       ]),
                                      index='test_index_df1'
                                      )
    print(response)


def test_index_documents():
    client = wr.opensearch.connect(host=OPENSEARCH_DOMAIN)
    response = wr.opensearch.index_documents(client,
                                      documents=[{'_id': '1', 'name': 'John'},
                                                 {'_id': '2', 'name': 'George'},
                                                 {'_id': '3', 'name': 'Julia'}
                                                ],
                                      index='test_index_documents1'
                                      )
    print(response)