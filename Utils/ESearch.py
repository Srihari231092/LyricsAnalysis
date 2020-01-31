"""
@file_name : ElasticSearchUtils.py
@author : Srihari Seshadri
@description : This file contains class for working with the elastic search instance
@date : 01-29-2019
"""

import json
import hashlib
import numpy as np

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Mapping

# DO NOT EVER CHANGE THE ELASTIC_SEARCH_ENDPOINT VARIABLE
ELASTIC_SEARCH_ENDPOINT = "https://search-testdomain-25rapy5h2xx5yznlinkatpnr6i.us-west-2.es.amazonaws.com/"


class ESearch():

    def __init__(self):
        """
        Initialize class parameters
        """
        # Connection object
        self._es = None
        self._index_name = "article_data"
        self._hash_field = "URL"
        self._dict_of_duplicate_docs = {}

    def connect_to_es(self, host_name=ELASTIC_SEARCH_ENDPOINT):
        """
        Establishes a connection to the Elastic search server.
        If server if pingable, returns connection object.
        Else return None
        :return: connection-object
        """
        self._es = Elasticsearch(hosts=[host_name], timeout=60)
        # Ping the connection to check if it's alive
        if self._es.ping():
            return self._es
        return None

    def index_exists(self, index_name=None):
        if not index_name:
            index_name = self._index_name
        return self._es.indices.exists(index_name)

    def _make_mapping(self):
        """
        Creates the index with the correct mapping
        :return:
        """
        m = Mapping()
        # add fields
        m.field('Title', 'text')
        m.field('Text', 'text')
        m.field('Publish_Date', 'date') # date type complicates matters across websites
        m.field('URL', 'text')
        m.field('Scrape_Date', 'date')  # date type complicates matters across websites
        m.field('Source', 'text')
        m.field('Search_Keyword', 'text')   # save list as text?
        m.field('SE_Is_Risk', 'boolean')
        m.field('GP_Is_Risk', 'boolean')
        m.field('RG_Is_Risk', 'boolean')
        m.field('SE_Risk_Rating', 'float')
        m.field('GP_Risk_Rating', 'float')
        m.field('RG_Risk_Rating', 'float')
        m.field('SE_SnP_Open', 'float')
        m.field('SE_SnP_Close', 'float')
        m.field('SE_AbbV_Open', 'float')
        m.field('SE_AbbV_Close', 'float')
        m.field('SE_XBI_Open', 'float')
        m.field('SE_XBI_Close', 'float')
        m.field('SE_SnP_Open_Plus1', 'float')
        m.field('SE_SnP_Close_Plus1', 'float')
        m.field('SE_AbbV_Open_Plus1', 'float')
        m.field('SE_AbbV_Close_Plus1', 'float')
        m.field('SE_XBI_Open_Plus1', 'float')
        m.field('SE_XBI_Close_Plus1', 'float')
        m.field('SE_SentimentScore', 'float')
        m.field('SE_SentimentPolarity', 'float')
        m.field('CompositeScore', 'float')
        m.field('RG_FDA_Warning', 'boolean')
        m.field('GP_SentimentScore', 'float')
        m.field('GP_SentimentPolarity', 'float')
        m.field('GP_Location', 'text')
        m.field('GP_Country', 'text')
        m.field('Article_references', 'float')
        m.field('Is_source_type_RG', 'boolean')
        m.field('Is_source_type_SE', 'boolean')
        m.field('Is_source_type_GP', 'boolean')

        # save the mapping into index 'my-index'
        try:
            m.save(self._index_name)
        except Exception as e:
            print("Could not save schema!",e)

    def create_index(self):
        """
        Creates the index if it doesn't exist
        :return:
        """
        # create the index if it doesn't exist
        if not self.index_exists():
            try:
                index.create()
                self._make_mapping()
                print("Index was created :", index.exists())
            except Exception as e:
                print("~~~Index exists error")
                print(e)
                return -1
        else:
            print("Index already exists", self._index_name)
        return 0

    def get_index_mapping(self):
        """
        Retrieves the index mapping
        :return: Index mapping JSON object if success, -1 if error
        """
        try:
            return self._es.indices.get_mapping(index=self._index_name)
        except Exception as e:
            print("~~~Get index mapping error")
            print(e)
            return -1

    def get_count(self, search_obj=None):
        return self._es.count(index=self._index_name, body=search_obj)

    def upload_dataframe(self, df):
        """
        Uploads a dataframe into the index
        :param df: Dataframe (pandas)
        :return: 0 if success, -1 if failure
        """
        def rec_to_actions(df):
            for record in df.to_dict(orient="records"):
                yield ('{ "index" : { "_index" : "%s", "_type" : "%s" }}' % (self._index_name, "_doc"))
                yield (json.dumps(record, default=int))

        if not self.index_exists():
            print("!!!INDEX DOES NOT EXIST -- RETURNING!!!")
            return -1

        try:
            # make the bulk call, and get a response
            response = self._es.bulk(rec_to_actions(df))  # return a dict
            if not response["errors"]:
                print("Records uploaded")
            else:
                print("Could not upload data ")
                print(response)
                return -1
        except Exception as e:
            print("\nERROR:", e)
            return -1

        return 0

    # Process documents returned by the current search/scroll
    def _populate_dict_of_duplicate_docs(self, hits):
        for item in hits:
            combined_key = str(item['_source'][self._hash_field])

            _id = item["_id"]
            # _Title = item["_source"]["Title"]

            hashval = hashlib.md5(combined_key.encode('utf-8')).digest()

            # If the hashval is new, then we will create a new key
            # in the dict_of_duplicate_docs, which will be
            # assigned a value of an empty array.
            # We then immediately push the _id onto the array.
            # If hashval already exists, then
            # we will just push the new _id onto the existing array
            self._dict_of_duplicate_docs.setdefault(hashval, []).append(_id)

    # Loop over all documents in the index, and populate the
    # dict_of_duplicate_docs data structure.
    def _scroll_over_all_docs(self):
        data = self._es.search(index=self._index_name, scroll='1m', body={"query": {"match_all": {}}})

        # Get the scroll ID
        sid = data['_scroll_id']
        scroll_size = len(data['hits']['hits'])

        # Before scroll, process current batch of hits
        self._populate_dict_of_duplicate_docs(data['hits']['hits'])

        while scroll_size > 0:
            data = self._es.scroll(scroll_id=sid, scroll='2m')

            # Process current batch of hits
            self._populate_dict_of_duplicate_docs(data['hits']['hits'])

            # Update the scroll ID
            sid = data['_scroll_id']

            # Get the number of results that returned in the last scroll
            scroll_size = len(data['hits']['hits'])

    def _loop_over_hashes_and_remove_duplicates(self):
        urls_to_delete = []
        ids_to_delete = []
        # Search through the hash of doc values to see if any
        # duplicate hashes have been found
        for hashval, array_of_ids in self._dict_of_duplicate_docs.items():
            if len(array_of_ids) > 1:
                # print("********** Duplicate docs hash=%s **********" % hashval)
                # Get the documents that have mapped to the current hasval
                matching_docs = self._es.mget(index=self._index_name, body={"ids": array_of_ids})
                # Check if the URLs are truly the same URLs
                dict_url_ids = {}
                for doc in matching_docs['docs']:
                    dict_url_ids.setdefault(doc["_source"].get("URL"), []).append(doc["_id"])
                # remove only the first ID from the list
                dict_url_ids = {key: value[1:] for (key, value) in dict_url_ids.items()}
                for i in list(dict_url_ids.keys()):
                    urls_to_delete.append(i)
                # Delete all the IDs now
                for i in list(dict_url_ids.values()):
                    ids_to_delete.extend(i)

        for u in urls_to_delete:
            print(u)

        for idd in ids_to_delete:
            try:
                del_return = self._es.delete(index=self._index_name, id=idd)
            except Exception as e:
                print(e)
                break

    def remove_duplicates(self):
        self._scroll_over_all_docs()
        self._loop_over_hashes_and_remove_duplicates()
