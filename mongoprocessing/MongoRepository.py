from pymongo import MongoClient


class MongoRepository:
    def __init__(self, uri, database, collection):
        client = MongoClient(uri, document_class=dict)
        db = client[database]
        self._coll = db[collection]

    def update(self, doc, data, *time_fields):
        """
        Updates a document and (optionally) updates timestamps in the document.
        :param doc: The document to update
        :param data: A dictionary with all updates to make
        :param time_fields: All properties that should have their value set to the current time
        """

        update_dict = {'$set': data}
        if time_fields is not None and len(time_fields) > 0:
            update_dict['$currentDate'] = dict()
            for time_field in time_fields:
                update_dict['$currentDate'][time_field] = True

        self._coll.update_one({'_id': doc['fullDocument']['_id']}, update_dict, upsert=True)

    def watch(self, match):
        """
        Watch the collection using a filter.
        :param match: BSON document specifying the filter criteria
        :return: A stream of documents as they get inserted/replaced/updated
        """
        return self._coll.watch([{'$match': match}], full_document='updateLookup')
