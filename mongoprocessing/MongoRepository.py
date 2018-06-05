import os
import pickle
import threading
import time

from pymongo import MongoClient


class MongoRepository:
    def __init__(self, connection_string, database, collection, *time_fields):
        """
        MongoCollection wrapper class to allow easy specialized updates.
        :param connection_string: MongoDB connection string
        :param database: Database to use
        :param collection: Collection to use
        :param time_fields: Any timeFields that should be set to the current time whenever the collection is updated
        """
        client = MongoClient(connection_string, document_class=dict)
        db = client[database]
        self._coll = db[collection]
        self._time_fields = list(time_fields)
        self._save_lock = threading.Lock()
        self._last_save = time.time()
        self._save_interval = 5

    def get_by_id(self, doc_id):
        """
        Gets a document from the collection by ID.
        :param doc_id: The document ID
        :return: The document (if it exists)
        """
        return self.get('_id', doc_id)[0]

    def get_multiple_by_ids(self, ids):
        """
        Get multiple documents by their IDs.
        :param ids: All IDs that should be found
        :return: Instance of cursor corresponding to the query
        """
        return self._coll.find({'_id': {'$in': ids}})

    def get(self, key, value):
        """
        Get all documents from the collection that match document[key] == value
        :param key: The name of the key
        :param value: The value that the property must have
        :return: Instance of cursor corresponding to the query
        """
        return self._coll.find({key: value})

    def insert(self, doc_id, doc):
        """
        Insert a document into the collection.
        :param doc_id: ID of the document
        :param doc: A dictionary representing a document
        """
        if '_id' not in doc:
            doc['_id'] = doc_id

        self._coll.insert_one(doc)

    def update(self, doc_id, data, *time_fields):
        """
        Updates a document and (optionally) updates timestamps in the document.
        :param doc_id: The ID of the document to update
        :param data: A dictionary with all updates to make
        :param time_fields: All properties that should have their value set to the current time
        """
        update_dict = self._get_base_update_dict(*time_fields)
        update_dict['$set'] = data

        self._coll.update_one({'_id': doc_id}, update_dict, upsert=True)

    def increment(self, doc_id, key, value, *time_fields):
        """
        Increment the value of a property in a document.
        :param doc_id: The ID of the document to update
        :param key: The key of the property to increment
        :param value: The increment value
        :param time_fields: All properties that should have their value set to the current time
        """
        update_dict = self._get_base_update_dict(*time_fields)
        update_dict['$inc'] = {key: value}

        self._coll.update_one({'_id': doc_id}, update_dict, upsert=True)

    def watch(self, match, resume=True):
        """
        Watch the collection using a filter.
        :param match: BSON document specifying the filter criteria
        :param resume: Whether to resume the stream from where it stopped last time
        :return: A stream of documents as they get inserted/replaced/updated
        """
        if resume:
            resume_token = self._load_resume_token()
            if resume_token is not None:
                return self._coll.watch([{'$match': match}], full_document='updateLookup', resume_after=resume_token)

        return self._coll.watch([{'$match': match}], full_document='updateLookup')

    def start_process(self, doc_id, process_name, *time_fields):
        """
        Manually start a process
        :param doc_id: The ID of the affected document
        :param process_name: The name of the process to be started
        :param time_fields: All properties that should have their value set to the current time
        """
        updates = {'{}.success'.format(process_name): False, '{}.isRunning'.format(process_name): True}
        all_time_fields = list(time_fields)
        all_time_fields.append('{}.startTime'.format(process_name))

        self.update(doc_id, updates, *all_time_fields)

    def end_process(self, doc_id, process_name, success, results, *time_fields):
        """
        Manually end a process
        :param doc_id: The ID of the affected document
        :param process_name: The name of the process to be ended
        :param success: Whether the process executed successfully
        :param results: Any results to save with the process
        :param time_fields: All properties that should have their value set to the current time
        """
        updates = {'{}.success'.format(process_name): success, '{}.isRunning'.format(process_name): False}

        for key in results:
            updates['{}.{}'.format(process_name, key)] = results[key]

        all_time_fields = list(time_fields)
        all_time_fields.append('{}.endTime'.format(process_name))

        self.update(doc_id, updates, *all_time_fields)

    def _get_base_update_dict(self, *time_fields):
        update_dict = dict()

        if time_fields is not None and len(time_fields) > 0:
            if '$currentDate' not in update_dict:
                update_dict['$currentDate'] = dict()

            for time_field in time_fields:
                update_dict['$currentDate'][time_field] = True

        if self._time_fields is not None and len(self._time_fields) > 0:
            if '$currentDate' not in update_dict:
                update_dict['$currentDate'] = dict()

            for time_field in self._time_fields:
                update_dict['$currentDate'][time_field] = True

        return update_dict

    def save_resume_token(self, doc):
        threading.Thread(target=self._save_resume_token, args=[doc.get('_id')]).start()

    def _save_resume_token(self, resume_token):
        if self._save_lock.acquire():
            if time.time() - self._last_save > self._save_interval:
                with open('resume_token', 'wb') as token_file:
                    pickle.dump(resume_token, token_file)
                self._last_save = time.time()
            self._save_lock.release()

    def _load_resume_token(self):
        resume_token = None
        if self._save_lock.acquire():
            if os.path.exists('resume_token'):
                with open('resume_token', 'r') as token_file:
                    resume_token = pickle.load(token_file)
            self._save_lock.release()
        else:
            raise IOError('Unable to acquire lock for loading resume token!')

        return resume_token
