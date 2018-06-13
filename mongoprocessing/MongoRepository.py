import cPickle as pickle
import os
import time
from threading import Lock, Thread

from pymongo import MongoClient

import helper


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
        self._save_lock = Lock()
        self._last_save = time.time()
        self._save_interval = 5
        self._logger = helper.get_log()

    def get(self, key, value):
        """
        Get all documents from the collection that match document[key] == value
        :param key: The name of the key
        :param value: The value that the property must have
        :return: Instance of cursor corresponding to the query
        """
        return self._coll.find({key: value})

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

    def update_key_value(self, doc_id, key, value, *time_fields):
        """
        Updates a document and (optionally) updates timestamps in the document.
        :param doc_id: The ID of the document to update
        :param key: The key of the property to update
        :param value: The new value of the given property
        :param time_fields: All properties that should have their value set to the current time
        """

        update_dict = self._get_base_update_dict(*time_fields)
        update_dict['$set'] = {key: value}

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

    def add_to_set(self, doc_id, key, value, *time_fields):
        """
        Adds the given value to the set with the given key.
        :param doc_id: The ID of the document to update
        :param key: The key of the set
        :param value: The value to add to the set
        :param time_fields: All properties that should have their value set to the current time
        """
        update_dict = self._get_base_update_dict(*time_fields)
        update_dict['$addToSet'] = {key: value}
        self._coll.update_one({'_id': doc_id}, update_dict)

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
                try:
                    self._logger.info('Successfully loaded resume token')
                    watch = self._coll.watch([{'$match': match}], full_document='updateLookup',
                                             resume_after=resume_token)
                    self._logger.info('Successfully resumed watch')
                    return watch

                except:
                    self._logger.exception('Unable to resume, probably because the oplog is too small. Try again '
                                           'without resuming...')

                    return self.watch(match, resume=False)

        watch = self._coll.watch([{'$match': match}], full_document='updateLookup')
        self._logger.info('Successfully started watch')
        return watch

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

    def register_time_field(self, *time_fields):
        self._time_fields.extend(time_fields)

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
        if self._save_lock.acquire():
            Thread(target=self._save_resume_token, args=[doc]).start()
            self._save_lock.release()

    def _save_resume_token(self, doc):
        if time.time() - self._last_save > self._save_interval:
            resume_token = doc.get('_id')
            with open('resume_token.bin', 'wb') as token_file:
                pickle.dump(resume_token, token_file)
            self._last_save = time.time()

    def _load_resume_token(self):
        resume_token = None
        if self._save_lock.acquire():
            try:
                if os.path.exists('resume_token'):
                    with open('resume_token.bin', 'rb') as token_file:
                        resume_token = pickle.load(token_file)
            except:
                self._logger.exception('Unable to load resume token')
            self._save_lock.release()
        else:
            raise IOError('Unable to acquire lock for loading resume token!')

        return resume_token
