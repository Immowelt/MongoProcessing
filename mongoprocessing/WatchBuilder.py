import threading
import time
import sys


class ProcessRequirement:
    def __init__(self, process_name, trigger_if_rerun=True, *required_results):
        """
        Create a new process requirement. Use WatchBuilder.add_process_requirement() to add it to the watch.
        :param process_name: The process that must have finished successfully
        :param trigger_if_rerun: If true, re-run the new process if this old process has been run again
        :param required_results: The property names of all results that the process must have written
        """
        self.process_name = process_name
        self.trigger_if_rerun = trigger_if_rerun
        self.required_results = required_results


class WatchBuilder:
    def __init__(self, mongo_repository):
        """
        Create a watch builder. Supplies various methods to add conditions for when the watch should trigger.
        :param mongo_repository: A MongoRepository connected to an existing collection
        """
        self._mongo_repository = mongo_repository
        self._operation_types = []
        self._process_requirements = []

    def listen_to(self, operation_type):
        """
        Specify which operation types should trigger the watch.
        :param operation_type: Any of the following: 'insert', 'replace' or 'update'
        :return: The WatchBuilder itself to allow method chaining
        """
        if operation_type not in self._operation_types:
            self._operation_types.append(operation_type)
        return self

    def listen_to_all(self):
        """
        Listen to all possible operation types ('insert', 'replace' and 'update').
        :return: The WatchBuilder itself to allow method chaining
        """
        self.listen_to('insert')
        self.listen_to('replace')
        self.listen_to('update')
        return self

    def add_process_requirement(self, process_requirement):
        """
        Add a ProcessRequirement to the watch.
        :param process_requirement: A ProcessRequirement object
        :return: The WatchBuilder itself to allow method chaining
        """
        self._process_requirements.append(process_requirement)
        return self

    def start_worker(self, name, acknowledge_callback, process_callback):
        """
        Start a new worker.
        :param name: Name of the worker
        :param acknowledge_callback: A function that takes a document as a parameter and returns True if the process
        should run on the document, False otherwise
        :param process_callback: A function that takes a document as a parameter and returns two values: A boolean
        indicating whether the process executed successfully and a dictionary containing all results
        :return: The WatchBuilder itself to allow registering multiple (parallel) workers
        """

        threading.Thread(target=self._run_watch_thread, args=[name, acknowledge_callback, process_callback]).start()
        return self

    def _get_filter(self, name):
        match = {'operationType': {'$in': self._operation_types}}

        or_filters = [{'fullDocument.{}'.format(name): {'$exists': False}}]

        for process in self._process_requirements:
            match['fullDocument.{}.success'.format(process.process_name)] = True

            for result in process.required_results:
                match['fullDocument.{}.{}'.format(process.process_name, result)] = {'$exists': True}

            if process.trigger_if_rerun:
                or_filters.append({
                    '$expr': {
                        '$and': [{
                            '$gt': [
                                '$fullDocument.{}.endTime'.format(process.process_name),
                                '$fullDocument.{}.endTime'.format(name)
                            ]}, {
                            '$gt': [
                                '$fullDocument.{}.startTime'.format(process.process_name),
                                '$fullDocument.{}.startTime'.format(name)
                            ]},
                        ]}
                })

        match['$or'] = or_filters
        return match

    def _run_watch_thread(self, name, acknowledge_callback, process_callback):
        match = self._get_filter(name)
        with self._mongo_repository.watch(match) as stream:
            sys.stdout.write('Worker "{}" started successfully\n'.format(name))
            for doc in stream:
                threading.Thread(target=self._run_process_thread,
                                 args=[doc, name, acknowledge_callback, process_callback]).start()

    def _run_process_thread(self, doc, name, acknowledge_callback, process_callback):
        if acknowledge_callback(doc['fullDocument']):
            self._mongo_repository.update(doc, {'{}.success'.format(name): False}, '{}.startTime'.format(name))
            time.sleep(1)
            success, results = process_callback(doc['fullDocument'])

            update_dict = {'{}.success'.format(name): success}
            for key in results:
                update_dict['{}.{}'.format(name, key)] = results[key]

            self._mongo_repository.update(doc, update_dict, '{}.endTime'.format(name))
