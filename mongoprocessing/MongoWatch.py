import sys
import threading


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


class MongoWatch:
    def __init__(self, mongo_repository, operation_type, *process_dependencies):
        """
        Create a watch builder. Supplies various methods to add conditions for when the watch should trigger.
        :param mongo_repository: A MongoRepository connected to an existing collection
        :param operation_type: Any of the following: 'insert', 'replace' or 'update'
        :param process_dependencies: One or multiple ProcessRequirement objects
        """
        self._mongo_repository = mongo_repository
        self._operation_type = operation_type
        self._process_requirements = process_dependencies

    def start_worker(self, name, acknowledge_callback, process_callback, resume=True):
        """
        Start a new worker.
        :param name: Name of the worker
        :param acknowledge_callback: A function that takes a document as a parameter and returns True if the process
        should run on the document, False otherwise
        :param process_callback: A function that takes a document as a parameter and returns two values: A boolean
        indicating whether the process executed successfully and a dictionary containing all results
        :param resume: Whether to resume the stream from where it stopped last time
        :return: The WatchBuilder itself to allow registering multiple (parallel) workers
        """

        threading.Thread(target=self._run_watch_thread, args=[name, acknowledge_callback, process_callback]).start()

        return self

    def _get_filter(self, name):
        match = {'operationType': self._operation_type}

        or_filters = [{'fullDocument.{}'.format(name): {'$exists': False}}]

        outer_and_list = []

        if self._process_requirements is not None and len(self._process_requirements) > 0:
            for process in self._process_requirements:
                match['fullDocument.{}.success'.format(process.process_name)] = True

                if self._operation_type == 'update':
                    success_true = self._equals_dot_workaround('{}.success'.format(process.process_name), True)
                    outer_and_list.append(success_true)

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

        or_dict = {'$or': or_filters}
        outer_and_list.append(or_dict)

        match['$and'] = outer_and_list

        return match

    def _equals_dot_workaround(self, field, value):
        or_dict = {
            "$or": [
                {
                    '$expr': {
                        '$eq': [
                            {
                                '$let': {
                                    'vars': {
                                        'foo': {
                                            '$arrayElemAt': [
                                                {
                                                    '$filter': {
                                                        'input': {
                                                            '$objectToArray': '$updateDescription.updatedFields'
                                                        },
                                                        'cond': {'$eq': [field, '$$this.k']}
                                                    }
                                                },
                                                0
                                            ]
                                        }
                                    },
                                    'in': '$$foo.v'
                                }
                            },
                            value
                        ]
                    }
                },
                {'updateDescription.updatedFields.{}'.format(field): value}
            ]
        }

        return or_dict

    def _run_watch_thread(self, name, acknowledge_callback, process_callback):
        match = self._get_filter(name)
        with self._mongo_repository.watch(match) as stream:
            sys.stdout.write('Worker "{}" started successfully\n'.format(name))
            for doc in stream:
                if doc is not None:
                    threading.Thread(target=self._run_process_thread,
                                     args=[doc, name, acknowledge_callback, process_callback]).start()

    def _run_process_thread(self, doc, name, acknowledge_callback, process_callback):
        document = doc['fullDocument']
        is_running = False

        if name in document:
            is_running = document[name]["isRunning"]

        if not is_running:
            if acknowledge_callback(document):
                self._mongo_repository.start_process(document["_id"], name)

                success = False
                results = {}

                try:
                    success, results = process_callback(document)
                except:
                    sys.stdout.write('An error occured while executing process {}:\n'.format(name))
                    sys.stdout.write('{}\n'.format(sys.exc_info()[0]))

                self._mongo_repository.end_process(document['_id'], name, success, results)
                self._mongo_repository.save_resume_token(doc)
