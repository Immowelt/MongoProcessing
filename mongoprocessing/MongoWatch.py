from threading import Thread

from RunningWorker import RunningWorker

class ProcessDependency:
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
    def __init__(self, mongo_repository, operation_type):
        """
        Create a watch builder. Supplies various methods to add conditions for when the watch should trigger.
        :param mongo_repository: A MongoRepository connected to an existing collection
        :param operation_type: Any of the following: 'insert', 'replace' or 'update'
        """
        self.mongo_repository = mongo_repository
        self.operation_type = operation_type

        self.required_keys = []
        self.required_values = {}
        self.process_dependencies = []
        self.running_workers = {}

        self.logger = mongo_repository.logger

    def add_process_dependencies(self, *process_dependencies):
        """
        Add ProcessDependencies to the watch.
        :param process_dependencies: One or multiple ProcessDependency objects
        :return: The MongoWatch object itself to allow method chaining
        """
        self.process_dependencies.extend(process_dependencies)
        return self

    def add_required_keys(self, *keys):
        """
        To trigger the watch, the given key must exist.
        :param keys: One or multiple keys that must exist
        :return: The MongoWatch object itself to allow method chaining
        """
        self.required_keys.extend(keys)
        return self

    def add_required_value(self, key, value):
        """
        To trigger the watch, the given key must exist and have the given value.
        :param key: The key of the property
        :param value: The value that the property must have
        :return: The MongoWatch object itself to allow method chaining
        """
        self.required_values[key] = value
        return self

    def start_worker(self, name, acknowledge_callback, process_callback, resume=True):
        """
        Start a new worker.
        :param name: Name of the worker
        :param acknowledge_callback: A function that takes a document as a parameter and returns True if the process
        should run on the document, False otherwise
        :param process_callback: A function that takes a document as a parameter and returns two values: A boolean
        indicating whether the process executed successfully and a dictionary containing all results
        :param resume: Whether to resume the stream from where it stopped last time
        """

        if name in self.running_workers:
            self.logger.error('Worker {} is already running!'.format(name))
            raise Exception('Worker {} is already running!'.format(name))

        match = self._get_filter(name)
        running_worker = RunningWorker(name, acknowledge_callback, process_callback, self.mongo_repository, match,
                                       resume)
        self.running_workers[name] = running_worker

        running_worker.start()

    def stop_all(self):
        self.logger.info('Stopping all workers')

        stop_tasks = []

        for worker in self.running_workers:
            task = Thread(target=self._stop_task, args=[worker])
            stop_tasks.append(task)
            task.start()

        for task in stop_tasks:
            task.join()

        self.running_workers = {}

        self.logger.info('Successfully stopped all workers')

    def _stop_task(self, worker):
        self.running_workers[worker].stop()
        del self.running_workers[worker]

    def _get_filter(self, name):
        match = {'operationType': self.operation_type}

        or_filters = [{'fullDocument.{}'.format(name): {'$exists': False}}]

        outer_and_list = []

        for required_key in self.required_keys:
            match['fullDocument.{}'.format(required_key)] = {'$exists': True}

        for key in self.required_values:
            match['fullDocument.{}'.format(key)] = self.required_values[key]

        if self.process_dependencies is not None and len(self.process_dependencies) > 0:
            for process in self.process_dependencies:
                match['fullDocument.{}.success'.format(process.process_name)] = True

                if self.operation_type == 'update':
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
