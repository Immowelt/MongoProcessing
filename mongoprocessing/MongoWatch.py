from threading import Thread

from RunningWorker import RunningWorker
from dependencies import MultipleDependency

class MongoWatch(object):
    def __init__(self, mongo_repository):
        """
        Create a watch builder. Supplies various methods to add conditions for when the watch should trigger.
        :param mongo_repository: A MongoRepository connected to an existing collection
        :param operation_type: Any of the following: 'insert', 'replace' or 'update'
        """
        self.mongo_repository = mongo_repository

        self.dependency = MultipleDependency([])
        self.running_workers = {}

        self.logger = mongo_repository.logger

    def add_dependency(self, dependency):
        self.dependency.add_dependency(dependency)

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

        for op_type in set(self.dependency.operation_types):
            key = name + '_' + op_type
            if key in self.running_workers:
                raise Exception('Worker {} is already running!'.format(key))

            match = self._get_filter(name, op_type)
            running_worker = RunningWorker(name, acknowledge_callback, process_callback, self.mongo_repository, match,
                                           resume)
            self.running_workers[key] = running_worker

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

    def _get_filter(self, name, op_type):
        if len(self.dependency) == 0:
            raise Exception('Since v. 0.5.0 you have to add at least one dependency to mongowatch. Consider using OperationTypeDependency')

        match = {'operationType': op_type}

        or_filters = [{'fullDocument.{}'.format(name): {'$exists': False}}]

        outer_and_list = []

        self.dependency._add_match_condition(name, match, outer_and_list, or_filters, op_type)

        or_dict = {'$or': or_filters}
        outer_and_list.append(or_dict)

        match['$and'] = outer_and_list

        return match

