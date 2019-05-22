import time
from threading import Thread
from multiprocessing.pool import ThreadPool

class RunningWorker(object):
    def __init__(self, name, acknowledge_callback, process_callback, mongo_repository, match, resume=True, num_threads=5):
        self.name = name
        self.acknowledge_callback = acknowledge_callback
        self.process_callback = process_callback
        self.mongo_repository = mongo_repository

        self.logger = mongo_repository.logger

        self.abort = False
        self.running = False

        self.running_tasks = ThreadPool(num_threads)

        self.task = Thread(target=self._run_watch_thread, args=[match, resume])

    def start(self):
        self.logger.info('Starting thread for worker "{}"'.format(self.name))
        self.running = True
        self.task.start()

    def stop(self):
        self.logger.info('Stopping worker "{}"'.format(self.name))
        self.abort = True

        self.running_tasks.terminate()
        self.running_tasks.join()
        self.logger.info('Successfully stopped worker "{}"'.format(self.name))

    def _run_watch_thread(self, match, resume):
        with self.mongo_repository.watch(match, resume=resume) as stream:
            self.logger.info('Worker thread "{}" started successfully\n'.format(self.name))
            for doc in stream:
                if self.abort:
                    return
                if not self.running:
                    continue

                if doc is not None:
                    self.running_tasks.apply_async(func=self._run_process_thread, args=[doc])

        self.logger.info('Worker thread "{}" stopped successfully'.format(self.name))

    def _run_process_thread(self, doc):
        document = doc['fullDocument']
        is_running = False

        if self.name in document:
            is_running = document[self.name]['isRunning']

        if not is_running:
            self._execute_process(document)
        else:
            self.logger.error('Process "{}" is already running'.format(self.name))

        self.mongo_repository.save_resume_token(doc)

    def _execute_process(self, document):
        required = False

        try:
            required = self.acknowledge_callback(document)
        except:
            self.logger.exception('An error occurred while trying to process data')

        if required:
            self.mongo_repository.start_process(document["_id"], self.name)

            success = False
            results = {}

            try:
                success, results = self.process_callback(document)
            except:
                self.logger.exception('An error occurred while trying to process data')

            self.mongo_repository.end_process(document['_id'], self.name, success, results)
