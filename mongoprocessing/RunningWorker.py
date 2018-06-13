import time
from threading import Thread

import helper


class RunningWorker:
    def __init__(self, name, acknowledge_callback, process_callback, mongo_repository, match, resume=True):
        self._name = name
        self._acknowledge_callback = acknowledge_callback
        self._process_callback = process_callback
        self._mongo_repository = mongo_repository

        self._logger = helper.get_log()

        self._abort = False
        self._running = False

        self._running_tasks = []
        self._task = Thread(target=self._run_watch_thread, args=[match, resume])

    def start(self):
        self._logger.info('Starting thread for worker "{}"'.format(self._name))
        self._running = True
        self._task.start()

    def stop(self):
        self._logger.info('Stopping worker "{}"'.format(self._name))
        self._abort = True

        if len(self._running_tasks) > 0:
            self._logger.info('{} processes still running'.format(len(self._running_tasks)))

            end_time = time.time() + 5

            for thread in self._running_tasks:
                wait_for = max(1.0, round(end_time - time.time()))
                thread.join(wait_for)

            if len(self._running_tasks) > 0:
                self._logger.info('Terminating {} processes'.format(len(self._running_tasks)))

            for thread in self._running_tasks:
                thread.terminate()

            self._logger.info('Successfully stopped worker "{}"'.format(self._name))

    def _run_watch_thread(self, match, resume):
        with self._mongo_repository.watch(match, resume=resume) as stream:
            self._logger.info('Worker thread "{}" started successfully\n'.format(self._name))
            for doc in stream:
                if self._abort:
                    return
                if not self._running:
                    continue

                if doc is not None:
                    thread = Thread(target=self._run_process_thread, args=[doc])

                    self._running_tasks.append(thread)
                    thread.start()
                    thread.join()
                    self._running_tasks.remove(thread)

        self._logger.info('Worker thread "{}" stopped successfully'.format(self._name))

    def _run_process_thread(self, doc):
        document = doc['fullDocument']
        is_running = False

        if self._name in document:
            is_running = document[self._name]['isRunning']

        if not is_running:
            self._execute_process(document)
        else:
            self._logger.error('Process "{}" is already running'.format(self._name))

        self._mongo_repository.save_resume_token(doc)

    def _execute_process(self, document):
        required = False

        try:
            required = self._acknowledge_callback(document)
        except:
            self._logger.exception('An error occurred while trying to process data')

        if required:
            self._mongo_repository.start_process(document["_id"], self._name)

            success = False
            results = {}

            try:
                success, results = self._process_callback(document)
            except:
                self._logger.exception('An error occurred while trying to process data')

            self._mongo_repository.end_process(document['_id'], self._name, success, results)
