import time
from threading import Thread


class RunningWorker:
    def __init__(self, name, acknowledge_callback, process_callback, mongo_repository, match, resume=True):
        self.name = name
        self.acknowledge_callback = acknowledge_callback
        self.process_callback = process_callback
        self.mongo_repository = mongo_repository

        self.logger = mongo_repository.logger

        self.abort = False
        self.running = False

        self.running_tasks = []
        self.task = Thread(target=self._run_watch_thread, args=[match, resume])

    def start(self):
        self.logger.info('Starting thread for worker "{}"'.format(self.name))
        self.running = True
        self.task.start()

    def stop(self):
        self.logger.info('Stopping worker "{}"'.format(self.name))
        self.abort = True

        if len(self.running_tasks) > 0:
            self.logger.info('{} processes still running'.format(len(self.running_tasks)))

            end_time = time.time() + 5

            for thread in self.running_tasks:
                wait_for = max(1.0, round(end_time - time.time()))
                thread.join(wait_for)

            if len(self.running_tasks) > 0:
                self.logger.info('Terminating {} processes'.format(len(self.running_tasks)))

            for thread in self.running_tasks:
                thread.terminate()

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
                    thread = Thread(target=self._run_process_thread, args=[doc])

                    self.running_tasks.append(thread)
                    thread.start()
                    thread.join()
                    self.running_tasks.remove(thread)

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
