import multiprocessing
from subprocess import Popen
import os
from database import DataBase


class ExchangeQueue(multiprocessing.Process):

    def __init__(self, task_queue, result_queue):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue

    def run(self):
        pname = self.name

        while True:
            temp_task = self.task_queue.get()

            if temp_task is None:
                print('Exiting %s...' % pname)
                self.task_queue.task_done()
                break

            print('%s processing task: %s' % (pname, temp_task))

            answer = temp_task.auto_exchange()
            self.task_queue.task_done()
            self.result_queue.put(answer)


class ExchangeProcess:
    def __init__(self, data, hash_key, process_status):
        self.data = data
        self.hash_key = hash_key
        self.process_status = process_status

    def auto_exchange(self):
        abspath = os.path.abspath(os.curdir)
        main_server = 'srvglobal'

        program = f'\\\{main_server}\\User\db.adm\\1cv77.adm\\BIN\\1cv7s.exe ' \
                  f'CONFIG /M /D{self.data[1]} /Nadm /Padm ' \
                  f'/@{abspath}\\Conf\\{self.process_status}_{self.hash_key}_{self.data[0]}.prm'
        with Popen(program) as proc:
            proc.wait()
        return self.data


class StartProcess:
    @staticmethod
    def start(data, hash_key):
        tasks = multiprocessing.JoinableQueue()
        results = multiprocessing.Queue()
        n_consumers = multiprocessing.cpu_count()

        print('Spawning %i consumers...' % n_consumers)

        for process_status in range(1, 4):
            consumers = [ExchangeQueue(tasks, results) for i in range(n_consumers)]
            for consumer in consumers:
                consumer.start()

            if process_status == (1 and 3):
                for item in data:
                    tasks.put(ExchangeProcess(item, hash_key, process_status))
            elif process_status == 2:
                data = ['REK', '\\\\srvfiler\\user\\db.adm\\tr5']
                tasks.put(ExchangeProcess(data, hash_key, process_status))

            for i in range(n_consumers):
                tasks.put(None)

            tasks.join()

            for i in range(len(data)):
                temp_result = results.get()
                print('Result:', temp_result)

            print('Done.')
        db = DataBase()
        db.update_status_operation(hash_key, 3)
        db.close_connect()
