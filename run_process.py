from multiprocessing.managers import BaseManager
from multiprocessing import Queue
import multiprocessing
from subprocess import Popen
import getpass
import os


class ExchangeQueue(multiprocessing.Process):

    def __init__(self, task_queue, result_queue, process_status):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.process_status = process_status

    def run(self):
        pname = self.name

        while True:
            temp_task = self.task_queue.get()

            if temp_task is None:
                print('Exiting %s...' % pname)
                self.task_queue.task_done()
                break

            print('%s processing task: %s' % (pname, temp_task))

            answer = temp_task.auto_exchange(self.process_status)
            self.task_queue.task_done()
            self.result_queue.put(answer)


class ExchangeProcess:
    def __init__(self, data):
        self.data = data

    def auto_exchange(self, process_status):

        if process_status == 0:
            writeto = self.data[1]
            readfrom = ''
        elif process_status == 1:
            writeto = self.data[1]
            readfrom = ''
        elif process_status == 2:
            writeto = ''
            readfrom = self.data[1]
        else:
            return
        abspath = os.path.abspath(os.curdir)
        main_server = 'srvglobal'
        shared_mode = '1'
        text = f'''
        [General]\n
        Output={os.path.abspath(os.curdir)}\\Log\\{process_status}_{self.data[1]}.log\n
        Quit=1\n
        AutoExchange=1\n
        [AutoExchange]\n
        SharedMode={shared_mode}\n
        WriteTo={writeto}
        ReadFrom={readfrom}
        '''
        param = f'{abspath}\Conf\{process_status}_{self.data[1]}.prm'
        with open(param, mode='w', encoding='utf-8') as file:
            file.write(text)

        program = f'\\\{main_server}\\User\db.adm\\1cv77.adm\\BIN\\1cv7s.exe ' \
                  f'CONFIG /M /D\\\{self.data[0]}\\user\\db.adm\\tr5 /Nadm /Padm ' \
                  f'/@{abspath}\\Conf\\{process_status}_{self.data[1]}.prm'
        with Popen(program) as proc:
            proc.wait()
        return self.data


class StartProcess:
    @staticmethod
    def start(data):
        tasks = multiprocessing.JoinableQueue()
        results = multiprocessing.Queue()

        # spawning consumers with respect to the
        # number cores available in the system
        n_consumers = multiprocessing.cpu_count()
        print('Spawning %i consumers...' % n_consumers)
        for process_status in range(3):
            consumers = [ExchangeQueue(tasks, results, process_status) for i in range(n_consumers)]
            for consumer in consumers:
                consumer.start()
            # enqueueing jobs
            data = [['srvglobal', 'REK'], ['srvfiler', 'GLB']]
            for item in data:
                tasks.put(ExchangeProcess(item))

            for i in range(n_consumers):
                tasks.put(None)

            tasks.join()

            for i in range(len(data)):
                temp_result = results.get()
                print('Result:', temp_result)

            print('Done.')

    def insert_data_to_database(self):
        shared_mode = 1
        username = getpass.getuser()
        ip = '127.0.0.1'
        status = 0
