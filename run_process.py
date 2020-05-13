# -*- coding: utf-8 -*-

import multiprocessing
from subprocess import Popen
import os
from database import DataBase
from pyparsing import delimitedList, Word, alphanums


class ExchangeQueue(multiprocessing.Process):

    def __init__(self, task_queue, result_queue):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue

    def run(self):
        while True:
            temp_task = self.task_queue.get()

            if temp_task is None:
                self.task_queue.task_done()
                break

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

        program = f'C:\\User\\db.adm\\1cv77.adm\\BIN\\1cv7s.exe ' \
                  f'CONFIG /M /D{self.data[1]} /NЮляП /Pцунами ' \
                  f'/@{abspath}\\Conf\\{self.process_status}_{self.hash_key}_{self.data[0]}.prm'

        with Popen(program) as proc:
            proc.wait()
        return self.data


class StartProcess:
    @staticmethod
    def start(data, hash_key):
        db = DataBase()
        tasks = multiprocessing.JoinableQueue()
        results = multiprocessing.Queue()
        n_consumers = multiprocessing.cpu_count()

        print('Spawning %i consumers...' % n_consumers)

        for process_status in range(1, 4):

            for item in data:
                db.update_suboperation_table(hash_key, 2, item[0])
                db.insert_suboperation_log(item[0], hash_key, 2, 'Start exchange', process_status)

            consumers = [ExchangeQueue(tasks, results) for i in range(n_consumers)]
            for consumer in consumers:
                consumer.start()

            if process_status == 1:
                for item in data:
                    tasks.put(ExchangeProcess(item, hash_key, process_status))
            elif process_status == 2:
                item = ('REK', r'\\srvfiler\user\db.adm\tr5')
                tasks.put(ExchangeProcess(item, hash_key, process_status))
            elif process_status == 3:
                for item in data:
                    tasks.put(ExchangeProcess(item, hash_key, process_status))

            for i in range(n_consumers):
                tasks.put(None)

            tasks.join()

            for i in range(len(data)):
                results.get()

            print('Done ', process_status)

            for item in data:
                log_info = check_log(process_status, hash_key, item[0])
                if process_status == 2:
                    break
                else:
                    if log_info != 1:
                        db.update_suboperation_table(hash_key, 4, item[0])
                        db.insert_suboperation_log(item[0], hash_key, 4, log_info[5], process_status)
                        db.update_operation_table(hash_key, 4)
                    elif log_info == 0:
                        db.update_suboperation_table(hash_key, 4, item[0])
                        db.insert_suboperation_log(item[0], hash_key, 4, 'Config not exists', process_status)
                        db.update_operation_table(hash_key, 4)
                    else:
                        db.update_suboperation_table(hash_key, 3, item[0])
                        db.insert_suboperation_log(item[0], hash_key, 3, 'Exchange passed success', process_status)
                        db.update_operation_table(hash_key, 3)

        db.close_connect()


def check_log(process, hash_key, code_1c):
    file_name = f'Log/{process}_{hash_key}_{code_1c}.log'

    try:
        with open(file_name) as file_handler:
            for line in file_handler:
                if 'DistUplFail' in line:
                    parser = Word(alphanums + r':\+-./')
                    grammar = delimitedList(parser, delim=';')
                    return grammar.parseString(line)
            return 1
    except IOError:
        return 0
