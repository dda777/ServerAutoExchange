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
                  f'CONFIG /M /D{self.data[1]} /NAUTO /Pautoobmen ' \
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
                db.insert_suboperation_log(item[0], hash_key, 2, 'Start_exchange', process_status)

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
            error_log_return, fails_log_return = 1, 1
            if process_status == 2:
                error_log_return, fails_log_return = check_log(process_status, hash_key, 'REK')
            else:
                for item in data:
                    error_log_return, fails_log_return = check_log(process_status, hash_key, item[0])
            try:
                if error_log_return == 0 and fails_log_return == 0:
                    for item in data:
                        db.update_suboperation_table(hash_key, 4, item[0])
                        db.insert_suboperation_log(item[0], hash_key, 4, "Конфигуратор занят", process_status)
                        db.update_operation_table(hash_key, 4)
                        data.pop(data.index(item))
                    return print('Конфиг занят')

                elif error_log_return == 1 and fails_log_return == 1:
                    for item in data:
                        db.update_suboperation_table(hash_key, 3, item[0])
                        db.insert_suboperation_log(item[0], hash_key, 3, 'Exchange_passed_success', process_status)
                        db.update_operation_table(hash_key, 3)

                elif error_log_return and fails_log_return:
                    if process_status == 2:
                        for item in data:
                            db.update_suboperation_table(hash_key, 3, item[0])
                            db.insert_suboperation_log(item[0], hash_key, 3, 'Exchange_passed_success', process_status)
                            db.update_operation_table(hash_key, 3)
                    else:
                        for error in error_log_return:
                            for item in data:
                                if item[0] in error[7]:
                                    db.update_suboperation_table(hash_key, 4, item[0])
                                    db.insert_suboperation_log(item[0], hash_key, 4, error[7], process_status)
                                    db.update_operation_table(hash_key, 4)
                                    data.pop(data.index(item))
                                    break

                elif error_log_return and fails_log_return == 1:
                    for error in error_log_return:
                        for item in data:
                            if item[0] in error[7]:
                                db.update_suboperation_table(hash_key, 4, item[0])
                                db.insert_suboperation_log(item[0], hash_key, 4, error[7], process_status)
                                db.update_operation_table(hash_key, 4)
                                data.pop(data.index(item))
                                break

                elif error_log_return == 1 and fails_log_return:
                    for error in fails_log_return:
                        for item in data:
                            if item[0] in error[7]:
                                db.update_suboperation_table(hash_key, 4, item[0])
                                db.insert_suboperation_log(item[0], hash_key, 4, error[5], process_status)
                                db.update_operation_table(hash_key, 4)
                                data.pop(data.index(item))
                                break

            except Exception as er:
                for item in data:
                    db.update_suboperation_table(hash_key, 4, item[0])
                    db.insert_suboperation_log(item[0], hash_key, 4, er, process_status)
                    db.update_operation_table(hash_key, 4)
                    data.pop(data.index(item))
                return

        db.close_connect()


def check_log(process, hash_key, code_1c):
    file_name = f'Log/{process}_{hash_key}_{code_1c}.log'
    error = ["DistBatchErr", "DistUplErr", "DistDnldErr"]
    fails = ["DistUplFail", "DistDnldFail"]
    error_log_return = []
    fails_log_return = []
    try:
        with open(file_name) as log:
            for line in log:
                for line_phrases in error:
                    if line_phrases in line:
                        error_log_return.append(line.split(';'))
                for line_phrases in fails:
                    if line_phrases in line:
                        fails_log_return.append(line.split(';'))
    except IOError:
        return 0, 0
    if not error_log_return and not fails_log_return:
        return 1, 1
    elif error_log_return and not fails_log_return:
        return error_log_return, 1
    elif not error_log_return and fails_log_return:
        return 1, fails_log_return
    else:
        return error_log_return, fails_log_return

#
# db = DataBase()
# data = [('GLB', '\\\\SRVGLOBAL\\user\\db.adm\\tr5')]
# hash_key = '8a23ad63bb0446fc9a630ab5de6912f6'
# process_status = 2
# error_log_return, fails_log_return = check_log(2, '8a23ad63bb0446fc9a630ab5de6912f6', 'REK')
# print(fails_log_return)
#
# if error_log_return == 1 and fails_log_return == 1:
#     for item in data:
#         db.update_suboperation_table(hash_key, 3, item[0])
#         db.insert_suboperation_log(item[0], hash_key, 3, 'Exchange_passed_success', process_status)
#         db.update_operation_table(hash_key, 3)
#
# elif error_log_return and fails_log_return:
#     if process_status == 2:
#         for item in data:
#             db.update_suboperation_table(hash_key, 3, item[0])
#             db.insert_suboperation_log(item[0], hash_key, 3, 'Exchange_passed_success', process_status)
#             db.update_operation_table(hash_key, 3)
#     else:
#         for error in error_log_return:
#             for item in data:
#                 if item[0] in error[7]:
#                     db.update_suboperation_table(hash_key, 4, item[0])
#                     db.insert_suboperation_log(item[0], hash_key, 4, error[7], process_status)
#                     db.update_operation_table(hash_key, 4)
#                     data.pop(data.index(item))
#                     break
#
# db.close_connect()
