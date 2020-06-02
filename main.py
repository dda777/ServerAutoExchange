from server import Server
from multiprocessing import Process, JoinableQueue
from subprocess import Popen
import os
from database import DataBase
from Config.config import PARAMS_1CV7 as _PRM


class SubProcess1c7(Process):

    def __init__(self, task_queue):
        super().__init__()
        self.task_queue = task_queue

    def run(self):
        while True:
            pr = self.task_queue.get()

            if pr is None:
                self.task_queue.task_done()
                break

            pr.launch_pr()
            self.task_queue.task_done()


class LaunchProcess(DataBase):
    def __init__(self, shared_mode, unique_key, mag_info, pr_stage, all_cod1c=None):
        super().__init__()
        self.shared_mode = shared_mode
        self.mag_info = mag_info
        self.all_cod1c = all_cod1c
        self.unique_key = unique_key
        self.pr_stage = pr_stage
        self._conf_path = os.path.abspath('./Conf')
        self._log_path = os.path.abspath('./Log')
        self.log = self.gen_path_to_file()
        self.conf = self.gen_path_to_file(file_type='prm')

    def launch_pr(self):
        self.generate_config_file()
        _start = _PRM['path_to_1c7'] + _PRM['mode'] + self.mag_info['path_to_tr5'] + _PRM['login'] + _PRM[
            'password'] + '/@' + self.conf
        print(_start)
        with Popen(_start) as pr:
            pr.wait()

    def check_log(self):
        beg = ["DistUplBeg", "DistDnldBeg"]
        index_list_beg = []
        index_list_end = []
        try:
            with open(self.log) as l:
                exchange_log = l.readlines()
        except IOError:
            self.insert_suboperations_log('', self.unique_key, pr_type=self.pr_stage, status=4,
                                         infotext='Конфигуратор занят')
        for item in exchange_log:
            ind = exchange_log.index(item)
            for b in beg:
                if b in item:
                    index_list_beg.append(ind)
                    if ind == 0:
                        continue
                    else:
                        index_list_end.append(ind - 1)
        index_list_end.append(len(exchange_log) - 1)
        for i in range(len(index_list_end)):
            start = exchange_log[index_list_beg[i]]
            end = exchange_log[index_list_end[i]]
            if "DistUplBeg" in start:
                if 'DistUplFail' in end:
                    self.insert_suboperation_log(start[74:77], self.unique_key, pr_type=self.pr_stage, status=4,
                                                 infotext=exchange_log[index_list_end[i] - 1])
                else:
                    self.insert_suboperation_log(start[74:77], self.unique_key, pr_type=self.pr_stage, status=3,
                                                 infotext=exchange_log[index_list_end[i] - 1])
            elif 'DistDnldBeg' in start:
                if 'DistDnldFail' in end:
                    self.insert_suboperation_log(start[74:77], self.unique_key, pr_type=self.pr_stage, status=4,
                                                 infotext=exchange_log[index_list_end[i] - 1])
                else:
                    self.insert_suboperation_log(start[74:77], self.unique_key, pr_type=self.pr_stage, status=3,
                                                 infotext=exchange_log[index_list_end[i] - 1])

    def generate_config_file(self):
        if self.pr_stage == 'LoadToMain':
            write_to = 'REK'
            read_from = ''
        elif self.pr_stage == 'LoadFromMain':
            write_to = ''
            read_from = 'REK'
        else:
            write_to = ','.join(self.all_cod1c)
            read_from = ','.join(self.all_cod1c)
        text = \
            f'''
                [General]
                Output={self.log}
                Quit=1
                AutoExchange=1
                [AutoExchange]
                SharedMode={self.shared_mode}
                WriteTo={write_to}
                ReadFrom={read_from}
            '''
        with open(self.conf, mode='w', encoding='utf-8') as file:
            file.write(text)

    def gen_path_to_file(self, file_type='log'):
        if file_type == 'log':
            return os.path.join(self._log_path,
                                self.pr_stage + self.unique_key + self.mag_info['cod1c'] + '.' + file_type)
        else:
            return os.path.join(self._conf_path,
                                self.pr_stage + self.unique_key + self.mag_info['cod1c'] + '.' + file_type)


class AutoExchange(DataBase):
    def __init__(self, unique_key):
        super().__init__()
        self.unique_key = unique_key
        self.mags_info = self.get_enterprise_data(self.unique_key)

        self.shared_mode = self.select_shared_mode(self.unique_key)[0][0]
        self.prepare_pr()

    def prepare_pr(self):
        cods = []
        for cod in self.mags_info:
            cods.append(str(cod['cod1c']))
        qt_consumers = len(self.mags_info)
        stages = {
            'LoadToMain': 1,
            'MainExchange': 2,
            'LoadFromMain': 3,
        }
        for stage in stages:
            print(stage, 'Consumers :', qt_consumers)
            self.insert_suboperations_log(self.mags_info, self.unique_key, status=2)
            tasks = JoinableQueue()
            consumers = [SubProcess1c7(tasks) for i in range(qt_consumers)]
            for consumer in consumers:
                consumer.start()
            if stage == 'MainExchange':
                mag_info = {
                    'cod1c': 'REK',
                    'path_to_tr5': r'\\srvfiler\user\db.adm\tr5'
                }
                tasks.put(LaunchProcess(self.shared_mode, self.unique_key, mag_info, stage, cods))
            else:
                for mag_info in self.mags_info:
                    tasks.put(LaunchProcess(self.shared_mode, self.unique_key, mag_info, stage))
            tasks.join()
            print(stage, 'Done')


class ServerAutoExchange(Server):
    def handle(self, message):
        try:
            AutoExchange(message.decode('utf-8'))
        except Exception as e:
            print(f"Error: {e}, {__class__}")


if __name__ == "__main__":
    print("ServerAutoExchange started.")
    getter = ServerAutoExchange('172.16.9.63', 8887)
    getter.start_server()
    getter.loop()
    getter.stop_server()
