import pyodbc
import os
from run_process import StartProcess
from server import Server
from database import DataBase


class AutoExchange:

    def __init__(self, hash_key):
        self.db = DataBase()
        self.hash_key = hash_key

        data = self.db.get_enterprise_data(self.hash_key)
        shared_mode = self.db.select_shared_mode(self.hash_key)[0][0]

        self.generate_config_file(data, shared_mode)
        self.db.update_operation_table(self.hash_key, 2)
        self.db.close_connect()
        StartProcess.start(data, self.hash_key)

    def generate_config_file(self, enterprise_data, shared_mode):
        write_to = ''
        read_from = ''
        abspath = os.path.abspath(os.curdir)
        process_status = 1
        for data in enterprise_data:
            for i in range(1, 3):
                if i == 1:
                    process_status = 1
                    write_to = 'REK'
                    read_from = ''
                elif i == 2:
                    process_status = 3
                    write_to = ''
                    read_from = 'REK'
                text = \
                    f'''
                        [General]
                        Output={abspath}\\Log\\{process_status}_{self.hash_key}_{data[0]}.log
                        Quit=1
                        AutoExchange=1
                        [AutoExchange]
                        SharedMode={shared_mode}
                        WriteTo={write_to}
                        ReadFrom={read_from}
                    '''
                param = f'{abspath}\Conf\{process_status}_{self.hash_key}_{data[0]}.prm'
                with open(param, mode='w', encoding='utf-8') as file:
                    file.write(text)

        write_to = ''
        read_from = ''

        for i in range(len(enterprise_data)):
            write_to += enterprise_data[i][0] + ','
            read_from += enterprise_data[i][0] + ','
        text = \
            f'''
                [General]
                Output={abspath}\\Log\\2_{self.hash_key}_REK.log
                Quit=1
                AutoExchange=1
                [AutoExchange]
                SharedMode=1
                WriteTo={write_to}
                ReadFrom={read_from}
            '''
        param = f'{abspath}\Conf\\2_{self.hash_key}_REK.prm'
        with open(param, mode='w', encoding='utf-8') as file:
            file.write(text)


class ServerAutoExchange(Server):
    def handle(self, message):
        try:
            AutoExchange(message.decode('utf-8'))
        except Exception as e:
            print("Error: {}".format(e))


if __name__ == "__main__":
    print("ServerAutoExchange started.")
    getter = ServerAutoExchange('172.16.9.63', 8887)
    getter.start_server()
    getter.loop()
    getter.stop_server()
