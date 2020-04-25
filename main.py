import pyodbc
import os
from run_process import StartProcess
from server import Server

class AutoExchange:
    def __init__(self, hash_key):
        self.hash_key = hash_key
        self.generate_config_file()
        StartProcess.start(self.get_enterprise_data, self.hash_key)

    def get_enterprise_data(self):
        conn = pyodbc.connect(
            'DRIVER={SQL Server};SERVER=HOMEDES001\SQLEXPRESS;DATABASE=autoexchange;UID=d.dikiy;PWD=Rhjyjc2910')
        cursor = conn.cursor()
        cursor.execute('{CALL enterprise_info (?)}', self.hash_key)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        print(rows)
        return rows

    def generate_config_file(self, shared_mode=1):
        enterprise_data = self.get_enterprise_data()
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
                SharedMode={shared_mode}
                WriteTo={write_to}
                ReadFrom={read_from}
            '''
        param = f'{abspath}\Conf\\2_{self.hash_key}_REK.prm'
        with open(param, mode='w', encoding='utf-8') as file:
            file.write(text)


class ServerAutoExchange(Server):
    def handle(self, message):
        try:
            AutoExchange(message)

        except Exception as e:
            print("Error: {}".format(e))


if __name__ == "__main__":
    print("ServerAutoExchange started.")
    getter = ServerAutoExchange("localhost", 8887)
    getter.start_server()
    getter.loop()
    getter.stop_server()

