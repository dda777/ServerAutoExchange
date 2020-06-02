import pyodbc
from subprocess import check_output, CalledProcessError


class ExclusiveAuto:

    def __init__(self, db_name, db_server, server_name, path_close):
        self.db_name = db_name
        self.db_server = db_server
        self.server_name = server_name
        self.path_close = path_close
        try:
            self.connect = pyodbc.connect(
                'DRIVER={SQL Server};'
                f'SERVER={self.db_server};'
                f'DATABASE={self.db_name};'

                )
        except pyodbc.Error as err:
            print('Error conn', err)

    def __del__(self):
        self.connect.close()

    def close_sql_connect(self):
        cursor = self.connect.cursor()
        sql = 'SELECT * FROM sys.sysprocesses'
        try:
            cursor.execute(sql)
            reqest = cursor.fetchall()
        except pyodbc.Error as err:
            reqest = f'Error get enterprise data, {err}'

        cursor.close()
        return reqest

    def close_open_files(self):
        data = []

        script = fr'Openfiles /Query /s {self.server_name} /fo csv  | find /i "{self.path_close}"'
        request = check_output(script, encoding='866', shell=True).split('\n')

        for item in request:
            data += item.replace('"', '').split(',')

        for i in range(0, len(data), 4):
            script = f'Openfiles /Disconnect /s {self.server_name} /ID {data[i]}'
            try:
                request = check_output(script, encoding='866', shell=True)
            except CalledProcessError as e:
                request = f'{script} \n {e}'
        return request

    def close_open_process(self):
        script = f'taskkill /s {self.server_name} /FI "IMAGENAME eq 1cv7s.exe"'
        try:
            request = check_output(script, encoding='866', shell=True)
        except CalledProcessError as e:
            request = f'{script} \n {e}'
        return request


c = ExclusiveAuto(db_server='srvdb01', db_name='gagarin', server_name='srvzatoka3', path_close='db.adm')
q = c.close_open_files()
print(q)
del c
