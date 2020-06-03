import pyodbc
from subprocess import check_output, CalledProcessError


class ExclusiveAuto:

    def __init__(self, db_name, db_server, server_name, path_close= 'db.adm'):
        self.db_name = db_name
        self.db_server = db_server
        self.server_name = server_name
        self.path_close = path_close
        try:
            self.connect = pyodbc.connect(
                'DRIVER={SQL Server};'
                f'SERVER={self.db_server};'
                f'DATABASE=master;'
                )
        except pyodbc.Error as err:
            print('Error conn', err)

    def __del__(self):
        self.connect.close()

    def close_sql_connect(self):
        cursor = self.connect.cursor()
        get_pid = f'''
                SELECT spid FROM sys.sysprocesses P
                JOIN sys.sysdatabases D ON (D.dbid = P.dbid)
                WHERE D.Name = '{self.db_name}' AND program_name = '1cv7'
            '''
        try:
            cursor.execute(get_pid)
            request = []
            for row in cursor:
                for i in row:
                    request.append(i)
        except pyodbc.Error as err:
            request = f'Error get pid data, {err}'

        try:
            for pid in request:
                kill_pid = f'KILL {pid}'
                cursor.execute(kill_pid)
                cursor.commit()
        except pyodbc.Error as err:
            request = f'Error with kill_pid, {err}'

        cursor.close()
        return request

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


c = ExclusiveAuto(db_server='srvdb10', db_name='SKLAD_GLOBAL', server_name='srvzatoka3', path_close='db.adm')
q = c.close_sql_connect()
print(q)
del c
