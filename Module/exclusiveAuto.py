# -*- coding: utf-8 -*-
import pyodbc
from subprocess import check_output, CalledProcessError
import ctypes

kernel32 = ctypes.WinDLL('kernel32', use_last_error=True)
if not kernel32.SetConsoleCP(1251):
    raise print(ctypes.WinError(ctypes.get_last_error()))
# Set the active output codepage to UTF-8
if not kernel32.SetConsoleOutputCP(65001):
    raise print(ctypes.WinError(ctypes.get_last_error()))


class ExclusiveAuto:

    def __init__(self, db_name, db_server, server_name, path_close='db.adm'):
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
                WHERE D.Name = '{self.db_name}' AND program_name LIKE '1C%'
            '''
        try:
            cursor.execute(get_pid)
            request = []
            for row in cursor:
                for i in row:
                    request.append(i)
        except pyodbc.Error as err:
            request = f'Error get pid data, {err}'

        for pid in request:
            cursor.rollback()
            kill_pid = f'KILL {pid};'
            cursor.execute(kill_pid)
            cursor.commit()

        cursor.close()
        return request

    def close_open_files(self):
        data = []
        try:
            script = fr'Openfiles /Query /s {self.server_name} /fo csv  | find /i "{self.path_close}"'
            request = check_output(script, encoding='866', shell=True).split('\n')
        except CalledProcessError as e:
            return 0

        for item in request:
            data += item.replace('"', '').split(',')

        for i in range(0, len(data), 4):
            if data[i] == '':
                continue
            script = f'Openfiles /Disconnect /s {self.server_name} /ID {data[i]}'
            try:
                check_output(script, encoding='utf-8',shell=True)
            except CalledProcessError:
                return 0

    def close_open_process(self):
        script = f'taskkill /f /s {self.server_name} /FI  "IMAGENAME eq 1cv7s.exe"'
        try:
            request = check_output(script, encoding='866', shell=True)
        except CalledProcessError as e:
            request = f'{script} \n {e}'
        return request


c = ExclusiveAuto(db_server='srvdb09', db_name='gresklad', server_name='srvGrecheskaya', path_close='db.adm')
#c.close_sql_connect()
c.close_open_files()
#c.close_open_process()
del c
