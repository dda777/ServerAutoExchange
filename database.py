# -*- coding: utf-8 -*-
import pyodbc, re


class DataBase:
    def __init__(self, hash_key):
        self.hash_key = hash_key
        try:
            self.connect = pyodbc.connect(
                'DRIVER={SQL Server};SERVER=srvprintinfo;DATABASE=autoexchange;UID=admsuppdb;PWD=nhfycajhvfnjh')
            self.cursor = self.connect.cursor()
        except pyodbc.Error as err:
            print('Error conn', err)
        except:
            print('Error conn also')

    def get_enterprise_data(self):
        rows = None
        sql = f"EXEC enterprise_info @HashKey='{self.hash_key}'"
        try:
            self.cursor.execute(sql)
            rows = self.cursor.fetchall()
        except pyodbc.Error as err:
            print('Error get enterprise data', err)

        enterprise = []
        for row in rows:
            enterprise.append({'cod1c': row[0], 'path_to_tr5': row[1]})
        return enterprise

    # OLD
    # def select_shared_mode(self):
    #     rows = None
    #     sql = f"EXEC select_shared_mode @HashKey='{self.hash_key}'"
    #     try:
    #         self.cursor.execute(sql)
    #         rows = self.cursor.fetchall()
    #     except pyodbc.Error as err:
    #         print('Error select_shared_mode', err)
    #
    #     return rows

    def get_enterprise_data_all(self):
        rows = None
        sql = 'SELECT EnterpriseData_Name, EnterpriseData_Tr5Path, EnterpriseData_Code1c7 FROM S_EnterprisesData'
        try:
            self.cursor.execute(sql)
            rows = self.cursor.fetchall()
        except pyodbc.Error as err:
            print('Error get enterprise data', err)

        return rows

    # OLD
    # def update_operation_table(self, hash_key, status):
    #     cursor = self.connect.cursor()
    #     sql = f"EXEC update_operation_table @HashKey='{hash_key}', @Status={status}"
    #     try:
    #         cursor.execute(sql)
    #         cursor.commit()
    #     except pyodbc.Error as err:
    #         print('Error update_operation_table', err)
    #
    #     cursor.close()

    # OLD
    # def update_suboperation_table(self, hash_key, status, code1c7):
    #     cursor = self.connect.cursor()
    #     sql = f"EXEC update_suboperation_table @HashKey='{hash_key}', @Status={status}, @Code1c7={code1c7}"
    #     try:
    #         cursor.execute(sql)
    #         cursor.commit()
    #     except pyodbc.Error as err:
    #         print('Error update_suboperation_table', err)
    #
    #     cursor.close()

    def select_enterprise_database_info(self, code1c7):
        response = []
        rows = None
        sql = f"EXEC select_enterprise_database_info @Code1c7={code1c7}"
        try:
            self.cursor.execute(sql)
            rows = self.cursor.fetchall()
            for i in rows:
                response.append(i[0].split('\\')[2])
                response.append(i[1].strip())
                response.append(i[2].strip())

        except pyodbc.Error as err:
            print('Error update_suboperation_table', err)
        return response

    def insert_suboperations_log(self, mag_info, status='1', infotext='Start Exchange', pr_type='1'):
        for mag in mag_info:
            sql = f"EXEC insert_suboperationlog " \
                  f"@code1c7='{mag['cod1c']}', " \
                  f"@hashKey='{self.hash_key}', " \
                  f"@status={status}, " \
                  f"@infotext='{infotext}', " \
                  f"@prtype={pr_type} "
            try:
                self.cursor.execute(sql)
                self.cursor.commit()
            except pyodbc.Error as err:
                print('Error insert_suboperation_log', err)

    def insert_suboperation_log(self, mag, pr_type, status='1', infotext='Start Exchange'):
        infotext = re.sub(r"[']", "", infotext)
        sql = f"EXEC insert_suboperationlog " \
              f"@code1c7='{mag}', " \
              f"@hashKey='{self.hash_key}', " \
              f"@status={status}, " \
              f"@infotext='{infotext}', " \
              f"@prtype={pr_type} "
        try:
            self.cursor.execute(sql)
            self.cursor.commit()
        except pyodbc.Error as err:
            print('Error insert_suboperation_log', err)

    def close(self):
        self.cursor.close()
        self.connect.close()

d = DataBase('123')
q= d.select_enterprise_database_info('MAY')
print(q)