# -*- coding: utf-8 -*-
import pyodbc


class DataBase:
    def __init__(self):
        try:
            self.connect = pyodbc.connect(
                'DRIVER={SQL Server};SERVER=srvprintinfo;DATABASE=autoexchange;UID=admsuppdb;PWD=nhfycajhvfnjh')
        except pyodbc.Error as err:
            print('Error conn', err)
        except:
            print('Error conn also')

    def get_enterprise_data(self, hash_key):
        rows = None
        cursor = self.connect.cursor()
        sql = f"EXEC enterprise_info @HashKey='{hash_key}'"
        try:
            cursor.execute(sql)
            rows = cursor.fetchall()
        except pyodbc.Error as err:
            print('Error get enterprise data', err)

        cursor.close()
        enterprise = []
        for row in rows:
            enterprise.append({'cod1c': row[0], 'path_to_tr5': row[1]})
        return enterprise

    def select_shared_mode(self, hash_key):
        rows = None
        cursor = self.connect.cursor()
        sql = f"EXEC select_shared_mode @HashKey='{hash_key}'"
        try:
            cursor.execute(sql)
            rows = cursor.fetchall()
        except pyodbc.Error as err:
            print('Error select_shared_mode', err)

        cursor.close()
        return rows

    def get_enterprise_data_all(self):
        rows = None
        cursor = self.connect.cursor()
        sql = 'SELECT EnterpriseData_Name, EnterpriseData_Tr5Path, EnterpriseData_Code1c7 FROM S_EnterprisesData'
        try:
            cursor.execute(sql)
            rows = cursor.fetchall()
        except pyodbc.Error as err:
            print('Error get enterprise data', err)

        cursor.close()
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

    def insert_suboperations_log(self, mag_info, hash_key, status='1', infotext='Start Exchange', pr_type='1'):
        cursor = self.connect.cursor()
        for mag in mag_info:
            sql = f"EXEC insert_suboperationlog " \
                  f"@code1c7='{mag['cod1c']}', " \
                  f"@hashKey='{hash_key}', " \
                  f"@status={status}, " \
                  f"@infotext='{infotext}', " \
                  f"@prtype={pr_type} "
            try:
                cursor.execute(sql)
                cursor.commit()
            except pyodbc.Error as err:
                print('Error insert_suboperation_log', err)

        cursor.close()

    def insert_suboperation_log(self, mag, hash_key, pr_type,  status='1', infotext='Start Exchange'):
        cursor = self.connect.cursor()

        sql = f"EXEC insert_suboperationlog " \
              f"@code1c7='{mag['cod1c']}', " \
              f"@hashKey='{hash_key}', " \
              f"@status={status}, " \
              f"@infotext='{infotext}', " \
              f"@prtype={pr_type} "
        try:
            cursor.execute(sql)
            cursor.commit()
        except pyodbc.Error as err:
            print('Error insert_suboperation_log', err)

        cursor.close()

    def close_connect(self):
        self.connect.close()
