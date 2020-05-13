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
        return rows

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

    def update_operation_table(self, hash_key, status):
        cursor = self.connect.cursor()
        sql = f"EXEC update_operation_table @HashKey='{hash_key}', @Status={status}"
        try:
            cursor.execute(sql)
            cursor.commit()
        except pyodbc.Error as err:
            print('Error update_operation_table', err)

        cursor.close()

    def update_suboperation_table(self, hash_key, status, code1c7):
        cursor = self.connect.cursor()
        sql = f"EXEC update_suboperation_table @HashKey='{hash_key}', @Status={status}, @Code1c7={code1c7}"
        try:
            cursor.execute(sql)
            cursor.commit()
        except pyodbc.Error as err:
            print('Error update_suboperation_table', err)

        cursor.close()

    def insert_suboperation_log(self, code1c7, hash_key, status, infotext, prtype):
        cursor = self.connect.cursor()
        sql = f"EXEC insert_suboperationlog @code1c7={code1c7}, @hashKey='{hash_key}', @status={status}, @infotext={infotext}, @prtype={prtype} "
        try:
            cursor.execute(sql)
            cursor.commit()
        except pyodbc.Error as err:
            print('Error insert_suboperation_log', err)

        cursor.close()

    def close_connect(self):
        self.connect.close()


