import pyodbc


class DataBase:
    def __init__(self):
        try:
            self.connect = pyodbc.connect(
                'DRIVER={SQL Server};SERVER=HOMEDES001\SQLEXPRESS;DATABASE=autoexchange;UID=d.dikiy;PWD=Rhjyjc2910')
        except pyodbc.Error as err:
            print('Error', err)

    def get_enterprise_data(self, hash_key):
        rows = None
        cursor = self.connect.cursor()
        sql = f"EXEC enterprise_info @HashKey='{hash_key}'"
        try:
            cursor.execute(sql)
            rows = cursor.fetchall()
        except pyodbc.Error as err:
            print('Error', err)
            cursor.close()
        cursor.close()
        return rows

    def update_status_operation(self, hash_key, status):
        cursor = self.connect.cursor()
        sql = f"EXEC update_status @HashKey='{hash_key}', @Status={status}"
        try:
            cursor.execute(sql)
            cursor.commit()
        except pyodbc.Error as err:
            print('Error', err)
            cursor.close()
        cursor.close()

    def close_connect(self):
        self.connect.close()
