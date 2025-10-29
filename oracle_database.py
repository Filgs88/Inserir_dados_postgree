import pyodbc

class Conexao:
    def conection():
        con = pyodbc.connect(
        'Driver={DRIVER};'
        'dbq=IP;'
        'Uid=USER;'
        'Pwd=PASSWORD;'
        )
        return con
        
    def close(con):

        con.close()
