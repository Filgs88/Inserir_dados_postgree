import pyodbc

class Conexao:
    def conection():
        con = pyodbc.connect(
        'Driver={Oracle em OraClient11g_home1};'
        'dbq=168.194.255.144:1521/cs0084p;'
        'Uid=fatimaagro;'
        'Pwd=A#QNK2bdJh8US;'
        )
        return con
        
    def close(con):
        con.close()