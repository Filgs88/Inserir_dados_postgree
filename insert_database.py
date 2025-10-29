import pandas as pd, sqlite3
from sqlalchemy import create_engine, text
from oracle_database import Conexao
from ConexaoPostgre import ConexaoPostgre

def criar_database(dataframe, nome_db):

	colunas = tuple(dataframe.columns)

	sql_query = f'create table if not exists {nome_db} {colunas}'

	con_post = ConexaoPostgre.connection()
	conn = con_post.connect()
	cursor = con.begin()
	cursor.execute(text(sql_post))
	cursor.commit()
	con_post.close()
	conn.close()

def inserir_sql_postgre(dataframe, nome_db):

	for index, row in dataframe.iterrows():
		row = row.fillna(pd.NaT).astype(str).replace('NaT', '')
		colunas = tuple(df.columns)
		linha = tuple(row)

		# Se for a primeira vez que for utilizar essa database descomentar essa linha primeiramente
		#sql_post = f'insert into "{nome_db}" {colunas} values {linha}'
		
		sql_post = f'insert into {nome_db} {colunas} values {linha} on conflict ("{colunas[0]}") do update set {', '.join([f'"{c}" = EXCLUDED."{c}"' for c in colunas])}'
		
		con_post = ConexaoPostgre.connection()
		conn = con_post.connect()
		cursor = con.begin()
		cursor.execute(text(sql_post))
		cursor.commit()
		con_post.close()
		conn.close()
		
	print("Banco de dados atualizado!")

# Ler a Sql que será adicionada ao banco de dados.
with open("./SQLS/sql_estoque_de_materiais.sql", "r") as arquivo:
	sql = arquivo.read()

# Estabelece conexão e cria o dataframe
con = Conexao.conection()
df = pd.read_sql_query(sql, con)
df.fillna('', inplace=True)
df = df.convert_dtypes()
df = df.astype(str).map(lambda x: x.replace('.', ',') if isinstance(x, str) else x)

nome_db = 'Estoque'

# Se caso for necessario criar a tabela passar por essa parte
criar_db = input('Deseja criar a database? (digite "s" se sim): ')
if criar_db == 's':
	criar_database(df, nome_db)

# Insere os dados no banco de dados, não esquecer de colocar o nome da tabela que será usada
inserir_sql_postgre(df, nome_db)
con.close()