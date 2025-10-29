from sqlalchemy import create_engine, text

class ConexaoPostgre:
    @staticmethod
    def connection():
        """
        Cria e retorna uma engine SQLAlchemy para o banco PostgreSQL.
        """
        engine = create_engine(
            "postgresql+psycopg2://postgres:202323@192.168.68.168:5432/postgres",
            pool_pre_ping=True  # Verifica se a conexão ainda é válida
        )
        return engine

    @staticmethod
    def close(engine):
        """
        Fecha as conexões ativas e libera a engine.
        """
        engine.dispose()
