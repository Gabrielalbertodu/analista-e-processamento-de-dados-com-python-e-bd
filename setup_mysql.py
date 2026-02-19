import pymysql
import os
from database import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME

def create_database():
    print(f"🔌 Conectando ao MySQL em {DB_HOST}...")
    try:
        conn = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            port=int(DB_PORT)
        )
        cursor = conn.cursor()
        
        print(f"🔨 Criando banco de dados '{DB_NAME}' se não existir...")
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
        conn.commit()
        conn.close()
        print("✅ Banco de dados verificado.")
    except Exception as e:
        print(f"❌ Erro ao conectar/criar banco: {e}")
        print("⚠️ Verifique se o MySQL está rodando e as credenciais em database.py estão corretas.")

def create_tables():
    from database import get_engine
    from sqlalchemy import text
    
    try:
        engine = get_engine()
        
        # DDL baseado no teste3.sql
        ddl_commands = [
            """
            CREATE TABLE IF NOT EXISTS despesas_consolidadas (
                CNPJ VARCHAR(14),
                RazaoSocial VARCHAR(255),
                Trimestre VARCHAR(2),
                Ano INTEGER,
                ValorDespesas DECIMAL(18, 2),
                PRIMARY KEY (CNPJ, Trimestre, Ano)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS operadoras_ativas (
                CNPJ VARCHAR(14) PRIMARY KEY,
                RegistroANS VARCHAR(20),
                Modalidade VARCHAR(100),
                UF CHAR(2)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS despesas_agregadas (
                RazaoSocial VARCHAR(255),
                UF CHAR(2),
                TotalDespesas DECIMAL(18, 2),
                MediaDespesas DECIMAL(18, 2),
                DesvioPadraoDespesas DECIMAL(18, 2)
            )
            """
        ]
        
        print("🏗️ Criando tabelas...")
        with engine.connect() as conn:
            for ddl in ddl_commands:
                conn.execute(text(ddl))
                conn.commit()
        print("✅ Tabelas criadas com sucesso.")
    except Exception as e:
        print(f"❌ Erro ao criar tabelas: {e}")

if __name__ == "__main__":
    create_database()
    create_tables()