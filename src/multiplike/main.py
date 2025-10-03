import pandas as pd 
import sqlite3
import logging
import numpy as np



DB_PATH = 'data/multiplike_db.db'
CSV_PATH = 'data/teste pratico engenheiro de dados - dados_brutos.csv'
CHUNCK_SIZE = 2500

logging.basicConfig(
    level = logging.INFO,
    format = "%(asctime)s - %(levelname)s - %(message)s"
)


def executar_etl():

    try:
        with sqlite3.connect(DB_PATH) as conn:
            with conn: 
                cursor = conn.cursor()

                cursor.execute("""
                    DROP TABLE IF EXISTS usuarios_silver                    
                """)
                
                '''cursor.execute("""
                    CREATE TABLE usuarios_silver (
                        id INTEGER PRIMARY KEY,
                        nome TEXT,     
                        idade INTEGER,  
                        email TEXT, 
                        data_nascimento DATE,                               
                        cidade as TEXT,
                        dia_nascimento INTEGER,
                        mes_nascimento INTEGER,                              
                        ano_nascimento INTEGER
                    )
                """)'''

                for i, chunck in enumerate(pd.read_csv(CSV_PATH, encoding='utf-8', sep=',', chunksize=CHUNCK_SIZE)):

                    chunck.columns = (
                        chunck.columns
                        .str.strip()
                        .str.lower()
                        .str.replace(" ","_",regex=True)
                        .str.replace(r"[^a-z0-9_]","",regex=True)
                    )

                    chunck_nome_split = chunck['nome'].str.split('.', n=1, expand=True)

                    # título: se existir coluna [1], pega [0], senão "Não Possui"
                    chunck['titulo'] = np.where(
                        chunck_nome_split[1].isna(),  # verifica linha a linha
                        "Não Possui",
                        chunck_nome_split[0].str.strip()
                    )

                    # nome: se existir coluna [1], pega ela, senão mantém original
                    chunck['nome'] = np.where(
                        chunck_nome_split[1].isna(),
                        chunck['nome'],
                        chunck_nome_split[1].str.strip()
                    )

                    # capitalização
                    chunck['nome'] = chunck['nome'].str.title().str.strip()

                    chunck['cidade'] = chunck['cidade'].str.strip().str.title()
                    chunck['idade'] = pd.to_numeric(chunck['idade'], errors='coerce')
                    chunck['idade'] = chunck['idade'].fillna(0).astype(int)
                    chunck['data_nascimento'] = pd.to_datetime(chunck['data_nascimento'], format="%d/%m/%Y", errors='coerce')
                    chunck['dia_nascimento'] = chunck['data_nascimento'].dt.day
                    chunck['mes_nascimento'] = chunck['data_nascimento'].dt.month
                    chunck['ano_nascimento'] = chunck['data_nascimento'].dt.year
                    chunck = chunck.drop_duplicates(subset = ['id'])
                    chunck = chunck.dropna(subset = ['id'])


                    chunck.to_sql('usuarios_silver', conn, if_exists ='append', index=False) 

    except Exception as e:
        logging.exception(f"Erro detectado = {e}")
        raise

if __name__== '__main__':
    executar_etl()