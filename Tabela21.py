import pandas as pd
from io import BytesIO
import gcp
import re
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import io
def duck(verf_plus,planilha):
    
    # Box Memory:
    dados_1 = []
    dados_2 = []
    dados_3 = []
    dados_4 = []
    dados_5 = []
    dados_6 = []
    dados_7 = []
    dados_8 = []    
    
    conn = duckdb.connect(database=':memory:', read_only=False)
    for i, (nome_da_tabela, df) in enumerate(planilha.items(), start=1):
        if i == 1 and not dados_1:
            dados_1 = df
        elif i == 2 and not dados_2:
            dados_2 = df
        elif i == 3 and not dados_3:
            dados_3 = df 
        elif i == 4 and not dados_4:
            dados_4 = df
        elif i == 5 and not dados_5:
            dados_5 = df
        elif i == 6 and not dados_6:
            dados_6 = df
        elif i == 7 and not dados_7:
            dados_7 = df
        elif i == 8 and not dados_8:
            dados_8 = df

    if verf_plus > 1:
        for year in dados_1:
            year_vef = re.findall(r'(202[0-9])', year)
            print(year_vef)
            if year_vef:
                saida_year = year_vef[0]
                break
        
    conn = duckdb.connect(database=':memory:')
    schema = [
        ("AnoMES", "VARCHAR(255)"),
        ("CATODOS_SX_EW_SX_EW_CATHODES_MINA_MINE", "DOUBLE"),
        ("CONCENTRADOS_CONCENTRATES_MINA_MINE", "DOUBLE"),
        ("TOTAL_MINA_MINE", "DOUBLE"),
        ("SMELTER_FUNDICION", "DOUBLE"),
        ("CATODOS_E_R_ER_CATHODES_REFINADO ","DOUBLE"),
        ("RAF_FIRE_REFINED","DOUBLE"),
        ("TOTAL_REFINADO ","DOUBLE") ]
        
    DodosDuck = zip(dados_1, dados_2, dados_3, dados_4, dados_5, dados_6,dados_7,dados_8)

    try:
        conn.execute(f"CREATE TABLE my_table ({', '.join(f'{col_name} {col_type}' for col_name, col_type in schema)})")
    except:
        conn.execute(f"CREATE TABLE my_table ({', '.join(f'{col_name} DOUBLE PRECISION' for col_name, col_type in schema)})")
    
    conn.executemany("INSERT INTO my_table VALUES (?, ?, ?, ?, ?, ?, ?, ?)", DodosDuck) 
    if verf_plus > 1:
        query = """
            UPDATE my_table
            SET AnoMES = REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    REGEXP_REPLACE(
                                        REGEXP_REPLACE(
                                            REGEXP_REPLACE(
                                                REGEXP_REPLACE(
                                                    REGEXP_REPLACE(
                                                        REGEXP_REPLACE(
                                                            REGEXP_REPLACE(
                                                                REGEXP_REPLACE(
                                                                    REGEXP_REPLACE(
                                                                        AnoMES, 
                                                                        '(ENE/JAN) {}', '{}-01-01'
                                                                    ), 
                                                                    'FEB', '{}-02-01'
                                                                ), 
                                                                'MAR', '{}-03-01'
                                                            ), 
                                                            'ABR/APR', '{}-04-01'
                                                        ), 
                                                        'MAY', '{}-05-01'
                                                    ), 
                                                    'JUN', '{}-06-01'
                                                ), 
                                                'JUL', '{}-07-01'
                                            ), 
                                            'AGO/AUG', '{}-08-01'
                                        ), 
                                        'SEP', '{}-09-01'
                                    ), 
                                    'OCT', '{}-10-01'
                                ), 
                                'NOV', '{}-11-01'
                            ), 
                            'DIC/DEC', '{}-12-01'
                        ); """.format(
                            saida_year, saida_year, saida_year, saida_year, 
                            saida_year, saida_year, saida_year, saida_year, 
                            saida_year, saida_year, saida_year, saida_year,
                            saida_year
                        )

        # Executar a consulta
        conn.execute(query)
    results = conn.execute("SELECT * FROM my_table WHERE CATODOS_SX_EW_SX_EW_CATHODES_MINA_MINE NOT IN ('NaN');").df()
        
    print(results)
    return results

def main():
    caminho_planilha = r'C:\Users\gfigu\Desktop\Projeto\Randon\Cochilco_dowload\arquivo1.xlsx'  
    df = pd.read_excel(caminho_planilha, sheet_name=2)
    df = df.iloc[2:]
    novos_nomes_col = ['AnoMES', 'CÁTODOS_SX_EW_SX_EW_CATHODES_MINA_MINE', 'CONCENTRADOS_CONCENTRATES_MINA_MINE', 'TOTAL_MINA_MINE', 'SMELTER_FUNDICION', 'CATODOS_E_R_ER_CATHODES_REFINADO ', 'RAF_FIRE_REFINED', 'TOTAL_REFINADO ']
    df.columns = novos_nomes_col
    # tranformação de dados
    # Remover pontos dos campos de texto
    df = df.applymap(lambda x: x.replace('.', '') if isinstance(x, str) else x)
    df = df.applymap(lambda x: x.replace('(P)', '') if isinstance(x, str) else x)
    # Substituir vírgulas por pontos nos campos de texto
    df = df.applymap(lambda x: x.replace(',', '.') if isinstance(x, str) else x)

    # Substituir traços por 0.0 nos campos de texto
    df = df.applymap(lambda x: '0.0' if isinstance(x, str) and x == '-' else x)
    dados_tabelas = []

    faixas = [(0, 4), (5, 17), (18, 30), (31, 43), (44, 56)]

    # Criar um dicionário para armazenar as planilhas
    planilhas = {}
    dataframes_list = []
    # Iterar sobre as faixas e criar as planilhas
    for i, faixa in enumerate(faixas, start=1):
        nome_planilha = f"Planilha_{i}"
        planilhas[nome_planilha] = df.iloc[faixa[0]:faixa[1]]
    verf_plus = 0
    # Agora você pode acessar cada DataFrame individualmente pelo nome da planilha:
    for nome_planilha, planilha in planilhas.items():
        verf_plus += 1
        saida = duck(verf_plus,planilha)
        if verf_plus > 1:
            df = pd.DataFrame(saida)
            
            dataframes_list.append(df)
            combined_df = pd.concat(dataframes_list, ignore_index=True)
            print('\n')
            print ('tabelão de entrada')
            print(combined_df)
        else:
            name = 'PRODUCCIÓN_CHILENA_COBRE_PRODUCTOS_ANUAL '
            tipo = 'Ministerio_de_Mineria'
            
            table = pa.Table.from_pandas(saida)
            buffer = io.BytesIO()
            pq.write_table(table, buffer)         
            
            #gcp.main(buffer,name,tipo)
    
    # PARQUET Tabela GERAL
    name = 'PRODUCCIÓN_CHILENA_COBRE_PRODUCTOS'
    table = pa.Table.from_pandas(combined_df)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)         
            
    gcp.main(buffer,name,tipo)

           
if __name__ == "__main__":
    pl = main()
        