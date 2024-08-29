import pandas as pd
import duckdb
import re
import numpy as np
import Tabela21
from datetime import datetime
from io import BytesIO
import gcp
import re
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import io


def Mes():
    now = datetime.now()
    ATL_current_month = now.month
    if ATL_current_month == 1:
        ANT_current_month = 12
    else:
        ANT_current_month = ATL_current_month - 1
    return ANT_current_month

def duck(dados_tratados,verf_plus,nome,tabela):
    print("\n")
    print(tabela)
    print(nome)
    # Box Memory:
    dados_1 = []
    dados_2 = []
    dados_3 = []
    dados_4 = []
    dados_5 = []
    dados_6 = []
    dados_7 = []
    dados_8 = []

    # Conecta ao banco de dados DuckDB
    conn = duckdb.connect(database=':memory:', read_only=False)
    # Tratamentos dados de entrada:
    for nome_da_tabela, df in dados_tratados.items():
        if not df.empty:
            df = df.apply(lambda x: x.replace('.', '') if isinstance(x, str) else x)
            # Substitui vírgulas por pontos nos dados
            df = df.apply(lambda x: x.replace(',', '.') if isinstance(x, str) else x)
            # Substitui traços por 0.0 nos dados
            df = df.apply(lambda x: x.replace('-', '0.0') if isinstance(x, str) else x)
        
        if nome_da_tabela == 0:
            dados_1 = df
        elif nome_da_tabela == 1:
            dados_2 = df
        elif nome_da_tabela == 2:
            dados_3 = df 
        elif nome_da_tabela == 3:
            dados_4 = df
        elif nome_da_tabela == 4:
            dados_5 = df
        elif nome_da_tabela == 5:
            dados_6 = df
        elif nome_da_tabela == 6:
            dados_7 = df
        elif nome_da_tabela == 7:
            dados_8 = df
    
    saida_year = None
    if verf_plus > 1:
        for year in dados_1:
            year_vef = re.findall(r'(202[0-9])', year)
            print(year_vef)
            if year_vef:
                saida_year = year_vef[0]
                break
        
        for i, year in enumerate(dados_1):
            year_vef = re.findall(r'(202[0-9])', year)
            if year_vef:
                dados_1[i] = "FEVV"
                break         
    
    conn = duckdb.connect(database=':memory:')    
    match tabela:
            
            case 'tabela_1':
                schema = [
                    ("AnoMES", "VARCHAR(255)"),
                    ("BML_LME_Nominal", "DOUBLE"),
                    ("COMEX_Nominal", "DOUBLE"),
                    ("BML_LME_Real", "DOUBLE"),
                    ("COMEX_Real", "DOUBLE") ]
                DodosDuck = zip(dados_1, dados_2, dados_3, dados_4, dados_5)
                select =  "SELECT * FROM my_table WHERE AnoMES NOT IN ('lixo') AND BML_LME_Nominal NOT IN ('NaN');"
            
            case 'tabela_9':
                schema = [
                    ("AnoMES", "VARCHAR(255)"),
                    ("DEALERS_OXIDE2", "DOUBLE"),
                    ("MEAN3", "DOUBLE"),
                    ("MB_MIN_LOW4", "DOUBLE"),
                    ("MB_MAX_HIGH4", "DOUBLE"),
                    ("US_FeMo_MEAN5","DOUBLE") ]
                DodosDuck = zip(dados_1, dados_2, dados_3, dados_4, dados_5, dados_6)
                select =  "SELECT * FROM my_table WHERE AnoMES NOT IN ('lixo') AND DEALERS_OXIDE2 NOT IN ('NaN');"
            case 'tabela_7.1':
                schema = [
                    ("AnoMES", "VARCHAR(255)"),
                    ("LBM_INICIAL_INITIAL_UK", "DOUBLE"),
                    ("LBM_FINAL_UK", "DOUBLE"),
                    ("HANDY_HARMAN_USA", "DOUBLE"),
                    ("COMEX_Pos_USA", "DOUBLE")]
                DodosDuck = zip(dados_1, dados_2, dados_3, dados_4, dados_5)
                select =  "SELECT * FROM my_table WHERE AnoMES NOT IN ('lixo') AND LBM_INICIAL_INITIAL_UK NOT IN ('NaN');"
            case 'tabela_8.1':
                schema = [
                    ("AnoMES", "VARCHAR(255)"),
                    ("LONDON_BULLION_MARKET_UK", "DOUBLE"),
                    ("HANDY_HARMAN_USA", "DOUBLE"),
                    ("COMEX_Pos_USA", "DOUBLE")]
                DodosDuck = zip(dados_1, dados_2, dados_3, dados_4)
                select =  "SELECT * FROM my_table WHERE AnoMES NOT IN ('lixo') AND LONDON_BULLION_MARKET_UK NOT IN ('NaN');"
            
            case 'tabela_21':
                schema = [
                    ("AnoMES", "VARCHAR(255)"),
                    ("CÁTODOS_SX_EW_SX_EW_CATHODES_MINA_MINE", "DOUBLE"),
                    ("CONCENTRADOS_CONCENTRATES_MINA_MINE", "DOUBLE"),
                    ("TOTAL_MINA_MINE", "DOUBLE"),
                    ("SMELTER_FUNDICION", "DOUBLE"),
                    ("CATODOS_E_R_ER_CATHODES_REFINADO ","DOUBLE"),
                    ("RAF_FIRE_REFINED","DOUBLE"),
                    ("TOTAL_REFINADO ") ]
                DodosDuck = zip(dados_1, dados_2, dados_3, dados_4, dados_5, dados_6,dados_7,dados_8)
                select =  "SELECT * FROM my_table WHERE AnoMES NOT IN ('lixo') AND CÁTODOS_SX_EW_SX_EW_CATHODES_MINA_MINE NOT IN ('NaN');"
            
            case 'tabela_24':
                schema = [
                    ("AnoMES", "VARCHAR(255)"),
                    ("KgOrofino_KgGoldContent", "DOUBLE"),
                    ("KgPlataFina_KgSilverContent", "DOUBLE")]
                DodosDuck = zip(dados_1, dados_2, dados_3)
                select =  "SELECT * FROM my_table WHERE AnoMES NOT IN ('lixo') AND KgOrofino_KgGoldContent NOT IN ('NaN');"
            
            case 'tabela_26':
                schema = [
                    ("AnoMES", "VARCHAR(255)"),
                    ("REFINADOS_REFINED", "DOUBLE"),
                    ("BLISTER", "DOUBLE"),
                    ("GRANELES_BULK", "DOUBLE"),
                    ("TOTAL", "DOUBLE"),
                    ("MOLIBDENO_TM_fino_MOLYBDENUM_MT_Content","DOUBLE") ]            
                DodosDuck = zip(dados_1, dados_2, dados_3, dados_4, dados_5, dados_6)
                select =  "SELECT * FROM my_table WHERE AnoMES NOT IN ('lixo') AND REFINADOS_REFINED NOT IN ('NaN');" 
    try:
        conn.execute(f"CREATE TABLE my_table ({', '.join(f'{col_name} {col_type}' for col_name, col_type in schema)})")
    except:
        conn.execute(f"CREATE TABLE my_table ({', '.join(f'{col_name} DOUBLE PRECISION' for col_name, col_type in schema)})")
 
    # Insere os dados na tabela

    match tabela:
        case 'tabela_1':
            conn.executemany("INSERT INTO my_table VALUES ( ?, ?, ?, ?, ?)", DodosDuck)
                        
        case 'tabela_9':
            conn.executemany("INSERT INTO my_table VALUES (?, ?, ?, ?, ?, ?)", DodosDuck)
            
        case 'tabela_7.1':
            conn.executemany("INSERT INTO my_table VALUES (?, ?, ?, ?, ?)", DodosDuck)                    
        
        case 'tabela_8.1':
            conn.executemany("INSERT INTO my_table VALUES (?, ?, ?, ?)", DodosDuck)                    
            
        case 'tabela_21':
            conn.executemany("INSERT INTO my_table VALUES (?, ?, ?, ?, ?, ?, ?, ?)", DodosDuck)                    
                        
        case 'tabela_24':
            conn.executemany("INSERT INTO my_table VALUES (?, ?, ?)", DodosDuck)
                        
        case 'tabela_26':
            conn.executemany("INSERT INTO my_table VALUES (?, ?, ?, ?, ?, ?)", DodosDuck)        
    
    # Alteração de update
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
                                                                    REGEXP_REPLACE(
                                                                        AnoMES, 
                                                                        'ENE/JAN', '{}-01-01'
                                                                    ), 
                                                                    'FEVV', '{}-02-01'
                                                                ), 
                                                                'FEB', '{}-03-01'
                                                            ), 
                                                            'MAR', '{}-04-01'
                                                        ), 
                                                        'ABR/APR', '{}-05-01'
                                                    ), 
                                                    'MAY', '{}-06-01'
                                                ), 
                                                'JUN', '{}-07-01'
                                            ), 
                                            'JUL', '{}-08-01'
                                        ), 
                                        'AGO/AUG', '{}-09-01'
                                    ), 
                                    'SEP', '{}-10-01'
                                ), 
                                'OCT', '{}-11-01'
                            ), 
                            'NOV', '{}-12-01'
                        ), 
                        'DIC/DEC', 'lixo'
                    );""".format(saida_year, saida_year, saida_year, saida_year, saida_year, saida_year, saida_year, saida_year, saida_year, saida_year, saida_year, saida_year, saida_year)
        # Executar a consulta
        conn.execute(query)
    
    results = conn.execute(select).df()
   
    return results,saida_year

def main (nome,tabela):
    print('============================')
    print(tabela)
    print('============================')
    print("\n")


    caminho_arquivo = r"C:\Users\gfigu\Desktop\Projeto\Randon\Cochilco_dowload\arquivo1.xlsx"
    tipo = 'Ministerio_de_Mineria'
    
    if tabela == 'tabela_21':
        Tabela21.main()
        print("Tabela 21")
    else:
        dados_terceira_aba = pd.read_excel(caminho_arquivo, sheet_name=2)
        dados_terceira_aba = dados_terceira_aba.iloc[2:]# Descarta as primeiras três linhas

        # Reseta o índice para começar de zero após a remoção das primeiras três linhas
        dados_terceira_aba.reset_index(drop=True, inplace=True)

        # Lista para armazenar os dados das tabelas
        dados_tabelas = []
        dataframes_list = []
        # Transpõe o DataFrame
        dados_transpostos = dados_terceira_aba.T
        print(dados_transpostos)
        verf_plus = 0
        # Itera sobre as colunas transpostas do DataFrame
        for nome_da_coluna in dados_transpostos.columns:
            # Verifica se a segunda linha contém valores NaN
            if dados_transpostos.iloc[2, :].isnull().all():
                print("Tabela", nome_da_coluna, "ignorada pois a segunda linha contém valores NaN.")
                continue

            # Lista para armazenar os dados da tabela atual
            dados_tabela_atual = []

            # Itera sobre os dados da tabela
            for linha in dados_transpostos[nome_da_coluna]:
               
                # Verifica se a linha é uma instância de string e se contém dados
                if isinstance(linha, str) and linha.strip():
                    dados = linha.split() # Divida a linha em dados separados por espaço

                    # Adicione os dados da linha à lista de dados da tabela atual
                    dados_tabela_atual.append(dados)

            # Se houver dados na tabela atual
            if dados_tabela_atual:
                # Cria um DataFrame para a tabela atual e adiciona à lista de tabelas
                tabela_atual = pd.DataFrame(dados_tabela_atual, columns=range(len(dados_tabela_atual[0])))
                dados_tabelas.append((nome_da_coluna, tabela_atual))

                dados_tratados = tabela_atual.T
                verf_plus += 1
                saida,ano1 = duck (dados_tratados,verf_plus,nome,tabela)
                
                if verf_plus == 1:
                    nome_Ano = nome + '_ANUAL'
                    table = pa.Table.from_pandas(saida)
                    buffer = io.BytesIO()
                    pq.write_table(table, buffer)         
                    gcp.main(buffer,nome_Ano,tipo)

                if verf_plus > 1 and verf_plus < 6:
                    df = pd.DataFrame(saida)
                    dataframes_list.append(df)
                    combined_df = pd.concat(dataframes_list, ignore_index=True)

                    # Se desejar, você pode verificar o DataFrame combinado antes de convertê-lo
                    print(combined_df)
                    if verf_plus == 5:
                        table = pa.Table.from_pandas(combined_df)
                        buffer = io.BytesIO()
                        pq.write_table(table, buffer)
                        gcp.main(buffer,nome,tipo)
                
                elif verf_plus == 6:

                    #Alteração das linhas AnoMES para novos Nomes
                    MesRef = Mes()
                    ano2 = int(ano1) - 1
                    nova_coluna = [f'{ano1}-{MesRef}-01',f'{ano2}-{MesRef}-01']
                    saida['AnoMES'] =pd.Series(nova_coluna)
                    nome +='_MediaAno'
                    
                    table = pa.Table.from_pandas(saida)
                    buffer = io.BytesIO()
                    pq.write_table(table, buffer)         
                    gcp.main(buffer,nome,tipo)

                elif verf_plus == 7:
                    print('saida de %')

            else:
                print("Tabela", nome_da_coluna, "ignorada pois não contém dados.")
    
if __name__ == "__main__":
    main()