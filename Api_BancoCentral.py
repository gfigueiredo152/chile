from io import BytesIO
from google.cloud import storage
import pandas as pd
import requests
import io
from datetime import datetime
# ------------------------------ Bibliotecas de area externas ------------------------------
import Programa.star_index as star_index
import Programa.Email_Error as Email_Error
import gcp

def conf_day():
    now = datetime.now()
    ATL_current_year = now.year
    ATL_current_month = now.month
    ATL_current_day = now.day
    ATL_current_TODAY = now.strftime("%Y-%m-%d")
    ANT_current_month = ATL_current_month - 1
    ANT_current_year = ATL_current_year
    if ANT_current_month == 0:
        ANT_current_month = 12
        ANT_current_year -= 1
    print(ATL_current_TODAY)
    return ATL_current_TODAY

def main():
    date = conf_day()
    
    Cod_API = star_index.API()  # Desempacotando os resultados da função Ent_API()
    # Acessando a coluna 'web' do DataFrame
    Name_API = Cod_API['name']
    API = Cod_API['cod_API']
    

    for idx, name in enumerate(Cod_API['name']):
        codigo = API.iloc[idx]
        print(name)
        
        url = f-----------------------------------------------------------------------
        payload = {}
        headers = {
          'Cookie': 'ASP.NET_SessionId=4exmbw34ctl4zia4p1fpymjp'
        }
        try:
            response = requests.get(url, headers=headers)
            data = response.json()  # Converte a resposta em JSON para um dicionário Python
            series = data["Series"]
            obs = series["Obs"]

            # Normaliza os dados do JSON e cria um DataFrame
            df = pd.json_normalize(obs)
            
            # Criando um buffer de memória
            buffer = BytesIO()
            
            # Exibe o DataFrame
            print(df)
            df.to_parquet(buffer, index=False)
            gcp.main(buffer,name)
        
        except:
            Email_Error.main(codigo,name)
            print("Erro no codigo API: ",codigo,"Nome da API: ",name)

if __name__ == "__main__":
    main()
