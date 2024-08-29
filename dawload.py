import os
import tempfile
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import pandas as pd
import openpyxl
import star_index
import leitura_de_aba

def limpeza():
    
    for root, dirs, files in os.walk(r'C:\Users\gfigu\Desktop\Projeto\Randon\Cochilco_dowload', topdown=False):
        for name in files:
            os.remove(os.path.join(root, name))
        for name in dirs:
            os.rmdir(os.path.join(root, name))
    
    return True
def conversor (xls_path):
    
    output_file = r"C:\Users\gfigu\Desktop\Projeto\Randon\Cochilco_dowload\arquivo1.xlsx"
       
    dfs = pd.read_html(xls_path)
    with pd.ExcelWriter(output_file) as writer:
        for i, df in enumerate(dfs):
            df.to_excel(writer, sheet_name=f'Tabela {i+1}', index=False)

    print(f"O arquivo Excel '{output_file}' foi criado com sucesso.")
    
    return True

def main ():
    urls, df = star_index.site()
    urls_name = df["name"]
    urls_web = df['web']
    urls_tipo = df['tipo']
    
    for idx, name in enumerate(df['name']):
        site = urls_web.iloc[idx]
        nome = urls_name.iloc[idx]
        tabela = urls_tipo.iloc[idx]
    
        # Configurações do Chrome WebDriver
        chrome_options = Options()

        download_path = r"C:\Users\gfigu\Desktop\Projeto\Randon\Cochilco_dowload"
# Evita que o navegador solicite permissão para fazer o download
        chrome_options.add_experimental_option("prefs", {
            "download.default_directory": download_path,
            "download.prompt_for_download": False,  
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        })

        chrome_options.add_argument("--headless")  # Execução em modo headless, sem interface gráfica
        service = Service(r'C:\Users\gfigu\Desktop\Projeto\Randon\chromedriver_win32\chromedriver.exe')

        # Inicializando o WebDriver do Chrome com as opções e o serviço configurados
        driver = webdriver.Chrome(options=chrome_options)
        driver.get(site)

        # Botão de download
        download_button = driver.find_element(By.XPATH, "//a[img[contains(@src, 'boletin_xls')]]")
        download_button.click()

        # Espera no máximo 60 segundos
        max_wait_time = 60
        wait_time = 0
        while not any(fname.endswith('.xls') for fname in os.listdir(download_path)) and wait_time < max_wait_time:
            time.sleep(1)
            wait_time += 1

        if wait_time >= max_wait_time:
            print("Tempo máximo de espera atingido. O download pode não ter sido concluído.")

        else:
        # Procurando pelo arquivo XLS baixado na pasta de download
            xls_file = [f for f in os.listdir(download_path) if f.endswith('.xls')]
        if xls_file:
            xls_path = os.path.join(download_path, xls_file[0])

            print("dowload OK:",xls_path)
        
        driver.quit()

        conversor(xls_path)
        leitura_de_aba.main(nome,tabela)
        limpeza()
if __name__ == "__main__":
    main()