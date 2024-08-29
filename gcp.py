import os
import pandas as pd
from google.cloud import storage

def main(buffer, name, tipo):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\gfigu\Desktop\Projeto\Randon\Programa\dados-mercado-412719-f797ad54da6a.json"

    # Especifica o nome do bucket
    bucket_name = 'mercado-chile'
    
    # Converte o buffer de memória em bytes
    
    buffer.seek(0)
    file_bytes = buffer.read()
    if tipo == "Banco_Central":
        # Define o caminho do blob
        destination_blob_path = f'Banco_Central/{name}.parquet'
    elif tipo == "Ministerio_de_Mineria":
        destination_blob_path = f'Boletin_Cochilco/{name}.parquet'
    
    # Inicializa o cliente de armazenamento
    storage_client = storage.Client()

    # Obtém o bucket
    bucket = storage_client.get_bucket(bucket_name)

    # Cria um novo blob e o carrega com os bytes do buffer
    blob = bucket.blob(destination_blob_path)
    blob.upload_from_string(file_bytes, content_type='application/octet-stream')

    print(f'O arquivo foi enviado para o bucket do GCP: {destination_blob_path}')
if __name__ == "__main__":
    main()