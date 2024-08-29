import multiprocessing
import logging
import traceback
import dawload
import Api_BancoCentral
import Email_Error

# Configuração do logger
logging.basicConfig(filename='error.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

def codigo1():
    try:
        dawload.main()
    except Exception as e:
        logging.error(f'Erro em codigo1: {e}')
        traceback.print_exc()
        codigo = 'Ministerio da mineração'
        Email_Error.main(codigo,traceback)
def codigo2():
    try:
        Api_BancoCentral.main()
    except Exception as e:
        logging.error(f'Erro em codigo2: {e}')
        traceback.print_exc()
        codigo = 'Banco Central'
        Email_Error.main(codigo,traceback)

processo1 = multiprocessing.Process(target=codigo1)
processo2 = multiprocessing.Process(target=codigo2)

processo1.start()
processo2.start()