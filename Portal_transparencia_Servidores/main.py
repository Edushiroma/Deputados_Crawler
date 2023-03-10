# -*- coding: utf-8 -*-

import os
from abc import ABC
from selenium.webdriver import DesiredCapabilities
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import Select
import time
import os.path
from selenium.common.exceptions import TimeoutException
from selenium import webdriver
from selenium.webdriver.common.by import By
from utils.logs import logging_to_kafka, log_input
import requests
import pprint
import uuid
import uvicorn
from fastapi import FastAPI
from fastapi import Request
from conector import Connector
from datetime import datetime

app = FastAPI()

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

class SeleniumFirefox(ABC):

    def __init__(self, folder, uuid):
        self.uuid_unico = uuid
        self.abs_path = os.getcwd()
        profile = webdriver.FirefoxProfile()
        profile.set_preference("browser.download.folderList", 2)
        profile.set_preference("browser.download.manager.showWhenStarting", False)
        profile.set_preference("browser.download.dir", self.abs_path + "/" + folder)
        profile.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/octet-stream")
        profile.set_preference("browser.helperApps.alwaysAsk.force", False)
        profile.set_preference('webdriver.load.strategy', 'unstable')

        profile.set_preference('browser.helperApps.neverAsk.saveToDisk',
                               'application/zip,application/octet-stream,application/x-zip-compressed,'
                               'multipart/x-zip,application/x-rar-compressed, application/octet-stream,'
                               'application/msword,application/vnd.ms-word.document.macroEnabled.12,'
                               'application/vnd.openxmlformats-officedocument.wordprocessingml.document,'
                               'application/vnd.ms-excel,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,'
                               'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,'
                               'application/vnd.openxmlformats-officedocument.wordprocessingml.document,'
                               'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/rtf,'
                               'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/vnd.ms-excel,'
                               'application/vnd.ms-word.document.macroEnabled.12,application/vnd.openxmlformats-officedocument.wordprocessingml.document,'
                               'application/xls,application/msword,text/csv,application/vnd.ms-excel.sheet.binary.macroEnabled.12,text/plain,text/csv/xls/xlsb,'
                               'application/csv,application/download,application/vnd.openxmlformats-officedocument.presentationml.presentation,application/octet-stream')

        options = Options()
        options.add_argument('--headless')

        cap = DesiredCapabilities().FIREFOX
        cap["acceptInsecureCerts"] = True
        cap["marionette"] = True

        self.driver = webdriver.Firefox(firefox_profile=profile,
                                        executable_path=r"/usr/local/bin/geckodriver",
                                        options=options,
                                        capabilities=cap)

        t = time.time()
        self.driver.set_page_load_timeout(-1)

        try:
            self.driver.get('https://www.portaltransparencia.gov.br/download-de-dados/servidores')
        except TimeoutException:
            pass

        print('Time consuming:', time.time() - t)

        self.driver.implicitly_wait(30)
        self.folder = folder

    @logging_to_kafka('selecionando ano', 'ano selecionado com sucesso')
    def selecionar_ano(self):
        select = Select(self.driver.find_element(By.ID, 'links-anos'))
        select.select_by_index(0)

        return self

    @logging_to_kafka('selecionando mes', 'mes selecionado com sucesso')
    def selecionar_mes(self):
        select = Select(self.driver.find_element(By.ID, 'links-meses'))
        select.select_by_index(len(select.options) - 1)
        
        return self

    @logging_to_kafka('selecionando origem siape', 'origem siape selecionada com sucesso')
    def selecionar_origem_siape(self):
        select = Select(self.driver.find_element(By.ID, 'links-origens-mes'))
        select.select_by_value('Servidores_SIAPE')

        return self

    @logging_to_kafka('selecionando origem bacen', 'origem bacen selecionada com sucesso')
    def selecionar_origem_bacen(self):
        select = Select(self.driver.find_element(By.ID, 'links-origens-mes'))
        select.select_by_value('Servidores_BACEN')

        return self

    @logging_to_kafka('selecionando ano', 'ano selecionado com sucesso')
    def wait_for_downloads(self):
        print("Waiting for downloads", end="")
        while any([filename.endswith(".part") for filename in os.listdir(self.folder)]):
            time.sleep(2)
            print(".", end="")

        print("done!")

    @logging_to_kafka('selecionando ano', 'ano selecionado com sucesso')
    def download_file(self):
        self.driver.find_element_by_id('btn').click()
        self.wait_for_downloads()


class IngestaoAutomatica(ABC):
    def __init__(self):
        pass

    @staticmethod
    def enviar_requisicao(prefixo, pasta_ingestao, fonte, tabela_stage):
        JSON = {
            "u.pipeline": "INGESTAO_ARQUIVO_CONSULTAR_PIPELINE",
            "u.arquivo": prefixo,
            "u.arquivo.caminho": "/" + pasta_ingestao,
            "u.arquivo.fonte": fonte,
            "u.ignorar.origem": "true",
            "u.metadata.tabela.ingestao": tabela_stage,
            "u.tabela.dataset.v2": tabela_stage
        }

        print('\n')
        pprint.pprint(JSON)
        print('\n')

        resp = requests.post('http://hadoopin-gsi-prod01.mpmg.mp.br:9095/pipeline', json=JSON)
        print("Status Code: " + str(resp.status_code) + ' \nResposta: ' + str(resp.json()))

    def enviar_arquivos_ingestao(self, folder, arquivo, scp_folder):
        # subprocess.call(['scp', folder + '/*.csv', "ufmg.iquerino@hadoopin-gsi-prod01.mpmg.mp.br:~/../../" + scp_folder])
        # os.system(
        #     "scp " + folder + os.sep + arquivo + " ufmg.iquerino@hadoopin-gsi-prod01.mpmg.mp.br:~/../../" + scp_folder)
        # return self

        os.system('cp ' + folder + '/' + arquivo + ' ' + scp_folder)


@app.get("/")
def read_root():
    return {"Coletor": "crawler_servidor",
            "Fonte": "Portal Transparencia",
            "Base": "Servidores",
            "versão": "0.1.1",
            "atualização": "08/08/2022"}

@app.post("/servidores")
async def run_coletor(request: Request):
    uuid_unico = uuid.uuid4()
    parametros = await request.json()
    parametros['uuid'] = str(uuid_unico)
    log_input['uuid'] = str(uuid_unico)

    def valida_dados_disponiveis():
        
        return ((SeleniumFirefox("valida siape", uuid_unico).selecionar_ano().selecionar_mes().selecionar_origem_siape() is not None)\
        and (SeleniumFirefox("valida bacen", uuid_unico).selecionar_ano().selecionar_mes().selecionar_origem_bacen() is not None))
    
    def valida_existencia_tabela(data_ref):      
                
        nome_tabela = "portal_transparencia_servidor_cadastro_" + data_ref
        query = "show tables in dataset_v2 like '" + nome_tabela + "'" 
        
        return Connector().execute_query(query) 

    def run_coletor_siape():
        SeleniumFirefox("download", uuid_unico).selecionar_ano().selecionar_mes().selecionar_origem_siape().download_file()

        os.system("unzip download/*.zip -d datasets")

        files = os.listdir('datasets')
        data_ref = ''

        for file in files:
            name = file.split('_')[-1].split('.')[0]
            if 'CADASTRO' in name.upper():
                data_ref = file.split('_')[0]

                novo_nome_arquivo = 'SERVIDOR_CADASTRO_SIAPE' + data_ref + '.csv'
                os.system('mv datasets/' + file + ' datasets/' + novo_nome_arquivo)

                remove_files = os.listdir('datasets')
                remove_files.remove(novo_nome_arquivo)

                for l in remove_files:
                    os.system('rm datasets/' + l)

        return data_ref

    def run_coletor_bacen():
        os.system('rm download/*.zip')

        SeleniumFirefox("download", uuid_unico).selecionar_ano().selecionar_mes().selecionar_origem_bacen().download_file()

        os.system("unzip download/*.zip -d datasets")

        files = os.listdir('datasets')

        for file in files:
            name = file.split('_')[-1].split('.')[0]
            if 'CADASTRO' in name.upper():
                data_ref = file.split('_')[0]

                novo_nome_arquivo = 'SERVIDOR_CADASTRO_BACEN' + data_ref + '.csv'

                os.system('mv datasets/' + file + ' datasets/' + novo_nome_arquivo)

                remove_files = os.listdir('datasets')
                remove_files.remove(novo_nome_arquivo)
                remove_files.remove('SERVIDOR_CADASTRO_SIAPE' + data_ref + '.csv')

                for l in remove_files:
                    os.system('rm datasets/' + l)

    comand1 = 'head -n 1 datasets/SERVIDOR_CADASTRO_SIAPE*.csv > datasets/SERVIDOR_CADASTRO.out'
    comand2 = 'tail -n+2 -q datasets/*.csv >> datasets/SERVIDOR_CADASTRO.out'
    comand3 = 'mv datasets/SERVIDOR_CADASTRO.out datasets/SERVIDOR_CADASTRO.csv'

    os.system('rm -rf download/')
    os.system('rm -rf datasets/')


    os.system('mkdir -p download')
    os.system('mkdir -p datasets')
    
    if valida_dados_disponiveis():
        data_ref = run_coletor_siape()
        run_coletor_bacen()
        
        if valida_existencia_tabela(data_ref) is None:
            
            # Une os dois .csv
            os.system(comand1)
            os.system(comand2)

            # Altera a extensão de .out para .csv
            os.system(comand3)

            file = 'SERVIDOR_CADASTRO.csv'
            os.system('mkdir -p /datalake/portal_transparencia/')
            pasta_ingestao = '/datalake/portal_transparencia'

            novo_nome_arquivo = 'SERVIDOR_CADASTRO_' + data_ref + '.csv'
            os.system('mv datasets/' + file + ' datasets/' + novo_nome_arquivo)

            IngestaoAutomatica().enviar_arquivos_ingestao('datasets', novo_nome_arquivo, pasta_ingestao)

            name_table = "portal_transparencia_" + novo_nome_arquivo.split('.')[0].lower()
            IngestaoAutomatica().enviar_requisicao(novo_nome_arquivo, pasta_ingestao, os.getenv('FONTE'), name_table)
        else:
            print(valida_existencia_tabela(data_ref))
    else:
        print("dados nao disponiveis")

        
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=80, log_level="debug")
