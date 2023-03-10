import urllib3
import uuid

from utils.logs import logging_to_kafka
from utils.mensagens import Mensagens


UUID = uuid.uuid4()

class AuxilioBrasil():
    def __init__(self, path, ano, mes):
        self.path = path
        self.name_file = f'{ano}{mes}_AuxilioBrasil.zip'
        self.ano = ano
        self.mes = mes

    @logging_to_kafka(Mensagens.msg_download_inicio.format('AUXILIO BRASIL'), \
                        Mensagens.msg_download_fim.format('AUXILIO BRASIL'), UUID)
    def descargar_arquivos(self) -> None:
        http = urllib3.PoolManager(cert_reqs='CERT_NONE')

        URL_BASE = 'https://www.portaltransparencia.gov.br/download-de-dados/auxilio-brasil/'

        url = f'{URL_BASE}{self.ano}{self.mes}'

        r = http.request('GET', url,preload_content=False)

        if r.status != 404:
            with open(self.path+self.name_file,'wb+') as file_aux:
                file_aux.write(r.data)

    def get_complete_path_file(self) -> str:
        return self.path + self.name_file
