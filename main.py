import os
import uvicorn

from fastapi import FastAPI
from fastapi import Request
from datetime import datetime

from auxilio_brasil import AuxilioBrasil, UUID
from utils.logs import logging_to_kafka, log_input
from utils.mensagens import Mensagens
from utils.publicador import Publicador

KAFKA_IN_TOPIC = os.environ.get('KAFKA_IN_TOPIC')



app = FastAPI()

@app.get("/")
def read_root():
    return {
            "Coletor": "crawler_auxilio_brasil",
            "Fonte": "Portal da Transparência",
            "Base": "DETALHAMENTO DOS BENEFÍCIOS AO CIDADÃO",
            "versão": "1.0.0",
            "atualização": "29/11/2022"
           }


@app.post("/auxilio_brasil")
async def run_coletor(request: Request):
    """
    Função para coletar, tranformar e enviar a estrutura
    da requisição dos dados coletados do Auxilio Brasil
    """

    ano = datetime.now().strftime('%Y')
    mes = datetime.now().strftime('%m')

    parametros = await request.json()

    parametros['u.arquivo'] = f'auxilio_brasil_{datetime.now().strftime("%Y%m")}.csv'

    parametros['fonte'] = os.environ.get('FONTE')
    parametros['base'] = os.environ.get('BASE')
    parametros['nome.coletor'] = os.environ.get('NOME_COLETOR')
    parametros['nome.tabela'] = "auxilio_brasil_" + ano + mes
    parametros['u.arquivo.caminho'] = '/datalake/portal_transparencia_br/auxilio_brasil/'

    log_input['fonte'] = os.environ.get('FONTE')
    log_input['base'] = os.environ.get('BASE')
    log_input['categoria'] = 'coletor'
    log_input['nome.coletor'] = os.environ.get('NOME_COLETOR')

    publicador_kafka = Publicador(os.environ.get('KAFKA_BROKER'))
    crawler = AuxilioBrasil('/datalake/portal_transparencia_br/auxilio_brasil/')

    @logging_to_kafka(Mensagens.msg_kafka_inicio.format(KAFKA_IN_TOPIC)
                     ,Mensagens.msg_kafka_fim.format(KAFKA_IN_TOPIC)
                     ,UUID)
    def enviar_a_kafka(parametros):
        publicador_kafka.enviar(KAFKA_IN_TOPIC, parametros)

    @logging_to_kafka(Mensagens.msg_datalake_inicio.format(parametros['u.arquivo']
                                                          ,parametros['u.arquivo.caminho'])
                     ,Mensagens.msg_datalake_fim.format(parametros['u.arquivo']
                                                       ,parametros['u.arquivo.caminho'])
                     ,UUID)
    def copiar_a_datalake(parametros):
        os.system('mkdir -p ' + parametros['u.arquivo.caminho'])
        os.system('cp /datasets/* ' + parametros['u.arquivo.caminho'])

    @logging_to_kafka(Mensagens.msg_coletor_finalizando.format(parametros['nome.coletor'])
                     ,Mensagens.msg_coletor_fim.format(parametros['nome.coletor'])
                     ,UUID)
    def fechar():
        publicador_kafka.fechar()

    # Descarga o arquivo   
    crawler.descargar_arquivos()

    # Faz o tratamento do arquivo
    parametros['u.arquivo'] = crawler.get_complete_path_file()

    # Copia o arquivo tranformado para datalake
    copiar_a_datalake(parametros)

    # Envia a kafka a requisição construida
    if 'enviar' not in parametros or parametros['enviar']:
        enviar_a_kafka(parametros)

    fechar()

    print(parametros)


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, log_level="debug")
