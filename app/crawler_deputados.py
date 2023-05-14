# Importar as bibliotecas necessárias
from fastapi import FastAPI
from fastapi import HTTPException
from pydantic import BaseModel
from typing import Optional
import os
import zipfile
import wget



class CrawlerDeputados():
    
    def __init__(self):
        pass
    
    def download(self, ano: Optional[int], database: str, formato: str):
        # Verificar se a base de dados é válida
        if database not in ["cotas", "proposicoes","proposicoes","proposicoesTemas","proposicoesAutores","eventos","eventosOrgaos","eventosPresencaDeputados","eventosRequerimentos","votacoes","votacoesOrientacoes","votacoesVotos","votacoesObjetos","votacoesProposicoes","licitacoes","licitacoesContratos","licitacoesItens","licitacoesPedidos","licitacoesPropostas","frentes","frentesDeputados","deputados","deputadosOcupacoes","deputadosProfissoes"]:
            raise HTTPException(status_code=400, detail="Base de dados inválida")
        
        if formato not in ["csv", "xlsx","ods","json","xml"]:
            raise HTTPException(status_code=400, detail="Formato de dados inválida")        

        # Montar a URL para baixar o arquivo
        if database == "cotas":
            if not ano:
                raise HTTPException(status_code=400, detail="O ano é obrigatório para esta base de dados")
            url = f"http://www.camara.leg.br/{database}/Ano-{ano}.{formato}.zip"
            filename = url.split("/")[-1]
            filepath = os.path.join("C:/Users/Documents/datalake", filename)  #inserir informação da pasta que deseja carregar as tabelas
            if not os.path.exists(filepath):
                file_zip = wget.download(url)
                if not file_zip:
                    raise HTTPException(status_code=400, detail="Falha ao baixar o arquivo")
                with zipfile.ZipFile(file_zip) as z:
                    z.extractall("C:/Users/Documents/datalake") #inserir informação da pasta que deseja carregar as tabelas
                os.remove(file_zip)        
        elif database in ["frentes","frentesDeputados","deputados","deputadosOcupacoes","deputadosProfissoes"]:
            if ano:
                raise HTTPException(status_code=400, detail="Esta base não aceita ano")
            url = f"http://dadosabertos.camara.leg.br/arquivos/{database}/{formato}/{database}.{formato}"
            filename = f"{database}.{formato}"
            filepath = os.path.join("C:/Users/Documents/datalake", filename)  #inserir informação da pasta que deseja carregar as tabelas
            if os.path.exists(filepath):
                raise HTTPException(status_code=400, detail="Arquivo já existe no datalake")     
            if not os.path.exists(filepath):
                wget.download(url, out="C:/Users/Documents/datalake")    #inserir informação da pasta que deseja carregar as tabelas
        else:
            if not ano:
                raise HTTPException(status_code=400, detail="O ano é obrigatório para esta base de dados")
            url = f"http://dadosabertos.camara.leg.br/arquivos/{database}/{formato}/{database}-{ano}.{formato}"
            filename = f"{database}-{ano}.{formato}"
            filepath = os.path.join("C:/Users/Documents/datalake", filename)  #inserir informação da pasta que deseja carregar as tabelas
            if os.path.exists(filepath):
                raise HTTPException(status_code=400, detail="Arquivo já existe no datalake")
            if not os.path.exists(filepath):
                wget.download(url, out="C:/Users/Documents/datalake")  #inserir informação da pasta que deseja carregar as tabelas




        # Retornar a mensagem de sucesso
        return {"message": f"Arquivo baixado com sucesso: datalake/{filename}"}
