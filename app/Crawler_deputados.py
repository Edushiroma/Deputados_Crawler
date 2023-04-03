# Importar as bibliotecas necessárias
from fastapi import FastAPI
from fastapi import HTTPException
from pydantic import BaseModel
from typing import Optional
import os
import zipfile
import uvicorn
import wget


class CrawlerDeputados():

    def __init__(self):
        pass
    
    def download_csv(self, year: int, database: str, formato: str):
        # Verificar se a base de dados é válida
        if database not in ["deputado", "cotas"]:
            raise HTTPException(status_code=400, detail="Base de dados inválida")

        # Montar a URL para baixar o arquivo
        url = f"http://www.camara.leg.br/{database}/Ano-{year}.{formato}.zip"

        # Fazer download do arquivo
        file_zip = wget.download(url)

        # Verificar se o arquivo foi baixado com sucesso
        if not file_zip:
            raise HTTPException(status_code=500, detail="Falha ao baixar o arquivo")

        # Descompactar o arquivo
        with zipfile.ZipFile(file_zip) as z:
            z.extractall("../datasets")

        # Remover o arquivo compactado
        os.remove(file_zip)

        # Retornar a mensagem de sucesso
        return {"message": f"Arquivo baixado e descompactado com sucesso: {database}/Ano-{year}.{formato}"}
    
if __name__ == "__main__":
  c = Crawler_deputados()