# Importar as bibliotecas necessárias
from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
from typing import Optional
import wget
from crawler_deputados import CrawlerDeputados



# Definir a classe do modelo para o JSON de entrada
class RequestParams(BaseModel):
    ano: Optional[int]
    database: str
    formato: str

# Definir o app do FastAPI
app = FastAPI()

@app.get("/")
def read_root():
    return {
            "Coletor": "crawler_deputados",
            "Fonte": "Camara legislativa do Brasil",
            "Base": "deputado",
            "versão": "1.0.0",
            "atualização": "03/04/2023"
            }

# Instanciar o crawler
crawler = CrawlerDeputados()

# Definir o endpoint da API
@app.post("/download")
async def download(request_params: RequestParams):
    ano = request_params.ano
    database = request_params.database
    formato = request_params.formato
    
    # Executar o crawler
    result = crawler.download(ano, database, formato)
    
    # Retornar a mensagem de sucesso
    return result


# Executar o app com o Uvicorn
if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8000)
