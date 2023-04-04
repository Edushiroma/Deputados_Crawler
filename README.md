# API Crawler Deputados
Este código é um exemplo de um crawler desenvolvido em Python para disponibilizar uma API RESTful para download de dados da Câmara dos Deputados do Brasil.
https://dadosabertos.camara.leg.br/swagger/api.html

## Instalação:
Para utilizar esse código, é necessário a instalação das bibliotecas destacadas no arquivo app/requirements.txt instaladas.

## Uso:
1) Clone o código para sua máquina local.
2) Execute o script **deputados.py**
3) Faça a requisição POST para o download dos arquivos, abaixo exemplos de requisições.

# API

A API disponibiliza um `endpoint /download` que aceita uma requisição POST com os seguintes parâmetros:

ano: ano do arquivo que deve ser baixado (opcional)
database: nome da base de dados que deve ser baixada (obrigatório)
formato: formato do arquivo que deve ser baixado (obrigatório)

Exemplo de requisição:

##### Json
```
{
  "ano": 2022,
  "database": "cotas",
  "formato": "csv"
}
```

### Desenvolvedor
Autor: Eduardo Shiroma

Link: https://www.linkedin.com/in/eduardo-shiroma-44b82ab3/

Código desenvolvido para Trabalho de Conclusão de Curso II
