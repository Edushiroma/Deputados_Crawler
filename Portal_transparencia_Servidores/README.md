# Coletor de dados dos servidores do executivo (BACEN e SIAPE)

API para realizar a coleta dos dados de servidores (https://www.portaltransparencia.gov.br/download-de-dados/servidores)

Coleta os dados de servidores do portal transparencia todos os dias.

## Comandos Docker:
### Docker build:
    sudo docker build . -t servidores_executivo:2.0.0

### Docker run:
    sudo docker run -d -p 8787:80 -e KAFKA_BROKER='hadoopdn-gsi-prod04.mpmg.mp.br:6667,hadoopdn-gsi-prod05.mpmg.mp.br:6667,hadoopdn-gsi-prod06.mpmg.mp.br:6667,hadoopdn-gsi-prod07.mpmg.mp.br:6667,hadoopdn-gsi-prod08.mpmg.mp.br:6667,hadoopdn-gsi-prod09.mpmg.mp.br:6667,hadoopdn-gsi-prod10.mpmg.mp.br:6667' -e KAFKA_IN_TOPIC='ufmg_g01_2021_crawler_ingestao_dev' -e KAFKA_LOG_TOPIC='ufmg_g01_2021_crawler_log_dev' --restart unless-stopped -e DATALAKE_PATH='/datalake' -e FONTE='portal_transparencia' -e BASE='servidores' -e CATEGORIA='coletor' -e NOME_COLETOR='portal_transparencia_servidores' -v /datalake:/datalake --name servidores_executivo servidores_executivo:2.0.0

## Endpoints:
### /servidores

* Tipo de requisição: POST

## Exemplos de requisição:
### Requisitar coleta para a data corrente:
    http://localhost:8787/servidores
