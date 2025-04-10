# Painel Curupira

Esse painel tem o objetivo de apresentar os focos de queimadas no Brasil utilizando os dados do [Programa de Queimadas do INPE (Instituto Nacional de Pesquisas Espaciais)](https://terrabrasilis.dpi.inpe.br/queimadas/portal/dados-abertos/).

## Arquitetura

Foi desenvolvido um pipeline de Ingestão, Processamento e Carregamento dos dados. Para isso, foram utilizados containers Docker.

![Arquitetura](https://github.com/julianabfreitas/brazil-wildfires/blob/main/images/arquitetura.png)

## Como utilizar o projeto

### 1) Criar a rede utilizada nos containers dockers

```
bash create-network.sh
```

### 2) Subir os containers

```
docker-compose -f docker-airflow/docker-compose.yml -f docker-minio/docker-compose.yml -f docker-postgis/docker-compose.yml -f docker-spark/docker-compose.yml -f docker-streamlit/docker-compose.yml up --build
```

### 4) Executar os pipelines dos dados no airflow

### 5) Acessar [localhost:8501](http://localhost:8501/)

#### Aba de Boas Vindas
![Boas Vindas](https://github.com/julianabfreitas/brazil-wildfires/blob/main/images/boas-vindas.jpg)

#### Aba de Visão Geral
![Visão Geral](https://github.com/julianabfreitas/brazil-wildfires/blob/main/images/visao-geral.jpg)

#### Aba de Gráficos
![Visão Geral](https://github.com/julianabfreitas/brazil-wildfires/blob/main/images/graficos.jpg)

