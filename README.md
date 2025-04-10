# Painel Curupira

Esse painel tem o objetivo de apresentar os focos de queimadas no Brasil utilizando os dados do [Programa de Queimadas do INPE (Instituto Nacional de Pesquisas Espaciais)](https://terrabrasilis.dpi.inpe.br/queimadas/portal/dados-abertos/).

## Arquitetura

Foi desenvolvido um pipeline de Ingestão, Processamento e Carregamento dos dados. Para isso, foram utilizados containers Docker de Airflow, MinIO, Spark, PostGIS e Streamlit.

![Arquitetura](https://github.com/julianabfreitas/brazil-wildfires/blob/main/images/arquitetura.png)

## Modelagem

Os dados foram modelados em esquema estrela para facilitar o consumo da aplicação.

```mermaid
erDiagram
	dm_data }o--|| ft_queimada : references
	dm_municipio }o--|| ft_queimada : references
	dm_bioma }o--|| ft_queimada : references

	ft_queimada {
		INTEGER id_queimada
		INTEGER id_data
		INTEGER id_municipio
		INTEGER id_bioma
		VARCHAR(255) des_satelite
		INTEGER qtd_dias_sem_chuva
		DECIMAL vl_precipitacao
		DECIMAL vl_risco_fogo
		DECIMAL vl_frp
		POINT geom_foco
	}

	dm_data {
		INTEGER id_data
		INTEGER qtd_dia
		INTEGER qtd_mes
		INTEGER qtd_ano
		DATE dt_data
	}

	dm_municipio {
		INTEGER id_municipio
		VARCHAR(255) des_nome
		VARCHAR(2) des_sigla
		DECIMAL vl_area
		POLYGON geom_municipio
	}

	dm_bioma {
		INTEGER id_bioma
		INTEGER vl_codigo
		VARCHAR(255) des_nome
		POLYGON geom_bioma
	}
```

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
![Airflow](https://github.com/julianabfreitas/brazil-wildfires/blob/main/images/airflow-print.jpg)

### 5) Acessar [localhost:8501](http://localhost:8501/)

#### Aba de Boas Vindas
![Boas Vindas](https://github.com/julianabfreitas/brazil-wildfires/blob/main/images/boas-vindas.jpg)

#### Aba de Visão Geral
![Visão Geral](https://github.com/julianabfreitas/brazil-wildfires/blob/main/images/visao-geral.jpg)

#### Aba de Gráficos
![Visão Geral](https://github.com/julianabfreitas/brazil-wildfires/blob/main/images/graficos.jpg)

