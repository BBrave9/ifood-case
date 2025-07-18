# 🚕 Desafio Técnico: Analytics Engineer — NYC Taxi Trip Data

## 📌 Visão Geral do Projeto
Este repositório apresenta a solução desenvolvida para o desafio técnico de Engenheiro(a) de Dados Pleno, com foco na ingestão, transformação e análise de dados de corridas de táxis da cidade de Nova York.

A proposta demonstra habilidades em:
- Engenharia de Dados/Software
- Análise e Modelagem de Dados
- Construção de um pipeline robusto desde os dados brutos até sua disponibilização para consumo e geração de insights

## 🎯 Objetivo do Desafio
O desafio consistiu em:
- Ingerir dados de corridas de táxis de NYC em um Data Lake
- Disponibilizar os dados para consumo via SQL
- Realizar análises específicas

🗓 Os dados utilizados referem-se às corridas de Yellow Taxis, Green Taxis, FHV e HVFHV entre janeiro e maio de 2023, obtidos do [site oficial da TLC](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

## 🏗 Arquitetura da Solução
A arquitetura adotada segue o padrão *Medalhão* (Bronze/Bronze, Silver/Curated) para garantir qualidade e governança dos dados.

- **Armazenamento**: Amazon S3 como Data Lake principal, com buckets separados para as camadas *bronze* e *silver*
- **Processamento**: Databricks Community Edition com PySpark
- **Formato dos dados**: Delta Lake (transações ACID, schema enforcement/evolution, time travel)
- **Metadados**: Hive Metastore integrado ao Databricks
- **Consumo**: SQL via Databricks SQL Endpoints e PySpark para análises avançadas

## 🛠 Tecnologias Utilizadas
- **Linguagens**: Python (PySpark), SQL  
- **Plataforma de Dados**: Databricks Community Edition  
- **Armazenamento em Nuvem**: Amazon S3  
- **Formato de Dados**: Delta Lake  
- **Versionamento de Código**: Git  

## ⚙️ Configuração e Execução — Passos de Alto Nível

1. **Configuração AWS S3**
   - Crie dois buckets na mesma região:
     - `ifood-case-nyc-taxi-data-lake-raw`
     - `ifood-case-nyc-taxi-data-lake-silver`
   - Habilite o bloqueio de acesso público e o versionamento

2. **Download e Upload dos Dados**
   - Baixe arquivos PARQUET dos tipos: Yellow, Green, FHV e HVFHV (Jan-Mai/2023)
   - Estruture no S3 conforme:  
     `nyctlc/<trip_type>/parquet/year=<yyyy>/month=<mm>/`

3. **Clonagem do Repositório no Databricks**
   - `Repos → Add Repo → Git URL` do seu fork
   - Caminho final: `Workspace/Repos/<usuário>/ifood-case`

4. **Ajustes para Execução**
   - No arquivo `src/config.py`, defina o nome do database na linha 16:
     <img width="626" height="60" alt="image" src="https://github.com/user-attachments/assets/00c425d9-da5b-4c74-841c-1c38b2172cd5" />

   - No notebook `Driver`, insira o caminho do repositório em `repo_path`:
     <img width="886" height="136" alt="image" src="https://github.com/user-attachments/assets/dd4317b7-44af-440d-bbd2-69152fe1ebf2" />

5. **Pipeline de Ingestão**
   - Execute o notebook `Driver` para:
     - Transformar dados da camada Bronze em Silver
     - Criar tabelas Delta no Hive Metastore

6. **Execução das Análises**
   - Navegue até `analysis/` no Repos
   - Execute:
     - `q1_avg_monthly_total_amount.sql`
     - `q2_avg_passengers_per_hour_may.sql`

## 📊 Análises Realizadas
As queries respondem às seguintes perguntas:

1. **Média de valor total mensal (`total_amount`)** das corridas realizadas por yellow taxis  
2. **Média de passageiros (`passenger_count`) por hora do dia** no mês de maio considerando todas as corridas

---

🚀 Para dúvidas, contribuições ou sugestões, fique à vontade para abrir uma issue ou pull request!
