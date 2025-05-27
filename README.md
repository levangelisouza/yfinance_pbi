# Pipeline de Análise de Ações B3 com Python, MinIO e Power BI

Este projeto realiza a ingestão, tratamento e visualização de dados históricos de ações da B3, usando Python, MinIO (S3 local) e Power BI.

## Etapas do pipeline

1. **Ingestão:** Coleta dados do Yahoo Finance via yfinance para ativos como PETR4, VALE3, ITUB4, etc.
2. **Armazenamento:** Salva dados brutos e tratados em buckets MinIO (raw e gold) no formato Parquet.
3. **Transformação:** Calcula indicadores financeiros (MM7, MM21, volatilidade, variação diária).
4. **Unificação:** Junta todos os ativos em um único arquivo limpo para análise.
5. **Dashboard:** Visualização no Power BI com gráficos de preço, médias móveis, volatilidade e KPIs.

## Como executar

- Veja os scripts Python nesta pasta.
- Suba o MinIO (vide documentação oficial ou o comando Docker sugerido).
- Execute os scripts de ingestão e transformação.
- Importe o Parquet unificado no Power BI Desktop.

## Prints do dashboard

![Dashboard exemplo](docs/dashboard_exemplo.png)

## Stack utilizada

- Python (yfinance, pandas, pyarrow, minio)
- MinIO (S3 local)
- Power BI

Desenvolvido por Léo.

