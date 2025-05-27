import yfinance as yf
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from io import BytesIO

# Config MinIO
MINIO_ENDPOINT = "localhost:9000"
MINIO_USER = "admin"
MINIO_PASS = "admin123"
BUCKET = "raw"

# Conexão MinIO
client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_USER,
    secret_key=MINIO_PASS,
    secure=False
)

# Lista de ativos
tickers = ["PETR4.SA", "VALE3.SA", "ITUB4.SA"]
period = "5y"  # Últimos 5 anos

for ticker in tickers:
    print(f"Baixando {ticker}")
    df = yf.download(ticker, period=period, interval="1d")
    df.reset_index(inplace=True)
    
    # Salva em Parquet em memória
    buf = BytesIO()
    table = pa.Table.from_pandas(df)
    pq.write_table(table, buf)
    buf.seek(0)
    
    # Nome do arquivo no bucket
    filename = f"{ticker.replace('.','_')}_historico.parquet"
    
    # Envia para MinIO
    client.put_object(
        bucket_name=BUCKET,
        object_name=filename,
        data=buf,
        length=buf.getbuffer().nbytes,
        content_type='application/octet-stream'
    )
    print(f"{ticker} salvo em {BUCKET}/{filename}")

