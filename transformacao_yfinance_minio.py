import pandas as pd
from minio import Minio
from io import BytesIO
import pyarrow as pa
import pyarrow.parquet as pq
import os

# Config MinIO
MINIO_ENDPOINT = "localhost:9000"
MINIO_USER = "admin"
MINIO_PASS = "admin123"
RAW_BUCKET = "raw"
GOLD_BUCKET = "gold"

# Conexão MinIO
client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_USER,
    secret_key=MINIO_PASS,
    secure=False
)

# Lista dos arquivos no bucket raw
objects = client.list_objects(RAW_BUCKET, recursive=True)

for obj in objects:
    # Download do arquivo parquet para memória
    response = client.get_object(RAW_BUCKET, obj.object_name)
    parquet_bytes = response.read()
    buf = BytesIO(parquet_bytes)
    table = pq.read_table(buf)
    df = table.to_pandas()

    # Garantir que exista coluna 'Date' como datetime
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"])
        df = df.sort_values("Date")
    else:
        print(f"Coluna Date não encontrada em {obj.object_name}")
        continue

    # Calcular indicadores
    df["MM7"] = df["Close"].rolling(window=7).mean()
    df["MM21"] = df["Close"].rolling(window=21).mean()
    df["Volatilidade21"] = df["Close"].rolling(window=21).std()
    df["Var_pct_dia"] = df["Close"].pct_change()

    # Salvar Parquet em memória
    buf_out = BytesIO()
    table_out = pa.Table.from_pandas(df)
    pq.write_table(table_out, buf_out)
    buf_out.seek(0)

    # Nome do arquivo para a camada gold
    base_name = os.path.splitext(obj.object_name)[0]
    gold_name = base_name + "_gold.parquet"

    # Upload para o bucket gold
    client.put_object(
        bucket_name=GOLD_BUCKET,
        object_name=gold_name,
        data=buf_out,
        length=buf_out.getbuffer().nbytes,
        content_type='application/octet-stream'
    )
    print(f"{gold_name} salvo em {GOLD_BUCKET}")

print("Processamento finalizado.")

