import pandas as pd
from minio import Minio
from io import BytesIO
import pyarrow.parquet as pq

# Config MinIO
MINIO_ENDPOINT = "localhost:9000"
MINIO_USER = "admin"
MINIO_PASS = "admin123"
GOLD_BUCKET = "gold"

client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_USER,
    secret_key=MINIO_PASS,
    secure=False
)

# Lista de arquivos no bucket gold
objects = client.list_objects(GOLD_BUCKET, recursive=True)

dfs = []

for obj in objects:
    # Download do arquivo parquet para memória
    response = client.get_object(GOLD_BUCKET, obj.object_name)
    parquet_bytes = response.read()
    buf = BytesIO(parquet_bytes)
    table = pq.read_table(buf)
    df = table.to_pandas()
    # Extrai o nome do ticker do arquivo
    ticker = obj.object_name.split('_')[0]
    df['Ticker'] = ticker
    dfs.append(df)

# Junta tudo em um DataFrame só
df_final = pd.concat(dfs, ignore_index=True)

# Salva o DataFrame final como Parquet local
df_final.to_parquet("acoes_b3_gold_unificado.parquet", index=False)

print("Arquivo único criado: acoes_b3_gold_unificado.parquet")

