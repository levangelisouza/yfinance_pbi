import pandas as pd
from minio import Minio
from io import BytesIO
import pyarrow.parquet as pq
import pyarrow as pa
import re

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

objects = list(client.list_objects(GOLD_BUCKET, recursive=True))
dfs = []

for obj in objects:
    response = client.get_object(GOLD_BUCKET, obj.object_name)
    parquet_bytes = response.read()
    buf = BytesIO(parquet_bytes)
    table = pq.read_table(buf)
    df = table.to_pandas()

    # Se MultiIndex, pega só o nome base
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0] if col[0] else col[1] for col in df.columns]

    # Limpa nomes das colunas (remove tudo que não é letra, número ou _)
    def limpa_nome(nome):
        nome = re.sub(r'[^a-zA-Z0-9_]', '', nome)
        nome = nome[:15]  # Limita a 15 caracteres (ajuste se quiser menos)
        return nome

    df.columns = [limpa_nome(str(c)) for c in df.columns]

    ticker = obj.object_name.split('_')[0].replace('.', '').upper()
    df['Ticker'] = ticker[:8]  # Limita Ticker a 8 caracteres

    dfs.append(df)

# Concatena
df_final = pd.concat(dfs, ignore_index=True)

# Mantém só as colunas curtas e essenciais
colunas_desejadas = [
    'Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'AdjClose',
    'Volume', 'MM7', 'MM21', 'Volatilidade21', 'Var_pct_dia'
]
colunas_final = [c for c in colunas_desejadas if c in df_final.columns]
df_final = df_final[colunas_final]

# Salva Parquet em memória
buf_out = BytesIO()
table_out = pa.Table.from_pandas(df_final)
pq.write_table(table_out, buf_out)
buf_out.seek(0)

nome_final = "acoes_b3_gold_unificado.parquet"

client.put_object(
    bucket_name=GOLD_BUCKET,
    object_name=nome_final,
    data=buf_out,
    length=buf_out.getbuffer().nbytes,
    content_type='application/octet-stream'
)

print(f"Arquivo FINAL, com nomes curtos, salvo no bucket {GOLD_BUCKET} como {nome_final}")

