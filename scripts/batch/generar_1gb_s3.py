import os
import pymongo
import pandas as pd
import json
import boto3
import numpy as np
import uuid
from bson import ObjectId, json_util
from io import BytesIO
from datetime import datetime, timedelta
from dotenv import load_dotenv

# 1. Cargar variables de entorno
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
AWS_KEY = (os.getenv("S3_BUCKET_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY_ID") or "").strip()
AWS_SECRET = (os.getenv("S3_BUCKET_SECRET_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY") or "").strip()
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-2")
BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "abp-data-lake-heavycult")

def sanitizar_documento(doc):
    clean_doc = {}
    for k, v in doc.items():
        if isinstance(v, ObjectId):
            clean_doc[k] = str(v)
        elif isinstance(v, (dict, list)):
            clean_doc[k] = json.dumps(v, default=str)
        else:
            clean_doc[k] = v
    return clean_doc

def generar_dataset_masivo(df_semilla, objetivo_filas=3000000):
    """
    Multiplica el dataframe semilla hasta alcanzar N filas agregando variabilidad
    en UUIDs, fechas y valores para evitar duplicados idénticos.
    """
    print(f"🔄 Escalando dataset desde {len(df_semilla):,} muestras reales hasta {objetivo_filas:,} filas...")
    
    multiplicador = (objetivo_filas // len(df_semilla)) + 1
    dfs = []
    
    fecha_inicio = datetime(2024, 1, 1)
    
    for i in range(multiplicador):
        df_temp = df_semilla.copy()
        
        # Inyectar variación en campos clave
        if "order_number" in df_temp.columns:
            df_temp["order_number"] = [f"ORD-{uuid.uuid4().hex[:8].upper()}" for _ in range(len(df_temp))]
            
        if "fecha_venta" in df_temp.columns:
            # Variar fechas aleatoriamente a lo largo de 2 años
            dias_rand = np.random.randint(0, 730, size=len(df_temp))
            df_temp["fecha_venta"] = [fecha_inicio + timedelta(days=int(d)) for d in dias_rand]
            
        dfs.append(df_temp)
        
    df_masivo = pd.concat(dfs, ignore_index=True).iloc[:objetivo_filas]
    return df_masivo

def ejecutar_poblamiento_masivo_s3():
    print("==================================================")
    print("🚀 GENERADOR MASIVO DE DATOS PARA S3 (PRUEBA 1GB+)")
    print("==================================================\n")

    if not MONGO_URI or not AWS_KEY or not AWS_SECRET:
        raise ValueError("❌ Revisa las credenciales de tu archivo .env")

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        region_name=AWS_REGION
    )

    print("🔌 Conectando a MongoDB Atlas para extraer patrones base...")
    client = pymongo.MongoClient(MONGO_URI)
    db = client["heavycult"]

    # 1. Extraer dataset real como semilla
    ventas_docs = list(db.ventas_analiticas.find({}, {"_id": 0}))
    if not ventas_docs:
        raise ValueError("No se encontraron registros en 'ventas_analiticas'")

    df_base = pd.DataFrame([sanitizar_documento(d) for d in ventas_docs])
    print(f"📊 Registros base obtenidos de Atlas: {len(df_base):,}")

    # 2. Escalar a 3,000,000 de registros (aprox 1.2 GB en JSON uncompressed / 250 MB Parquet comprimido)
    df_3millones = generar_dataset_masivo(df_base, objetivo_filas=3000000)

    # --- SUBIR A CAPA RAW (JSON Pesado + Parquet) ---
    print("\n☁️ [1/3] Subiendo Capa RAW masiva a S3 (raw/fact_ventas_masivo_1gb.json)...")
    
    # Exportar JSON masivo que superará el 1 GB en S3
    buf_json = BytesIO()
    df_3millones.to_json(buf_json, orient="records", lines=True)
    size_json_gb = len(buf_json.getvalue()) / (1024 ** 3)
    
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key="raw/fact_ventas_masivo_1gb.json",
        Body=buf_json.getvalue()
    )
    print(f"   ✅ raw/fact_ventas_masivo_1gb.json subido con éxito ({size_json_gb:.2f} GB)")

    # Parquet de la misma capa RAW
    buf_parquet = BytesIO()
    df_3millones.to_parquet(buf_parquet, engine="pyarrow", index=False)
    size_parquet_mb = len(buf_parquet.getvalue()) / (1024 ** 2)
    
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key="raw/fact_ventas_masivo.parquet",
        Body=buf_parquet.getvalue()
    )
    print(f"   ✅ raw/fact_ventas_masivo.parquet subido con éxito ({size_parquet_mb:.2f} MB - Compresión Parquet)")

    # --- SUBIR A CAPAS PROCESSED Y CURATED ---
    print("\n☁️ [2/3] Generando agregaciones para PROCESSED...")
    df_processed = df_3millones.groupby("metodo_pago").agg(
        total_transacciones=("anio", "count"),
        venta_total_cop=("total_venta", "sum"),
        ganancia_total_cop=("ganancia_neta", "sum"),
        ticket_promedio=("total_venta", "mean")
    ).reset_index()

    buf_proc = BytesIO()
    df_processed.to_parquet(buf_proc, engine="pyarrow", index=False)
    s3_client.put_object(Bucket=BUCKET_NAME, Key="processed/metodos_pago_masivo.parquet", Body=buf_proc.getvalue())
    print("   ✅ processed/metodos_pago_masivo.parquet subido.")

    print("\n☁️ [3/3] Generando dataset para CURATED...")
    buf_curated = BytesIO()
    df_3millones.to_parquet(buf_curated, engine="pyarrow", index=False)
    s3_client.put_object(Bucket=BUCKET_NAME, Key="curated/fact_ventas_curated.parquet", Body=buf_curated.getvalue())
    print("   ✅ curated/fact_ventas_curated.parquet subido.")

    print("\n==================================================")
    print("🎉 ¡PRUEBA DE VOLUMEN COMPLETADA EN AMAZON S3!")
    print(f"📍 Volumen subido en RAW: {size_json_gb:.2f} GB (JSON) / {size_parquet_mb:.2f} MB (Parquet)")
    print(f"📍 Bucket: s3://{BUCKET_NAME}/")
    print("==================================================")
    client.close()

if __name__ == "__main__":
    ejecutar_poblamiento_masivo_s3()