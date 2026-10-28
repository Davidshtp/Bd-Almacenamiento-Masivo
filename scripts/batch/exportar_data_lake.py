import os
import pymongo
import pandas as pd
import json
import boto3
from bson import ObjectId, json_util
from io import BytesIO
from dotenv import load_dotenv

# 1. Cargar variables de entorno
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
AWS_KEY = (os.getenv("S3_BUCKET_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY_ID") or "").strip()
AWS_SECRET = (os.getenv("S3_BUCKET_SECRET_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY") or "").strip()
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-2")
BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "abp-data-lake-heavycult")

def sanitizar_documento(doc):
    """Convierte BSON (ObjectId, dicts/listas anidadas) a texto homogéneo."""
    clean_doc = {}
    for k, v in doc.items():
        if isinstance(v, ObjectId):
            clean_doc[k] = str(v)
        elif isinstance(v, (dict, list)):
            clean_doc[k] = json.dumps(v, default=str)
        else:
            clean_doc[k] = v
    return clean_doc

def subir_a_s3(s3_client, ruta_s3, dataframe, docs_json):
    """Subes un archivo Parquet y opcionalmente un JSON a la ruta S3 indicada."""
    # 1. Subir formato Parquet (Optimizado para Big Data)
    buf_parquet = BytesIO()
    dataframe.to_parquet(buf_parquet, engine="pyarrow", index=False)
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=f"{ruta_s3}.parquet",
        Body=buf_parquet.getvalue()
    )

    # 2. Subir formato JSON (Formato nativo NoSQL)
    buf_json = json_util.dumps(docs_json, indent=4, ensure_ascii=False).encode("utf-8")
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=f"{ruta_s3}.json",
        Body=buf_json
    )

def exportar_capa_raw_s3(db, s3_client):
    print("☁️ [RAW] Exportando colecciones OLTP (customers, products, orders) a S3...")

    # Clientes
    clientes_docs = list(db.customers.find())
    if clientes_docs:
        df = pd.DataFrame([sanitizar_documento(d) for d in clientes_docs])
        subir_a_s3(s3_client, "raw/customers_raw", df, clientes_docs)
        print("   ✅ raw/customers_raw (.parquet y .json)")

    # Productos
    productos_docs = list(db.products.find())
    if productos_docs:
        df = pd.DataFrame([sanitizar_documento(d) for d in productos_docs])
        subir_a_s3(s3_client, "raw/products_raw", df, productos_docs)
        print("   ✅ raw/products_raw (.parquet y .json)")

    # Órdenes
    ordenes_docs = list(db.orders.find())
    if ordenes_docs:
        df = pd.DataFrame([sanitizar_documento(d) for d in ordenes_docs])
        subir_a_s3(s3_client, "raw/orders_raw", df, ordenes_docs)
        print("   ✅ raw/orders_raw (.parquet y .json)")

def exportar_capas_processed_y_curated_s3(db, s3_client):
    print("\n☁️ [ANALITICA] Exportando colección OLAP a PROCESSED y CURATED en S3...")

    ventas_olap_docs = list(db.ventas_analiticas.find())
    if not ventas_olap_docs:
        print("   ⚠️ No se encontraron datos en la colección 'ventas_analiticas'.")
        return

    df_olap = pd.DataFrame([sanitizar_documento(d) for d in ventas_olap_docs])

    # Capa PROCESSED (Datos limpios y desnormalizados)
    subir_a_s3(s3_client, "processed/ventas_procesadas", df_olap, ventas_olap_docs)
    print("   ✅ processed/ventas_procesadas (.parquet y .json)")

    # Capa CURATED (Data Mart / Gold Layer para analítica final)
    subir_a_s3(s3_client, "curated/fact_ventas_olap", df_olap, ventas_olap_docs)
    print("   ✅ curated/fact_ventas_olap (.parquet y .json)")

def ejecutar_exportacion_s3():
    print("==================================================")
    print("🚀 HEAVYCULT - DATA LAKE CLOUD (AWS S3 MULTI-CAPA)")
    print("==================================================\n")

    if not MONGO_URI or not AWS_KEY or not AWS_SECRET:
        raise ValueError("❌ Revisa las credenciales de tu archivo .env (MONGO_URI, AWS_KEY, AWS_SECRET)")

    # Inicializar cliente de S3
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        region_name=AWS_REGION
    )

    print("🔌 Conectando a MongoDB Atlas...")
    client = pymongo.MongoClient(MONGO_URI)
    db = client["heavycult"]

    # Ejecutar exportaciones a S3
    exportar_capa_raw_s3(db, s3_client)
    exportar_capas_processed_y_curated_s3(db, s3_client)

    print("\n==================================================")
    print("🎉 ¡Todas las capas del Data Lake (RAW, PROCESSED, CURATED) fueron pobladas en S3!")
    print(f"📍 Bucket: s3://{BUCKET_NAME}/")
    print("==================================================")
    client.close()

if __name__ == "__main__":
    ejecutar_exportacion_s3()