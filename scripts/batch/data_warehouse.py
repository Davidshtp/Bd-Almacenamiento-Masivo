import os
import time
import pymongo
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

# Cargar variables de entorno desde .env
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "heavycult-dw")
GCP_DATASET_ID = os.getenv("GCP_DATASET_ID", "heavycult_dw")
CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "gcp_key.json")

def conectar_bigquery():
    if not os.path.exists(CREDENTIALS_PATH):
        raise FileNotFoundError(
            f"❌ No se encontró el archivo de credenciales GCP en: {CREDENTIALS_PATH}. "
            f"Asegúrate de haber guardado el archivo JSON de Google Cloud en la raíz de tu proyecto."
        )
    
    credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
    return bigquery.Client(credentials=credentials, project=GCP_PROJECT_ID)

def construir_data_warehouse_bigquery():
    print("==================================================")
    print("🏗️  Iniciando migración del Data Warehouse a Google BigQuery...")
    print("==================================================\n")
    
    inicio = time.time()
    
    if not MONGO_URI:
        raise ValueError("❌ Falta la variable MONGO_URI en el archivo .env")

    # 1. Conexión a MongoDB Atlas (Fuente OLTP / Staging)
    print("🔌 Conectando a MongoDB Atlas...")
    client_mongo = pymongo.MongoClient(MONGO_URI)
    db = client_mongo["heavycult"]

    # 2. Conexión a Google BigQuery
    print("☁️ Conectando a Google BigQuery...")
    bq_client = conectar_bigquery()

    # -------------------------------------------------------------------------
    # 1. DIMENSIÓN TIEMPO
    # -------------------------------------------------------------------------
    print("\n⏳ Construyendo 'dim_tiempo'...")
    pipeline_tiempo = [
        {
            "$group": {
                "_id": {
                    "fecha": "$fecha_venta",
                    "anio": "$anio",
                    "mes": "$mes",
                    "dia": "$dia"
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "fecha_id": {"$toString": "$_id.fecha"},
                "anio": "$_id.anio",
                "mes": "$_id.mes",
                "dia": "$_id.dia",
                "trimestre": {"$ceil": {"$divide": ["$_id.mes", 3]}}
            }
        }
    ]
    docs_tiempo = list(db.ventas_analiticas.aggregate(pipeline_tiempo, allowDiskUse=True))
    df_dim_tiempo = pd.DataFrame(docs_tiempo).drop_duplicates()
    print(f"   [OK] Dimension Tiempo procesada ({len(df_dim_tiempo):,} registros).")

    # -------------------------------------------------------------------------
    # 2. DIMENSIÓN CLIENTE
    # -------------------------------------------------------------------------
    print("👤 Construyendo 'dim_cliente'...")
    pipeline_cliente = [
        {
            "$group": {
                "_id": "$cliente.id",
                "nombre": {"$first": "$cliente.nombre"},
                "telefono": {"$first": "$cliente.telefono"},
                "ciudad": {"$first": "$cliente.ciudad"},
                "departamento": {"$first": "$cliente.departamento"},
                "compras_totales": {"$sum": 1},
                "gasto_acumulado": {"$sum": "$total_venta"},
                "ticket_promedio": {"$avg": "$total_venta"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "cliente_id": "$_id",
                "nombre": 1,
                "telefono": 1,
                "ciudad": 1,
                "departamento": 1,
                "compras_totales": 1,
                "gasto_acumulado": {"$round": ["$gasto_acumulado", 2]},
                "ticket_promedio": {"$round": ["$ticket_promedio", 2]}
            }
        }
    ]
    docs_cliente = list(db.ventas_analiticas.aggregate(pipeline_cliente, allowDiskUse=True))
    df_dim_cliente = pd.DataFrame(docs_cliente)
    print(f"   [OK] Dimension Cliente procesada ({len(df_dim_cliente):,} registros).")

    # -------------------------------------------------------------------------
    # 3. DIMENSIÓN PRODUCTO
    # -------------------------------------------------------------------------
    print("📦 Construyendo 'dim_producto'...")
    pipeline_producto = [
        {
            "$group": {
                "_id": "$producto.id",
                "nombre": {"$first": "$producto.nombre"},
                "sku": {"$first": "$producto.sku"},
                "categoria": {"$first": "$producto.categoria"},
                "unidades_vendidas": {"$sum": "$cantidad"},
                "ingresos_generados": {"$sum": "$total_venta"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "producto_id": "$_id",
                "nombre": 1,
                "sku": 1,
                "categoria": 1,
                "unidades_vendidas": 1,
                "ingresos_generados": {"$round": ["$ingresos_generados", 2]}
            }
        }
    ]
    docs_producto = list(db.ventas_analiticas.aggregate(pipeline_producto, allowDiskUse=True))
    df_dim_producto = pd.DataFrame(docs_producto)
    print(f"   [OK] Dimension Producto procesada ({len(df_dim_producto):,} registros).")

    # -------------------------------------------------------------------------
    # 4. TABLA DE HECHOS (fact_ventas)
    # -------------------------------------------------------------------------
    print("📊 Construyendo 'fact_ventas'...")
    pipeline_fact = [
        {
            "$project": {
                "_id": 0,
                "order_number": 1,
                "fecha_id": {"$toString": "$fecha_venta"},
                "cliente_id": "$cliente.id",
                "producto_id": "$producto.id",
                "cantidad": 1,
                "precio_unitario": 1,
                "total_venta": 1,
                "costo_proveedor": 1,
                "costo_flete": 1,
                "ganancia_neta": 1,
                "metodo_pago": 1,
                "estado": 1
            }
        }
    ]
    docs_fact = list(db.ventas_analiticas.aggregate(pipeline_fact, allowDiskUse=True))
    df_fact_ventas = pd.DataFrame(docs_fact)
    print(f"   [OK] Tabla de Hechos fact_ventas procesada ({len(df_fact_ventas):,} registros).")

    # -------------------------------------------------------------------------
    # 5. CARGA AL DATA WAREHOUSE EN GOOGLE BIGQUERY
    # -------------------------------------------------------------------------
    print("\n☁️ Cargando tablas al Data Warehouse en BigQuery...")
    
    # Convertir posibles objetos ObjectId o tipos nativos de Mongo a String para BigQuery
    for df in [df_dim_tiempo, df_dim_cliente, df_dim_producto, df_fact_ventas]:
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str)

    tablas = {
        "dim_tiempo": df_dim_tiempo,
        "dim_cliente": df_dim_cliente,
        "dim_producto": df_dim_producto,
        "fact_ventas": df_fact_ventas
    }

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

    for nombre_tabla, df_tabla in tablas.items():
        table_id = f"{GCP_PROJECT_ID}.{GCP_DATASET_ID}.{nombre_tabla}"
        print(f"   💾 Cargando '{nombre_tabla}' -> {table_id}...")
        job = bq_client.load_table_from_dataframe(df_tabla, table_id, job_config=job_config)
        job.result()  # Esperar confirmación de carga

    tiempo_total = time.time() - inicio

    # Resumen final
    print("\n" + "="*50)
    print("🎉 DATA WAREHOUSE CONFIGURADO Y MIGRADO A BIGQUERY CON ÉXITO")
    print(f"⏱️ Tiempo total: {tiempo_total:.2f} segundos")
    print("="*50)
    print("📋 Resumen de tablas creadas en Google Cloud:")
    print(f"📅 dim_tiempo:   {len(df_dim_tiempo):,} registros")
    print(f"👤 dim_cliente:  {len(df_dim_cliente):,} registros")
    print(f"📦 dim_producto: {len(df_dim_producto):,} registros")
    print(f"📊 fact_ventas:  {len(df_fact_ventas):,} registros")
    print("="*50)

    client_mongo.close()

if __name__ == "__main__":
    construir_data_warehouse_bigquery()