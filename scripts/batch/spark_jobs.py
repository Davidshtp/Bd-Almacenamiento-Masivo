import os
import glob
import pymongo
from dotenv import load_dotenv
from datetime import datetime

# Cargar variables de entorno para la conexión a MongoDB Atlas
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")

# Configurar variables para Windows antes de importar PySpark
os.environ['PYSPARK_SUBMIT_ARGS'] = '--master local[*] pyspark-shell'
os.environ['SPARK_LOCAL_HOSTNAME'] = 'localhost'

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def guardar_en_mongodb(df_pandas, nombre_coleccion):
    """Guarda un DataFrame de Pandas en MongoDB Atlas con reintentos para evitar micro-cortes de DNS"""
    if not MONGO_URI:
        print(f"⚠️ MONGO_URI no encontrado. No se pudo guardar {nombre_coleccion} en Atlas.")
        return
        
    registros = df_pandas.to_dict(orient="records")
    
    # Intentar hasta 3 veces en caso de timeout de red/DNS
    for intento in range(1, 4):
        try:
            # Configurar timeouts más tolerantes para conexiones inestables
            client = pymongo.MongoClient(
                MONGO_URI, 
                serverSelectionTimeoutMS=10000,
                connectTimeoutMS=10000
            )
            db = client["heavycult"]
            coleccion = db[nombre_coleccion]
            
            coleccion.delete_many({})  # Limpia la colección previa
            if registros:
                coleccion.insert_many(registros)
                print(f"   ☁️ [ATLAS] {len(registros)} registros guardados en '{nombre_coleccion}'.")
            
            client.close()
            break  # Éxito: salir del bucle de reintentos
            
        except Exception as e:
            if intento < 3:
                print(f"   ⚠️ Reintentando conexión con Atlas para {nombre_coleccion} (Intento {intento}/3)...")
                import time
                time.sleep(2)
            else:
                print(f"❌ Error al guardar {nombre_coleccion} en MongoDB Atlas: {e}")
def obtener_ultimo_archivo(ruta_carpeta):
    """Prioriza la lectura del CSV procesado en data_lake/processed/"""
    archivos = glob.glob(os.path.join(ruta_carpeta, "*.csv"))
    if not archivos:
        archivos = glob.glob(os.path.join(ruta_carpeta, "*.parquet"))
    if not archivos:
        archivos = glob.glob(os.path.join(ruta_carpeta, "*.json"))
    if not archivos:
        raise FileNotFoundError(f"No se encontraron archivos procesados en: {ruta_carpeta}")
    return max(archivos, key=os.path.getctime)

def crear_spark_session():
    """Inicia la SparkSession en modo local usando todos los núcleos del CPU"""
    return SparkSession.builder \
        .appName("HeavyCult-Spark-Analytics") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

def job1_ventas_por_metodo_pago(spark, df_ventas, ruta_salida):
    """Spark Job 1: Agrupa ingresos y transacciones por Método de Pago"""
    print("\n⚙️ Ejecutando Spark Job 1: Ventas por método de pago...")
    
    resultado = df_ventas.groupBy("metodo_pago") \
        .agg(
            F.count("order_number").alias("total_transacciones"),
            F.round(F.sum("total_venta"), 2).alias("ingresos_totales"),
            F.round(F.avg("total_venta"), 2).alias("ticket_promedio")
        ) \
        .orderBy(F.col("ingresos_totales").desc())
    
    resultado.show()
    
    # 1. Guardar localmente en Data Lake (Curated)
    df_pd = resultado.toPandas()
    df_pd.to_parquet(os.path.join(ruta_salida, "job1_metodos_pago.parquet"), index=False)
    print("   [OK] Resultado de Job 1 guardado en data_lake/curated/")
    
    # 2. Guardar en el clúster de MongoDB Atlas
    guardar_en_mongodb(df_pd, "job1_metodos_pago")

def job2_segmentacion_clientes(spark, df_ventas, ruta_salida):
    """Spark Job 2: Segmenta los clientes según sus niveles de gasto acumulado"""
    print("\n⚙️ Ejecutando Spark Job 2: Segmentación de clientes...")
    
    if "cliente" in df_ventas.columns:
        df = df_ventas.withColumn("cliente_id", F.col("cliente"))
    else:
        df = df_ventas.withColumn("cliente_id", F.col("order_number"))
    
    clientes_agrupados = df.groupBy("cliente_id") \
        .agg(
            F.count("order_number").alias("compras"),
            F.round(F.sum("total_venta"), 2).alias("gasto_total")
        )
    
    segmentado = clientes_agrupados.withColumn(
        "segmento",
        F.when(F.col("gasto_total") > 50000000, "VIP")
         .when(F.col("gasto_total") > 10000000, "Premium")
         .when(F.col("gasto_total") > 1000000, "Frecuente")
         .otherwise("Regular")
    )
    
    resultado = segmentado.groupBy("segmento") \
        .agg(
            F.count("cliente_id").alias("cantidad_clientes"),
            F.round(F.sum("gasto_total"), 2).alias("ingreso_acumulado")
        ) \
        .orderBy(F.col("ingreso_acumulado").desc())
        
    resultado.show()
    
    # 1. Guardar localmente en Data Lake (Curated)
    df_pd = resultado.toPandas()
    df_pd.to_parquet(os.path.join(ruta_salida, "job2_segmentacion_clientes.parquet"), index=False)
    print("   [OK] Resultado de Job 2 guardado en data_lake/curated/")
    
    # 2. Guardar en el clúster de MongoDB Atlas
    guardar_en_mongodb(df_pd, "job2_segmentacion_clientes")

def job3_series_temporales(spark, df_ventas, ruta_salida):
    """Spark Job 3: Tendencia temporal de ingresos agrupada por Año y Mes"""
    print("\n⚙️ Ejecutando Spark Job 3: Análisis de series temporales...")
    
    df_fecha = df_ventas.withColumn("fecha_parsed", F.to_date(F.col("fecha_venta")))
    df_tiempo = df_fecha.withColumn("anio", F.year("fecha_parsed")) \
                         .withColumn("mes", F.month("fecha_parsed"))
    
    resultado = df_tiempo.groupBy("anio", "mes") \
        .agg(
            F.count("order_number").alias("total_ventas"),
            F.round(F.sum("total_venta"), 2).alias("ingresos_mensuales"),
            F.round(F.sum("ganancia_neta"), 2).alias("ganancia_mensual")
        ) \
        .orderBy("anio", "mes")
        
    resultado.show()
    
    # 1. Guardar localmente en Data Lake (Curated)
    df_pd = resultado.toPandas()
    df_pd.to_parquet(os.path.join(ruta_salida, "job3_tendencia_mensual.parquet"), index=False)
    print("   [OK] Resultado de Job 3 guardado en data_lake/curated/")
    
    # 2. Guardar en el clúster de MongoDB Atlas
    guardar_en_mongodb(df_pd, "job3_tendencia_mensual")

def ejecutar_spark_jobs():
    print("==================================================")
    print("🔥 Iniciando procesador distribuido Apache Spark...")
    print("==================================================")
    
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    processed_dir = os.path.join(base_dir, "data_lake", "processed")
    curated_dir = os.path.join(base_dir, "data_lake", "curated")
    
    archivo_entrada = obtener_ultimo_archivo(processed_dir)
    print(f"📥 Leyendo dataset desde: {archivo_entrada}")
    
    spark = crear_spark_session()
    
    if archivo_entrada.endswith('.json'):
        df_ventas = spark.read.option("multiline", "true").json(archivo_entrada)
    elif archivo_entrada.endswith('.csv'):
        df_ventas = spark.read.option("header", "true").option("inferSchema", "true").csv(archivo_entrada)
    else:
        df_ventas = spark.read.parquet(archivo_entrada)
        
    # Ejecución secuencial de los 3 Jobs
    job1_ventas_por_metodo_pago(spark, df_ventas, curated_dir)
    job2_segmentacion_clientes(spark, df_ventas, curated_dir)
    job3_series_temporales(spark, df_ventas, curated_dir)
    
    spark.stop()
    print("\n==================================================")
    print("🔒 SparkSession finalizada correctamente.")
    print("==================================================")

if __name__ == "__main__":
    ejecutar_spark_jobs()