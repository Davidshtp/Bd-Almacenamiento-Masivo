import os
import pymongo
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# 1. Definir la ruta raíz del proyecto (un nivel arriba de la carpeta /scripts)
RUTA_SCRIPTS = os.path.dirname(os.path.abspath(__file__))
RUTA_RAIZ = os.path.dirname(RUTA_SCRIPTS)

# Cargar variables de entorno desde el .env en la raíz
load_dotenv(os.path.join(RUTA_RAIZ, ".env"))
MONGO_URI = os.getenv("MONGO_URI")

def obtener_tipo_dato(valor):
    """Mapea tipos de datos de Python a estándar de base de datos."""
    if isinstance(valor, bool):
        return "Boolean"
    elif isinstance(valor, int):
        return "Integer"
    elif isinstance(valor, float):
        return "Float"
    elif isinstance(valor, list):
        return "Array"
    elif isinstance(valor, dict):
        return "Object/Document"
    elif isinstance(valor, datetime):
        return "Timestamp"
    elif valor is None:
        return "Null"
    else:
        return "String"

def inspeccionar_documento(doc, coleccion, prefix=""):
    """Inspecciona recursivamente los campos de un documento de MongoDB, aplanando JSONs anidados."""
    campos = []
    
    for k, v in doc.items():
        nombre_campo = f"{prefix}{k}" if prefix else k
        tipo = obtener_tipo_dato(v)
        es_pk = "Sí (PK)" if nombre_campo == "_id" else "No"
        es_fk = "Sí (FK)" if nombre_campo in ["cliente.id", "producto.id", "cliente_id", "producto_id"] else "No"
        
        # Mapeo de descripciones de negocio
        descripciones = {
            "_id": "Identificador único del documento en MongoDB (ObjectId)",
            "anio": "Año de la transacción de venta",
            "mes": "Mes numérico de la transacción (1-12)",
            "total_venta": "Monto total facturado en la venta (COP)",
            "ganancia_neta": "Ganancia líquida calculada de la venta (COP)",
            "cantidad": "Número de unidades vendidas del producto",
            "estado": "Estado de la orden (delivered, cancelled, novelty, processing)",
            "metodo_pago": "Método de pago utilizado (Pago Contra Entrega, Nequi, Tarjeta)",
            "fecha_venta": "Fecha y hora exacta del registro de la transacción",
            "cliente.id": "Clave foránea del cliente asociado",
            "cliente.nombre": "Nombre completo del cliente registrado",
            "cliente.ciudad": "Ciudad de entrega del pedido",
            "cliente.departamento": "Departamento de ubicación del cliente",
            "cliente.segmento": "Categorización comercial del cliente (VIP, Frecuente, Regular)",
            "producto.sku": "Código de inventario único (SKU) del producto",
            "producto.nombre": "Nombre comercial del producto de vestuario",
            "producto.categoria": "Categoría de producto (Chaquetas, Morrales, etc.)"
        }
        
        desc = descripciones.get(nombre_campo, f"Campo de negocio correspondiente a {nombre_campo}")
        
        campos.append({
            "Colección": coleccion,
            "Campo": nombre_campo,
            "Tipo de Dato": tipo,
            "Primary Key": es_pk,
            "Foreign Key": es_fk,
            "Indexado": "Pendiente Verificación",
            "Descripción de Negocio": desc
        })
        
        if isinstance(v, dict) and k not in ["_id"]:
            campos.extend(inspeccionar_documento(v, coleccion, prefix=f"{nombre_campo}."))
            
    return campos

def generar_catalogo_datos():
    print("==================================================")
    print("🚀 GENERADOR AUTOMATIZADO DE CATÁLOGO DE DATOS")
    print("==================================================\n")

    if not MONGO_URI:
        raise ValueError("No se encontró MONGO_URI en el archivo .env de la raíz.")

    client = pymongo.MongoClient(MONGO_URI)
    db = client["heavycult"]

    # 1. Pestaña 1: Resumen Arquitectónico
    print("📊 [1/2] Extrayendo inventario arquitectónico de colecciones...")
    colecciones = db.list_collection_names()
    
    resumen_data = []
    
    roles_arquitectura = {
        "ventas_analiticas": ("OLAP / Data Warehouse", "Tabla de hechos consolidada con datos desnormalizados para BI y analítica"),
        "fact_ventas": ("Data Warehouse (Star Schema)", "Tabla de Hechos del modelo en estrella"),
        "dim_cliente": ("Data Warehouse (Star Schema)", "Tabla Dimensión de Clientes"),
        "dim_producto": ("Data Warehouse (Star Schema)", "Tabla Dimensión de Productos"),
        "dim_tiempo": ("Data Warehouse (Star Schema)", "Tabla Dimensión Temporal"),
        "job1_metodos_pago": ("Datamart / Aggregation", "Agregación precalculada por método de pago"),
        "job2_segmentacion_clientes": ("Datamart / Aggregation", "Métricas agregadas por segmento de cliente"),
        "job3_tendencia_mensual": ("Datamart / Aggregation", "Consolidado de ventas y ganancias por año/mes")
    }

    for col_name in colecciones:
        count = db[col_name].count_documents({})
        rol, desc = roles_arquitectura.get(col_name, ("OLTP / Staging", "Colección auxiliar del Data Lake"))
        
        resumen_data.append({
            "Colección / Tabla": col_name,
            "Rol en Arquitectura": rol,
            "Volumen de Datos (Docs)": count,
            "Descripción Arquitectónica": desc
        })

    df_resumen = pd.DataFrame(resumen_data)

    # 2. Pestaña 2: Diccionario de Datos
    print("📖 [2/2] Construyendo Diccionario de Datos mapeado...")
    diccionario_campos = []

    for col_name in colecciones:
        sample_doc = db[col_name].find_one()
        if sample_doc:
            campos_doc = inspeccionar_documento(sample_doc, col_name)
            
            indices_info = list(db[col_name].list_indexes())
            campos_indexados = []
            for idx in indices_info:
                campos_indexados.extend(list(idx["key"].keys()))
                
            for campo in campos_doc:
                if campo["Campo"] in campos_indexados or campo["Campo"] == "_id":
                    campo["Indexado"] = "Sí"
                else:
                    campo["Indexado"] = "No"
                    
            diccionario_campos.extend(campos_doc)

    df_diccionario = pd.DataFrame(diccionario_campos)

    # 3. Guardar directamente en la Raíz del proyecto
    timestamp = datetime.now().strftime("%Y%m%d")
    nombre_archivo = f"Catalogo_Datos_VentasDB_{timestamp}.xlsx"
    ruta_salida_excel = os.path.join(RUTA_RAIZ, nombre_archivo)
    
    with pd.ExcelWriter(ruta_salida_excel, engine='openpyxl') as writer:
        df_resumen.to_excel(writer, sheet_name="Resumen Arquitectónico", index=False)
        df_diccionario.to_excel(writer, sheet_name="Diccionario de Datos", index=False)

    print(f"\n==================================================")
    print(f"🎉 Catálogo generado exitosamente en la raíz:")
    print(f"📍 {ruta_salida_excel}")
    print("==================================================")
    client.close()

if __name__ == "__main__":
    generar_catalogo_datos()