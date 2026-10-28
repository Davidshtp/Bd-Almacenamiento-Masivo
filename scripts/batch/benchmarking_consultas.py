import os
import time
import json
import pymongo
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")

def crear_indice_seguro(coleccion, llaves, nombre_deseado):
    """Crea un índice de forma segura ignorando conflictos si ya existe con otro nombre."""
    try:
        coleccion.create_index(llaves, name=nombre_deseado)
    except pymongo.errors.OperationFailure as e:
        if "already exists with a different name" in str(e):
            pass # El índice ya existe funcionalmente en la base de datos
        else:
            raise e

def ejecutar_benchmark():
    print("[LANZAR] Iniciando Benchmarking Analitico de HeavyCult (10 Consultas)...\n")
    client = pymongo.MongoClient(MONGO_URI)
    db = client["heavycult"]
    coleccion = db["ventas_analiticas"]

    print("[LLAVE] Aplicando estrategias de indexacion analitica...")
    crear_indice_seguro(coleccion, [("anio", 1), ("mes", 1)], "idx_fecha")
    crear_indice_seguro(coleccion, [("producto.categoria", 1), ("ganancia_neta", -1)], "idx_cat_ganancia")
    crear_indice_seguro(coleccion, [("estado", 1), ("cliente.ciudad", 1)], "idx_estado_ciudad")
    crear_indice_seguro(coleccion, [("metodo_pago", 1), ("estado", 1)], "idx_pago_estado")
    crear_indice_seguro(coleccion, [("cliente.segmento", 1), ("cliente.ciudad", 1)], "idx_segmento_ciudad")
    print("[OK] Indices analiticos verificados exitosamente.\n")

    consultas = [
        {
            "nombre": "1. Facturacion y Ganancia Neta Mensual (Serie de Tiempo)",
            "pipeline": [
                {"$group": {
                    "_id": {"anio": "$anio", "mes": "$mes"},
                    "ventas_totales": {"$sum": "$total_venta"},
                    "ganancia_neta_total": {"$sum": "$ganancia_neta"},
                    "pedidos": {"$sum": 1}
                }},
                {"$sort": {"_id.anio": 1, "_id.mes": 1}}
            ]
        },
        {
            "nombre": "2. Top 5 Productos Mas Rentables por Categoria",
            "pipeline": [
                {"$group": {
                    "_id": {"categoria": "$producto.categoria", "sku": "$producto.sku"},
                    "ganancia": {"$sum": "$ganancia_neta"},
                    "unidades": {"$sum": "$cantidad"}
                }},
                {"$sort": {"_id.categoria": 1, "ganancia": -1}},
                {"$group": {
                    "_id": "$_id.categoria",
                    "top_skus": {"$push": {"sku": "$_id.sku", "ganancia": "$ganancia", "unidades": "$unidades"}}
                }},
                {"$project": {"categoria": "$_id", "ranking": {"$slice": ["$top_skus", 5]}, "_id": 0}}
            ]
        },
        {
            "nombre": "3. Rentabilidad y Novedades por Ciudad y Departamento",
            "pipeline": [
                {"$group": {
                    "_id": {"depto": "$cliente.departamento", "ciudad": "$cliente.ciudad"},
                    "ingresos": {"$sum": "$total_venta"},
                    "ganancia_neta": {"$sum": "$ganancia_neta"},
                    "novedades_o_cancelados": {
                        "$sum": {"$cond": [{"$in": ["$estado", ["novelty", "cancelled"]]}, 1, 0]}
                    }
                }},
                {"$sort": {"ingresos": -1}}
            ]
        },
        {
            "nombre": "4. Ticket Promedio y Volumen por Metodo de Pago",
            "pipeline": [
                {"$group": {
                    "_id": "$metodo_pago",
                    "total_transacciones": {"$sum": 1},
                    "ingresos_totales": {"$sum": "$total_venta"},
                    "ticket_promedio": {"$avg": "$total_venta"}
                }},
                {"$sort": {"ingresos_totales": -1}}
            ]
        },
        {
            "nombre": "5. Tendencia de Ventas por Dia de la Semana",
            "pipeline": [
                {"$group": {
                    "_id": {"$dayOfWeek": "$fecha_venta"},
                    "ingresos": {"$sum": "$total_venta"},
                    "ventas_count": {"$sum": 1}
                }},
                {"$sort": {"_id": 1}}
            ]
        },
        {
            "nombre": "6. Tasa de Cancelacion y Devoluciones por Metodo de Pago",
            "pipeline": [
                {"$group": {
                    "_id": "$metodo_pago",
                    "total_ventas": {"$sum": 1},
                    "canceladas": {
                        "$sum": {"$cond": [{"$eq": ["$estado", "cancelled"]}, 1, 0]}
                    }
                }},
                {"$project": {
                    "metodo_pago": "$_id",
                    "total_ventas": 1,
                    "canceladas": 1,
                    "tasa_cancelacion_pct": {
                        "$round": [{"$multiply": [{"$divide": ["$canceladas", "$total_ventas"]}, 100]}, 2]
                    },
                    "_id": 0
                }}
            ]
        },
        {
            "nombre": "7. Top 10 Clientes VIP por Gasto Acumulado",
            "pipeline": [
                {"$group": {
                    "_id": "$cliente.id",
                    "nombre": {"$first": "$cliente.nombre"},
                    "compras": {"$sum": 1},
                    "gasto_total": {"$sum": "$total_venta"}
                }},
                {"$sort": {"gasto_total": -1}},
                {"$limit": 10},
                {"$project": {
                    "cliente_id": "$_id",
                    "nombre": 1,
                    "compras": 1,
                    "gasto_total": {"$round": ["$gasto_total", 2]},
                    "_id": 0
                }}
            ]
        },
        {
            "nombre": "8. Analisis Estacional por Trimestre (Q1-Q4)",
            "pipeline": [
                {"$project": {
                    "anio": 1,
                    "total_venta": 1,
                    "trimestre": {
                        "$concat": [
                            "Q",
                            {"$toString": {"$ceil": {"$divide": ["$mes", 3]}}}
                        ]
                    }
                }},
                {"$group": {
                    "_id": {"anio": "$anio", "trimestre": "$trimestre"},
                    "ingresos": {"$sum": "$total_venta"}
                }},
                {"$sort": {"_id.anio": 1, "_id.trimestre": 1}}
            ]
        },
        {
            "nombre": "9. Ticket Promedio por Ciudad y Segmento de Cliente (Top 15)",
            "pipeline": [
                {"$group": {
                    "_id": {"ciudad": "$cliente.ciudad", "segmento": "$cliente.segmento"},
                    "ticket_promedio": {"$avg": "$total_venta"},
                    "transacciones": {"$sum": 1}
                }},
                {"$sort": {"ticket_promedio": -1}},
                {"$limit": 15},
                {"$project": {
                    "ciudad": "$_id.ciudad",
                    "segmento": "$_id.segmento",
                    "ticket_promedio": {"$round": ["$ticket_promedio", 2]},
                    "transacciones": 1,
                    "_id": 0
                }}
            ]
        },
        {
            "nombre": "10. Top 10 Productos con Mayor Rotacion (Unidades Vendidas)",
            "pipeline": [
                {"$group": {
                    "_id": "$producto.nombre",
                    "unidades_vendidas": {"$sum": "$cantidad"},
                    "ingresos_generados": {"$sum": "$total_venta"}
                }},
                {"$sort": {"unidades_vendidas": -1}},
                {"$limit": 10},
                {"$project": {
                    "producto": "$_id",
                    "unidades_vendidas": 1,
                    "ingresos_generados": {"$round": ["$ingresos_generados", 2]},
                    "_id": 0
                }}
            ]
        }
    ]

    print("[ENGRAJE] Ejecutando benchmark de las 10 consultas...\n" + "-"*70)
    tiempos = []

    for consulta in consultas:
        print(f"[PLAY] {consulta['nombre']}")
        inicio = time.perf_counter()
        resultados = list(coleccion.aggregate(consulta['pipeline'], allowDiskUse=True))
        fin = time.perf_counter()
        tiempo_ms = (fin - inicio) * 1000
        tiempos.append(tiempo_ms)
        
        print(f"    [CRONOMETRO] Latencia: {tiempo_ms:.2f} ms | Filas retornadas: {len(resultados)}")
        if len(resultados) > 0:
            muestra = json.loads(json.dumps(resultados[0], default=str))
            print(f"    [BOMBILLA] Muestra: {muestra}")
        print("-" * 70)

    promedio = sum(tiempos) / len(tiempos)
    print("\n" + "="*70)
    print("[GRAFICO] RESUMEN DE RENDIMIENTO DE CONSULTAS (10 CONSULTAS)")
    print("="*70)
    print(f"[CRONOMETRO] Latencia PROMEDIO por consulta: {promedio:.2f} ms")
    print(f"[LANZAR] Latencia MAXIMA registrada: {max(tiempos):.2f} ms")
    print(f"[CAJA] Documentos analiticos procesados: {coleccion.count_documents({}):,}")
    print("="*70)
    
    client.close()

if __name__ == "__main__":
    ejecutar_benchmark()