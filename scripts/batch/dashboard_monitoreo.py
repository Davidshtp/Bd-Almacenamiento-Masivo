import os
import time
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import pymongo
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")

def obtener_metricas_sistema():
    """Conecta a MongoDB Atlas y extrae los contadores reales de las colecciones."""
    if not MONGO_URI:
        raise ValueError("No se encontró la variable MONGO_URI en el archivo .env")
        
    client = pymongo.MongoClient(MONGO_URI)
    db = client["heavycult"]
    
    colecciones = db.list_collection_names()
    conteo_colecciones = {}
    total_documentos = 0
    
    for col_name in colecciones:
        count = db[col_name].count_documents({})
        conteo_colecciones[col_name] = count
        total_documentos += count
        
    # Obtener serie temporal para gráfico de tendencias
    pipeline_tendencia = [
        {"$group": {
            "_id": {
                "anio": "$anio",
                "mes": "$mes"
            },
            "ingresos": {"$sum": "$total_venta"}
        }},
        {"$sort": {"_id.anio": 1, "_id.mes": 1}}
    ]
    
    tendencia_raw = list(db.ventas_analiticas.aggregate(pipeline_tendencia))
    fechas = []
    ingresos = []
    for item in tendencia_raw:
        if item["_id"]["anio"] and item["_id"]["mes"]:
            fechas.append(f"{item['_id']['anio']}-{item['_id']['mes']:02d}")
            ingresos.append(item["ingresos"] / 1e6) # En millones COP
            
    client.close()
    
    return conteo_colecciones, total_documentos, fechas, ingresos

def generar_dashboard():
    print("==================================================")
    print("DASHBOARD DE MONITOREO DE ARQUITECTURA")
    print("==================================================")
    
    conteo_cols, total_docs, fechas_trend, ingresos_trend = obtener_metricas_sistema()
    
    # 1. Cálculos del Sistema
    almacenamiento_gb = round(total_docs * 0.00000095, 2)
    latencia_promedio_ms = 238.08 # Medida del benchmarking
    costo_mensual_usd = 57.04    # Estimado en M10
    meses_con_datos = len(fechas_trend)
    
    print(f"\nESTADISTICAS DEL DASHBOARD")
    print("=" * 50)
    print(f"Total de documentos en el sistema: {total_docs:,}")
    print(f"Almacenamiento estimado: {almacenamiento_gb} GB")
    print(f"Latencia promedio de consultas: {latencia_promedio_ms} ms")
    print(f"Costo mensual estimado: ${costo_mensual_usd}")
    print(f"Meses con datos: {meses_con_datos}")
    print("=" * 50 + "\n")
    
    # 2. Configuración de Lienzo Matplotlib
    fig = plt.figure(figsize=(19, 14), facecolor='#f8f9fa')
    fig.suptitle('DASHBOARD DE MONITOREO - ARQUITECTURA DE DATOS', fontsize=20, fontweight='bold', color='#2c3e50', y=0.97)
    
    gs = gridspec.GridSpec(2, 3, figure=fig, hspace=0.60, wspace=0.38)
    
    # --- GRÁFICO 1: Documentos por Colección ---
    ax1 = fig.add_subplot(gs[0, 0])
    cols_nom = list(conteo_cols.keys())
    cols_val = list(conteo_cols.values())
    colores = ['#2ecc71', '#f39c12', '#9b59b6', '#e74c3c', '#3498db', '#1abc9c', '#f1c40f', '#e84393']
    
    bars = ax1.bar(cols_nom, cols_val, color=colores[:len(cols_nom)], width=0.55, alpha=0.85, edgecolor='#2c3e50', linewidth=0.8)
    ax1.set_title('Documentos por Coleccion', fontsize=12, fontweight='bold', color='#34495e', pad=12)
    ax1.set_ylabel('Numero de Documentos', fontsize=10)
    
    ax1.set_xticks(range(len(cols_nom)))
    ax1.set_xticklabels(cols_nom, rotation=65, ha='right', fontsize=8.5)
    ax1.grid(axis='y', linestyle='--', alpha=0.4)
    
    max_y1 = max(cols_val) if cols_val else 10
    ax1.set_ylim(0, max_y1 * 1.25)
    
    for bar in bars:
        yval = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2, yval + (max_y1 * 0.02), f'{yval:,}', 
                 ha='center', va='bottom', fontsize=7.5, fontweight='bold', color='#2c3e50')

    # --- GRÁFICO 2: Rendimiento de Consultas (SLA) ---
    ax2 = fig.add_subplot(gs[0, 1])
    consultas_nom = ['Ventas\nTotales', 'Agrupacion\nPago', 'Analisis\nTemporal']
    latencias = [230.5, 242.1, 241.6]
    
    bars2 = ax2.bar(consultas_nom, latencias, color='#2ecc71', width=0.45, edgecolor='#27ae60', linewidth=1)
    ax2.axhline(y=500, color='#e74c3c', linestyle='--', linewidth=1.5, label='SLA Max (<500ms)')
    ax2.set_title('Rendimiento de Consultas (SLA)', fontsize=12, fontweight='bold', color='#34495e', pad=12)
    ax2.set_ylabel('Tiempo (ms)', fontsize=10)
    ax2.set_ylim(0, 580)
    ax2.legend(loc='upper right', fontsize=9)
    ax2.grid(axis='y', linestyle='--', alpha=0.4)
    
    for bar in bars2:
        yval = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2, yval + 12, f'{yval:.1f} ms', 
                 ha='center', va='bottom', fontsize=8, fontweight='bold')

    # --- GRÁFICO 3: Estimación Costos Mensuales (OPTIMIZADO Y SIN TEXTOS TRASLAPADOS) ---
    ax3 = fig.add_subplot(gs[0, 2])
    costos_labels = ['Base (M10): $56.95', 'Almacenamiento: $0.07', 'Operaciones: $0.02']
    costos_valores = [56.95, 0.07, 0.02]
    explode = (0, 0.1, 0.25) # Desplaza ligeramente los sectores pequeños
    
    wedges, texts, autotexts = ax3.pie(
        costos_valores, 
        autopct=lambda pct: f'{pct:.1f}%' if pct > 1 else '', # Muestra porcentaje solo si es legible
        startangle=140, 
        colors=['#3498db', '#e74c3c', '#9b59b6'],
        explode=explode,
        pctdistance=0.6,
        wedgeprops=dict(width=0.45, edgecolor='w', linewidth=2)
    )
    
    plt.setp(autotexts, size=9, weight="bold", color="white")
    ax3.set_title('Estimacion Costos Mensuales ($ USD)', fontsize=12, fontweight='bold', color='#34495e', pad=12)
    
    # Leyenda independiente externa sin traslapes
    ax3.legend(
        wedges, costos_labels,
        title="Componentes",
        loc="center left",
        bbox_to_anchor=(0.85, 0.5),
        fontsize=8.5,
        title_fontsize=9,
        frameon=True
    )

    # --- GRÁFICO 4: Tendencia de Ventas Mensuales ---
    ax4 = fig.add_subplot(gs[1, 0])
    if fechas_trend and ingresos_trend:
        ax4.plot(fechas_trend, ingresos_trend, marker='o', color='#e67e22', linewidth=2.5, markersize=5, label='Ingresos')
        ax4.set_title('Tendencia de Ventas Mensuales', fontsize=12, fontweight='bold', color='#34495e', pad=12)
        ax4.set_ylabel('Ingresos (Millones COP)', fontsize=10)
        ax4.tick_params(axis='x', rotation=55, labelsize=8)
        ax4.grid(True, linestyle='--', alpha=0.4)
        ax4.legend(loc='upper left', fontsize=8)

    # --- GRÁFICO 5: Métricas de Almacenamiento ---
    ax5 = fig.add_subplot(gs[1, 1])
    metricas_nom = ['Total\nDocumentos', 'GB\nEstimados', 'Meses\ncon Datos']
    metricas_val = [total_docs, almacenamiento_gb, meses_con_datos]
    
    bars5 = ax5.bar(metricas_nom, [100, (almacenamiento_gb/10)*100, (meses_con_datos/24)*100], color='#3498db', alpha=0.85, width=0.45)
    ax5.set_title('Metricas de Almacenamiento', fontsize=12, fontweight='bold', color='#34495e', pad=12)
    ax5.set_ylabel('Valor Normalizado (%)', fontsize=10)
    ax5.set_ylim(0, 125)
    ax5.grid(axis='y', linestyle='--', alpha=0.4)
    
    for i, bar in enumerate(bars5):
        val_str = f'{metricas_val[i]:,}' if isinstance(metricas_val[i], int) else f'{metricas_val[i]:.2f}'
        ax5.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 3, val_str, 
                 ha='center', va='bottom', fontsize=8, fontweight='bold', color='#2c3e50')

    # --- PANEL 6: Resumen Ejecutivo del Sistema ---
    ax6 = fig.add_subplot(gs[1, 2])
    ax6.axis('off')
    
    resumen_text = (
        "┌──────────────────────────────────────────┐\n"
        "│     RESUMEN EJECUTIVO DEL SISTEMA        │\n"
        "├──────────────────────────────────────────┤\n"
        f"│  Total Documentos:   {total_docs:>13,}  │\n"
        f"│  Almacenamiento:     {almacenamiento_gb:>10.2f} GB  │\n"
        f"│  Latencia Promedio:  {latencia_promedio_ms:>10.2f} ms │\n"
        f"│  Costo Estimado:     ${costo_mensual_usd:>10.2f}   │\n"
        "│                                          │\n"
        "│  Estado del Sistema: OPERATIVO           │\n"
        f"│  Colecciones Activas: {len(conteo_cols):>12}  │\n"
        "└──────────────────────────────────────────┘"
    )
    
    ax6.text(0.02, 0.15, resumen_text, fontsize=9.5, fontfamily='monospace',
             bbox=dict(boxstyle='round,pad=1.2', facecolor='#ffffff', edgecolor='#d35400', alpha=0.9, linewidth=1.5))

    # Guardar imagen PNG
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    nombre_archivo = f"dashboard_monitoreo_{timestamp}.png"
    plt.savefig(nombre_archivo, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"Dashboard generado exitosamente: {nombre_archivo}")
    print("Abre el archivo PNG para ver la grafica de costos optimizada.\n")

if __name__ == "__main__":
    generar_dashboard()