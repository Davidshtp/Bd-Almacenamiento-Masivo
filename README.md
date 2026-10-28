# Parcial BD Primer Corte - Sistema de Ventas con MongoDB Atlas

Guia practica del documento `PARCIAL BD PRIMER CORTE.docx`.

## Estructura
```
.
├── .env                            # MONGO_URI (no se sube a git)
├── requirements.txt
├── conexion.py                     # Fase 1 - prueba de conexion
└── scripts/
    ├── crear_estructura.py         # Fase 2 - OLTP + OLAP
    ├── generar_datos_masivos.py    # Fase 3 - 5k clientes / 1k productos / 50k ventas
    ├── transformar_oltp_a_olap.py  # Fase 4 - Aggregation Pipeline ETL
    └── benchmark.py                # Fase 5 - indices compuestos + 5 consultas
```

## Requisitos
- Python 3.12
- Cuenta MongoDB Atlas (cluster M0 gratis)
- `pip install -r requirements.txt`

## Uso paso a paso (orden del documento)
1. Editar `.env` con tu connection string:
   ```
   MONGO_URI=mongodb+srv://usuario:password@cluster0.xxxxx.mongodb.net/?retryWrites=true&w=majority
   ```
2. Verificar conexion:
   ```
   python conexion.py
   ```
3. Crear estructura:
   ```
   python scripts/crear_estructura.py
   ```
4. Generar datos masivos:
   ```
   python scripts/generar_datos_masivos.py
   ```
5. Transformar OLTP -> OLAP:
   ```
   python scripts/transformar_oltp_a_olap.py
   ```
6. Benchmarking:
   ```
   python scripts/benchmark.py
   ```

## Troubleshooting (del documento)
- `bad auth : authentication failed` -> restablecer password en Atlas > Database Access
- `E11000 duplicate key` -> ya manejado con `ordered=False`
- `$cond takes exactly 3 arguments` -> anidar `$cond`
- Consultas lentas -> revisar indices compuestos
