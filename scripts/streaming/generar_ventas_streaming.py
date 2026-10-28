import os
import random
import pymongo
import time
from dotenv import load_dotenv
from faker import Faker

# Cargar variables de entorno buscando en la raíz si es necesario
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")

if not MONGO_URI:
    # Intenta cargar el .env de la carpeta raíz
    load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))
    MONGO_URI = os.getenv("MONGO_URI")

fake = Faker('es_CO')

CIUDADES = ["Bogotá", "Medellín", "Cali", "Barranquilla", "Bucaramanga"]

def generar_ventas_continuas():
    client = pymongo.MongoClient(MONGO_URI)
    db = client["heavycult"]
    
    # Obtener clientes y productos existentes para hacer referencias válidas
    clientes_ids = [c["_id"] for c in db.customers.find({}, {"_id": 1})]
    productos = list(db.products.find({}, {"_id": 1, "name": 1, "pricing": 1, "variants": 1}))

    if not clientes_ids or not productos:
        print("❌ Error: No hay clientes o productos en la BD. Ejecuta primero los scripts de carga masiva.")
        return

    print("⚡ Generador de eventos en tiempo real iniciado. Presiona Ctrl+C para detener.\n")

    try:
        while True:
            # Generar un ID de orden garantizado como único usando timestamp + random
            timestamp_id = int(time.time() * 1000)
            rand_suffix = random.randint(100, 999)
            order_num = f"HC-STRM-{timestamp_id}-{rand_suffix}"

            producto = random.choice(productos)
            variante = random.choice(producto["variants"])
            cantidad = random.randint(1, 3)
            precio = producto["pricing"]["sale_price"] * cantidad
            flete = random.choice([14000, 15000, 18000])

            nueva_orden = {
                "order_number": order_num,
                "customer_id": random.choice(clientes_ids),
                "items": [{
                    "product_id": producto["_id"],
                    "product_name": producto["name"],
                    "sku": variante["sku"],
                    "quantity": cantidad,
                    "unit_price": producto["pricing"]["sale_price"]
                }],
                "total_amount": round(precio, 2),
                "shipping_cost": float(flete),
                "payment_method": random.choice(["PSE", "Nequi", "Pago Contra Entrega"]),
                "status": "delivered",
                "created_at": fake.date_time_between(start_date='-1d', end_date='now')
            }

            db.orders.insert_one(nueva_orden)
            print(f"📤 Venta emitida: {nueva_orden['order_number']} por ${nueva_orden['total_amount']:,.2f}")
            
            # Esperar entre 2 y 4 segundos antes de emitir el siguiente evento
            time.sleep(random.uniform(2, 4))

    except KeyboardInterrupt:
        print("\n🛑 Emisión de eventos detenida.")
        client.close()

if __name__ == "__main__":
    generar_ventas_continuas()