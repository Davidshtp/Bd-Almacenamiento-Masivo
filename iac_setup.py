import os
import sys
import subprocess

def imprimir_titulo(texto):
    print("\n" + "="*60)
    print(f"🚀 {texto}")
    print("="*60)

def validar_python():
    print("[1/5] 🐍 Verificando versión de Python...")
    version = sys.version_info
    if version.major < 3 or (version.major == 3 and version.minor < 8):
        print(f"❌ Se requiere Python 3.8 o superior. Versión actual: {version.major}.{version.minor}")
        sys.exit(1)
    print(f"   ✅ Versión de Python válida: {version.major}.{version.minor}.{version.micro}")

def gestionar_entorno_virtual():
    print("[2/5] 📦 Verificando entorno virtual (.venv)...")
    venv_dir = os.path.join(os.getcwd(), ".venv")
    
    if not os.path.exists(venv_dir):
        print("   ⏳ Creando entorno virtual .venv...")
        subprocess.run([sys.executable, "-m", "venv", ".venv"], check=True)
        print("   ✅ Entorno virtual .venv creado con éxito.")
    else:
        print("   ✅ Entorno virtual .venv detectado.")

def instalar_dependencias():
    print("[3/5] 📚 Instalando dependencias desde requirements.txt...")
    pip_path = os.path.join(os.getcwd(), ".venv", "Scripts", "pip.exe") if os.name == 'nt' else os.path.join(os.getcwd(), ".venv", "bin", "pip")
    
    requirements_file = "requirements.txt"
    if not os.path.exists(requirements_file):
        with open(requirements_file, "w") as f:
            f.write("pymongo\npandas\nmatplotlib\npyspark\npython-dotenv\nplotly\n")
            
    try:
        subprocess.run([pip_path if os.path.exists(pip_path) else "pip", "install", "-r", requirements_file], check=True)
        print("   ✅ Dependencias instaladas/verificadas.")
    except Exception as e:
        print(f"⚠️ Advertencia al instalar dependencias: {e}")

def aprovisionar_data_lake():
    print("[4/5] 📁 Aprovisionando directorios del Data Lake (IaC)...")
    capas = [
        "data_lake/raw",
        "data_lake/processed",
        "data_lake/curated",
        "logs"
    ]
    for capa in capas:
        if not os.path.exists(capa):
            os.makedirs(capa)
            print(f"   [+] Creado directorio: {capa}/")
        else:
            print(f"   [=] Directorio existente: {capa}/")

def prueba_salud_atlas():
    print("[5/5] 🔌 Ejecutando prueba de salud (Health Check) en MongoDB Atlas...")
    
    if not os.path.exists(".env"):
        print("⚠️ Advertencia: No se encontró el archivo .env en la raíz.")
        return

    try:
        from dotenv import load_dotenv
        import pymongo
        
        load_dotenv()
        mongo_uri = os.getenv("MONGO_URI")
        
        if not mongo_uri:
            print("⚠️ Variable MONGO_URI no definida en .env")
            return

        client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        db = client["heavycult"]
        doc_count = db.ventas_analiticas.count_documents({})
        print(f"   ✅ Conexión exitosa a MongoDB Atlas!")
        print(f"   📊 Colección ventas_analiticas disponible con {doc_count:,} documentos.")
        client.close()
    except Exception as e:
        print(f"❌ Error al conectar con Atlas: {e}")

def main():
    imprimir_titulo("ORQUESTADOR DE INFRAESTRUCTURA Y CONFIGURACIÓN COMO CÓDIGO (IaC Setup)")
    validar_python()
    gestionar_entorno_virtual()
    instalar_dependencias()
    aprovisionar_data_lake()
    prueba_salud_atlas()
    imprimir_titulo("ENTORNO CONFIGURADO Y COMPLETAMENTE OPERATIVO 🎉")

if __name__ == "__main__":
    main()