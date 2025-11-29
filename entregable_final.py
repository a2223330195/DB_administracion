import os
import sys
import time
import threading
import shutil
import logging
import random
import pandas as pd
import numpy as np # type: ignore
from datetime import datetime, date, timedelta
from decimal import Decimal, ROUND_HALF_UP
import mysql.connector
import psutil
from mysql.connector import Error, pooling, cursor, MySQLConnection
from mysql.connector.abstracts import MySQLConnectionAbstract
from mysql.connector.pooling import PooledMySQLConnection
from tqdm import tqdm
import math
import json
import hashlib
import uuid
from itertools import combinations
import re
from typing import Dict, FrozenSet, List, Tuple, Any, Optional, Union

from concurrent.futures import ThreadPoolExecutor, as_completed
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

class Metrics:
    """Clase simple para centralizar la recolección de métricas de ejecución."""
    def __init__(self):
        self.error_counts: Dict[str, int] = {}

    def increment_error(self, error_type: str):
        self.error_counts[error_type] = self.error_counts.get(error_type, 0) + 1

    def log_summary(self):
        logging.info("--- Resumen de Métricas de Ejecución ---")
        if self.error_counts:
            logging.info(f"Errores por tipo: {json.dumps(self.error_counts)}")
        logging.info("-----------------------------------------")

def get_int_env(key: str, default: int) -> int:
    """Obtiene una variable de entorno como entero de forma segura."""
    value = os.getenv(key, str(default))
    try:
        return int(value)
    except (ValueError, TypeError):
        logging.warning(f"Variable de entorno '{key}' con valor no válido ('{value}'). Usando el valor por defecto: {default}.")
        return default

def get_float_env(key: str, default: float) -> float:
    """Obtiene una variable de entorno como flotante de forma segura."""
    value = os.getenv(key, str(default))
    try:
        return float(value)
    except (ValueError, TypeError):
        logging.warning(f"Variable de entorno '{key}' con valor no válido ('{value}'). Usando el valor por defecto: {default}.")
        return default

def calcular_batch_optimo() -> Tuple[int, int]:
    """
    Calcula el tamaño óptimo de los lotes para executemany basándose en la memoria disponible de la VM.
    """
    mem_disponible_gb = psutil.virtual_memory().available / (1024**3)
    logging.info(f"Memoria disponible en VM: {mem_disponible_gb:.2f} GB")

    # Valores base para los tamaños de lote de executemany, ajustados por la memoria de la VM
    if mem_disponible_gb > 8:
        base_batch_trans = 50000
        batch_tickets_for_executemany = 20000
    elif mem_disponible_gb > 4: # Para VMs con >4GB y <=8GB
        base_batch_trans = 25000
        batch_tickets_for_executemany = 10000
    else: # Para VMs de 4GB o menos (como Standard B2s)
        base_batch_trans = 15000
        batch_tickets_for_executemany = 5000
    
    logging.info(f"Batch optimizado para executemany: BATCH_TICKETS={batch_tickets_for_executemany}, BATCH_TRANSACCIONES={base_batch_trans}")
    return batch_tickets_for_executemany, base_batch_trans

# --- Configuración de la Base de Datos ---
DB_HOST = os.getenv("DB_HOST", "dbsql.mysql.database.azure.com")
DB_USER = os.getenv("DB_USER", "admin_db")
DB_PASSWORD = os.getenv("DB_PASSWORD", "TZvo987652004")
DB_NAME = os.getenv("DB_NAME", "productos_bd")
DB_PORT = get_int_env("DB_PORT", 3306)
# Añadir una advertencia de seguridad si se usa la contraseña por defecto.
if DB_PASSWORD == "TZvo987652004":
    logging.warning("⚠️ ADVERTENCIA DE SEGURIDAD: Se está utilizando la contraseña por defecto. "
                    "Para un entorno de producción, configure la variable de entorno DB_PASSWORD.")

POOL_SIZE = get_int_env("DB_POOL_SIZE", 6)
POOL_ENABLE = os.getenv("DB_POOL_ENABLE", "1") == "1"
DB_CONN_RETRIES = get_int_env("DB_CONN_RETRIES", 3)
DB_CONN_RETRY_DELAY = get_float_env("DB_CONN_RETRY_DELAY", 1.5)
DB_SSL_MODE = os.getenv("DB_SSL_MODE", "REQUIRED").upper() # Opciones: REQUIRED, DISABLED
SET_SQL_MODE = os.getenv("DB_SET_SQL_MODE", "1") == "1"

_pool: Optional[pooling.MySQLConnectionPool] = None

def _base_config() -> Dict[str, Any]:
    """Construye la configuración base de conexión, manejando SSL de forma flexible."""
    ssl_cert_path = os.getenv("DB_SSL_CA_PATH", "DigiCertGlobalRootCA.crt.pem")

    config = {
        "host": DB_HOST, "user": DB_USER, "password": DB_PASSWORD, "database": DB_NAME,
        "port": DB_PORT, "autocommit": False, "charset": "utf8mb4", "collation": "utf8mb4_unicode_ci"
    }

    if DB_SSL_MODE == "REQUIRED":
        config['ssl_disabled'] = False
        if os.path.exists(ssl_cert_path):
            config.update({'ssl_ca': ssl_cert_path, 'ssl_verify_identity': False})
            logging.info(f"SSL/TLS: REQUIRED. Certificado encontrado en '{ssl_cert_path}'.")
        else:
            logging.error(f"SSL/TLS: REQUIRED, pero el certificado no se encontró en '{ssl_cert_path}'.")
            raise FileNotFoundError(f"El modo SSL 'REQUIRED' está activado, pero el archivo de certificado no se encontró en la ruta: {ssl_cert_path}")
    elif DB_SSL_MODE == "DISABLED":
        config['ssl_disabled'] = True
        logging.warning("SSL/TLS: DISABLED. La conexión a la base de datos no será encriptada.")
    else:
        # Comportamiento por defecto (y más seguro) si se establece un valor no válido.
        logging.error(f"Valor de DB_SSL_MODE ('{DB_SSL_MODE}') no válido. Debe ser 'REQUIRED' o 'DISABLED'. Abortando.")
        raise ValueError("DB_SSL_MODE debe ser 'REQUIRED' o 'DISABLED'.")

    return config


_pool_lock = threading.Lock()

def _init_pool():
    global _pool
    with _pool_lock:
        if _pool or not POOL_ENABLE:
            return
        try:
            logging.info(f"Inicializando pool de conexiones 'POOL_DW' con tamaño {POOL_SIZE}.")
            _pool = pooling.MySQLConnectionPool(pool_name="POOL_DW", pool_size=POOL_SIZE, **_base_config())
        except Error as db_err:
            logging.error(f"No se pudo inicializar el pool de conexiones: {db_err}")
            _pool = None


def _post_connect(cn: Union[MySQLConnectionAbstract, PooledMySQLConnection]):
    cur = None
    try:
        cur = cn.cursor()
        cur.execute(
            "SET SESSION sql_mode='STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'")
    except Error as sql_mode_error:
        logging.warning(f"No se pudo establecer el sql_mode: {sql_mode_error}")
    finally:
        if cur: cur.close()


def crear_conexion() -> Union[MySQLConnection, PooledMySQLConnection]:
    _init_pool()
    last_err = None
    for attempt in range(DB_CONN_RETRIES):
        try:
            if _pool:
                cn = _pool.get_connection()
                if SET_SQL_MODE: # Aplicar también a conexiones del pool
                    _post_connect(cn)
            else: # Fallback a conexión directa si el pool está deshabilitado o falló al inicializar
                cn = mysql.connector.connect(**_base_config())
                if SET_SQL_MODE:
                    _post_connect(cn)
            return cn
        except Error as e:
            last_err = e
            logging.warning(f"Intento de conexión {attempt + 1}/{DB_CONN_RETRIES} fallido: {e}")
            if attempt < DB_CONN_RETRIES - 1:
                time.sleep(DB_CONN_RETRY_DELAY)

    logging.critical(f"No se pudo establecer conexión con la base de datos después de {DB_CONN_RETRIES} intentos.")
    raise last_err


metrics = Metrics()

class get_conexion:
    """Context manager para obtener y gestionar una conexión a la base de datos."""
    def __init__(self):
        self._cn: Optional[Union[MySQLConnection, PooledMySQLConnection]] = None

    def __enter__(self) -> Union[MySQLConnection, PooledMySQLConnection]:
        self._cn = crear_conexion()
        return self._cn

    def __exit__(self, exc_type, exc_val, exc_tb):
        # El cursor debe cerrarse explícitamente en el código que lo usa.
        # Aquí solo manejamos la conexión.
        if self._cn:
            if exc_type:
                metrics.increment_error("DB_TRANSACTION_ROLLBACK")
                logging.error(f"Excepción en 'with get_conexion', realizando rollback: {exc_val}")
                try:
                    self._cn.rollback()
                except Error as rb_err:
                    logging.error(f"Error durante el rollback: {rb_err}")
            else:
                self._cn.commit()
            self._cn.close()


# --- Configuración de Simulación (Entregable 1) ---
DIAS_SIMULACION = get_int_env("DIAS_SIMULACION", 730)
TICKETS_POR_DIA = get_int_env("TICKETS_POR_DIA", 4000)
PRODUCTOS_MIN = get_int_env("PRODUCTOS_MIN", 10)
PRODUCTOS_MAX = get_int_env("PRODUCTOS_MAX", 25)
COMMIT_INTERVAL_DIAS = get_int_env("COMMIT_INTERVAL_DIAS", 7)
SEED = get_int_env("SEED_SIMULACION", 42) # Procesar 30 días a la vez
FECHA_INICIO = os.getenv("FECHA_INICIO", "2022-01-01")
MAX_INTENTOS_PRODUCTO = get_int_env("MAX_INTENTOS_PRODUCTO", 300) # Construir una ruta absoluta al archivo CSV para mayor robustez.
# Esto asegura que el script encuentre el archivo sin importar desde qué directorio se ejecute.
script_dir = os.path.dirname(os.path.abspath(__file__))
default_csv_path = os.path.join(script_dir, "Productos.csv")
ARCHIVO_PRODUCTOS = os.getenv("CSV_PRODUCTOS", default_csv_path)

NUM_WORKERS = get_int_env("NUM_WORKERS", 2) # Ajustado para 2 vCPUs de Azure Standard_B2s
WORKER_RETRIES = get_int_env("WORKER_RETRIES", 2) # Número de reintentos para un worker que falla
WORKER_RETRY_DELAY = get_float_env("WORKER_RETRY_DELAY", 2.0) # Segundos de espera entre reintentos

# Calcular BATCH_TICKETS y BATCH_TRANSACCIONES dinámicamente
_BATCH_TICKETS_CALCULATED, _BATCH_TRANSACCIONES_CALCULATED = calcular_batch_optimo()
BATCH_TICKETS = get_int_env("BATCH_TICKETS", _BATCH_TICKETS_CALCULATED)
BATCH_TRANSACCIONES = get_int_env("BATCH_TRANSACCIONES", _BATCH_TRANSACCIONES_CALCULATED)
FRECUENCIAS = {"Muy Frecuente": 0.40, "Frecuente": 0.25, "Regular": 0.20, "Esporádicamente": 0.15}
ANCLA_CATS = set(os.getenv("CATS_ANCLA", "Electrónica,Juguetes,Hogar").split(","))


def _to_decimal_precio(x: Any) -> Decimal:
    if isinstance(x, (int, float, Decimal)):
        d = Decimal(str(x))
    else:
        d = Decimal(str(x).replace('$', '').replace(',', '').strip())
    return d.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)


def _executemany_lotes(cur: cursor.MySQLCursor, sql: str, data: List[Tuple], batch_size: int, desc: str):
    if not data: return
    with tqdm(total=len(data), desc=desc, unit="rec", disable=not sys.stdout.isatty()) as pbar:
        for i in range(0, len(data), batch_size):
            lote = data[i:i + batch_size]
            cur.executemany(sql, lote)
            pbar.update(len(lote))


def cargar_productos(cur: cursor.MySQLCursor):
    """Carga productos desde un archivo CSV a la base de datos."""
    if not os.path.exists(ARCHIVO_PRODUCTOS):
        raise FileNotFoundError(f"No se encontró el archivo CSV: {ARCHIVO_PRODUCTOS}")
    # Intentar leer con UTF-8 (estándar) y luego con latin-1 para manejar problemas de codificación.
    try:
        df = pd.read_csv(ARCHIVO_PRODUCTOS, encoding="utf-8")
    except UnicodeDecodeError:
        logging.warning("No se pudo leer el CSV como UTF-8, intentando con latin-1.")
        df = pd.read_csv(ARCHIVO_PRODUCTOS, encoding="latin-1")

    if df.empty:
        raise ValueError("El archivo CSV de productos está vacío o no contiene datos válidos.")
    
    # Normalización de columnas mejorada para manejar variaciones en espacios y mayúsculas.
    df.columns = [re.sub(r'\s+', '_', c.strip().lower()) for c in df.columns]

    requeridas = ["identificador", "producto", "precio", "categoria"]
    if not all(col in df.columns for col in requeridas):
        raise ValueError(f"Faltan columnas requeridas en el CSV: {requeridas}")

    if "frecuencia" not in df.columns:
        df["frecuencia"] = np.random.choice(list(FRECUENCIAS.keys()), size=len(df), p=list(FRECUENCIAS.values()))
    
    df['precio_decimal'] = df['precio'].apply(_to_decimal_precio)
    rows = df[['identificador', 'producto', 'precio_decimal', 'frecuencia', 'categoria']].values.tolist()

    if df.empty or not rows:
        raise ValueError("No se pudieron extraer productos válidos del archivo CSV. Verifique el formato de los datos.")
    
    sql = "INSERT INTO Productos (Identificador, Producto, Precio, Frecuencia, categoria) VALUES (%s, %s, %s, %s, %s)"
    _executemany_lotes(cur, sql, rows, BATCH_TRANSACCIONES, "Cargando productos")


def _get_asociados(productos: List[Tuple]) -> Dict[int, int]:
    """Crea un mapa de productos 'ancla' a productos 'asociados' para forzar patrones."""
    asociados = {}
    # Asegurar que k sea al menos 1 si hay productos, para evitar k=0 con pocos productos.
    num_productos = len(productos)
    if num_productos == 0: return {}
    
    k_asociados = max(1, num_productos // 5)
    for ancla_id in random.sample([p[0] for p in productos], k=min(k_asociados, num_productos)):
        # Elige un producto asociado que no sea él mismo
        candidatos = [p[0] for p in productos if p[0] != ancla_id]
        if candidatos:
            asociado_id = random.choice(candidatos)
            asociados[ancla_id] = asociado_id
    return asociados


def insertar_lotes_ventas(cn: Union[MySQLConnection, PooledMySQLConnection], cur: cursor.MySQLCursor, tickets_con_uuid: List[Tuple], detalles_por_uuid: Dict[str, List[Tuple]]):
    """Inserta un lote de tickets y sus transacciones, mapeando UUIDs a IDs de forma segura."""
    if not tickets_con_uuid: return
    
    _executemany_lotes(cur, "INSERT INTO Tickets (external_uuid, fecha, monto_total, cantidad_productos_vendidos) VALUES (%s, %s, %s, %s)",
                       tickets_con_uuid, BATCH_TICKETS, "Insertando Tickets")

    uuids_insertados = [t[0] for t in tickets_con_uuid]
    mapa_uuid_a_id = {}

    # Se elimina la lógica frágil basada en `lastrowid`.
    # Siempre se utiliza el método seguro de consultar los IDs a través de los UUIDs
    # para garantizar la integridad de los datos, especialmente en entornos concurrentes.
    if not uuids_insertados:
        return

    # El tamaño del lote para el IN es seguro porque está limitado por BATCH_TICKETS.
    placeholders = ', '.join(['%s'] * len(uuids_insertados))
    cur.execute(f"SELECT id, external_uuid FROM Tickets WHERE external_uuid IN ({placeholders})", uuids_insertados)
    for db_id, ext_uuid in cur.fetchall():
        mapa_uuid_a_id[ext_uuid] = db_id
    
    if not mapa_uuid_a_id:
        logging.warning("No se pudo mapear ningún UUID a ID de ticket después de la inserción. Se omitirán las transacciones.")
        return

    transacciones = []
    for ext_uuid, ticket_id in mapa_uuid_a_id.items():
        detalles = detalles_por_uuid.get(ext_uuid)
        if detalles:
            for prod_id, cant, precio_unitario in detalles:
                precio_total_linea = (precio_unitario * cant).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                transacciones.append((ticket_id, prod_id, cant, precio_total_linea))

    _executemany_lotes(cur,
                       "INSERT INTO Transacciones (id_ticket, producto_id, cantidad_productos, precio_venta) VALUES (%s, %s, %s, %s)",
                       transacciones, BATCH_TRANSACCIONES, "Insertando Transacciones")


def procesar_lote_dias(rango_dias: range, fecha_inicio: datetime, by_cat: Dict, anclas: List, cat_keys: List, productos_asociados: Dict, productos_por_id: Dict) -> int:
    """Función worker para generar datos de un rango de días en un hilo separado."""
    # Implementación de lógica de reintentos para el worker.
    for intento in range(WORKER_RETRIES + 1):
        cn_worker = None
        cur_worker = None
        try:
            cn_worker = crear_conexion()
            cur_worker = cn_worker.cursor()
            
            tickets_lote_con_uuid = []
            detalles_por_uuid = {}
            for i in rango_dias:
                fecha = fecha_inicio + timedelta(days=i)
                for _ in range(TICKETS_POR_DIA):
                    t, d = generar_tickets_v2(fecha, by_cat, anclas, cat_keys, productos_asociados, productos_por_id)
                    if t and d:
                        ticket_uuid = str(uuid.uuid4())
                        tickets_lote_con_uuid.append((ticket_uuid, t[0], t[1], t[2]))
                        detalles_por_uuid[ticket_uuid] = d
            
            insertar_lotes_ventas(cn_worker, cur_worker, tickets_lote_con_uuid, detalles_por_uuid)
            cn_worker.commit()
            return len(tickets_lote_con_uuid) # Éxito, devolver el conteo y salir del bucle de reintentos

        except Error as e:
            logging.warning(f"Error en worker de generación de datos (intento {intento + 1}/{WORKER_RETRIES + 1}): {e}")
            if intento < WORKER_RETRIES:
                time.sleep(WORKER_RETRY_DELAY)
            else:
                logging.error(f"El worker falló después de {WORKER_RETRIES + 1} intentos. Rango de días: {rango_dias.start}-{rango_dias.stop-1}.", exc_info=True)
                raise  # Re-lanzar la excepción para que el hilo principal la capture
        finally:
            if cur_worker:
                cur_worker.close()
            if cn_worker:
                if cn_worker.is_connected():
                    cn_worker.close()

    return 0 # No debería llegar aquí, pero es un fallback seguro.

total_tickets_generados = 0

def main_entregable_1():
    random.seed(SEED)
    np.random.seed(SEED) # type: ignore
    try:
        cn_prep, cur_prep = None, None
        try:
            cn_prep = crear_conexion()
            cur_prep = cn_prep.cursor()
            logging.info("Recreando tablas operacionales (Productos, Tickets, Transacciones)...")
            cur_prep.execute("SET FOREIGN_KEY_CHECKS=0")
            # Usar DROP/CREATE en lugar de TRUNCATE para asegurar la estructura correcta y evitar errores si las tablas no existen.
            cur_prep.execute("DROP TABLE IF EXISTS Transacciones")
            cur_prep.execute("DROP TABLE IF EXISTS Tickets")
            cur_prep.execute("DROP TABLE IF EXISTS Productos")

            # Se mueven las definiciones de las tablas operacionales al script para hacerlo más autónomo.
            cur_prep.execute("""
                CREATE TABLE Productos (
                    Identificador INT UNSIGNED PRIMARY KEY,
                    Producto VARCHAR(255) NOT NULL,
                    Precio DECIMAL(10,2) NOT NULL,
                    Frecuencia VARCHAR(50),
                    categoria VARCHAR(100) NOT NULL,
                    INDEX idx_categoria (categoria)
                ) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
            """)
            cur_prep.execute("""
                CREATE TABLE Tickets (
                    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    fecha DATETIME NOT NULL,
                    external_uuid CHAR(36) NOT NULL,
                    monto_total DECIMAL(12,2) NOT NULL,
                    cantidad_productos_vendidos INT UNSIGNED NOT NULL,
                    INDEX idx_fecha (fecha),
                    INDEX idx_monto_fecha (monto_total, fecha),
                    UNIQUE KEY uq_external_uuid (external_uuid)
                ) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
            """)
            cur_prep.execute("""
                CREATE TABLE Transacciones (
                    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    id_ticket INT UNSIGNED NOT NULL,
                    producto_id INT UNSIGNED NOT NULL,
                    cantidad_productos INT UNSIGNED NOT NULL,
                    precio_venta DECIMAL(10,2) NOT NULL,
                    FOREIGN KEY (id_ticket) REFERENCES Tickets(id) ON DELETE CASCADE,
                    FOREIGN KEY (producto_id) REFERENCES Productos(Identificador) ON DELETE CASCADE,
                    INDEX idx_prod_ticket (producto_id, id_ticket)
                ) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
            """)

            cur_prep.execute("SET FOREIGN_KEY_CHECKS=1")
            cn_prep.commit()

            logging.info("Cargando productos...")
            cargar_productos(cur_prep)
            cn_prep.commit()
            cur_prep.execute("SELECT Identificador, Producto, Precio, categoria, Frecuencia FROM Productos")
            productos = [(pid, nom, Decimal(str(p)), cat, FRECUENCIAS.get(freq, 0.15)) for pid, nom, p, cat, freq in cur_prep.fetchall()]
        finally:
            if cur_prep: cur_prep.close()
            if cn_prep: cn_prep.close()
        by_cat = {cat: [p for p in productos if p[3] == cat] for cat in set(p[3] for p in productos)}
        productos_asociados = _get_asociados(productos)
        productos_por_id = {p[0]: p for p in productos}
        anclas = [p for p in productos if p[3] in ANCLA_CATS]
        if not anclas:
            logging.warning(f"No se encontraron productos en las categorías ancla especificadas ({ANCLA_CATS}). Se usarán todos los productos como posibles anclas.")
            anclas = productos

        logging.info(f"Iniciando generación y carga de datos en paralelo con {NUM_WORKERS} workers, en chunks de {COMMIT_INTERVAL_DIAS} día(s).")
        fecha_inicio_dt = datetime.strptime(FECHA_INICIO, "%Y-%m-%d")

        global total_tickets_generados
        start_time_generation = time.time()
        for dia_inicio_chunk in tqdm(range(0, DIAS_SIMULACION, COMMIT_INTERVAL_DIAS), desc="Procesando chunks", disable=not sys.stdout.isatty()):

            dias_en_este_chunk = min(COMMIT_INTERVAL_DIAS, DIAS_SIMULACION - dia_inicio_chunk)
            
            dias_por_worker_sub_chunk = (dias_en_este_chunk + NUM_WORKERS - 1) // NUM_WORKERS
            total_tickets_en_chunk = 0
            with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
                futuros = []
                for i in range(NUM_WORKERS):
                    # Calcula el rango de días para este worker específico dentro del chunk actual
                    inicio_worker_dia = dia_inicio_chunk + i * dias_por_worker_sub_chunk
                    fin_worker_dia = min(inicio_worker_dia + dias_por_worker_sub_chunk, dia_inicio_chunk + dias_en_este_chunk)
                    
                    if inicio_worker_dia >= fin_worker_dia:
                        continue # Salta si este worker no tiene días que procesar en este sub-chunk
                    
                    rango_dias_worker = range(inicio_worker_dia, fin_worker_dia)
                    
                    futuro = executor.submit(
                        procesar_lote_dias, rango_dias_worker, fecha_inicio_dt, 
                        by_cat, anclas, list(by_cat.keys()), 
                        productos_asociados, productos_por_id
                    )
                    futuros.append(futuro)
                
                for futuro in as_completed(futuros):
                    try:
                        # Ya no se acumulan listas, solo se suma el conteo de tickets procesados.
                        tickets_procesados = futuro.result()
                        total_tickets_en_chunk += tickets_procesados
                        total_tickets_generados += tickets_procesados
                    except Exception as exc: # Si un worker falla, no detener todo el proceso.
                        metrics.increment_error("WORKER_FAILURE")
                        logging.error(f"Un worker de generación de datos falló: {exc}", exc_info=True)
                        # Se registra el error, pero se permite que los otros workers del chunk terminen.
                        # En un escenario crítico, se podría optar por 'raise' para detener todo.

            elapsed_time_minutes = (time.time() - start_time_generation) / 60
            tickets_per_minute = (total_tickets_generados / elapsed_time_minutes) if elapsed_time_minutes > 0 else 0
            # Se mueve el log fuera del bucle de futuros para que se muestre una sola vez por chunk.
            if total_tickets_en_chunk > 0:
                logging.info(f"Chunk completado. Tickets generados hasta ahora: {total_tickets_generados:,}. Tickets/minuto (promedio): {tickets_per_minute:.2f}")
        logging.info("Generación y carga de datos completada.")
        
        logging.info("Realizando verificación de integridad post-carga...")
        cn_val, cur_val = None, None
        try:
            cn_val = crear_conexion()
            cur_val = cn_val.cursor()
            cur_val.execute("SELECT COUNT(*) FROM Tickets")
            total_tickets = cur_val.fetchone()[0]
            cur_val.execute("SELECT COUNT(*) FROM Transacciones")
            total_trans = cur_val.fetchone()[0]
            
            promedio_prod = total_trans / total_tickets if total_tickets > 0 else 0
            logging.info(f"Validación: {total_tickets:,} tickets, {total_trans:,} líneas de transacción generadas (unidades totales).")
            logging.info(f"Promedio de unidades totales por ticket: {promedio_prod:.2f}")

            # Validar productos ÚNICOS, no cantidad total de unidades.
            cur_val.execute("""
                SELECT AVG(productos_unicos) as prom_unicos
                FROM (
                    SELECT id_ticket, COUNT(DISTINCT producto_id) as productos_unicos
                    FROM Transacciones
                    GROUP BY id_ticket
                ) sub
            """)
            promedio_unicos = cur_val.fetchone()[0]
            logging.info(f"Promedio de productos únicos por ticket: {promedio_unicos:.2f}" if promedio_unicos is not None else "Promedio de productos únicos por ticket: N/A")

            # La cantidad total promedio será mayor (por las cantidades). El rango esperado de productos únicos es más flexible.
            if promedio_unicos and not (8 <= promedio_unicos <= 20):
                logging.warning(f"⚠️ El promedio de productos únicos por ticket ({promedio_unicos:.2f}) está fuera del rango esperado [8, 20].")
                
            # Verificaciones de integridad adicionales
            cur_val.execute("SELECT COUNT(*) FROM Tickets WHERE cantidad_productos_vendidos = 0")
            if cur_val.fetchone()[0] > 0:
                logging.error("❌ Error de integridad: Se encontraron tickets con 0 productos vendidos.")

            cur_val.execute("SELECT COUNT(*) FROM Transacciones t LEFT JOIN Tickets tk ON t.id_ticket = tk.id WHERE tk.id IS NULL")
            if cur_val.fetchone()[0] > 0:
                logging.error("❌ Error de integridad: Se encontraron transacciones huérfanas (sin ticket asociado).")
        finally:
            if cur_val: cur_val.close()
            if cn_val: cn_val.close()
    except (Error, FileNotFoundError, ValueError) as err: # Capturar excepciones específicas y detener la ejecución.
        metrics.increment_error("ENTREGABLE_1_CRITICAL")
        logging.critical(f"❌ Error crítico en Entregable_1 (verifique la conexión a la BD y la existencia de '{ARCHIVO_PRODUCTOS}'): {err}", exc_info=True)
        raise RuntimeError("Deteniendo ejecución debido a un error crítico en la carga de datos.") from err
    finally:
        # Poblar dim_tiempo aquí para que esté disponible para el Entregable 2 (Apriori).
        # Esto resuelve la dependencia circular donde Apriori necesitaba dim_tiempo antes de que el SP la creara.
        logging.info("Poblando la dimensión de tiempo (dim_tiempo) post-generación de datos...")
        _poblar_dim_tiempo()

def _poblar_dim_tiempo():
    """
    Puebla la tabla dim_tiempo basándose en las fechas únicas de la tabla Tickets.
    Esta función es idempotente y solo insertará fechas que no existan.
    """
    with get_conexion() as cn:
        with cn.cursor() as cur:
            try:
                # La tabla dim_tiempo se crea en el script SQL, aquí solo la poblamos.
                # Se reemplaza INSERT IGNORE por ON DUPLICATE KEY UPDATE para no silenciar errores inesperados.
                # Esto solo ignora errores de clave duplicada, pero fallará si hay otros problemas (ej. tipos de datos).
                sql_poblar_dim = """
                INSERT INTO dim_tiempo (fecha_key, fecha, anio, mes, dia, dia_semana, nombre_mes, nombre_dia_semana, trimestre, semana_anio, es_fin_semana, periodo_anio, temporada)
                SELECT 
                    CAST(DATE_FORMAT(t.fecha_unica, '%Y%m%d') AS UNSIGNED),
                    t.fecha_unica, YEAR(t.fecha_unica), MONTH(t.fecha_unica), DAY(t.fecha_unica), DAYOFWEEK(t.fecha_unica),
                    CASE MONTH(t.fecha_unica) WHEN 1 THEN 'Enero' WHEN 2 THEN 'Febrero' WHEN 3 THEN 'Marzo' WHEN 4 THEN 'Abril' WHEN 5 THEN 'Mayo' WHEN 6 THEN 'Junio' WHEN 7 THEN 'Julio' WHEN 8 THEN 'Agosto' WHEN 9 THEN 'Septiembre' WHEN 10 THEN 'Octubre' WHEN 11 THEN 'Noviembre' ELSE 'Diciembre' END,
                    CASE DAYOFWEEK(t.fecha_unica) WHEN 1 THEN 'Domingo' WHEN 2 THEN 'Lunes' WHEN 3 THEN 'Martes' WHEN 4 THEN 'Miércoles' WHEN 5 THEN 'Jueves' WHEN 6 THEN 'Viernes' ELSE 'Sábado' END,
                    QUARTER(t.fecha_unica), WEEK(t.fecha_unica, 1),
                    CASE WHEN DAYOFWEEK(t.fecha_unica) IN (1,7) THEN 1 ELSE 0 END,
                    CONCAT(YEAR(t.fecha_unica), '-', LPAD(MONTH(t.fecha_unica), 2, '0')),
                    CASE WHEN MONTH(t.fecha_unica) IN (12,1,2) THEN 'Invierno' WHEN MONTH(t.fecha_unica) IN (3,4,5) THEN 'Primavera' WHEN MONTH(t.fecha_unica) IN (6,7,8) THEN 'Verano' ELSE 'Otoño' END
                FROM (SELECT DISTINCT DATE(fecha) as fecha_unica FROM Tickets) AS t 
                ON DUPLICATE KEY UPDATE fecha_key=VALUES(fecha_key)
                """
                cur.execute(sql_poblar_dim)
                logging.info(f"{cur.rowcount} nuevas fechas añadidas a dim_tiempo.")
            except Error as e:
                logging.error(f"No se pudo poblar dim_tiempo: {e}")
                raise

MAX_INTENTOS_TICKET = 100
def generar_tickets_v2(fecha: datetime, by_cat: Dict, anclas: List, cat_keys: List, productos_asociados: Dict[int, int], productos_por_id: Dict) -> Tuple[
    Optional[Tuple], Optional[List[Tuple]]]:
    """
    Genera tickets simulando una "misión de compra" para hacer los datos más realistas
    a pesar de los altos valores de PRODUCTOS_MIN y PRODUCTOS_MAX.
    """
    max_posible = min(PRODUCTOS_MAX, sum(len(v) for v in by_cat.values()))
    if max_posible < PRODUCTOS_MIN:
        logging.warning(f"No hay suficientes productos únicos disponibles ({max_posible}) para cumplir el mínimo ({PRODUCTOS_MIN}). Deteniendo la generación para este ticket.")
        return None, None

    intentos = 0
    while intentos < MAX_INTENTOS_TICKET:
        
        intentos += 1
        ancla = random.choice(anclas) if anclas else None
        if not ancla:
            logging.warning("La lista de productos 'ancla' está vacía. No se pueden generar tickets.")
            return None, None

        productos_en_ticket = {ancla[0]}
        detalles_ticket = [(ancla[0], random.randint(1, 2), ancla[2])]

        if random.random() < 0.90 and ancla[0] in productos_asociados:
            asociado_id = productos_asociados[ancla[0]]
            if asociado_id not in productos_en_ticket:
                prod_asociado = productos_por_id[asociado_id]
                productos_en_ticket.add(asociado_id)
                detalles_ticket.append((asociado_id, random.randint(1, 2), prod_asociado[2]))

        cat_ancla = ancla[3]
        candidatos_cat_ancla = [p for p in by_cat.get(cat_ancla, []) if p[0] not in productos_en_ticket]
        if candidatos_cat_ancla:
            num_extra_en_cat = random.randint(1, min(3, len(candidatos_cat_ancla)))
            productos_extra = random.sample(candidatos_cat_ancla, k=num_extra_en_cat)
            for p_extra in productos_extra:
                productos_en_ticket.add(p_extra[0])
                detalles_ticket.append((p_extra[0], 1, p_extra[2]))

        n_productos = random.randint(PRODUCTOS_MIN, max_posible)

        while len(detalles_ticket) < n_productos:
            if random.random() < 0.30:
                cat_seleccionada = random.choice([c for c in cat_keys if c != cat_ancla])
            else:
                cat_seleccionada = cat_ancla
            
            candidatos_ids = set(p[0] for p in by_cat.get(cat_seleccionada, [])) - productos_en_ticket
            candidatos = [productos_por_id[pid] for pid in candidatos_ids if pid in productos_por_id]

            if not candidatos:
                cat_alternativa = random.choice(cat_keys)
                candidatos_ids_alt = set(p[0] for p in by_cat.get(cat_alternativa, [])) - productos_en_ticket
                candidatos = [productos_por_id[pid] for pid in candidatos_ids_alt if pid in productos_por_id]
                if not candidatos:
                    break

            try:
                # Añadir bloque try-except para manejar el caso en que 'candidatos' esté vacío,
                # lo que causaría un IndexError en random.choices.
                pesos = [c[4] for c in candidatos]
                producto_elegido = random.choices(candidatos, weights=pesos, k=1)[0]
                productos_en_ticket.add(producto_elegido[0])
                detalles_ticket.append((producto_elegido[0], 1, producto_elegido[2]))
            except (ValueError, IndexError):
                continue
        
        if len(detalles_ticket) >= PRODUCTOS_MIN:
            monto_total = sum((p * c).quantize(Decimal('0.01'), ROUND_HALF_UP) for _, c, p in detalles_ticket)
            cantidad_total = sum(c for _, c, _ in detalles_ticket)
            if monto_total > 0 and cantidad_total > 0:
                return (fecha, monto_total, cantidad_total), detalles_ticket

    logging.warning(f"No se pudo generar un ticket válido después de {MAX_INTENTOS_TICKET} intentos.")
    return None, None

# =====================================================================
# --- Configuración de Apriori (Entregable 2) ---
MIN_CONFIANZA = get_float_env("APR_MIN_CONF", 0.005) # Ajustado: por defecto más permisivo para no filtrar reglas con confianza baja
MAX_TAMANO_CONJUNTO = get_int_env("APR_MAX_TAM", 3)
# Aumentar límites para el cálculo k=3 con precaución en una VM Standard D2s_v3 (2 vCPU, 8GB RAM)
LIMITE_L2_PARA_K3 = get_int_env("APR_L2_LIMIT_K3", 800000)
LIMITE_CANDIDATOS_K3 = get_int_env("APR_C3_CANDIDATE_LIMIT", 2500000) # Mayor tolerancia para candidatos C3
TAMANO_LOTE_INSERCION = get_int_env("APR_BATCH_INSERT", 40000) # Ligeramente menor para lotes más pequeños en memoria limitada
DESACTIVAR_K3 = os.getenv("APR_DISABLE_K3", "0") == "1"
TABLA_TRANSACCIONES = "Transacciones"
TABLA_FRECUENTES_DW = "frecuentes_apriori"
TABLA_REGLAS_DW = "reglas_apriori"
MIN_SOPORTE_RELATIVO = get_float_env("APR_MIN_SOP", 0.000025) # AJUSTADO: Reducido aún más para simulación de 730 días

DiccionarioSoportes = Dict[FrozenSet[int], int]


def generar_hash_estable(itemset_json_text: str) -> str:
    return hashlib.md5(itemset_json_text.encode('utf-8')).hexdigest()

def main_entregable_2():
    try:
        cn, cur = None, None
        try:
            cn = crear_conexion()
            cur = cn.cursor()
            preparar_tablas_apriori(cur)
            cn.commit()
            cur.execute("SELECT COUNT(DISTINCT id) FROM Tickets")
            total_tickets = cur.fetchone()[0]
            if not total_tickets:
                logging.error("No hay transacciones para analizar. Ejecute el Entregable 1 primero.")
                return

            min_soporte_abs = math.ceil(MIN_SOPORTE_RELATIVO * total_tickets)
            # Se usa el formateador de porcentaje (`.4%`) para consistencia y legibilidad.
            logging.info(f"Total de tickets en BD: {total_tickets:,}. Usando soporte relativo de: {MIN_SOPORTE_RELATIVO:.4%}")
            
            s1, s2, s3, total_tickets_analizados = contar_conjuntos_frecuentes_sql(cur, min_soporte_abs, total_tickets)
            soportes_totales = {**s1, **s2, **s3}

            datos_frecuentes_dw = [(json.dumps(sorted(list(k)), separators=(',', ':')), len(k), v, v / total_tickets_analizados, # El hash se genera a partir del texto JSON.
                                    generar_hash_estable(json.dumps(sorted(list(k)), separators=(',', ':')))) for k, v in
                                   soportes_totales.items()]
            sql_dw_frec = f"""
                INSERT INTO {TABLA_FRECUENTES_DW} (itemset_text, tam_itemset, soporte_abs, soporte_rel, itemset_hash) VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE soporte_abs=VALUES(soporte_abs), soporte_rel=VALUES(soporte_rel), fecha_calculo=NOW()
            """
            _executemany_lotes(cur, sql_dw_frec, datos_frecuentes_dw, TAMANO_LOTE_INSERCION,
                               f"Actualizando {TABLA_FRECUENTES_DW}")

            sample_rate = get_float_env("APR_SAMPLE_RATE", 0.2)
            generar_y_guardar_reglas(cur, soportes_totales, total_tickets_analizados, sample_rate)
            cn.commit()
        finally:
            if cur: cur.close()
            if cn: cn.close()
    except Error as db_err:
        metrics.increment_error("ENTREGABLE_2_MYSQL_ERROR")
        logging.error(f"❌ Error de MySQL en Entregable_2: {db_err}")

def preparar_tablas_apriori(cur: cursor.MySQLCursor):
    logging.info("Verificando y/o creando tablas de destino para Apriori...")
    logging.info(f"Limpiando tablas Apriori: {TABLA_FRECUENTES_DW} y {TABLA_REGLAS_DW}.")
    # Separar en sentencias individuales.
    cur.execute(f"TRUNCATE TABLE {TABLA_REGLAS_DW}")
    cur.execute(f"TRUNCATE TABLE {TABLA_FRECUENTES_DW}")


def contar_conjuntos_frecuentes_sql(cur: cursor.MySQLCursor, min_abs: int, total_tickets_global: int) -> Tuple[
    DiccionarioSoportes, DiccionarioSoportes, DiccionarioSoportes, int]:
    # Implementación robusta usando tablas de staging no-temporales con sufijo por ejecución
    SAMPLE_RATE = get_float_env("APR_SAMPLE_RATE", 0.15) # Muestrear 15% por defecto para reducir carga pero mantener señal
    min_abs_ajustado = min_abs
    logging.info("Iniciando conteo de k=1 (productos individuales)...")
    cur.execute(
        f"SELECT producto_id, COUNT(DISTINCT id_ticket) FROM {TABLA_TRANSACCIONES} GROUP BY producto_id HAVING COUNT(DISTINCT id_ticket) >= {min_abs}")
    soportes_k1 = {frozenset([int(pid)]): int(c) for pid, c in cur.fetchall()}
    logging.info(f"Se encontraron {len(soportes_k1):,} conjuntos frecuentes de tamaño 1.")

    logging.info("Iniciando conteo de k=2 (pares de productos)...")
    # Generar sufijo único por ejecución para evitar colisiones y problemas de concurrencia
    run_suffix = uuid.uuid4().hex[:8]
    tbl_trans = f"staging_trans_unicas_{run_suffix}"
    tbl_trans_read = f"staging_trans_unicas_read_{run_suffix}"
    tbl_L2 = ''
    tbl_C3 = ''

    try:
        # Crear tabla de staging (NO TEMPORARY) y poblarla
        cur.execute(f"DROP TABLE IF EXISTS {tbl_trans}")
        cur.execute(f"""
            CREATE TABLE {tbl_trans} (
                id_ticket INT UNSIGNED,
                producto_id INT UNSIGNED,
                PRIMARY KEY (id_ticket, producto_id)
            ) ENGINE=InnoDB
        """)

        if SAMPLE_RATE < 1.0:
            min_abs_ajustado = max(1, int(min_abs * SAMPLE_RATE))
            cur.execute("SELECT COUNT(*) FROM dim_tiempo")
            dim_tiempo_tiene_datos = cur.fetchone()[0] > 0
            if dim_tiempo_tiene_datos:
                logging.warning(f"APRIORI: Usando una muestra ESTRATIFICADA del {SAMPLE_RATE:.0%} de los tickets (por temporada) para el análisis.")
                logging.info(f"Soporte absoluto mínimo para la muestra: {min_abs_ajustado} (calculado a partir del soporte global de {min_abs})")
                cur.execute(f"""
                    INSERT INTO {tbl_trans} (id_ticket, producto_id)
                    WITH TicketsConTemporada AS (
                        SELECT 
                            t.id,
                            dt.temporada
                        FROM Tickets t
                        JOIN dim_tiempo dt ON CAST(DATE_FORMAT(t.fecha, '%Y%m%d') AS UNSIGNED) = dt.fecha_key
                    ),
                    TicketsMuestreados AS (
                        SELECT 
                            id,
                            ROW_NUMBER() OVER (PARTITION BY temporada ORDER BY RAND()) as rn,
                            COUNT(*) OVER (PARTITION BY temporada) as total_en_estrato
                        FROM TicketsConTemporada
                    )
                    SELECT 
                        trans.id_ticket, 
                        trans.producto_id
                    FROM {TABLA_TRANSACCIONES} trans
                    JOIN TicketsMuestreados s ON trans.id_ticket = s.id
                    WHERE s.rn <= s.total_en_estrato * %(sample_rate)s
                """, {'sample_rate': SAMPLE_RATE})
            else:
                logging.warning("APRIORI: La tabla 'dim_tiempo' está vacía. Realizando muestreo aleatorio simple.")
                cur.execute(f"INSERT INTO {tbl_trans} (id_ticket, producto_id) SELECT id_ticket, producto_id FROM {TABLA_TRANSACCIONES} WHERE RAND() < %(sample_rate)s", {'sample_rate': SAMPLE_RATE})
        else:
            cur.execute(f"INSERT INTO {tbl_trans} (id_ticket, producto_id) SELECT DISTINCT id_ticket, producto_id FROM {TABLA_TRANSACCIONES};")

        cur.execute(f"SELECT COUNT(DISTINCT id_ticket) FROM {tbl_trans}")
        total_tickets_analizados = cur.fetchone()[0]

        # Crear una copia materializada de solo lectura para joins complejos (no temporal)
        cur.execute(f"DROP TABLE IF EXISTS {tbl_trans_read}")
        cur.execute(f"CREATE TABLE {tbl_trans_read} ENGINE=InnoDB AS SELECT id_ticket, producto_id FROM {tbl_trans}")
        # Crear índice para acelerar joins posteriores (mejora la performance en conteos k=2/k=3)
        try:
            cur.execute(f"CREATE INDEX idx_{tbl_trans_read}_ticket_prod ON {tbl_trans_read} (id_ticket, producto_id)")
        except Exception:
            # No crítico: si el índice falla (p. ej. por permisos), continuar sin él
            logging.debug(f"No se pudo crear índice en {tbl_trans_read}; continuando sin índice.")
        logging.info(f"{tbl_trans_read} creado correctamente; usando copia de sólo-lectura para joins.")

        if SAMPLE_RATE < 1.0:
            logging.info(f"La muestra contiene {total_tickets_analizados:,} tickets (aprox. {total_tickets_analizados/total_tickets_global:.2%} de la población total).")

        # K=2 usando la tabla staging de solo-lectura
        sql_table_for_k2 = tbl_trans_read
        sql_k2 = f"""
            SELECT t1.producto_id, t2.producto_id, COUNT(t1.id_ticket) AS soporte_conjunto
            FROM {sql_table_for_k2} t1
            JOIN (SELECT id_ticket, producto_id FROM {sql_table_for_k2}) t2 ON t1.id_ticket = t2.id_ticket AND t1.producto_id < t2.producto_id
            GROUP BY t1.producto_id, t2.producto_id
            HAVING soporte_conjunto >= %(min_soporte)s
        """
        cur.execute(sql_k2, {'min_soporte': min_abs_ajustado})
        soportes_k2 = {frozenset([int(a), int(b)]): int(c) for a, b, c in cur.fetchall()}
        logging.info(f"Se encontraron {len(soportes_k2):,} conjuntos frecuentes de tamaño 2.")

        # K=3
        soportes_k3 = {}
        if MAX_TAMANO_CONJUNTO >= 3 and soportes_k2 and not DESACTIVAR_K3:
            num_pares_l2 = len(soportes_k2)
            if num_pares_l2 > LIMITE_L2_PARA_K3:
                logging.warning(f"Se omite cálculo de k=3. El número de pares frecuentes ({num_pares_l2:,}) excede el límite ({LIMITE_L2_PARA_K3:,}).")
                return soportes_k1, soportes_k2, soportes_k3, total_tickets_analizados

            logging.info("Generando y evaluando candidatos para k=3...")
            tbl_L2 = f"staging_L2_{run_suffix}"
            tbl_C3 = f"staging_C3_{run_suffix}"
            cur.execute(f"DROP TABLE IF EXISTS {tbl_L2}")
            cur.execute(f"CREATE TABLE {tbl_L2} (prod_a INT, prod_b INT, INDEX(prod_a, prod_b)) ENGINE=InnoDB")
            l2_data = [(sorted(list(par))[0], sorted(list(par))[1]) for par in soportes_k2.keys()]
            _executemany_lotes(cur, f"INSERT INTO {tbl_L2} (prod_a, prod_b) VALUES (%s, %s)", l2_data, 50000, "Cargando L2 en tabla staging")

            cur.execute(f"SELECT COUNT(*) FROM {tbl_L2} l1 JOIN {tbl_L2} l2 ON l1.prod_a = l2.prod_a AND l1.prod_b < l2.prod_b")
            num_candidatos_c3 = cur.fetchone()[0]
            if num_candidatos_c3 > LIMITE_CANDIDATOS_K3:
                logging.warning(f"Se omite cálculo de k=3. El número de tríos candidatos ({num_candidatos_c3:,}) excede el límite de seguridad ({LIMITE_CANDIDATOS_K3:,}).")
                return soportes_k1, soportes_k2, soportes_k3, total_tickets_analizados

            if num_candidatos_c3 == 0:
                logging.info("No se generaron candidatos para k=3. Finalizando búsqueda de conjuntos frecuentes.")
                return soportes_k1, soportes_k2, soportes_k3, total_tickets_analizados

            logging.info(f"Iniciando conteo de k=3 (tríos de productos) para {num_candidatos_c3:,} candidatos...")
            cur.execute(f"DROP TABLE IF EXISTS {tbl_C3}")
            cur.execute(f"CREATE TABLE {tbl_C3} (p1 INT UNSIGNED, p2 INT UNSIGNED, p3 INT UNSIGNED, PRIMARY KEY (p1,p2,p3)) ENGINE=InnoDB")
            cur.execute(f"INSERT INTO {tbl_C3} (p1, p2, p3) SELECT l1.prod_a, l1.prod_b, l2.prod_b FROM {tbl_L2} l1 JOIN {tbl_L2} l2 ON l1.prod_a = l2.prod_a AND l1.prod_b < l2.prod_b")

            sql_k3_select = f"""
                SELECT c.p1, c.p2, c.p3, COUNT(DISTINCT t1.id_ticket) AS soporte
                FROM {tbl_C3} c
                JOIN {tbl_trans_read} t1 ON t1.producto_id = c.p1
                JOIN {tbl_trans_read} t2 ON t2.id_ticket = t1.id_ticket AND t2.producto_id = c.p2
                JOIN {tbl_trans_read} t3 ON t3.id_ticket = t1.id_ticket AND t3.producto_id = c.p3
                GROUP BY c.p1, c.p2, c.p3
                HAVING soporte >= %(min_soporte)s
            """
            try:
                cur.execute(sql_k3_select, {'min_soporte': min_abs_ajustado})
                soportes_k3 = {frozenset([int(p1), int(p2), int(p3)]): int(s) for p1, p2, p3, s in cur.fetchall()}
                logging.info(f"Se encontraron {len(soportes_k3):,} conjuntos frecuentes de tamaño 3.")
            except Exception as e:
                logging.warning(f"Fallo conteo k=3 usando JOIN completo: {e}. Intentando fallback por batches.")
                # Fallback por batches: mantener la lógica previa pero apuntando a las tablas staging
                C3_BATCH = get_int_env("APR_C3_CANDIDATE_BATCH", 2000)
                cur.execute(f"SELECT p1, p2, p3 FROM {tbl_C3}")
                candidatos = cur.fetchall()
                soportes_k3 = {}
                if candidatos:
                    total_cand = len(candidatos)
                    logging.info(f"Procesando {total_cand:,} candidatos C3 en batches de {C3_BATCH}...")
                    for i in range(0, total_cand, C3_BATCH):
                        batch = candidatos[i:i+C3_BATCH]
                        union_parts = []
                        for (p1, p2, p3) in batch:
                            part = (
                                f"SELECT {int(p1)} AS p1, {int(p2)} AS p2, {int(p3)} AS p3, COUNT(*) AS soporte FROM ("
                                f"SELECT id_ticket FROM {tbl_trans} WHERE producto_id IN ({int(p1)},{int(p2)},{int(p3)}) "
                                f"GROUP BY id_ticket HAVING COUNT(DISTINCT producto_id) = 3) q"
                            )
                            union_parts.append(part)
                        sql_batch = " UNION ALL ".join(union_parts)
                        try:
                            cur.execute(sql_batch)
                            rows = cur.fetchall()
                            for p1, p2, p3, s in rows:
                                if int(s) >= min_abs_ajustado:
                                    soportes_k3[frozenset([int(p1), int(p2), int(p3)])] = int(s)
                        except Exception as batch_err:
                            logging.warning(f"Fallo procesando batch C3 (índice {i}): {batch_err}")
                            for (p1, p2, p3) in batch:
                                try:
                                    sql_single = (
                                        f"SELECT COUNT(*) FROM (SELECT id_ticket FROM {tbl_trans} "
                                        f"WHERE producto_id IN ({int(p1)},{int(p2)},{int(p3)}) "
                                        f"GROUP BY id_ticket HAVING COUNT(DISTINCT producto_id) = 3) q"
                                    )
                                    cur.execute(sql_single)
                                    cnt = cur.fetchone()[0]
                                    if int(cnt) >= min_abs_ajustado:
                                        soportes_k3[frozenset([int(p1), int(p2), int(p3)])] = int(cnt)
                                except Exception:
                                    logging.debug(f"No se pudo contar candidato C3 ({p1},{p2},{p3}) en fallback individual.")
                    logging.info(f"Se encontraron {len(soportes_k3):,} conjuntos frecuentes de tamaño 3 (fallback por batches).")

        return soportes_k1, soportes_k2, soportes_k3, total_tickets_analizados

    finally:
        logging.info("Limpiando tablas staging de Apriori...")
        tablas_a_limpiar = [tbl_trans, tbl_trans_read, tbl_L2, tbl_C3]
        for tabla in tablas_a_limpiar:
            try:
                if tabla:
                    cur.execute(f"DROP TABLE IF EXISTS {tabla}")
            except Error as e:
                logging.debug(f"Error no crítico al limpiar tabla staging {tabla}: {e}")


def generar_y_guardar_reglas(cur: cursor.MySQLCursor, soportes_totales: DiccionarioSoportes, total_trans_analizados: int, sample_rate: float):
    logging.info("Generando reglas de asociación...")
    conjuntos_para_reglas = {k: v for k, v in soportes_totales.items() if len(k) >= 2}
    sql_dw = f"""
        INSERT INTO {TABLA_REGLAS_DW} (antecedente_text, consecuente_text, antecedente_hash, consecuente_hash, soporte_abs, soporte_rel, confianza, lift, conviccion, leverage) 
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE 
            soporte_abs=IF(soporte_abs <> VALUES(soporte_abs), VALUES(soporte_abs), soporte_abs),
            soporte_rel=VALUES(soporte_rel),
            confianza=VALUES(confianza),
            lift=VALUES(lift),
            conviccion=VALUES(conviccion),
            leverage=VALUES(leverage),
            fecha_calculo=NOW()
    """

    batch: List[Tuple] = []
    BATCH_SIZE = TAMANO_LOTE_INSERCION or 5000

    for conjunto, cont_soporte in tqdm(conjuntos_para_reglas.items(), desc="Generando reglas", disable=not sys.stdout.isatty()):
        soporte_conjunto_rel = cont_soporte / total_trans_analizados
        items = list(conjunto)
        for i in range(1, len(items)):
            for antecedente_tuple in combinations(items, i):
                antecedente = frozenset(antecedente_tuple)
                consecuente = conjunto - antecedente
                soporte_antecedente = soportes_totales.get(antecedente)
                if not soporte_antecedente:
                    continue

                confianza = cont_soporte / soporte_antecedente
                if confianza < MIN_CONFIANZA:
                    continue

                soporte_consecuente = soportes_totales.get(consecuente)
                if not soporte_consecuente:
                    continue

                lift = confianza / (soporte_consecuente / total_trans_analizados) if soporte_consecuente > 0 else 0
                leverage = soporte_conjunto_rel - (soporte_antecedente / total_trans_analizados) * (
                        soporte_consecuente / total_trans_analizados)
                conviction = (1 - (soporte_consecuente / total_trans_analizados)) / (1 - confianza) if confianza < 0.999999 else 999999.0

                lhs_txt = json.dumps(sorted(list(antecedente)), separators=(',', ':'))
                rhs_txt = json.dumps(sorted(list(consecuente)), separators=(',', ':'))

                # Si se usó muestreo, extrapolar el soporte absoluto al total de la población usando ceil
                if sample_rate < 1.0 and sample_rate > 0:
                    soporte_abs_final = int(math.ceil(cont_soporte / sample_rate))
                else:
                    soporte_abs_final = cont_soporte

                ant_hash = hashlib.md5(lhs_txt.encode('utf-8')).hexdigest()
                con_hash = hashlib.md5(rhs_txt.encode('utf-8')).hexdigest()

                batch.append((lhs_txt, rhs_txt, ant_hash, con_hash, soporte_abs_final, soporte_conjunto_rel, confianza, lift, conviction, leverage))

                if len(batch) >= BATCH_SIZE:
                    _executemany_lotes(cur, sql_dw, batch, BATCH_SIZE, f"Actualizando {TABLA_REGLAS_DW} (batch)")
                    batch = []

    # Insertar restantes
    if batch:
        _executemany_lotes(cur, sql_dw, batch, BATCH_SIZE, f"Actualizando {TABLA_REGLAS_DW} (final)")

    # Confirmar y loguear número de reglas persistidas en la BD para facilitar diagnóstico
    try:
        cur.execute(f"SELECT COUNT(*) FROM {TABLA_REGLAS_DW}")
        total_rules = cur.fetchone()[0]
        logging.info(f"Reglas Apriori persistidas en BD: {total_rules}")
    except Error as e:
        logging.warning(f"No se pudo obtener el conteo de {TABLA_REGLAS_DW}: {e}")

# =====================================================================
# --- Configuración del Ejecutor DWH (Entregable 5) ---
MAX_EXECUTION_TIME_SEC = get_int_env("MAX_EXEC_TIME_SEC", 64800) # AJUSTADO: Aumentado a 18 horas para simulación de 730 días
POLL_INTERVAL_SEC = get_float_env("POLL_INTERVAL_SEC", 5.0)
DISK_CHECK_PATH = os.getenv("DISK_CHECK_PATH", "..")
DISK_WARN_GB = get_int_env("DISK_WARN_GB", 5)
DISK_CRIT_GB = get_int_env("DISK_CRIT_GB", 1)
DISK_CHECK_ENABLED = os.getenv("DISK_CHECK_ENABLED", "1") == "1" # Activado por defecto

SP_NOMBRE = os.getenv("SP_NOMBRE", "sp_procesar_casos_de_uso_v2")

def get_free_space_gb(path: str) -> float:
    try:
        _, _, free = shutil.disk_usage(path)
        return free / (1024 ** 3)
    except Exception as e:
        logging.warning(f"No se pudo verificar espacio en disco: {e}")
        return float('inf')

def consultar_progreso_con_reconexion(max_reintentos: int = 3) -> Optional[Dict[str, Any]]:
    """
    Consulta el progreso de la ejecución del SP, manejando reconexiones si la conexión se ha perdido.
    Esto es crucial para monitoreos largos en plataformas como Azure que cierran conexiones inactivas.
    """
    last_err = None
    for intento in range(max_reintentos):
        conn, cur = None, None
        try:
            # Siempre obtener una nueva conexión para cada consulta de monitoreo.
            # Esto es más robusto que intentar mantener y reconectar una conexión de larga duración.
            conn = crear_conexion()
            cur = conn.cursor(dictionary=True) # Obtener todos los campos para un monitoreo más detallado.
            cur.execute("SELECT paso_actual, indice, total, pct FROM dw_progreso_runtime WHERE id = 1")
            return cur.fetchone()
        except Error as e:
            last_err = e
            logging.warning(f"Error al consultar progreso (intento {intento + 1}): {e}. Reintentando en 2s...")
            if intento == max_reintentos - 1:
                logging.error(f"No se pudo recuperar la conexión de monitoreo después de {max_reintentos} intentos.")
                raise last_err
            time.sleep(2)
        finally:
            if cur:
                cur.close()
            if conn and conn.is_connected():
                conn.close()
    return None

def ejecutar_sp_en_hilo(holder: Dict, sp_params: Tuple):
    """Ejecuta el Stored Procedure principal en un hilo separado."""
    # Usar try/finally para asegurar que el cursor y la conexión se cierren
    # incluso si hay un error, en lugar de depender únicamente del context manager.
    cn, cur = None, None
    try:
        cn = crear_conexion()
        cur = cn.cursor()
        holder["connection_id"] = cn.connection_id
        logging.info(f"Hilo ejecutando: CALL {SP_NOMBRE}{sp_params} en la conexión ID: {cn.connection_id}...")
        cur.callproc(SP_NOMBRE, sp_params)
        cn.commit() # El SP no hace commit, el cliente debe hacerlo.
        logging.info("Stored Procedure finalizado.")
        holder["result"] = "OK"
    except (Error, Exception) as db_err:
        # Este error captura fallos tanto en la conexión (get_conexion) como en la ejecución.
        metrics.increment_error("SP_EXECUTION_FAILURE")
        holder["error"] = f"Error en hilo de SP: {db_err}. Revise la tabla 'dw_log_ejecucion' en la base de datos para más detalles."
        logging.error(holder["error"])
    finally:
        if cur: cur.close()
        if cn: cn.close()

def main_entregable_5():
    sp_thread = None
    logging.info("=== INICIANDO EJECUTOR DE DWH CON MONITOREO ===")
    if DISK_CHECK_ENABLED: # La verificación de espacio en disco ahora está activada por defecto.
        espacio_libre_gb = get_free_space_gb(DISK_CHECK_PATH)
        if espacio_libre_gb < DISK_CRIT_GB:
            logging.error(f"Espacio en disco crítico ({espacio_libre_gb:.2f}GB). Abortando ejecución.")
            metrics.increment_error("DISK_SPACE_CRITICAL")
            return
        elif espacio_libre_gb < DISK_WARN_GB:
            logging.warning(f"Espacio en disco bajo ({espacio_libre_gb:.2f}GB).")
    
    try:
        # Sincronizar el soporte relativo del SP con el de Apriori.
        # Se usa el mismo valor de MIN_SOPORTE_RELATIVO para que ambos análisis partan de una base similar.
        sp_min_sop_rel = os.getenv("SP_MIN_SOP_REL", str(MIN_SOPORTE_RELATIVO))
        logging.info(f"Ejecutando SP con soporte relativo mínimo: {sp_min_sop_rel}")
        sp_params = (Decimal(sp_min_sop_rel).quantize(Decimal('0.00000001')), get_int_env("SP_EXEC_ANALYZE", 0)) # Asegurar que el tipo es Decimal y coincide con la precisión del SP.
        
        holder = {"result": None, "error": None}
        sp_thread = threading.Thread(target=ejecutar_sp_en_hilo, args=(holder, sp_params), daemon=True)
        sp_thread.start()

        start_time = time.time()
        last_prog_str = ""  # Para no repetir logs idénticos
        last_prog_tuple = (None, None) # (paso, pct)
        last_log_time = 0   # Para forzar el log periódicamente
        is_tty = sys.stdout.isatty()  # Verificamos si la salida es una terminal interactiva
        
        # El bucle de monitoreo ahora está separado de la lógica de timeout.
        while sp_thread.is_alive():
            sp_thread.join(timeout=POLL_INTERVAL_SEC)
            
            # Implementación de la lógica de terminación del SP por timeout.
            if (time.time() - start_time) > MAX_EXECUTION_TIME_SEC:
                logging.warning(f"El SP excedió el tiempo límite de {MAX_EXECUTION_TIME_SEC}s. Intentando terminar el proceso en la base de datos...")
                holder["error"] = "Timeout: El proceso excedió el tiempo máximo de ejecución."
                conn_id_to_kill = holder.get("connection_id")
                if conn_id_to_kill:
                    conn_kill, cur_kill = None, None
                    try:
                        # Crear una nueva conexión dedicada para matar la consulta.
                        conn_kill = crear_conexion()
                        cur_kill = conn_kill.cursor()
                        logging.info(f"Ejecutando KILL QUERY en la conexión ID: {conn_id_to_kill}")
                        cur_kill.execute(f"KILL QUERY {conn_id_to_kill}")
                        conn_kill.commit()
                    except Error as kill_err:
                        metrics.increment_error("SP_KILL_FAILURE")
                        logging.error(f"No se pudo terminar el proceso del SP (ID: {conn_id_to_kill}): {kill_err}")
                    finally:
                        if cur_kill: cur_kill.close()
                        if conn_kill: conn_kill.close()
                
                break
            
            prog = consultar_progreso_con_reconexion()
            if isinstance(prog, dict):
                current_time = time.time()
                elapsed_seconds = current_time - start_time
                elapsed_time_str = str(timedelta(seconds=int(elapsed_seconds)))

                paso = prog.get('paso_actual', 'desconocido') # Usar los nuevos campos para un log más claro.
                indice = prog.get('indice', 0) # Se mantiene para el log detallado
                total = prog.get('total', 1)   # Se mantiene para el log detallado
                pct = prog.get('pct', 0.0)     # Usar directamente el 'pct' calculado por el SP.

                current_prog_tuple = (paso, pct)

                # Mostrar el progreso como porcentaje del paso actual, no del total.
                prog_str = f"Progreso DWH: {paso} - {pct:.2f}% ({indice}/{total}) [Tiempo: {elapsed_time_str}]"
                if is_tty:
                    sys.stdout.write(f"\r{prog_str}")
                    sys.stdout.flush() # Para logs no interactivos: loguear si el progreso cambió o ha pasado más de un minuto.
                # Para logs no interactivos (ej. CI/CD), loguear solo si el progreso cambió o ha pasado más de un minuto.
                elif current_prog_tuple != last_prog_tuple or (current_time - last_log_time > 60):
                    logging.info(prog_str)
                    last_prog_tuple = current_prog_tuple
                    last_log_time = current_time

        # Realizar una última consulta de progreso después de que el hilo termine
        # para asegurar que se captura el estado final (100% o error), solucionando la condición de carrera.
        final_prog = consultar_progreso_con_reconexion()
        if isinstance(final_prog, dict):
            elapsed_seconds = time.time() - start_time
            elapsed_time_str = str(timedelta(seconds=int(elapsed_seconds)))
            paso = final_prog.get('paso_actual', 'finalizado')
            indice = final_prog.get('indice', 0)
            total = final_prog.get('total', 1)
            pct = final_prog.get('pct', 100.0)
            prog_str = f"Progreso DWH: {paso} - {pct:.2f}% ({indice}/{total}) [Tiempo: {elapsed_time_str}]"
            if is_tty:
                sys.stdout.write(f"\r{prog_str}")
                sys.stdout.flush()
            else:
                # En modo no interactivo, loguear el estado final si es diferente al último logueado.
                current_prog_tuple = (paso, pct)
                if current_prog_tuple != last_prog_tuple:
                    logging.info(prog_str)

        if is_tty:
            sys.stdout.write('\n') # Asegura que el siguiente log empiece en una nueva línea
        if holder.get("error"):
            logging.error(f"El hilo del SP falló: {holder['error']}")
        elif holder.get("result"):
            logging.info("✓ El Stored Procedure se ejecutó correctamente.")
        elif not holder.get("result"):
             logging.warning("El SP finalizó, pero no reportó un estado 'OK'. Revise los logs de la DB.")

    except Error as db_err:
        metrics.increment_error("MONITOR_CONNECTION_ERROR")
        logging.error(f"Error en el hilo de monitoreo: {db_err}")


# =====================================================================
# --- Configuración de Reportes (API) ---
LIM_MBA = get_int_env("API_LIM_MBA", 50)
LIM_CAT = get_int_env("API_LIM_CAT", 50)
LIM_REC = get_int_env("API_LIM_REC", 50)
LIM_ANOM = get_int_env("API_LIM_ANOM", 30)
LIM_BND = get_int_env("API_LIM_BND", 30)
LIM_REGLAS_APR = get_int_env("API_LIM_REGLAS_APR", 50)
LIM_CMP = get_int_env("API_LIM_CMP", 50)
OUTPUT_JSON_PATH = os.getenv("API_OUTPUT_JSON", "reportes_finales.json")


class DecimalEncoder(json.JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, Decimal): return float(o)
        if isinstance(o, (datetime, date)): return o.isoformat()
        return super().default(o)


def extraer_datos_para_reportes() -> Dict[str, Any]:
    consultas_reportes = {
        "market_basket_analysis": f"""
            SELECT mba.producto_a_key, mba.producto_b_key, dp1.nombre_producto AS nombre_a, dp2.nombre_producto AS nombre_b, mba.lift, mba.confianza_a_b
            FROM market_basket_analysis mba
            JOIN dim_producto dp1 ON dp1.producto_key = mba.producto_a_key
            JOIN dim_producto dp2 ON dp2.producto_key = mba.producto_b_key
            ORDER BY mba.lift DESC, mba.producto_a_key, mba.producto_b_key LIMIT {LIM_MBA}
        """,
        "categoria_proximidad": f"SELECT * FROM categoria_proximidad ORDER BY score_proximidad DESC, categoria_a, categoria_b LIMIT {LIM_CAT}",
        "patron_recomendaciones": f"""
            SELECT trig.nombre_producto as trigger_nombre, rec.nombre_producto as recomendado_nombre, pr.*
            FROM patron_recomendaciones pr
            JOIN dim_producto trig ON pr.producto_trigger_key = trig.producto_key
            JOIN dim_producto rec ON pr.producto_recomendado_key = rec.producto_key
            ORDER BY pr.score_recomendacion DESC, pr.producto_trigger_key, pr.producto_recomendado_key, pr.fuente LIMIT {LIM_REC}
        """,
        # La descripción de la anomalía ya contiene el monto, por lo que el JOIN a Tickets es redundante.
        # Esto hace la consulta más rápida y evita posibles problemas si un ticket es borrado pero la anomalía no.
        "deteccion_anomalias": f"""
            SELECT id, ticket_id, tipo_anomalia, score_anomalia, descripcion_anomalia FROM anomalias_compra ORDER BY score_anomalia DESC, id LIMIT {LIM_ANOM}
        """,
        "oportunidades_bundling": f"""
            SELECT nombre_bundle, productos_bundle, tipo_bundle, potencial_incremento
            FROM bundling_opportunities ORDER BY potencial_incremento DESC, nombre_bundle LIMIT {LIM_BND}
        """,
        "reglas_apriori": f"SELECT * FROM reglas_apriori ORDER BY lift DESC, confianza DESC, id LIMIT {LIM_REGLAS_APR}",
        "comparativo_mba_apriori": f"""
            SELECT c.*, da.nombre_producto as nombre_a, db.nombre_producto as nombre_b
            FROM comparativo_mba_apriori c
            JOIN dim_producto da ON c.producto_a_key = da.producto_key
            JOIN dim_producto db ON c.producto_b_key = db.producto_key
            ORDER BY ABS(c.delta_lift) DESC, c.producto_a_key, c.producto_b_key LIMIT {LIM_CMP}
        """
    }
    datos_extraidos = {}
    cn, cur_dict = None, None
    try:
        cn = crear_conexion()
        cur_dict = cn.cursor(dictionary=True) # Crea un cursor de diccionario
        for nombre, sql in tqdm(consultas_reportes.items(), desc="Extrayendo reportes", disable=not sys.stdout.isatty()):
            cur_dict.execute(sql)
            resultados = cur_dict.fetchall()
            if not resultados:
                logging.warning(f"La consulta para el reporte '{nombre}' no devolvió resultados.")
            datos_extraidos[nombre] = resultados
    except Error as db_err:
        metrics.increment_error("REPORT_EXTRACTION_ERROR")
        logging.error(f"Error extrayendo datos para reportes: {db_err}", exc_info=True)
    finally:
        if cur_dict: cur_dict.close()
        if cn: cn.close()
    return datos_extraidos

def ejecutar_script_sql(sql_script_path: str):
    """Ejecuta un script SQL desde un archivo, manejando delimitadores."""
    logging.info(f"Ejecutando script de inicialización de BD desde: {sql_script_path}")

    try:
        with get_conexion() as cn:
            cur = cn.cursor()
            try:
                with open(sql_script_path, 'r', encoding='utf-8') as f:
                    sql_script = f.read()

                # Procesa el script para manejar delimitadores personalizados como 'DELIMITER $$'
                # Implementamos un parser por líneas que mantiene el estado del delimitador actual.
                delimiter = ';'
                statements = []
                current_lines = []

                for raw_line in sql_script.splitlines():
                    # Conservamos la línea sin eliminar espacios internos; solo eliminamos saltos de línea
                    line = raw_line.rstrip('\r\n')
                    stripped = line.strip()

                    # Ignorar líneas vacías o comentarios de una sola línea
                    if not stripped or stripped.startswith('--') or stripped.startswith('#'):
                        continue

                    # Cambiar el delimitador cuando aparece una línea DELIMITER
                    if stripped.lower().startswith('delimiter'):
                        # Volcar cualquier statement incompleto previo
                        if current_lines:
                            statements.append('\n'.join(current_lines).strip())
                            current_lines = []
                        parts = stripped.split()
                        if len(parts) >= 2:
                            delimiter = parts[1]
                        else:
                            delimiter = ';'
                        logging.info(f"Cambio de delimitador SQL: ahora '{delimiter}'")
                        continue

                    # Añadir la línea al statement actual
                    current_lines.append(line)

                    # Detectar si la línea termina con el delimitador actual (permitiendo espacios finales)
                    if stripped.endswith(delimiter):
                        # Quitar únicamente la última ocurrencia del delimitador del final de la última línea
                        last = current_lines[-1]
                        idx = last.rfind(delimiter)
                        if idx != -1:
                            current_lines[-1] = last[:idx]
                        statements.append('\n'.join(current_lines).strip())
                        current_lines = []

                # Si quedó algo acumulado, añadir como última sentencia
                if current_lines:
                    statements.append('\n'.join(current_lines).strip())

                # Ejecuta cada comando individualmente
                for idx, statement in enumerate(statements, start=1):
                    stmt = statement.strip()
                    if not stmt:
                        logging.debug(f"Saltando sentencia vacía en índice {idx}.")
                        continue
                    # Mostrar una versión truncada para debugging, pero loggear completa si falla
                    logging.info(f"Ejecutando sentencia SQL #{idx} (primeros 200 chars): {stmt[:200].replace('\n',' ')}")
                    try:
                        cur.execute(stmt)
                    except mysql.connector.errors.ProgrammingError as pe:
                        logging.error(f"Error ejecutando sentencia SQL #{idx}: {pe}\n--- Sentencia completa start ---\n{stmt}\n--- Sentencia completa end ---")
                        raise
                    except Exception as ex:
                        logging.error(f"Error no esperado ejecutando sentencia SQL #{idx}: {ex}\n--- Sentencia completa start ---\n{stmt}\n--- Sentencia completa end ---")
                        raise

                cn.commit()
                logging.info("Script SQL ejecutado y cambios confirmados.")
            finally:
                try:
                    cur.close()
                except Exception:
                    pass

    except Error as db_err:
        logging.error(f"Error de MySQL al ejecutar el script SQL: {db_err}")
        raise  # Vuelve a lanzar el error para que el programa principal se detenga


# =====================================================================
# --- Ejecución Principal ---
if __name__ == "__main__":
    logging.info("=== Iniciando Script Integrado (Versión Definitiva) ===")
    try:
        # Paso 0: Validar la existencia de archivos críticos
        if not os.path.exists(ARCHIVO_PRODUCTOS):
            raise FileNotFoundError(f"No se encontró el archivo de productos requerido: {ARCHIVO_PRODUCTOS}")
        if DB_SSL_MODE == "REQUIRED" and not os.path.exists(_base_config().get('ssl_ca', '')):
            raise FileNotFoundError(f"Modo SSL 'REQUIRED' pero no se encontró el certificado: {_base_config().get('ssl_ca')}")

        # Paso 1: Ejecutar el script SQL para asegurar que toda la estructura de la BD existe.
        sql_script_path = os.path.join(script_dir, "scrip_final.sql")
        ejecutar_script_sql(sql_script_path)

        main_entregable_1()
        main_entregable_2()
        main_entregable_5()

        reportes = extraer_datos_para_reportes()
        if reportes and OUTPUT_JSON_PATH:
            with open(OUTPUT_JSON_PATH, "w", encoding="utf-8") as f_json:
                json.dump(reportes, f_json, cls=DecimalEncoder, ensure_ascii=False, indent=4)
            logging.info(f"✓ Reportes guardados en '{OUTPUT_JSON_PATH}'")
        elif reportes:
            logging.info("Resumen de reportes generados:")
            for key, value in reportes.items():
                print(f"- {key}: {len(value)} registros.")
    except KeyboardInterrupt:
        logging.warning("\nProceso interrumpido por el usuario.")
        metrics.increment_error("KEYBOARD_INTERRUPT")
    except Exception as main_error:
        logging.error(f"❌ Error fatal en la ejecución principal: {main_error}", exc_info=True)
        metrics.increment_error("UNHANDLED_MAIN_EXCEPTION")
        sys.exit(1)
    finally:
        metrics.log_summary()

    logging.info("\n=== Script Integrado Finalizado ===")
