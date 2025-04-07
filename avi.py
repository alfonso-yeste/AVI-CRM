import functions_framework
import numpy as np
import requests
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta
import json
import re
from google.cloud import bigquery

client = bigquery.Client()

# Configuración
API_URL = "http://desk.avicrm.net/clientapi/v1/list/"
TOKEN = "f377d9c5fb0a470eb61f506f0909a786"
TABLE_ID = "data-project-443316.staging_area.avi_crm_v2"

# Normalización de nombres de columnas
COLUMN_MAPPING = {
    "lead_id": "lead_id",
    "lead_tipo": "lead_tipo",
    "origen_lead": "origen_lead",
    "detalle_origen_lead": "detalle_origen_lead",
    "detalle_origen_raw": "detalle_origen_raw",
    "observaciones": "observaciones",
    "link_ficha": "link_ficha",
    "campana": "campana",
    "usuario_creacion_lead": "usuario_creacion_lead",
    "cita_id": "cita_id",
    "estado_cita": "estado_cita",
    "motivo_no_compra": "motivo_no_compra",
    "usuario_alta": "usuario_alta",
    "lead_creacion": "lead_creacion",
    "primera_llamada": "primera_llamada",
    "ultima_llamada": "ultima_llamada",
    "fecha_agendada": "fecha_agendada",
    "fecha_modificacion": "fecha_modificacion",
    "usuario_ultima_modificacion": "usuario_ultima_modificacion",
    "fecha_venta": "fecha_venta",
    "paga_y_senal": "paga_y_senal",
    "usuario_asignado_lead": "usuario_asignado_lead",
    "vendedor_nombre": "vendedor_nombre",      # Se repetirá más abajo
    "vendedor_apellido": "vendedor_apellido",
    "asistencia": "asistencia",
    "centro": "centro",
    "cliente_nombre": "cliente_nombre",
    "cliente_apellidos": "cliente_apellidos",
    "cliente_razon_social": "cliente_razon_social",
    "cliente_tipo": "cliente_tipo",
    "cliente_cp": "cliente_cp",
    "cliente_provincia": "cliente_provincia",
    "cliente_telefono": "cliente_telefono",
    "cliente_movil": "cliente_movil",
    "cliente_email": "cliente_email",
    "marca": "marca",
    "modelo": "modelo",
    "motor": "motor",
    "kilometros": "kilometros",
    "combustible": "combustible",
    "matricula": "matricula",
    "tipo_venta": "tipo_venta",
    "vendedor_nombre": "vendedor_nombre",       # Clave repetida: sobrescribirá la anterior
    "vendedor_apellidos": "vendedor_apellidos", # Similar caso a 'vendedor_nombre'
    "vendedor_telefono": "vendedor_telefono",
    "vendedor_email": "vendedor_email",
    "gclid": "gclid",
    "gad_source": "gad_source",
    "utm_source": "utm_source",
    "utm_medium": "utm_medium",
    "utm_campaign": "utm_campaign"
}

def fetch_data(start_date):
    """Obtiene los datos de la API en formato CSV"""

    params = {
        "token": TOKEN,
        "command": "csv_list",
        "list_type": "leadcenterleads",
        "list_header" : "alias",
        "list_format": "json",
        "desde": start_date,
        "hasta": (start_date + timedelta(days=1)),
        "date_criteria": "lead_creation",
        "idtaller": "b4146d91-9a86-485a-9080-00dc7c797723"
    }

    response = requests.get(API_URL, params=params)

    print("status code: ")
    print(response.status_code)

    response.raise_for_status()  # Lanza un error si la API falla

    return response.text  # Retorna el CSV en formato texto

def transform_csv_to_json(csv_data):
    """Convierte CSV a JSON con nombres de columnas normalizados"""
    df = pd.read_csv(StringIO(csv_data), sep=';', on_bad_lines='skip', engine='python')

    # Aplicamos las funciones de renombrado
    df = rename_duplicate_columns(df)
    df = clean_dot_suffixes(df)

    # Renombramos específicamente 'vendedor_apellidos' a 'vendedor_apellido_1'
    if 'vendedor_apellidos' in df.columns:
        df = df.rename(columns={'vendedor_apellidos': 'vendedor_apellido_1'})

    # -------------------------------------------------------------------
    # ETAPA 2: CONVERSIÓN DE TIPOS BÁSICOS Y NORMALIZACIÓN
    # -------------------------------------------------------------------

    # 1. Convertir columnas de fecha a datetime (formato dd/mm/yyyy HH:MM:SS)
    fecha_cols = ['lead_creacion', 'primera_llamada', 'ultima_llamada',
                  'fecha_agendada', 'fecha_modificacion', 'fecha_venta']
    for col in fecha_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')

    # 2. Convertir identificadores y códigos a string
    df['lead_id'] = df['lead_id'].astype(str)
    if 'cita_id' in df.columns:
        df['cita_id'] = df['cita_id'].astype(str)
    if 'cliente_cp' in df.columns:
        df['cliente_cp'] = df['cliente_cp'].apply(lambda x: str(int(x)).zfill(5) if pd.notnull(x) else None)

    # 3. Convertir números de teléfono a string
    telefonos = ['cliente_telefono', 'cliente_movil', 'vendedor_telefono']
    for col in telefonos:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: str(x) if pd.notnull(x) and pd.notna(x) else None)

    # 4. Asegurar que 'kilometros' es entero y convertir ciertas columnas a categoría
    if 'kilometros' in df.columns:
        df['kilometros'] = pd.to_numeric(df['kilometros'], errors='coerce').fillna(0).astype(int)
    cols_categoricas = ['lead_tipo', 'origen_lead', 'estado_cita', 'cliente_tipo', 'marca', 'modelo']
    for col in cols_categoricas:
        if col in df.columns:
            df[col] = df[col].astype('category')

    # 5. Limpieza básica de texto en campos largos
    texto_cols = ['detalle_origen_lead', 'detalle_origen_raw', 'observaciones']
    for col in texto_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # Normalizar correos y otros campos clave a minúsculas
    for col in ['cliente_email', 'vendedor_email']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.lower()


    # 3.1. Generar variable "equipo" según usuario_alta
    bdc_users = {"pol simon", "jordi gomez cuevas", "tomas duran", "laura gonzalez",
                 "olga mateos", "daniel  lopez ruiz", "paco munoz responsable call", "sistema avi automocion"}
    df['equipo'] = df['usuario_alta'].apply(
        lambda x: 'bdc' if pd.notnull(x) and str(x).strip().lower() in bdc_users else 'concesionario')


    # 3.2. Calcular tiempos de respuesta (en minutos, horas y días)
    df['tiempo_de_respuesta_minutos'] = round((df['primera_llamada'] - df['lead_creacion']).dt.total_seconds() / 60,2)
    df['tiempo_de_respuesta_horas'] = round(df['tiempo_de_respuesta_minutos'] / 60,2)
    df['tiempo_de_respuesta_dias'] = round(df['tiempo_de_respuesta_horas'] / 24,2)

    df['telefono_unificado'] = df.apply(unificar_telefono, axis=1)

    df['marca_normalizada'] = df.apply(normalizar_marca, axis=1)

    # 3.5. Generar variable "tipo_de_lead" a partir de "campana"
    # Se extrae la primera palabra de "campana" y se mapea:
    # 'vn' se interpreta como "vehiculo nuevo" y 'vo' como "vehiculo de ocasion"
    mapping_tipo_lead = {
        'vn': 'vehiculo nuevo',
        'vo': 'vehiculo de ocasion'
    }
    df['tipo_de_lead'] = df['campana'].apply(
        lambda x: mapping_tipo_lead.get(x.split()[0].strip().lower())
        if isinstance(x, str) and x.split() else None)

    # 3.6. Normalizar "tipo_venta": si está vacío, se intenta obtener de "campana"
    df['tipo_venta_normalizado'] = df['tipo_venta']
    df['tipo_venta_normalizado'] = df.apply(
        guess_tipo_venta_based_on_campaign, axis=1)

    # 3.7. Generar variable "fuente" a partir de "origen_lead" usando un diccionario de mapeo
    mapping_fuente = {
    'Landing Maas': 'web_propia',
    'Carnovo': 'portal',
    'Landing Maas CITROEN': 'web_propia',
    'Ebro Marca': 'web_propia',
    'Web Maas Pragauto': 'web_propia',
    'Nissan NSMIT': 'marca',
    'Web Maas': 'web_propia',
    'Web Exclusivas Pont OPEL': 'web_propia',
    'Dravit': 'web_propia',
    'Landing Exclusivas Pont': 'web_propia',
    'Web Santi Enrique': 'web_propia',
    'Coches.net': 'portal',
    'Coches.com': 'portal',
    'Seat COSMOS': 'portal',
    'CUSTOMER': 'marca',
    'Web Exclusivas Pont': 'web_propia',
    'VO CaixaBank': 'la_caixa',
    'Landing': 'web_propia',
    'Web Exclusivas Pont (Maas)': 'web_propia',
    'Das WeltAuto': 'marca',
    'Instagram': 'meta',
    'CUSTOMER CITROEN': 'marca',
    'Autocasión (Sumauto)': 'portal',
    'Skoda WEB': 'web_propia',
    'Web Semprocar': 'web_propia',
    'Wallapop': 'portal',
    'Carwow': 'portal',
    'Web Orbeauto EBRO': 'web_propia',
    'Landing Maas KIA': 'web_propia',
    'Web Semprocar (Maas)': 'web_propia',
    'Cupra COSMOS': 'marca',
    'Facebook': 'meta',
    'Web Ondinauto': 'web_propia',
    'Web Maas Mobility': 'web_propia',
    'KIA KARS VN/VO': 'portal',
    'Spoticar': 'portal',
    'Exposición': 'exposicion',
    'CUSTOMER OPEL': 'marca',
    'Motor.es': 'portal',
    'Autobiz': 'portal',
    'Landing Plan Moves 3': 'web_propia',
    'Facebook Exclusivas Pont': 'meta',
    'Web Ondinauto SEAT (Maas)': 'web_propia',
    'Web Ondinauto SEAT': 'web_propia',
    'Web Milanuncios': 'portal',
    'Web Maas DS': 'web_propia',
    'Niw': 'niw'
}
    df['fuente'] = df['origen_lead'].map(mapping_fuente).fillna('sin_fuente')

    # Convertir JSON
    return json.loads(df.to_json(orient="records"))


def guess_tipo_venta_based_on_campaign(row):

    if row['tipo_venta_normalizado'] != '' and es_nan(row['tipo_venta_normalizado']) == False:
        return row['tipo_venta_normalizado'].strip()

    if isinstance(row['campana'], str) and row['campana'].split():
        return row['campana'].split()[0].strip()

    return None

# 3.3. Unificar teléfonos: crear "telefono_unificado" a partir de "cliente_telefono" y "cliente_movil"
def unificar_telefono(row):

    tel_fijo = row.get('cliente_telefono', None)
    tel_movil = row.get('cliente_movil', None)

    tel_fijo = str(tel_fijo).strip().replace(".0", "") if pd.notnull(tel_fijo) and pd.notna(tel_fijo) else None
    tel_movil = str(tel_movil).strip().replace(".0", "") if pd.notnull(tel_movil) and pd.notna(tel_movil) else None

    if tel_movil is not None:
        return tel_movil

    if tel_fijo is not None:
        return tel_fijo

    return None

# 3.4. Normalizar y completar el campo "marca"
def normalizar_marca(row):

    marca = row.get('marca', None)

    if marca is not None and es_nan(marca) == False:

        if marca == "Citren" or marca == "Citröen":
            return "Citroen"

        return marca

    marcas_automocion_espana = [
        'seat', 'peugeot', 'renault', 'citroen', 'ford', 'volkswagen',
        'opel', 'hyundai', 'kia', 'toyota', 'dacia', 'skoda', 'fiat',
        'nissan', 'mazda', 'honda', 'suzuki', 'audi', 'bmw', 'mercedes-benz',
        'lexus', 'volvo', 'alfa romeo', 'ds', 'jaguar',
        'infiniti', 'genesis', 'porsche', 'tesla', 'maserati', 'ferrari',
        'lamborghini', 'aston martin', 'bentley', 'rolls-royce', 'iveco',
        'man', 'renault trucks', 'mercedes-benz vans', 'ford pro',
        'volkswagen vehículos comerciales', 'citroen business',
        'fiat professional', 'mg', 'byd', 'aiways', 'lynk & co',
        'polestar', 'cupra', 'smart', 'chevrolet', 'lancia', 'saab',
        'daewoo', 'rover'
    ]

    campaign = row.get('campana', None)

    if campaign is None:
        return None

    campaign_parts = str(campaign).split()

    if len(campaign_parts) < 2:
        return None

    guess_brand = campaign_parts[1]

    if guess_brand.lower().strip() in marcas_automocion_espana:
        return guess_brand.lower().strip().title()

    return None

def es_nan(valor):
    try:
        return np.isnan(valor)
    except TypeError:
        return False

# Función para renombrar columnas duplicadas añadiendo sufijos (_1, _2, ...)
def rename_duplicate_columns(df):
    cols_count = {}
    new_cols = []
    for col in df.columns:
        if col in cols_count:
            cols_count[col] += 1
            new_cols.append(f"{col}_{cols_count[col]}")
        else:
            cols_count[col] = 0
            new_cols.append(col)
    df.columns = new_cols
    return df

# Función para limpiar sufijos del tipo ".1" y reemplazarlos por "_1"
def clean_dot_suffixes(df):
    new_cols = [re.sub(r'\.(\d+)$', r'_\1', col) for col in df.columns]
    df.columns = new_cols
    return df
def insert_into_bigquery(data):

    errors = client.insert_rows_json(TABLE_ID, data)
    if errors:
        print("Encountered errors while inserting rows: {}".format(errors))

def last_day_imported():
    sql = (
            "SELECT imported_date "
            "FROM `data-project-443316.staging_area.imported_dates` "
            "ORDER BY imported_date DESC limit 1"
    )
    query_job = client.query(sql)
    rows = query_job.result()

    for row in rows:
        return datetime.strptime(str(row.imported_date), "%Y-%m-%d")

    return None


def insert_import_completed(date):
    sql = (
            "INSERT INTO `data-project-443316.staging_area.imported_dates` "
            "VALUES ('" + date.strftime("%Y-%m-%d") + "')"
    )

    query_job = client.query(sql)
    query_job.result()

def execute():
    """Función principal que se ejecuta en Google Cloud"""
    try:

        default_start_date = datetime.strptime("2024/01/01 00:00:00", "%Y/%m/%d 00:00:00")

        current_date = datetime.now()

        last_day_already_imported = last_day_imported()

        if last_day_already_imported is not None and last_day_already_imported > default_start_date:
            print("Fecha inicial cambiada por la ultima fecha guardada")
            default_start_date = last_day_already_imported + timedelta(days=1)

        start_date = default_start_date
        while start_date <= current_date:
            print("Obteniendo datos de la API del día: " + start_date.strftime("%Y-%m-%d"))
            csv_data = fetch_data(start_date)

            print("Transformando CSV a JSON...")
            json_data = transform_csv_to_json(csv_data)

            print("Insertando datos en BigQuery...")
            insert_into_bigquery(json_data)

            insert_import_completed(start_date)
            start_date += timedelta(days=1)

            print("Finalizando el día: " + start_date.strftime("%Y-%m-%d"))

        print("Proceso finalizado")

    except Exception as e:
        print(f"Error: {str(e)}")


# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def hello_pubsub(cloud_event):
    execute()
    
