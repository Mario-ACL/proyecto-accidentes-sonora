"""
PROYECTO: Análisis de Accidentes de Tránsito en Hermosillo, Sonora
ETAPA 3: COMPRENSIÓN Y CONEXIÓN A LOS DATOS (CRISP-DM)
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import warnings
warnings.filterwarnings('ignore')

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

# Configuración de PostgreSQL
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'user': 'postgres',
    'password': 'mario1',  # CAMBIAR por tu contraseña
    'database': 'accidentes_hermosillo'
}

# Ruta del archivo CSV
CSV_PATH = 'data\processed\inegi_tidy.csv'  # CAMBIAR por tu ruta

# Códigos para filtrar Hermosillo, Sonora
ID_ENTIDAD_SONORA = 26
ID_MUNICIPIO_HERMOSILLO = 30  # Hermosillo

# =============================================================================
# 2.1 DESCRIPCIÓN DE LA FUENTE DE DATOS
# =============================================================================

def descripcion_fuente():
    """Documenta la fuente de datos"""
    print("="*80)
    print("2.1 DESCRIPCIÓN DE LA FUENTE DE DATOS")
    print("="*80)
    print("\nFuente: INEGI - Accidentes de Tránsito Terrestre en Zonas Urbanas y Suburbanas")
    print("URL: https://www.inegi.org.mx/contenidos/programas/accidentes/datosabiertos/")
    print("Período: 2018-2024")
    print("Alcance geográfico: Nacional → Filtrado a Hermosillo, Sonora")
    print("Formato original: CSV comprimido (ZIP)")
    print("\n" + "="*80 + "\n")

# =============================================================================
# 2.2 EXPLORACIÓN INICIAL DEL CONJUNTO DE DATOS
# =============================================================================

def exploracion_inicial(df):
    """Realiza exploración inicial de los datos"""
    print("="*80)
    print("2.2 EXPLORACIÓN INICIAL DEL CONJUNTO DE DATOS")
    print("="*80)
    
    print(f"\n📊 DIMENSIONES DEL DATASET COMPLETO")
    print(f"   Registros totales: {len(df):,}")
    print(f"   Columnas: {len(df.columns)}")
    
    print(f"\n📅 PERÍODO DE DATOS")
    print(f"   Año mínimo: {df['ANIO'].min()}")
    print(f"   Año máximo: {df['ANIO'].max()}")
    
    print(f"\n🗺️ COBERTURA GEOGRÁFICA")
    print(f"   Entidades únicas: {df['ID_ENTIDAD'].nunique()}")
    print(f"   Municipios únicos: {df['ID_MUNICIPIO'].nunique()}")
    
    print("\n📋 PRIMERAS COLUMNAS DEL DATASET:")
    print(df.columns.tolist()[:15])
    
    print("\n🔍 TIPOS DE DATOS:")
    print(df.dtypes.value_counts())
    
    print("\n📊 VALORES NULOS:")
    nulos = df.isnull().sum()
    if nulos.sum() > 0:
        print(nulos[nulos > 0])
    else:
        print("   ✓ No hay valores nulos")
    
    print("\n" + "="*80 + "\n")

# =============================================================================
# FILTRADO DE HERMOSILLO
# =============================================================================

def filtrar_hermosillo(df):
    """Filtra solo los registros de Hermosillo, Sonora"""
    print("="*80)
    print("FILTRADO GEOGRÁFICO: HERMOSILLO, SONORA")
    print("="*80)
    
    print(f"\n🔍 Registros antes del filtro: {len(df):,}")
    
    # Convertir columnas a numérico si no lo están
    df['ID_ENTIDAD'] = pd.to_numeric(df['ID_ENTIDAD'], errors='coerce')
    df['ID_MUNICIPIO'] = pd.to_numeric(df['ID_MUNICIPIO'], errors='coerce')
    
    # Filtrar por Sonora y Hermosillo
    df_hermosillo = df[
        (df['ID_ENTIDAD'] == ID_ENTIDAD_SONORA) 
        # (df['ID_MUNICIPIO'] == ID_MUNICIPIO_HERMOSILLO)
    ].copy()
    
    print(f"✓ Registros después del filtro: {len(df_hermosillo):,}")
    print(f"📉 Reducción: {len(df) - len(df_hermosillo):,} registros")
    
    if len(df_hermosillo) > 0:
        print(f"📊 Porcentaje retenido: {(len(df_hermosillo)/len(df)*100):.2f}%")
        
        print(f"\n📅 DISTRIBUCIÓN POR AÑO EN HERMOSILLO:")
        distribucion = df_hermosillo['ANIO'].value_counts().sort_index()
        for anio, cantidad in distribucion.items():
            print(f"   {int(anio)}: {cantidad:,} accidentes")
    else:
        print("⚠️  No se encontraron registros para Hermosillo, Sonora")
        print(f"   Verifica los códigos: ID_ENTIDAD={ID_ENTIDAD_SONORA}")
    
    print("\n" + "="*80 + "\n")
    
    return df_hermosillo

# =============================================================================
# 2.3 DISEÑO DE LA BASE DE DATOS
# =============================================================================

def crear_base_datos():
    """Crea la base de datos PostgreSQL"""
    print("="*80)
    print("2.3 DISEÑO Y CREACIÓN DE LA BASE DE DATOS")
    print("="*80)
    
    try:
        # Conectar a PostgreSQL (base de datos por defecto)
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            database='postgres'
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Verificar si la base de datos existe
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DB_CONFIG['database']}'")
        exists = cursor.fetchone()
        
        if exists:
            print(f"\n⚠️  La base de datos '{DB_CONFIG['database']}' ya existe")
            respuesta = input("¿Deseas eliminarla y crearla de nuevo? (s/n): ")
            if respuesta.lower() == 's':
                cursor.execute(f"DROP DATABASE {DB_CONFIG['database']}")
                print(f"✓ Base de datos eliminada")
            else:
                print("✓ Usando base de datos existente")
                cursor.close()
                conn.close()
                return
        
        # Crear la base de datos
        cursor.execute(f"CREATE DATABASE {DB_CONFIG['database']}")
        print(f"\n✓ Base de datos '{DB_CONFIG['database']}' creada exitosamente")
        
        cursor.close()
        conn.close()
        
        print("\n📐 MODELO DE DATOS:")
        print("   Tabla principal: accidentes_hermosillo")
        print("   Estructura: Tabla única desnormalizada para análisis")
        print("   Justificación: Optimizada para consultas analíticas y EDA")
        
        print("\n" + "="*80 + "\n")
        
    except Exception as e:
        print(f"\n❌ Error al crear la base de datos: {e}")
        raise

def crear_tabla_accidentes(engine):
    """Crea la tabla de accidentes con el esquema adecuado"""
    print("="*80)
    print("CREACIÓN DE TABLA: accidentes_hermosillo")
    print("="*80)
    
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS accidentes_hermosillo (
        id SERIAL PRIMARY KEY,
        cobertura VARCHAR(50),
        id_entidad INTEGER,
        id_municipio INTEGER,
        anio INTEGER,
        mes INTEGER,
        id_hora INTEGER,
        id_minuto INTEGER,
        id_dia INTEGER,
        diasemana VARCHAR(20),
        urbana VARCHAR(100),
        suburbana VARCHAR(100),
        tipaccid VARCHAR(100),
        automovil INTEGER,
        campasaj INTEGER,
        microbus INTEGER,
        pascamion INTEGER,
        omnibus INTEGER,
        tranvia INTEGER,
        camioneta INTEGER,
        camion INTEGER,
        tractor INTEGER,
        ferrocarri INTEGER,
        motociclet INTEGER,
        bicicleta INTEGER,
        otrovehic INTEGER,
        causaacci VARCHAR(200),
        caparod VARCHAR(100),
        sexo VARCHAR(20),
        aliento VARCHAR(20),
        cinturon VARCHAR(20),
        id_edad INTEGER,
        condmuerto INTEGER,
        condherido INTEGER,
        pasamuerto INTEGER,
        pasaherido INTEGER,
        peatmuerto INTEGER,
        peatherido INTEGER,
        ciclmuerto INTEGER,
        ciclherido INTEGER,
        otromuerto INTEGER,
        otroherido INTEGER,
        nemuerto INTEGER,
        neherido INTEGER,
        clasacc VARCHAR(50),
        estatus VARCHAR(50),
        año VARCHAR(10)
    );
    
    CREATE INDEX idx_anio ON accidentes_hermosillo(anio);
    CREATE INDEX idx_mes ON accidentes_hermosillo(mes);
    CREATE INDEX idx_tipaccid ON accidentes_hermosillo(tipaccid);
    CREATE INDEX idx_causaacci ON accidentes_hermosillo(causaacci);
    """
    
    try:
        with engine.connect() as conn:
            conn.execute(text(create_table_sql))
            conn.commit()
        print("\n✓ Tabla 'accidentes_hermosillo' creada exitosamente")
        print("✓ Índices creados para optimizar consultas")
        print("\n" + "="*80 + "\n")
    except Exception as e:
        print(f"\n❌ Error al crear la tabla: {e}")
        raise

# =============================================================================
# 2.4 ETL (EXTRACT, TRANSFORM, LOAD)
# =============================================================================

def proceso_etl_completo():
    """Ejecuta el proceso ETL completo"""
    print("\n" + "="*80)
    print("2.4 PROCESO ETL (EXTRACT, TRANSFORM, LOAD)")
    print("="*80 + "\n")
    
    # EXTRACT
    print("🔄 FASE 1: EXTRACCIÓN (Extract)")
    print("-" * 80)
    try:
        # Leer CSV con manejo especial de columnas
        df = pd.read_csv(CSV_PATH, encoding='utf-8', low_memory=False)
        
        # Verificar si hay problemas con las columnas
        print(f"✓ Datos cargados desde: {CSV_PATH}")
        print(f"✓ Registros extraídos: {len(df):,}")
        print(f"✓ Columnas detectadas: {len(df.columns)}")
        
        # Limpiar nombres de columnas (espacios, saltos de línea)
        df.columns = df.columns.str.strip().str.replace('\n', '').str.replace('\r', '')
        
        # Mostrar primeras columnas para diagnóstico
        print(f"\n🔍 Primeras 5 columnas: {df.columns[:5].tolist()}")
        print(f"🔍 Últimas 5 columnas: {df.columns[-5:].tolist()}")
        
        # Verificar si hay columna duplicada "AÑO" o "año"
        if 'AÑO' in df.columns and 'año' in df.columns:
            print("⚠️  Detectadas columnas duplicadas 'AÑO' y 'año', eliminando la última...")
            df = df.drop(columns=['año'])
        elif 'AÑO' in df.columns:
            df = df.rename(columns={'AÑO': 'año'})
            
    except FileNotFoundError:
        print(f"❌ Error: No se encontró el archivo '{CSV_PATH}'")
        print(f"   Verifica que la ruta sea correcta")
        return None, None
    except Exception as e:
        print(f"❌ Error al leer el archivo: {e}")
        print(f"   Tipo de error: {type(e).__name__}")
        return None, None
    
    # Exploración inicial
    exploracion_inicial(df)
    
    # TRANSFORM
    print("\n🔄 FASE 2: TRANSFORMACIÓN (Transform)")
    print("-" * 80)
    
    # PRIMERO: Filtrar años válidos (2018-2024)
    print("📅 Filtrando años válidos (2018-2024)...")
    print(f"   Registros antes del filtro de años: {len(df):,}")
    
    # Convertir año a numérico
    df['ANIO'] = pd.to_numeric(df['ANIO'], errors='coerce')
    
    # Filtrar solo años 2018-2024
    df = df[(df['ANIO'] >= 2018) & (df['ANIO'] <= 2024)]
    
    print(f"   Registros después del filtro de años: {len(df):,}")
    print(f"   Registros eliminados: {len(df[df['ANIO'] < 2018]):,}")
    
    # Verificar años únicos
    años_unicos = sorted(df['ANIO'].unique())
    print(f"   Años únicos en el dataset: {años_unicos}")
    
    # Filtrar Hermosillo
    df_hermosillo = filtrar_hermosillo(df)
    
    # Limpieza adicional
    print("🧹 Limpieza de datos:")
    print(f"   • Valores nulos antes: {df_hermosillo.isnull().sum().sum()}")
    
    # Normalizar nombres de columnas
    df_hermosillo.columns = df_hermosillo.columns.str.lower().str.strip()
    print("   ✓ Nombres de columnas normalizados a minúsculas")
    
    # IMPORTANTE: Convertir tipos de datos para evitar errores
    print("\n🔧 Conversión de tipos de datos:")
    
    # Columnas numéricas que deben ser enteros
    columnas_entero = ['id_entidad', 'id_municipio', 'anio', 'mes', 'id_hora', 
                       'id_minuto', 'id_dia', 'automovil', 'campasaj', 'microbus', 
                       'pascamion', 'omnibus', 'tranvia', 'camioneta', 'camion', 
                       'tractor', 'ferrocarri', 'motociclet', 'bicicleta', 'otrovehic',
                       'id_edad', 'condmuerto', 'condherido', 'pasamuerto', 'pasaherido',
                       'peatmuerto', 'peatherido', 'ciclmuerto', 'ciclherido', 
                       'otromuerto', 'otroherido', 'nemuerto', 'neherido']
    
    for col in columnas_entero:
        if col in df_hermosillo.columns:
            try:
                # Convertir a numérico, forzando errores a NaN
                df_hermosillo[col] = pd.to_numeric(df_hermosillo[col], errors='coerce')
                # Rellenar NaN con 0
                df_hermosillo[col] = df_hermosillo[col].fillna(0).astype(int)
            except Exception as e:
                print(f"   ⚠️  Error al convertir columna '{col}': {e}")
    
    # Verificación adicional: eliminar registros con año inválido
    registros_antes = len(df_hermosillo)
    df_hermosillo = df_hermosillo[df_hermosillo['anio'] >= 2018]
    registros_despues = len(df_hermosillo)
    
    if registros_antes != registros_despues:
        print(f"   ⚠️  Eliminados {registros_antes - registros_despues:,} registros con año < 2018")
    
    print("   ✓ Tipos de datos convertidos correctamente")
    
    # Columnas de texto
    columnas_texto = ['cobertura', 'diasemana', 'urbana', 'suburbana', 'tipaccid',
                      'causaacci', 'caparod', 'sexo', 'aliento', 'cinturon', 
                      'clasacc', 'estatus', 'año']
    
    for col in columnas_texto:
        if col in df_hermosillo.columns:
            df_hermosillo[col] = df_hermosillo[col].astype(str).fillna('')
    
    print(f"   • Valores nulos después: {df_hermosillo.isnull().sum().sum()}")
    
    # Verificar tipos de datos finales
    print("\n📋 Tipos de datos finales (muestra):")
    print(df_hermosillo.dtypes.head(10))
    
    # LOAD
    print("\n🔄 FASE 3: CARGA (Load)")
    print("-" * 80)
    
    # Crear base de datos
    try:
        crear_base_datos()
    except Exception as e:
        print(f"❌ Error al crear base de datos: {e}")
        print("   Verifica que PostgreSQL esté instalado y corriendo")
        print("   Verifica usuario y contraseña en DB_CONFIG")
        return None, None
    
    # Crear engine para SQLAlchemy
    try:
        engine = create_engine(
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
            f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )
        # Probar conexión
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✓ Conexión a base de datos establecida")
    except Exception as e:
        print(f"❌ Error al conectar a PostgreSQL: {e}")
        print("\n🔧 SOLUCIONES POSIBLES:")
        print("   1. Verifica que PostgreSQL esté corriendo")
        print("   2. Verifica usuario y contraseña en DB_CONFIG")
        print("   3. Verifica que el puerto 5432 esté disponible")
        return None, None
    
    # Crear tabla
    try:
        crear_tabla_accidentes(engine)
    except Exception as e:
        print(f"❌ Error al crear tabla: {e}")
        return None, None
    
    # Cargar datos
    print("📤 Cargando datos a PostgreSQL...")
    try:
        df_hermosillo.to_sql(
            'accidentes_hermosillo',
            engine,
            if_exists='append',
            index=False,
            chunksize=1000
        )
        print(f"✓ {len(df_hermosillo):,} registros cargados exitosamente")
    except Exception as e:
        print(f"❌ Error al cargar datos: {e}")
        print(f"   Tipo de error: {type(e).__name__}")
        return None, None
    
    print("\n" + "="*80 + "\n")
    
    return engine, df_hermosillo

# =============================================================================
# 2.5 VALIDACIÓN DE LA CARGA
# =============================================================================

def validar_carga(engine):
    """Valida que los datos se hayan cargado correctamente"""
    print("="*80)
    print("2.5 VALIDACIÓN DE LA CARGA")
    print("="*80)
    
    queries_validacion = {
        "Total de registros": "SELECT COUNT(*) as total FROM accidentes_hermosillo",
        "Distribución por año": """
            SELECT anio, COUNT(*) as cantidad 
            FROM accidentes_hermosillo 
            GROUP BY anio 
            ORDER BY anio
        """,
        "Top 5 tipos de accidente": """
            SELECT tipaccid, COUNT(*) as cantidad 
            FROM accidentes_hermosillo 
            GROUP BY tipaccid 
            ORDER BY cantidad DESC 
            LIMIT 5
        """,
        "Top 5 causas de accidente": """
            SELECT causaacci, COUNT(*) as cantidad 
            FROM accidentes_hermosillo 
            GROUP BY causaacci 
            ORDER BY cantidad DESC 
            LIMIT 5
        """
    }
    
    try:
        with engine.connect() as conn:
            for nombre, query in queries_validacion.items():
                print(f"\n📊 {nombre.upper()}:")
                resultado = pd.read_sql(query, conn)
                print(resultado.to_string(index=False))
        
        print("\n✅ VALIDACIÓN COMPLETADA EXITOSAMENTE")
        print("="*80 + "\n")
        
    except Exception as e:
        print(f"\n❌ Error en validación: {e}")

# =============================================================================
# FUNCIÓN PRINCIPAL
# =============================================================================

def main():
    """Función principal que ejecuta todo el proceso"""
    print("\n" + "="*80)
    print("ETAPA 2: COMPRENSIÓN Y CONEXIÓN A LOS DATOS")
    print("Proyecto: Análisis de Accidentes de Tránsito en Hermosillo, Sonora")
    print("="*80 + "\n")
    
    # Descripción de la fuente
    descripcion_fuente()
    
    # Proceso ETL completo
    resultado = proceso_etl_completo()
    
    # Verificar si hubo error
    if resultado is None or resultado == (None, None):
        print("\n❌ ETAPA 2 TERMINÓ CON ERRORES")
        print("\n🔧 Revisa los mensajes de error arriba")
        return None, None
    
    engine, df_hermosillo = resultado
    
    # Validación
    if engine is not None:
        validar_carga(engine)
        
        return engine, df_hermosillo
    
    return None, None

# =============================================================================
# EJECUCIÓN
# =============================================================================

if __name__ == "__main__":
    engine, df = main()