# 🚗 Análisis de Accidentes de Tránsito en Sonora

## 📋 Descripción del Proyecto

Este proyecto analiza los accidentes de tránsito en Sonora, durante el período 2018-2024, utilizando datos abiertos del INEGI. El objetivo principal es identificar el vehículo más peligroso y sus causas asociadas.

**Pregunta de investigación:**  
*¿Cuál es el vehículo más peligroso de manejar en Sonora y cuáles son las causas?*

Si solo quieres ver los resultados rapidamente ve a 4AnalisisExp\EDA.ipynb y ve la libreta desde Github web.

---

## 🎯 Metodología: CRISP-DM

El proyecto sigue la metodología CRISP-DM con las siguientes etapas:

1. **Comprensión del problema** - Definición de objetivos y pregunta de investigación
2. **Comprensión y conexión a los datos** - ETL y carga a PostgreSQL
3. **Preparación de los datos** - Limpieza y transformación
4. **Análisis exploratorio (EDA)** - Series de tiempo y visualizaciones
5. **Modelado** - Clustering, PCA, análisis de correlaciones
6. **Conclusiones** - Hallazgos y recomendaciones

---

## 📁 Estructura del Proyecto

```
proyecto/
│
├── 1ComprensionDelProblema/
│   └── Pregunta_a_resolver.md   # Etapa 1: Definición del problema
│
├── 2ConexionADatos/
│   └── connect_inegi.py          # Etapa 2: Descarga de datos de INEGI
│   └── info_datos.md
├── 3PrepDatos/
│   └── ETL_postgreSQL.py         # Etapa 3: ETL y carga a PostgreSQL
│
├── 4AnalisisExp/
│   └── EDA.ipynb                 # Etapa 4, 5, 6: Análisis exploratorio, Modelado básico, Conclusiones accionables
│
├── requirements.txt              # Dependencias del proyecto
└── README.md                     # Este archivo
```

---

## 🔧 Instalación y Configuración

### 1. Prerequisitos

- **Python 3.8+**
- **PostgreSQL 12+** (instalado y corriendo)
- **Git** (opcional)

### 2. Clonar o descargar el proyecto

```bash
git clone <tu-repositorio>
cd proyecto-accidentes-sonora
```

### 3. Crear entorno virtual (recomendado)

```bash
# Windows
python -m venv venv
venv\Scripts\activate

# Linux/Mac
python3 -m venv venv
source venv/bin/activate
```

### 4. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 5. Configurar PostgreSQL

#### Crear usuario y base de datos (si es necesario):

```sql
-- Conectarse a PostgreSQL
psql -U postgres

-- Crear usuario (opcional)
CREATE USER tu_usuario WITH PASSWORD 'tu_contraseña';

-- El script creará automáticamente la base de datos
```

#### Configurar credenciales:

Edita las credenciales en **`3PrepDatos/ETL_postgreSQL.py`** (línea 23):

```python
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'user': 'postgres',          # ← CAMBIAR
    'password': 'tu_contraseña', # ← CAMBIAR
    'database': 'accidentes_hermosillo'
}
```

### 6. Configurar ruta del CSV

Edita la ruta en **`3PrepDatos/ETL_postgreSQL.py`** en caso de error (línea 28):

```python
CSV_PATH = 'data\processed\inegi_tidy.csv'  # ← CAMBIAR
```

---

## 🚀 Ejecución del Proyecto

### Ejecución Manual
Con la carpeta correcta abierta, 

Ejecutar paso por paso:

```bash
# Paso 1: Descargar datos
python 2ConexionADatos/connect_inegi.py
```
```bash

# Paso 2: ETL y carga a PostgreSQL
python 3PrepDatos/ETL_postgreSQL.py
```
```bash
# (Opcional) Paso 3: para ver Análisis exploratorio (Jupyter)
cd 4AnalisisExp
jupyter notebook EDA.ipynb
```

---

## 📊 Notebooks de Jupyter

El notebook contiene el análisis interactivo con visualizaciones:

### `4AnalisisExp/EDA.ipynb`
- Series de tiempo (años, meses, días, horas)
- Análisis de vehículos involucrados
- Principales causas de accidentes
- Tipos de accidente más comunes
- Análisis de severidad
- Modelado avanzado (clustering, PCA, correlaciones)
- Matriz de riesgo: Vehículo × Causa
- Combinaciones más peligrosas

**Para abrir el notebook:**

```bash
cd 4AnalisisExp
jupyter notebook EDA.ipynb
```

---

## 📈 Resultados Principales

### Hallazgos Clave:

1. **Vehículo más involucrado en accidentes:** Automóvil (65,071 accidentes)
2. **Vehículo más mortal por accidente:** Omnibus (por cada 100 accidentes hay casi 3 muertos)
3. **Causa principal:** Por el error del conductor
4. **Hora más peligrosa:** Las 14:00 horas
5. **Combinaciones más mortales:** En motocicleta, camioneta y Otro Vehiculo la mayoria de accidentes son por culpa del peaton o pasajero

---

## 🗃️ Base de Datos

### Conexión a PostgreSQL:

```python
from sqlalchemy import create_engine

engine = create_engine(
    "postgresql://postgres:tu_contraseña@localhost:5432/accidentes_hermosillo"
)

# Consulta de ejemplo
import pandas as pd
query = "SELECT * FROM accidentes_hermosillo LIMIT 10"
df = pd.read_sql(query, engine)
```

### Estructura de la tabla `accidentes_hermosillo`:

- **Identificación:** id, cobertura, id_entidad, id_municipio
- **Temporal:** anio, mes, id_hora, id_minuto, id_dia, diasemana
- **Ubicación:** urbana, suburbana
- **Vehículos:** automovil, motociclet, camioneta, etc.
- **Causas:** causaacci, tipaccid
- **Severidad:** condmuerto, condherido, pasamuerto, pasaherido, etc.

---

## 🔍 Solución de Problemas

### Error: "No se puede conectar a PostgreSQL"

**Solución:**
1. Verifica que PostgreSQL esté corriendo:
   ```bash
   # Windows: Services → PostgreSQL → Start
   # Linux: sudo systemctl start postgresql
   # Mac: brew services start postgresql
   ```
2. Verifica credenciales en `DB_CONFIG`

### Error: "No se encontró el archivo CSV"

**Solución:**
1. Verifica la ruta en `CSV_PATH`
2. Asegúrate de que el archivo exista
3. Usa rutas absolutas si tienes problemas

### Error: "ModuleNotFoundError"

**Solución:**
```bash
pip install -r requirements.txt
```

## 📚 Dependencias Principales

- **pandas** - Análisis de datos
- **numpy** - Cálculos numéricos
- **matplotlib, seaborn** - Visualizaciones
- **sqlalchemy, psycopg2** - Conexión a PostgreSQL
- **scikit-learn** - Machine Learning (clustering, PCA)
- **scipy** - Tests estadísticos

---

## 👤 Autor

**Mario Alejandro Castro Lerma**  
Maestría en Ciencias de Datos 
Universidad: Universidad de Sonora 
Fecha: Diciembre 2024

---

## 📄 Licencia

Este proyecto utiliza datos abiertos del INEGI bajo licencia de datos abiertos de México.

---

## Agradecimientos

- **INEGI** - Por proporcionar los datos abiertos

---

**Última actualización:** Diciembre 2024
