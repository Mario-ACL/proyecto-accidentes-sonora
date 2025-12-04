# 🚗 Análisis de Accidentes de Tránsito en Sonora

## 📋 Descripción del Proyecto

Este proyecto analiza los accidentes de tránsito en Sonora, durante el período 2018-2024, utilizando datos abiertos del INEGI. El objetivo principal es identificar el vehículo más peligroso y sus causas asociadas.

**Pregunta de investigación:**  
*¿Cuál es el vehículo más peligroso de manejar en Sonora y cuáles son las causas?*

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
│
├── 3PrepDatos/
│   └── ETL_postgreSQL.py         # Etapa 3: ETL y carga a PostgreSQL
│
├── 4AnalisisExp/
│   └── EDA.ipynb                 # Etapa 4: Análisis exploratorio
│
├── run_project.py                # 🚀 Script principal de ejecución automática
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

Edita la ruta en **`3PrepDatos/ETL_postgreSQL.py`** (línea 28):

```python
CSV_PATH = 'ruta/a/tu/datos_accidentes_2018_2024.csv'  # ← CAMBIAR
```

---

## 🚀 Ejecución del Proyecto

### Opción 1: Ejecución Automática (Recomendado)

Ejecuta el script principal que automáticamente ejecutará todas las etapas:

```bash
python run_project.py
```

Este script:
- ✅ Verifica prerequisitos
- ✅ Ejecuta la descarga de datos (connect_inegi.py)
- ✅ Ejecuta el ETL y carga a PostgreSQL (ETL_postgreSQL.py)
- ✅ Genera un reporte final con tiempos de ejecución
- ✅ Te indica si hubo errores y dónde

### Opción 2: Ejecución Manual

Si prefieres ejecutar paso por paso:

```bash
# Paso 1: Descargar datos
python 2ConexionADatos/connect_inegi.py

# Paso 2: ETL y carga a PostgreSQL
python 3PrepDatos/ETL_postgreSQL.py

# Paso 3: Análisis exploratorio (Jupyter)
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
2. **Vehículo más mortal por accidente:** Bicicleta (2.59% mortalidad)
3. **Causa principal:** [Se determina en el análisis]
4. **Hora más peligrosa:** [Se determina en el análisis]
5. **Combinaciones más mortales:** [Se determinan en el modelado]

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

### Error: "Invalid input syntax for type integer"

**Solución:**
- El ETL ahora limpia automáticamente estos errores
- Verifica que tu CSV tenga los headers correctos
- Ejecuta de nuevo `python ETL_postgreSQL.py`

---

## 📚 Dependencias Principales

- **pandas** - Análisis de datos
- **numpy** - Cálculos numéricos
- **matplotlib, seaborn** - Visualizaciones
- **sqlalchemy, psycopg2** - Conexión a PostgreSQL
- **scikit-learn** - Machine Learning (clustering, PCA)
- **scipy** - Tests estadísticos

---

## 👤 Autor

**[Tu Nombre]**  
Maestría en [Tu Programa]  
Universidad: [Tu Universidad]  
Fecha: Diciembre 2024

---

## 📄 Licencia

Este proyecto utiliza datos abiertos del INEGI bajo licencia de datos abiertos de México.

---

## 🙏 Agradecimientos

- **INEGI** - Por proporcionar los datos abiertos
- **PostgreSQL** - Sistema de base de datos
- **Python Data Science Stack** - Herramientas de análisis

---

## 📞 Contacto

Para preguntas o sugerencias sobre este proyecto:
- Email: [tu_email@ejemplo.com]
- GitHub: [tu-usuario]

---

**Última actualización:** Diciembre 2024