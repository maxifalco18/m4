# Avance 2 — Ingesta de Datos con Airbyte

## Visión General

```
┌─────────────────────┐    ┌──────────────────┐    ┌──────────────────────────┐
│ OpenWeatherMap API  │    │ PostgreSQL Olist  │    │ Validación Post-Ingesta  │
│ (HTTP genérico)     │───▶│ (Supabase/Neon)   │───▶│ validate_raw_data.py     │
│                     │    │ 9 tablas          │    │ MVP: conteo + schema     │
└─────────────────────┘    └──────────────────┘    └──────────────────────────┘
         │                          │                          │
         ▼                          ▼                          ▼
    s3://bucket/              s3://bucket/               Schema Drift
    raw/weather_api/          raw/ecommerce/              Detection
    Parquet (Snappy)          Parquet (Snappy)            (Schema Registry)
```

## Archivos del Avance 2

| Archivo | Propósito |
|---------|-----------|
| `ingestion/scripts/ddl_olist.sql` | DDL de 9 tablas Olist con FK, indexes CDC, y readonly_user |
| `ingestion/scripts/seed_data.py` | Carga masiva de CSVs de Kaggle → PostgreSQL (execute_values) |
| `ingestion/scripts/validate_raw_data.py` | Validación post-ingesta (MVP + PRO schema drift) |
| `ingestion/airbyte/connections/postgres_olist_config.json` | Config conector PostgreSQL (9 streams) |
| `ingestion/airbyte/connections/api_weather_config.json` | Config conector HTTP (backoff + rate limit) |

---

## Paso 1: Configurar PostgreSQL (Supabase)

### 1.1 Crear proyecto en Supabase
1. Ir a [supabase.com](https://supabase.com/) y crear un proyecto gratuito
2. Copiar las credenciales de conexión al `.env`

### 1.2 Ejecutar DDL
```sql
-- En el SQL Editor de Supabase, ejecutar el contenido de:
-- ingestion/scripts/ddl_olist.sql
```

### 1.3 Cargar datos de Kaggle
```bash
# 1. Descargar el dataset de Kaggle
# https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

# 2. Descomprimir en ./data/olist/

# 3. Cargar en PostgreSQL
python ingestion/scripts/seed_data.py --data-dir ./data/olist/
```

**Tablas y conteos esperados:**
| Tabla | Filas aprox. | Sync Mode |
|-------|-------------|-----------|
| `olist_orders` | ~99K | Incremental (cursor: `order_purchase_timestamp`) |
| `olist_order_items` | ~113K | Incremental (cursor: `updated_at`) |
| `olist_order_payments` | ~104K | Incremental (cursor: `updated_at`) |
| `olist_order_reviews` | ~100K | Incremental (cursor: `review_creation_date`) |
| `olist_customers` | ~99K | Full Refresh |
| `olist_products` | ~33K | Full Refresh |
| `olist_sellers` | ~3K | Full Refresh |
| `olist_geolocation` | ~1M | Full Refresh |
| `olist_product_category_name_translation` | ~71 | Full Refresh |

---

## Paso 2: Configurar Conectores en Airbyte Cloud

### 2.1 Fuente: PostgreSQL Olist
Referencia: `connections/postgres_olist_config.json`

| Config | Valor |
|--------|-------|
| Host | `db.xxxxx.supabase.co` |
| Port | `5432` |
| Database | `postgres` |
| User | `readonly_user` |
| SSL Mode | `require` |
| Replication | Standard (cursor-based) |

### 2.2 Fuente: OpenWeatherMap
Referencia: `connections/api_weather_config.json`

| Config | Valor |
|--------|-------|
| Base URL | `https://api.openweathermap.org/data/2.5` |
| Auth | API Key como query param `appid` |
| Backoff | Exponencial: 2s → 4s → 8s → max 60s |
| Rate Limit | 55 calls/min (margen sobre el límite de 60) |
| Retries | 5 intentos en HTTP 429, 5xx |

### 2.3 Destino: S3
| Config | Valor |
|--------|-------|
| Bucket | Variable `S3_BUCKET_NAME` |
| Path Pattern | `raw/{source}/{stream}/{YYYY}/{MM}/{DD}/` |
| Format | Parquet (Snappy) |
| Auth | IAM Role `AirbyteS3Role` (sin access keys) |

---

## Paso 3: Validar Ingesta

```bash
# MVP: verificar archivos y conteos
python ingestion/scripts/validate_raw_data.py --bucket pi-m4-datalake-xxx

# PRO: registrar esquema actual como referencia (primera vez)
python ingestion/scripts/validate_raw_data.py --bucket pi-m4-datalake-xxx --register-schema

# Validar contra esquema registrado (ejecuciones posteriores)
python ingestion/scripts/validate_raw_data.py --bucket pi-m4-datalake-xxx --source ecommerce

# Dry-run local (sin AWS)
python ingestion/scripts/validate_raw_data.py --local-path ./test_data/raw/
```

### Niveles de Validación

| Nivel | Check | Qué detecta |
|-------|-------|-------------|
| **MVP** | Archivos existen | Airbyte falló en escribir |
| **MVP** | Tamaño > 0 | Archivos Parquet vacíos |
| **MVP** | Row count ≥ mínimo | Sync parcial o incompleta |
| **PRO** | Schema Drift - columnas agregadas | Airbyte cambió su output |
| **PRO** | Schema Drift - columnas eliminadas | Fuente cambió su esquema |
| **PRO** | Schema Drift - tipo cambiado | string→int que rompe Spark |
