# Avance 2 — Ingesta de Datos con Airbyte

## Configuración de Conectores

Este directorio documenta la configuración de los conectores de Airbyte para el Data Lake.

### Fuente 1: OpenWeatherMap (HTTP Genérico)

| Parámetro | Valor |
|-----------|-------|
| Connector | `Low-code HTTP API` (declarative YAML) |
| Base URL | `https://history.openweathermap.org/data/2.5/history/city` |
| Auth | Query param `appid` (API Key) |
| Destino | `s3://PI_BUCKET_NAME/raw/weather_api/YYYY/MM/DD/` |
| Formato | Parquet |
| Frecuencia | Manual (1x/día — límite Free Tier) |

**Parámetros requeridos** (configurar en Airbyte UI):
- `lat`: latitud (ej: -41.81 para Patagonia)
- `lon`: longitud (ej: -68.91)
- `type`: `hour`
- `cnt`: registros a extraer (max 168 = 7 días)
- `appid`: API Key de OpenWeatherMap (variable de entorno)

### Fuente 2: PostgreSQL E-commerce (Supabase/Neon)

| Parámetro | Valor |
|-----------|-------|
| Connector | `PostgreSQL` (nativo Airbyte) |
| Host | Variable de entorno `POSTGRES_HOST` |
| Port | 5432 |
| Database | `ecommerce` |
| User | `readonly_user` (sin permisos de escritura) |
| SSL | Requerido (`require`) |
| Sync Mode | Full Refresh inicial → Incremental Append (CDC con `updated_at`) |
| Destino | `s3://PI_BUCKET_NAME/raw/ecommerce/{tabla}/` |

**Tablas a sincronizar:**
- `orders` — CDC key: `updated_at`
- `customers` — CDC key: `updated_at`
- `products` — Full Refresh
- `order_items` — CDC key: `shipping_limit_date`
- `sellers` — Full Refresh
- `payments` — CDC key: `order_id`
- `reviews` — CDC key: `review_creation_date`
- `geolocation` — Full Refresh

## Validación Post-Ingesta

Después de cada sync, ejecutar:

```bash
python ingestion/scripts/validate_raw_data.py \
  --bucket PI_BUCKET_NAME \
  --layer raw/ecommerce
```

El script verifica:
- Existencia de archivos Parquet en el path esperado
- Row count > 0
- Schema consiste con el de sincronizaciones anteriores (Schema Evolution detection)
- No hay archivos vacíos (0 bytes)
