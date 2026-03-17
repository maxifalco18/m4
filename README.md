# 🏔️ Proyecto Integrador M4 — Data Engineering Pipeline

> Pipeline ETLT escalable sobre una arquitectura Data Lake en AWS (Medallion + Lambda)

[![CI Pipeline](https://github.com/maxifalco18/m4/actions/workflows/ci.yml/badge.svg)](https://github.com/maxifalco18/m4/actions/workflows/ci.yml)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## 📐 Arquitectura General

```
┌─────────────────────────────────────────────────────────────────────┐
│                         FUENTES DE DATOS                            │
│  [OpenWeatherMap API]          [PostgreSQL E-commerce (Supabase)]   │
└──────────────┬──────────────────────────────────┬───────────────────┘
               │             AIRBYTE              │
               ▼                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    AWS S3 — CAPA RAW (Bronze)                       │
│   raw/weather/YYYY/MM/DD/     raw/ecommerce/{tabla}/               │
└──────────────────────────────────┬──────────────────────────────────┘
                                   │          APACHE SPARK
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  AWS S3 — CAPA PROCESSED (Silver)                   │
│   dim_customers / dim_products / fact_orders / weather_enriched    │
└──────────────────────────────────┬──────────────────────────────────┘
                                   │          APACHE SPARK
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    AWS S3 — CAPA GOLD (Gold)                        │
│   KPIs de negocio listos para análisis (Parquet / Delta Lake)       │
└─────────────────────────────────────────────────────────────────────┘
               ↑ Arquitectura Lambda: Batch + Streaming (Kafka + Spark SS)

Gobernanza: AWS Lake Formation | Orquestación: Apache Airflow | CI/CD: GitHub Actions
```

## 📂 Estructura del Repositorio

```
proyecto_integrador_m4/
├── docs/                    # Documentación técnica y diagramas
├── infrastructure/          # Scripts de aprovisionamiento AWS
├── ingestion/               # Configuración Airbyte + validación
├── processing/              # Jobs PySpark (raw → processed → gold)
├── orchestration/           # Airflow DAGs + Docker Compose
├── streaming/               # Kafka + Spark Structured Streaming
├── quality/                 # Great Expectations + tests pytest
└── .github/workflows/       # CI/CD con GitHub Actions
```

## 🚀 Inicio Rápido

### Pre-requisitos
- Python 3.11+
- AWS CLI configurado (con IAM Role, sin access keys en texto plano)
- Docker & Docker Compose
- Cuenta Airbyte Cloud (Free Tier)

### Setup inicial
```bash
# 1. Clonar el repositorio
git clone https://github.com/maxifalco18/m4.git
cd m4

# 2. Copiar variables de entorno
cp .env.example .env
# Editar .env con tus valores (ARNs, nombres de bucket, etc.)

# 3. Crear el bucket S3 y la estructura de capas
pip install boto3
python infrastructure/s3/create_bucket.py

# 4. Configurar Lake Formation
python infrastructure/lake_formation/setup_lakeformation.py
```

## 🧱 Stack Tecnológico

| Capa | Herramienta | Rol |
|------|-------------|-----|
| Almacenamiento | AWS S3 | Data Lake (Bronze / Silver / Gold) |
| Gobernanza | AWS Lake Formation + IAM | Permisos, catálogo, auditoría |
| Ingesta | Airbyte Cloud | CDC + Full Refresh desde API y PG |
| Procesamiento | Apache Spark (PySpark) | Transformaciones distribuidas |
| Orquestación | Apache Airflow (Docker) | Scheduling, dependencias, alertas |
| Streaming | Apache Kafka + Spark SS | Arquitectura Lambda |
| Calidad | Great Expectations + pytest | Validación de datos |
| CI/CD | GitHub Actions | Lint, tests, deploy automático |
| Formato | Parquet / Delta Lake | Almacenamiento columnar eficiente |

## 📊 Preguntas de Negocio Respondidas

1. ¿Cuáles son los productos más vendidos por categoría y cómo varía su demanda en el tiempo?
2. ¿Qué clientes presentan mayor frecuencia de compra y cuál es su ticket promedio mensual?
3. ¿Qué tiendas/regiones generan mayores ingresos y cómo se comportan frente a la estacionalidad?
4. ¿Qué proporción de ventas proviene de clientes nuevos vs. recurrentes?
5. ¿Qué relación existe entre el precio promedio y el volumen de ventas?
6. ¿Cómo impactan las condiciones climáticas en el volumen de ventas en tiempo real?

## 🗺️ Avances del Proyecto

| Avance | Tema | Estado | Puntos |
|--------|------|--------|--------|
| 1 | Diseño de Arquitectura | ✅ Completado | 10 |
| 2 | Ingesta con Airbyte | ✅ Completado | 10 |
| 3 | Procesamiento con Spark | ✅ Completado | 15 |
| 4 | Orquestación y CI/CD | ✅ Completado | 15 |
| 5 | Streaming con Kafka | ✅ Completado | 10 |

## 🤝 Convenciones de Desarrollo

- **Ramas**: `main` (prod), `dev` (desarrollo), `feature/{avance}-{descripcion}`
- **Commits**: [Conventional Commits](https://www.conventionalcommits.org/) — `feat:`, `fix:`, `docs:`, `chore:`
- **PRs**: Un PR por avance con descripción y screenshots de evidencia

---

*Autor: Maximiliano Falco — ByHENRY Data Engineering Bootcamp M4*
