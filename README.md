# Planeta Zuko ETL

**Sincroniza datos de Zuko a BigQuery de forma rápida.**

**Subido como Cloud Run Job**

## Descripción

Este script realiza un flujo ETL completo para sincronizar datos de sesiones de Zuko con BigQuery:

* **Extracción gradual**: obtiene sesiones paginadas desde la API de Zuko, respetando el rango de fechas configurado.
* **Transformación dinámica**: aplana estructuras JSON anidadas a un formato de columnas planas, normaliza nombres de campo (espacios y guiones reemplazados por guiones bajos) y convierte todos los valores a texto.
* **Escritura por lotes**: guarda cada registro directamente en archivos CSV dentro de `tmp/`, evitando la acumulación en memoria.
* **Carga inteligente**: al procesar cada formulario:

  * Si no existen sesiones, crea la tabla vacía en BigQuery para ese formulario.
  * Si la tabla ya existe, solo inserta filas nuevas basadas en la clave única (`id`).
  * Si la tabla no existe y hay datos, crea la tabla con el esquema definido y carga todos los registros.

## Configuración

Variables de entorno (local o en Cloud Run Job):

```env
ZUKO_API_KEY=tu_api_key_zuko
DAYS_BACK=30                # días atrás a consultar
RECEIVER=email@dominio.com   # destinatarios de notificaciones
```

> En Cloud Run Job, monta los secretos `GOOGLE_CREDENTIALS_SECRET` y `ZUKO_API_KEY`.

## Ajustes opcionales

* Cambia `DAYS_BACK` según volumen deseado.
* Ajusta esquema de BigQuery en `csv_to_bq()` si añades campos.