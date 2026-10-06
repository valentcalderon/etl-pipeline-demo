import requests
from google.cloud import bigquery

PROJECT_ID = "etl-pipeline-demo-510819"
DATASET_ID = "raw_weather"
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.hourly_weather"

CITIES = {
    "Mexico City": (19.4326, -99.1332),
    "Madrid": (40.4168, -3.7038),
    "Buenos Aires": (-34.6037, -58.3816),
    "Bogota": (4.7110, -74.0721),
}


def fetch_city(city, lat, lon):
    """Descarga el clima por hora de los últimos 7 días para una ciudad."""
    response = requests.get(
        "https://api.open-meteo.com/v1/forecast",
        params={
            "latitude": lat,
            "longitude": lon,
            "hourly": "temperature_2m,precipitation,wind_speed_10m",
            "past_days": 7,
            "forecast_days": 1,
            "timezone": "UTC",
        },
        timeout=30,
    )
    response.raise_for_status()
    hourly = response.json()["hourly"]

    rows = []
    for i, ts in enumerate(hourly["time"]):
        rows.append({
            "city": city,
            "observed_at": f"{ts}:00Z",
            "temperature_c": hourly["temperature_2m"][i],
            "precipitation_mm": hourly["precipitation"][i],
            "wind_speed_kmh": hourly["wind_speed_10m"][i],
        })
    return rows


def load_to_bigquery(rows):
    client = bigquery.Client(project=PROJECT_ID)
    client.create_dataset(DATASET_ID, exists_ok=True)

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("observed_at", "TIMESTAMP"),
            bigquery.SchemaField("temperature_c", "FLOAT"),
            bigquery.SchemaField("precipitation_mm", "FLOAT"),
            bigquery.SchemaField("wind_speed_kmh", "FLOAT"),
        ],
        write_disposition="WRITE_TRUNCATE",  # reemplaza la tabla en cada corrida
    )
    job = client.load_table_from_json(rows, TABLE_ID, job_config=job_config)
    job.result()
    print(f"Loaded {len(rows)} rows into {TABLE_ID}")


if __name__ == "__main__":
    all_rows = []
    for city, (lat, lon) in CITIES.items():
        all_rows.extend(fetch_city(city, lat, lon))
    load_to_bigquery(all_rows)
