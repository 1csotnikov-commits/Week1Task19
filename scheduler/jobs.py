"""Логика выполнения задач: ``reminder``, ``weather_collection`` и ``pipeline``."""

from __future__ import annotations

import logging
from typing import Any

import httpx

# Не засорять вывод логами httpx (каждый запрос на уровне INFO).
logging.getLogger("httpx").setLevel(logging.WARNING)

GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"


class JobError(Exception):
    """Ошибка выполнения задачи."""


def _geocode(city: str) -> tuple[float, float]:
    """Геокодирует город в (широта, долгота)."""
    resp = httpx.get(
        GEOCODING_URL,
        params={"name": city, "count": 1, "language": "ru", "format": "json"},
        timeout=15.0,
    )
    resp.raise_for_status()
    results = resp.json().get("results") or []
    if not results:
        raise JobError(f'Город "{city}" не найден.')
    return results[0]["latitude"], results[0]["longitude"]


def _fetch_weather_snapshot(city: str, units: str) -> dict[str, Any]:
    """Собирает снимок текущей погоды для города (Open-Meteo)."""
    lat, lon = _geocode(city)
    temp_unit = "celsius" if units == "metric" else "fahrenheit"
    wind_unit = "kmh" if units == "metric" else "mph"
    resp = httpx.get(
        FORECAST_URL,
        params={
            "latitude": lat,
            "longitude": lon,
            "current": "temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,weather_code,wind_speed_10m",
            "timezone": "auto",
            "temperature_unit": temp_unit,
            "wind_speed_unit": wind_unit,
        },
        timeout=15.0,
    )
    resp.raise_for_status()
    current = resp.json().get("current") or {}
    return {
        "city": city,
        "units": units,
        "temperature_2m": current.get("temperature_2m"),
        "relative_humidity_2m": current.get("relative_humidity_2m"),
        "apparent_temperature": current.get("apparent_temperature"),
        "precipitation": current.get("precipitation"),
        "weather_code": current.get("weather_code"),
        "wind_speed_10m": current.get("wind_speed_10m"),
    }


def run_reminder(schedule: dict[str, Any]) -> dict[str, Any]:
    """Выполняет напоминание и возвращает payload."""
    text = (schedule.get("params") or {}).get("text", "")
    return {"text": text}


def run_weather_collection(schedule: dict[str, Any]) -> dict[str, Any]:
    """Собирает погоду для города и возвращает payload с данными."""
    params = schedule.get("params") or {}
    city = params.get("city", "")
    units = params.get("units", "metric")
    return _fetch_weather_snapshot(city, units)


def execute_job(schedule: dict[str, Any]) -> dict[str, Any]:
    """Выполняет задачу по её типу и возвращает payload."""
    type_ = schedule.get("type")
    if type_ == "reminder":
        return run_reminder(schedule)
    if type_ == "weather_collection":
        return run_weather_collection(schedule)
    raise JobError(f"Неизвестный тип задачи: {type_}")


async def run_pipeline_job(schedule: dict[str, Any], tool_caller: Any) -> dict[str, Any]:
    """Выполняет пайплайн-задачу и возвращает payload (путь к файлу + summary)."""
    from pipelines.engine import load_pipelines, run_pipeline
    from pipelines.errors import PipelineError

    if tool_caller is None:
        raise PipelineError("tool_caller не настроен — пайплайн выполнить нельзя.")

    params = schedule.get("params") or {}
    name = params.get("pipeline_name", "")
    args = params.get("args", {})
    pipelines = load_pipelines()
    if name not in pipelines:
        raise JobError(f"Пайплайн '{name}' не найден.")

    result, steps_log = await run_pipeline(pipelines[name], args, tool_caller)

    payload: dict[str, Any] = {"pipeline_name": name, "result": result}
    if isinstance(result, dict) and "path" in result:
        payload["path"] = result["path"]
    for step in steps_log:
        if step["tool"] == "summarize":
            payload["summary"] = step["output"]
    return payload
