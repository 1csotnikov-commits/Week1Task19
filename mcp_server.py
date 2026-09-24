"""MCP-сервер, предоставляющий инструменты для работы с локальным Git-репозиторием.

Транспорт — stdio. Сервер запускается как подпроцесс MCP-клиентом
(`python mcp_server.py`) и обменивается с ним JSON-RPC сообщениями через
stdin/stdout. Поэтому здесь запрещено писать что-либо в stdout (выводом
пользуется транспорт); логи идут в stderr.

Регистрация нового инструмента сводится к добавлению функции с декоратором
``@server.tool()`` — это одна из точек расширения проекта.
"""

from __future__ import annotations

import asyncio
import httpx
import logging
import os
import shutil
import subprocess
import sys
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path
from typing import Annotated, Any
from urllib.parse import quote

from mcp.server.mcpserver import MCPServer
from pydantic import Field

from scheduler import AppContext
from scheduler.db import Database, now_iso, now_utc, to_utc_iso
from scheduler.scheduler import Scheduler
from scheduler.summary import get_summary as summarize

# httpx логирует каждый HTTP-запрос на уровне INFO и засоряет stderr сервера;
# оставляем только предупреждения и ошибки.
logging.getLogger("httpx").setLevel(logging.WARNING)

#: Имя переменной окружения, задающей путь к Git-репозиторию по умолчанию.
GIT_REPO_PATH_ENV = "GIT_REPO_PATH"

#: Формат вывода ``git log``. Спецсимволы ``%x00``/``%x1e`` — это *литеральные*
#: последовательности в формате git (сам git подставит байты NUL и RS в вывод),
#: поэтому аргумент командной строки остаётся чистым ASCII и работает на Windows.
#: Состав: hash, автор (имя), автор (email), дата, тема коммита.
_LOG_PRETTY = "format:%H%x00%an%x00%ae%x00%ad%x00%s%x1e"

#: Разделитель записей в выводе ``git log`` (символ RS, не встречается в данных).
_LOG_RECORD_SEP = "\x1e"
#: Разделитель полей внутри одной записи ``git log`` (символ NUL).
_LOG_FIELD_SEP = "\x00"


class GitError(Exception):
    """Ошибка при работе с Git: не установлен, не репозиторий или сбой команды."""


def _resolve_repo_path(repo_path: str | None) -> str:
    """Определяет путь к репозиторию: явный аргумент, затем ``GIT_REPO_PATH``, затем cwd."""
    if repo_path:
        return repo_path
    return os.environ.get(GIT_REPO_PATH_ENV) or os.getcwd()


def _ensure_git_available() -> None:
    """Проверяет, что ``git`` доступен в PATH, иначе бросает :class:`GitError`."""
    if shutil.which("git") is None:
        raise GitError(
            "Git не найден в PATH. Установите git и добавьте его в переменную окружения PATH."
        )


def _run_git(repo_path: str, args: list[str]) -> str:
    """Запускает ``git`` для указанного репозитория и возвращает stdout.

    Бросает :class:`GitError` с понятным сообщением, если git недоступен,
    путь не существует, путь не является репозиторием или команда завершилась
    с ошибкой.
    """
    _ensure_git_available()

    if not os.path.exists(repo_path):
        raise GitError(f"Путь не существует: {repo_path}")
    if not os.path.isdir(repo_path):
        raise GitError(f"Путь не является директорией: {repo_path}")

    command = ["git", "-C", repo_path, *args]
    proc = subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    if proc.returncode != 0:
        message = (proc.stderr or proc.stdout or "").strip()
        if "not a git repository" in message:
            raise GitError(f"Указанный путь не является Git-репозиторием: {repo_path}")
        raise GitError(f"Команда 'git {' '.join(args)}' завершилась с ошибкой: {message or 'неизвестная ошибка'}")

    return proc.stdout


def _parse_git_log(raw: str) -> list[dict[str, str]]:
    """Разбирает вывод ``git log`` с разделителями в список словарей коммитов."""
    commits: list[dict[str, str]] = []
    for record in raw.split(_LOG_RECORD_SEP):
        record = record.strip()
        if not record:
            continue
        fields = record.split(_LOG_FIELD_SEP)
        if len(fields) != 5:
            continue
        commit_hash, name, email, date, subject = fields
        commits.append(
            {
                "hash": commit_hash,
                "author": f"{name} <{email}>".strip(),
                "date": date,
                "message": subject,
            }
        )
    return commits


# --- Погода (Open-Meteo, без API-ключа) ---

#: Базовые эндпоинты Open-Meteo (ключ и регистрация не требуются).
GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

#: Допустимые системы единиц и их отображение в параметрах/выводе.
_UNITS_MAP = {
    "metric": {"temperature_unit": "celsius", "wind_speed_unit": "kmh", "temp": "°C", "wind": "км/ч"},
    "imperial": {"temperature_unit": "fahrenheit", "wind_speed_unit": "mph", "temp": "°F", "wind": "mph"},
}

#: Упрощённые русские описания кодов погоды WMO.
_WEATHER_CODES = {
    0: "ясно",
    1: "преимущественно ясно",
    2: "переменная облачность",
    3: "пасмурно",
    45: "туман",
    48: "изморозь/туман",
    51: "лёгкая морось",
    53: "морось",
    55: "сильная морось",
    56: "ледяная морось",
    57: "сильная ледяная морось",
    61: "небольшой дождь",
    63: "дождь",
    65: "сильный дождь",
    66: "ледяной дождь",
    67: "сильный ледяной дождь",
    71: "небольшой снег",
    73: "снег",
    75: "сильный снег",
    77: "снежные зёрна",
    80: "небольшой ливень",
    81: "ливень",
    82: "сильный ливень",
    85: "снегопад",
    86: "сильный снегопад",
    95: "гроза",
    96: "гроза с градом",
    99: "сильная гроза с градом",
}


class WeatherError(Exception):
    """Ошибка доступа к API погоды (сеть, HTTP, некорректный ответ)."""


class CityNotFoundError(Exception):
    """Город не найден в геокодинге."""


def _fetch_json(url: str, params: dict[str, Any]) -> dict[str, Any]:
    """Выполняет GET-запрос и возвращает JSON. Бросает :class:`WeatherError` при ошибке."""
    try:
        response = httpx.get(url, params=params, timeout=15.0)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as exc:
        raise WeatherError(f"HTTP {exc.response.status_code}") from exc
    except httpx.RequestError as exc:
        raise WeatherError(str(exc)) from exc
    except ValueError as exc:
        raise WeatherError("некорректный ответ API (не JSON)") from exc


def _geocode(city: str) -> tuple[float, float, str]:
    """Возвращает (широта, долгота, отображаемое имя) для города.

    Бросает :class:`CityNotFoundError`, если город не найден.
    """
    data = _fetch_json(
        GEOCODING_URL,
        {"name": city, "count": 1, "language": "ru", "format": "json"},
    )
    results = data.get("results") or []
    if not results:
        raise CityNotFoundError(f'Город "{city}" не найден.')
    first = results[0]
    return first["latitude"], first["longitude"], first.get("name") or city


def _weather_code_description(code: int | None) -> str:
    """Возвращает русское описание кода погоды WMO."""
    if code is None:
        return "неизвестно"
    return _WEATHER_CODES.get(int(code), f"код {code}")


def _current_weather_params(units: str) -> dict[str, Any]:
    """Параметры запроса текущей погоды для выбранной системы единиц."""
    unit = _UNITS_MAP[units]
    return {
        "current": "temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,weather_code,wind_speed_10m",
        "timezone": "auto",
        "temperature_unit": unit["temperature_unit"],
        "wind_speed_unit": unit["wind_speed_unit"],
    }


def _format_current_weather(city_display: str, current: dict[str, Any], units: str) -> str:
    """Форматирует текущую погоду в читаемый текст."""
    unit = _UNITS_MAP[units]
    lines = [f"Текущая погода в городе {city_display}:"]
    lines.append(f"  Температура: {current.get('temperature_2m')}{unit['temp']}")
    lines.append(f"  Ощущается как: {current.get('apparent_temperature')}{unit['temp']}")
    lines.append(f"  Ветер: {current.get('wind_speed_10m')} {unit['wind']}")
    lines.append(f"  Влажность: {current.get('relative_humidity_2m')}%")
    lines.append(f"  Осадки: {current.get('precipitation')} мм")
    lines.append(f"  Описание: {_weather_code_description(current.get('weather_code'))}")
    return "\n".join(lines)


def _format_forecast(city_display: str, daily: dict[str, Any], units: str) -> str:
    """Форматирует прогноз погоды по дням в читаемый текст."""
    unit = _UNITS_MAP[units]
    times = daily.get("time", [])
    tmax = daily.get("temperature_2m_max", [])
    tmin = daily.get("temperature_2m_min", [])
    precip = daily.get("precipitation_sum", [])
    wind = daily.get("wind_speed_10m_max", [])
    codes = daily.get("weather_code", [])

    lines = [f"Прогноз погоды в городе {city_display} на {len(times)} дн.:"]
    for i, date in enumerate(times):
        code = codes[i] if i < len(codes) else None
        lines.append(
            f"  {date}: {tmin[i]}{unit['temp']} … {tmax[i]}{unit['temp']}, "
            f"осадки {precip[i]} мм, ветер до {wind[i]} {unit['wind']}, "
            f"{_weather_code_description(code)}"
        )
    return "\n".join(lines)


# --- Планировщик (псевдо-24/7) ---

PROJECT_ROOT = Path(__file__).resolve().parent

#: User-Agent для запросов к Wikipedia (иначе API возвращает 403).
WIKIPEDIA_HEADERS = {"User-Agent": "Week1Task19-MCP/0.1 (educational project)"}

db = Database()
app_context = AppContext(db)


async def _capture_session_middleware(ctx, call_next):
    """Захватывает сессию для фоновых push-уведомлений (первый входящий запрос)."""
    app_context.set_session(ctx.session)
    return await call_next(ctx)


async def _server_tool_caller(name: str, arguments: dict[str, Any]) -> Any:
    """Вызывает инструмент сервера по имени (используется движком пайплайнов)."""
    from pipelines.errors import PipelineError

    tool = server._tool_manager._tools.get(name)
    if tool is None:
        raise PipelineError(f"Инструмент '{name}' не найден на сервере.")
    result = await asyncio.to_thread(tool.fn, **arguments)
    if isinstance(result, dict) and "error" in result:
        raise PipelineError(str(result["error"]))
    return result


@asynccontextmanager
async def _lifespan(app):
    """Запускает фоновый планировщик на время жизни сервера."""
    scheduler = Scheduler(db, app_context, tool_caller=_server_tool_caller)
    scheduler.start()
    try:
        yield app_context
    finally:
        await scheduler.stop()


server = MCPServer(
    name="git-mcp-server",
    title="Git MCP Server",
    description="MCP-сервер: Git, погода и планировщик задач (псевдо-24/7).",
    version="0.1.0",
    middleware=[_capture_session_middleware],
    lifespan=_lifespan,
)


@server.tool(structured_output=False)
def git_status() -> str:
    """Возвращает статус Git-репозитория: вывод ``git status --short`` и ``git status -sb``.

    Репозиторий берётся из переменной окружения ``GIT_REPO_PATH`` или, если она
    не задана, из текущей директории.
    """
    try:
        repo_path = _resolve_repo_path(None)
        short = _run_git(repo_path, ["status", "--short"]).rstrip("\n")
        branch = _run_git(repo_path, ["status", "-sb"]).rstrip("\n")
        return (
            f"Репозиторий: {repo_path}\n\n"
            f"=== git status --short ===\n{short or '(нет изменений)'}\n\n"
            f"=== git status -sb ===\n{branch or '(нет изменений)'}"
        )
    except GitError as exc:
        return f"Ошибка: {exc}"
    except Exception as exc:  # noqa: BLE001 - не даём серверу упасть
        return f"Непредвиденная ошибка: {exc}"


@server.tool()
def git_log(
    limit: Annotated[
        int,
        Field(description="Сколько последних коммитов вернуть", ge=1),
    ] = 10,
    repo_path: Annotated[
        str | None,
        Field(description="Путь к репозиторию; если не задан, используется GIT_REPO_PATH или текущая директория"),
    ] = None,
) -> list[dict[str, str]]:
    """Возвращает список последних N коммитов репозитория: hash, author, date, message.

    Возвращаемое значение — список словарей, каждый с ключами ``hash``, ``author``,
    ``date`` и ``message``.
    """
    try:
        if limit < 1:
            return [{"error": "Параметр limit должен быть положительным числом."}]

        path = _resolve_repo_path(repo_path)
        raw = _run_git(
            path,
            [
                "log",
                f"-n{limit}",
                "--date=iso-strict",
                f"--pretty={_LOG_PRETTY}",
            ],
        )
        commits = _parse_git_log(raw)
        if not commits:
            return [{"message": "В репозитории нет коммитов."}]
        return commits
    except GitError as exc:
        return [{"error": str(exc)}]
    except Exception as exc:  # noqa: BLE001 - не даём серверу упасть
        return [{"error": f"Непредвиденная ошибка: {exc}"}]


@server.tool(structured_output=False)
def get_current_weather(
    city: Annotated[str, Field(description="Название города (например, Moscow, London)")],
    units: Annotated[
        str,
        Field(description="Единицы измерения: metric (Цельсий, км/ч) или imperial (Фаренгейт, mph)"),
    ] = "metric",
) -> str:
    """Возвращает текстовое описание текущей погоды для указанного города.

    Данные берутся из Open-Meteo (без API-ключа): температура, ветер, влажность,
    осадки и краткое описание.
    """
    try:
        if not city or not city.strip():
            return "Некорректный параметр: город не может быть пустым."
        if units not in _UNITS_MAP:
            return "Некорректный параметр: units должен быть 'metric' или 'imperial'."

        city = city.strip()
        lat, lon, city_display = _geocode(city)
        data = _fetch_json(
            FORECAST_URL,
            {"latitude": lat, "longitude": lon, **_current_weather_params(units)},
        )
        current = data.get("current") or {}
        return _format_current_weather(city_display, current, units)
    except CityNotFoundError as exc:
        return str(exc)
    except WeatherError as exc:
        return f"Не удалось получить данные о погоде: {exc}"
    except Exception as exc:  # noqa: BLE001 - не даём серверу упасть
        return f"Непредвиденная ошибка: {exc}"


@server.tool(structured_output=False)
def get_forecast(
    city: Annotated[str, Field(description="Название города (например, Moscow, London)")],
    days: Annotated[int, Field(description="Количество дней прогноза (максимум 7)")] = 3,
    units: Annotated[str, Field(description="Единицы измерения: metric или imperial")] = "metric",
) -> str:
    """Возвращает текстовый прогноз погоды на N дней для указанного города.

    По каждому дню: дата, температура (min/max), осадки, ветер и краткое описание.
    """
    try:
        if not city or not city.strip():
            return "Некорректный параметр: город не может быть пустым."
        if days < 1:
            return "Некорректный параметр: days должен быть не меньше 1."
        if days > 7:
            return "Некорректный параметр: days не может быть больше 7."
        if units not in _UNITS_MAP:
            return "Некорректный параметр: units должен быть 'metric' или 'imperial'."

        city = city.strip()
        lat, lon, city_display = _geocode(city)
        unit = _UNITS_MAP[units]
        data = _fetch_json(
            FORECAST_URL,
            {
                "latitude": lat,
                "longitude": lon,
                "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max,weather_code",
                "forecast_days": days,
                "timezone": "auto",
                "temperature_unit": unit["temperature_unit"],
                "wind_speed_unit": unit["wind_speed_unit"],
            },
        )
        daily = data.get("daily") or {}
        return _format_forecast(city_display, daily, units)
    except CityNotFoundError as exc:
        return str(exc)
    except WeatherError as exc:
        return f"Не удалось получить данные о погоде: {exc}"
    except Exception as exc:  # noqa: BLE001 - не даём серверу упасть
        return f"Непредвиденная ошибка: {exc}"


@server.tool()
def schedule_reminder(
    text: Annotated[str, Field(description="Текст напоминания")],
    at: Annotated[str | None, Field(description="Когда напомнить (ISO 8601)")] = None,
    in_minutes: Annotated[int | None, Field(description="Через сколько минут напомнить (альтернатива at)")] = None,
    interval_seconds: Annotated[int | None, Field(description="Если задано — напоминание периодическое (период в секундах)")] = None,
) -> dict[str, Any]:
    """Создаёт разовое или периодическое напоминание. Возвращает id задачи."""
    try:
        if not text or not text.strip():
            return {"error": "Некорректный параметр: текст напоминания не может быть пустым."}
        if at is not None and in_minutes is not None:
            return {"error": "Некорректный параметр: укажите либо at, либо in_minutes."}
        if in_minutes is not None and in_minutes <= 0:
            return {"error": "Некорректный параметр: in_minutes должен быть положительным."}
        if interval_seconds is not None and interval_seconds <= 0:
            return {"error": "Некорректный параметр: interval_seconds должен быть положительным."}

        if at:
            next_run = to_utc_iso(at)
        elif in_minutes is not None:
            next_run = (now_utc() + timedelta(minutes=in_minutes)).isoformat()
        else:
            next_run = now_iso()

        schedule_id = db.create_schedule(
            "reminder",
            {"text": text.strip()},
            next_run_at=next_run,
            interval_seconds=interval_seconds or None,
            run_at=to_utc_iso(at) if at else None,
        )
        return {"id": schedule_id, "next_run_at": next_run}
    except Exception as exc:  # noqa: BLE001
        return {"error": f"Некорректный параметр: {exc}"}


@server.tool()
def schedule_weather_collection(
    city: Annotated[str, Field(description="Название города (например, Moscow)")],
    interval_minutes: Annotated[int, Field(description="Период сбора в минутах (минимум 1)")],
    units: Annotated[str, Field(description="Единицы измерения: metric или imperial")] = "metric",
) -> dict[str, Any]:
    """Создаёт периодический сбор погоды для города. Возвращает id задачи."""
    try:
        if not city or not city.strip():
            return {"error": "Некорректный параметр: город не может быть пустым."}
        if interval_minutes < 1:
            return {"error": "Некорректный параметр: interval_minutes должен быть не меньше 1."}
        if units not in ("metric", "imperial"):
            return {"error": "Некорректный параметр: units должен быть 'metric' или 'imperial'."}

        now = now_iso()
        schedule_id = db.create_schedule(
            "weather_collection",
            {"city": city.strip(), "units": units},
            next_run_at=now,
            interval_seconds=interval_minutes * 60,
        )
        return {"id": schedule_id, "next_run_at": now}
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}


@server.tool()
def list_schedules(
    status: Annotated[str | None, Field(description="Фильтр по статусу: active, paused, cancelled, completed")] = None,
) -> list[dict[str, Any]]:
    """Возвращает список всех запланированных задач со статусом и временем следующего запуска."""
    try:
        return db.list_schedules(status or None)
    except Exception as exc:  # noqa: BLE001
        return [{"error": str(exc)}]


@server.tool()
def cancel_schedule(
    schedule_id: Annotated[str, Field(description="Идентификатор задачи")],
) -> dict[str, Any]:
    """Отменяет задачу по id (status = 'cancelled')."""
    try:
        if db.get_schedule(schedule_id) is None:
            return {"error": f"Задача {schedule_id} не найдена."}
        db.set_status(schedule_id, "cancelled")
        return {"id": schedule_id, "status": "cancelled"}
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}


@server.tool()
def pause_schedule(
    schedule_id: Annotated[str, Field(description="Идентификатор задачи")],
) -> dict[str, Any]:
    """Ставит задачу на паузу (status = 'paused')."""
    try:
        if db.get_schedule(schedule_id) is None:
            return {"error": f"Задача {schedule_id} не найдена."}
        db.set_status(schedule_id, "paused")
        return {"id": schedule_id, "status": "paused"}
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}


@server.tool()
def resume_schedule(
    schedule_id: Annotated[str, Field(description="Идентификатор задачи")],
) -> dict[str, Any]:
    """Снимает задачу с паузы (status = 'active')."""
    try:
        if db.get_schedule(schedule_id) is None:
            return {"error": f"Задача {schedule_id} не найдена."}
        db.set_status(schedule_id, "active")
        return {"id": schedule_id, "status": "active"}
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}


@server.tool()
def get_summary(
    city: Annotated[str | None, Field(description="Город (опционально)")] = None,
    days: Annotated[int, Field(description="За сколько дней (по умолчанию 1)")] = 1,
    include_reminders: Annotated[bool, Field(description="Включить сводку по сработавшим напоминаниям")] = False,
    include_all_cities: Annotated[bool, Field(description="По всем городам")] = False,
) -> dict[str, Any]:
    """Возвращает агрегированные данные по собранной погоде + текстовое summary от LLM."""
    try:
        if days < 1:
            return {"error": "Некорректный параметр: days должен быть не меньше 1."}
        return summarize(
            db,
            city=city or None,
            days=days,
            include_reminders=include_reminders,
            include_all_cities=include_all_cities,
        )
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}


@server.tool()
def get_due_results(
    since: Annotated[str | None, Field(description="С какого времени (ISO 8601)")] = None,
) -> dict[str, Any]:
    """Возвращает результаты и неотвеченные события, отмечая события прочитанными."""
    try:
        results = db.list_results(since or None)
        events = db.list_events(acknowledged=0, since=since or None)
        db.acknowledge_events([e["id"] for e in events])
        return {"results": results, "events": events}
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}


@server.tool()
def search(
    query: Annotated[str, Field(description="Поисковый запрос")],
    limit: Annotated[int, Field(description="Сколько результатов вернуть (максимум 5)")] = 1,
    language: Annotated[str, Field(description="Язык Wikipedia (например, ru, en)")] = "ru",
) -> dict[str, Any]:
    """Ищет страницы в Wikipedia и возвращает краткое содержимое лучших статей."""
    try:
        if not query or not query.strip():
            return {"error": "Некорректный параметр: query не может быть пустым."}
        if limit < 1 or limit > 5:
            return {"error": "Некорректный параметр: limit должен быть от 1 до 5."}

        api = f"https://{language}.wikipedia.org/w/api.php"
        search_resp = httpx.get(
            api,
            params={"action": "query", "list": "search", "srsearch": query.strip(), "srlimit": limit, "format": "json"},
            headers=WIKIPEDIA_HEADERS,
            timeout=15.0,
        )
        search_resp.raise_for_status()
        found = search_resp.json().get("query", {}).get("search", [])
        if not found:
            return {"results": [], "message": f"По запросу «{query.strip()}» ничего не найдено."}

        titles = [r["title"] for r in found]
        extract_resp = httpx.get(
            api,
            params={
                "action": "query",
                "prop": "extracts",
                "explaintext": 1,
                "titles": "|".join(titles),
                "format": "json",
            },
            headers=WIKIPEDIA_HEADERS,
            timeout=15.0,
        )
        extract_resp.raise_for_status()
        pages = extract_resp.json().get("query", {}).get("pages", {})
        by_title = {p.get("title"): p.get("extract", "") for p in pages.values()}

        results = []
        for r in found:
            title = r["title"]
            results.append(
                {
                    "title": title,
                    "extract": by_title.get(title, ""),
                    "url": f"https://{language}.wikipedia.org/wiki/{quote(title.replace(' ', '_'))}",
                }
            )
        return {"results": results}
    except httpx.HTTPStatusError as exc:
        return {"error": f"Wikipedia вернул ошибку HTTP {exc.response.status_code}"}
    except httpx.RequestError as exc:
        return {"error": f"Не удалось обратиться к Wikipedia: {exc}"}
    except Exception as exc:  # noqa: BLE001
        return {"error": f"Непредвиденная ошибка: {exc}"}


@server.tool(structured_output=False)
def summarize(
    text: Annotated[str, Field(description="Текст для сжатия")],
    max_length: Annotated[int, Field(description="Примерная длина summary в символах")] = 500,
    style: Annotated[str, Field(description="Стиль summary")] = "кратко и по делу",
) -> Any:
    """Сжимает текст через LLM (DeepSeek)."""
    from llm.provider import LLMError, ask

    try:
        if not text or not text.strip():
            return {"error": "Некорректный параметр: text не может быть пустым."}
        prompt = (
            f"Сожми следующий текст. Стиль: {style}. "
            f"Примерная длина: {max_length} символов.\n\nТекст:\n{text}"
        )
        return ask(prompt)
    except LLMError as exc:
        return {"error": str(exc)}
    except Exception as exc:  # noqa: BLE001
        return {"error": f"Непредвиденная ошибка: {exc}"}


@server.tool(name="saveToFile")
def save_to_file(
    content: Annotated[str, Field(description="Что сохранить")],
    path: Annotated[str | None, Field(description="Путь к файлу; если не задан — генерируется в reports/")] = None,
    format: Annotated[str, Field(description="Расширение файла: md, txt, json")] = "md",
) -> dict[str, Any]:
    """Сохраняет содержимое в файл (только внутри корня проекта)."""
    try:
        if not content:
            return {"error": "Некорректный параметр: content не может быть пустым."}
        if format not in ("md", "txt", "json"):
            return {"error": "Некорректный параметр: format должен быть md, txt или json."}

        if path:
            target = Path(path)
            if not target.is_absolute():
                target = PROJECT_ROOT / target
            resolved = target.resolve()
            if not resolved.is_relative_to(PROJECT_ROOT.resolve()):
                return {"error": "Некорректный параметр: запрещено записывать файлы вне корня проекта."}
        else:
            timestamp = now_utc().strftime("%Y%m%d_%H%M%S")
            reports_dir = PROJECT_ROOT / "reports"
            reports_dir.mkdir(parents=True, exist_ok=True)
            target = reports_dir / f"report_{timestamp}.{format}"

        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
        return {"path": str(target.resolve()), "size_bytes": target.stat().st_size}
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}


@server.tool()
def schedule_pipeline(
    pipeline_name: Annotated[str, Field(description="Имя пайплайна из pipelines.json")],
    args: Annotated[dict[str, Any], Field(description="Аргументы пайплайна (JSON-объект)")],
    interval_minutes: Annotated[int | None, Field(description="Период запуска в минутах (для периодического)")] = None,
    in_minutes: Annotated[int | None, Field(description="Через сколько минут запустить (для разового)")] = None,
) -> dict[str, Any]:
    """Создаёт задачу запуска пайплайна (разовую или периодическую)."""
    try:
        if not pipeline_name or not pipeline_name.strip():
            return {"error": "Некорректный параметр: pipeline_name не может быть пустым."}
        if interval_minutes is not None and in_minutes is not None:
            return {"error": "Некорректный параметр: укажите либо interval_minutes, либо in_minutes."}
        if interval_minutes is not None and interval_minutes < 1:
            return {"error": "Некорректный параметр: interval_minutes должен быть не меньше 1."}
        if in_minutes is not None and in_minutes <= 0:
            return {"error": "Некорректный параметр: in_minutes должен быть положительным."}

        if in_minutes is not None:
            next_run = (now_utc() + timedelta(minutes=in_minutes)).isoformat()
        else:
            next_run = now_iso()

        schedule_id = db.create_schedule(
            "pipeline",
            {"pipeline_name": pipeline_name.strip(), "args": args or {}},
            next_run_at=next_run,
            interval_seconds=interval_minutes * 60 if interval_minutes else None,
        )
        return {"id": schedule_id, "next_run_at": next_run}
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}


if __name__ == "__main__":
    # UTF-8 для stderr (логи), чтобы кириллица не ломалась при перенаправлении.
    for _stream in (sys.stdout, sys.stderr):
        if hasattr(_stream, "reconfigure"):
            _stream.reconfigure(encoding="utf-8", errors="replace")

    # stdio — единственный используемый транспорт.
    server.run(transport="stdio")
