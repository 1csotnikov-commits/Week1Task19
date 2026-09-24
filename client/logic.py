"""Высокоуровневая логика MCP-клиента.

Здесь решается, что вызывать и как обрабатывать/форматировать результат.
Низкоуровневый транспорт находится в :mod:`client.session` и сюда не
подмешивается: логика оперирует только абстракцией :class:`client.session.MCPSession`.

Точки расширения на будущее (сейчас не реализуются):
- автономный tool use агент — цикл «решение → вызов инструмента → анализ
  результата» ляжет сюда, поверх :meth:`MCPApp.call_tool`;
- отложенное/периодическое выполнение инструментов;
- дополнительные инструменты регистрируются на сервере, а здесь достаточно
  обобщённого :meth:`MCPApp.call_tool` — специальный код не требуется.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass
from typing import Any, Callable

import mcp_types as types

from client.session import MCPSession

#: Имя переменной окружения, задающей Git-репозиторий по умолчанию.
GIT_REPO_PATH_ENV = "GIT_REPO_PATH"

#: Переменные окружения, которые клиент транслирует в подпроцесс сервера.
#: Серверу нужны: GIT_REPO_PATH (Git), DEEPSEEK_API_KEY (LLM для /summary),
#: SCHEDULER_DB_PATH (путь к БД планировщика).
FORWARDED_ENV_VARS = ("GIT_REPO_PATH", "DEEPSEEK_API_KEY", "SCHEDULER_DB_PATH")


@dataclass
class ToolInfo:
    """Информация об инструменте, удобная для отображения и вызова."""

    name: str
    description: str
    input_schema: dict[str, Any]


@dataclass
class StatusInfo:
    """Снимок состояния клиента для команды ``/status``."""

    connected: bool
    server_name: str | None
    server_version: str | None
    protocol_version: str | None
    tool_names: list[str]
    last_call_time: float | None


class MCPApp:
    """Высокоуровневая логика клиента поверх :class:`MCPSession`."""

    def __init__(self, session: MCPSession) -> None:
        self._session = session
        self._tools: list[ToolInfo] = []
        self._last_call_time: float | None = None

    @property
    def tools(self) -> list[ToolInfo]:
        """Список известных инструментов (кэш, полученный с сервера)."""
        return self._tools

    @property
    def session(self) -> MCPSession:
        """Доступ к низкоуровневой сессии (для расширений)."""
        return self._session

    async def connect(self) -> None:
        """Устанавливает соединение и сразу получает список инструментов."""
        await self._session.connect()
        await self.refresh_tools()

    async def disconnect(self) -> None:
        """Корректно закрывает MCP-соединение."""
        await self._session.close()

    async def reconnect(self) -> None:
        """Закрывает текущую сессию и устанавливает новую, заново получая инструменты."""
        await self._session.close()
        await self.connect()

    async def refresh_tools(self) -> list[ToolInfo]:
        """Запрашивает у сервера список инструментов и обновляет кэш."""
        tools = await self._session.list_tools()
        self._tools = [
            ToolInfo(name=t.name, description=t.description or "", input_schema=t.input_schema or {})
            for t in tools
        ]
        return self._tools

    def find_tool(self, name: str) -> ToolInfo | None:
        """Возвращает инструмент по имени или None, если он не найден."""
        for tool in self._tools:
            if tool.name == name:
                return tool
        return None

    async def call_tool(self, name: str, arguments: dict[str, Any] | None = None) -> str:
        """Вызывает инструмент и возвращает отформатированный текст результата."""
        result = await self._session.call_tool(name, arguments)
        self._last_call_time = time.time()
        return format_call_result(result)

    async def call_tool_raw(self, name: str, arguments: dict[str, Any] | None = None) -> Any:
        """Вызывает инструмент и возвращает сырой результат (dict/list/str).

        Используется движком пайплайнов, которому нужны структурированные
        выходы шагов (а не отформатированный текст).
        """
        result = await self._session.call_tool(name, arguments)
        self._last_call_time = time.time()
        if result.is_error:
            raise RuntimeError(format_call_result(result))
        if result.structured_content is not None:
            data = result.structured_content
            if isinstance(data, dict) and set(data.keys()) == {"result"}:
                data = data["result"]
            return data
        return _content_to_text(result)

    async def get_weather(self, city: str, days: int | None = None, units: str = "metric") -> str:
        """Вызывает погодный инструмент (текущая погода или прогноз) и возвращает текст.

        Если ``days`` не задан — вызывается ``get_current_weather``, иначе ``get_forecast``.
        """
        if days is not None:
            return await self.call_tool("get_forecast", {"city": city, "days": days, "units": units})
        return await self.call_tool("get_current_weather", {"city": city, "units": units})

    async def ask_weather(self, city: str, question: str | None = None) -> str:
        """Получает данные о погоде через MCP и передаёт их в LLM для формирования ответа.

        Это точка расширения под автономный tool use: данные инструмента подаются
        модели вместе с вопросом пользователя, модель формирует текстовый ответ.
        """
        from llm.provider import ask

        data = await self.get_weather(city)
        if question:
            prompt = (
                f"Данные о погоде: {data}\n\n"
                f"Вопрос пользователя: {question}\n\n"
                "Ответь, используя эти данные."
            )
        else:
            prompt = f"Данные о погоде: {data}\n\nОпиши текущую погоду и дай рекомендацию по одежде."
        return await asyncio.to_thread(ask, prompt)

    async def status(self) -> StatusInfo:
        """Формирует снимок состояния клиента."""
        info = self._session.server_info
        return StatusInfo(
            connected=self._session.is_connected,
            server_name=info.name if info else None,
            server_version=info.version if info else None,
            protocol_version=self._session.protocol_version,
            tool_names=[tool.name for tool in self._tools],
            last_call_time=self._last_call_time,
        )


def create_app(notification_callback: Callable[[str], None] | None = None) -> MCPApp:
    """Создаёт приложение с настройками по умолчанию.

    Транслирует нужные серверу переменные окружения (``GIT_REPO_PATH``,
    ``DEEPSEEK_API_KEY``, ``SCHEDULER_DB_PATH``) в подпроцесс сервера.
    ``notification_callback`` вызывается при получении push-уведомлений.
    """
    env: dict[str, str] = {}
    for name in FORWARDED_ENV_VARS:
        value = os.environ.get(name)
        if value:
            env[name] = value

    session = MCPSession(env=env or None, notification_callback=notification_callback)
    return MCPApp(session)


def format_call_result(result: types.CallToolResult) -> str:
    """Преобразует сырой результат вызова инструмента в читаемый текст.

    Структурированный результат (``structured_content``) выводится как JSON;
    иначе склеиваются текстовые блоки из ``content``.
    """
    if result.is_error:
        text = _content_to_text(result)
        return f"Ошибка: {text}"

    if result.structured_content is not None:
        data = result.structured_content
        # Снимаем служебную обёртку pydantic {"result": ...}, если она единственная.
        if isinstance(data, dict) and set(data.keys()) == {"result"}:
            data = data["result"]
        if isinstance(data, str):
            return data
        return json.dumps(data, ensure_ascii=False, indent=2, default=str)

    text = _content_to_text(result)
    return text or "(пустой результат)"


def format_tools(tools: list[ToolInfo]) -> str:
    """Форматирует список инструментов для вывода пользователю."""
    if not tools:
        return "Сервер не объявил ни одного инструмента."

    lines = [f"Доступно инструментов: {len(tools)}", ""]
    for index, tool in enumerate(tools, start=1):
        lines.append(f"{index}. {tool.name}")
        if tool.description:
            lines.append(f"   Описание: {tool.description}")
        schema = json.dumps(tool.input_schema, ensure_ascii=False, indent=2, default=str)
        lines.append("   Входные параметры (JSON-схема):")
        for schema_line in schema.splitlines():
            lines.append(f"     {schema_line}")
        lines.append("")
    return "\n".join(lines).rstrip()


def format_status(status: StatusInfo) -> str:
    """Форматирует снимок состояния клиента для вывода пользователю."""
    from datetime import datetime

    lines: list[str] = []
    lines.append(f"Подключён: {'да' if status.connected else 'нет'}")
    lines.append(f"Имя сервера: {status.server_name or '—'}")
    if status.server_version:
        lines.append(f"Версия сервера: {status.server_version}")
    lines.append(f"Версия протокола: {status.protocol_version or '—'}")
    lines.append(f"Инструменты ({len(status.tool_names)}): {', '.join(status.tool_names) or '—'}")
    if status.last_call_time is not None:
        formatted = datetime.fromtimestamp(status.last_call_time).strftime("%Y-%m-%d %H:%M:%S")
        lines.append(f"Последний успешный вызов: {formatted}")
    else:
        lines.append("Последний успешный вызов: —")
    return "\n".join(lines)


def _content_to_text(result: types.CallToolResult) -> str:
    """Собирает текст из всех текстовых контент-блоков результата."""
    parts: list[str] = []
    for block in result.content or []:
        text = getattr(block, "text", None)
        if text is not None:
            parts.append(str(text))
        else:
            parts.append(str(block))
    return "\n".join(parts)

