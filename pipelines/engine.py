"""Движок пайплайнов: загрузка ``pipelines.json``, резолв шаблонов, выполнение."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Awaitable, Callable

from pipelines.errors import PipelineError, PipelineNotFoundError, PipelineStepError
from pipelines.resolver import resolve

#: Путь к ``pipelines.json`` по умолчанию (корень проекта).
DEFAULT_PATH = str(Path(__file__).resolve().parent.parent / "pipelines.json")

#: Вызыватель инструментов: (имя, аргументы) -> результат.
ToolCaller = Callable[[str, dict[str, Any]], Awaitable[Any]]
#: Колбэк отладки: получает информацию о шаге.
DebugCallback = Callable[[dict[str, Any]], None]


def load_pipelines(path: str | None = None) -> dict[str, dict[str, Any]]:
    """Читает ``pipelines.json`` и возвращает словарь пайплайнов."""
    path = path or DEFAULT_PATH
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError as exc:
        raise PipelineError(f"Файл пайплайнов не найден: {path}") from exc
    except json.JSONDecodeError as exc:
        raise PipelineError(f"Некорректный JSON в {path}: {exc}") from exc

    pipelines = data.get("pipelines", {})
    if not isinstance(pipelines, dict):
        raise PipelineError(f"Некорректная структура {path}: ожидается объект 'pipelines'.")
    return pipelines


def get_pipeline(name: str, path: str | None = None) -> dict[str, Any]:
    """Возвращает определение пайплайна по имени."""
    pipelines = load_pipelines(path)
    if name not in pipelines:
        raise PipelineNotFoundError(f"Пайплайн '{name}' не найден.")
    return pipelines[name]


async def run_pipeline(
    pipeline: dict[str, Any],
    args: dict[str, Any],
    tool_caller: ToolCaller,
    debug_callback: DebugCallback | None = None,
) -> tuple[Any, list[dict[str, Any]]]:
    """Выполняет пайплайн последовательно.

    Возвращает ``(результат последнего шага, лог шагов)``.
    При ошибке шага бросает :class:`PipelineStepError` с деталями.
    """
    steps = pipeline.get("steps", [])
    if not steps:
        raise PipelineError("Пайплайн не содержит шагов.")

    context: dict[str, Any] = {"args": args or {}, "steps": {}}
    steps_log: list[dict[str, Any]] = []

    for step in steps:
        name = step.get("name")
        tool = step.get("tool")
        if not name or not tool:
            raise PipelineError("Каждый шаг пайплайна должен иметь поля 'name' и 'tool'.")

        try:
            resolved_input = resolve(step.get("input", {}), context)
        except PipelineError as exc:
            raise PipelineStepError(name, tool, str(exc), steps_log) from exc

        if debug_callback:
            debug_callback({"phase": "start", "step": name, "tool": tool, "input": resolved_input})

        try:
            output = await tool_caller(tool, resolved_input)
        except Exception as exc:  # noqa: BLE001
            raise PipelineStepError(name, tool, str(exc), steps_log) from exc

        context["steps"][name] = {"output": output}
        steps_log.append({"step": name, "tool": tool, "input": resolved_input, "output": output})

        if debug_callback:
            debug_callback({"phase": "done", "step": name, "tool": tool, "input": resolved_input, "output": output})

    last_name = steps[-1].get("name")
    return context["steps"][last_name]["output"], steps_log
