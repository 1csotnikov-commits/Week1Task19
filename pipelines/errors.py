"""Ошибки движка пайплайнов."""

from __future__ import annotations

from typing import Any


class PipelineError(Exception):
    """Базовая ошибка пайплайна."""


class PipelineNotFoundError(PipelineError):
    """Пайплайн не найден в конфигурации."""


class TemplateResolveError(PipelineError):
    """Не удалось разрешить ссылку ``{{...}}`` в шаблоне."""


class PipelineStepError(PipelineError):
    """Шаг пайплайна завершился ошибкой.

    Несёт имя упавшего шага, имя инструмента, текст ошибки и лог уже
    выполненных шагов (для отладки).
    """

    def __init__(
        self,
        step_name: str,
        tool_name: str,
        message: str,
        steps_log: list[dict[str, Any]],
    ) -> None:
        self.step_name = step_name
        self.tool_name = tool_name
        self.message = message
        self.steps_log = steps_log
        super().__init__(f"Шаг '{step_name}' (инструмент {tool_name}) упал: {message}")
