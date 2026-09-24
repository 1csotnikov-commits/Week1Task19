"""Пакет движка пайплайнов.

Предоставляет загрузку конфигурации, резолвер шаблонов ``{{...}}`` и
последовательное выполнение шагов с вызовом MCP-инструментов.
"""

from pipelines.engine import (
    get_pipeline,
    load_pipelines,
    run_pipeline,
)
from pipelines.errors import (
    PipelineError,
    PipelineNotFoundError,
    PipelineStepError,
    TemplateResolveError,
)
from pipelines.resolver import resolve, resolve_path, resolve_template

__all__ = [
    "get_pipeline",
    "load_pipelines",
    "run_pipeline",
    "resolve",
    "resolve_path",
    "resolve_template",
    "PipelineError",
    "PipelineNotFoundError",
    "PipelineStepError",
    "TemplateResolveError",
]
