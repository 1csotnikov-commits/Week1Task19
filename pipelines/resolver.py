"""Резолвер шаблонов ``{{args.x}}`` и ``{{steps.y.output.z}}``.

Поддерживает обращение к полям объектов (``output.field``), индексам массивов
(``output.results[0]``) и их комбинациям (``output.results[0].extract``).
"""

from __future__ import annotations

import json
import re
from typing import Any

from pipelines.errors import TemplateResolveError

_TEMPLATE_RE = re.compile(r"\{\{\s*([^{}]+?)\s*\}\}")
_FULL_TEMPLATE_RE = re.compile(r"^\{\{\s*([^{}]+?)\s*\}\}$")


def _parse_path(path: str) -> list[Any]:
    """Разбирает путь вида ``results[0].extract`` в список ключей/индексов."""
    tokens: list[Any] = []
    for part in path.split("."):
        part = part.strip()
        if not part:
            continue
        m = re.match(r"^([^\[\]]+)?((?:\[\d+\])+)$", part)
        if m:
            if m.group(1):
                tokens.append(m.group(1))
            for idx in re.findall(r"\[(\d+)\]", m.group(2)):
                tokens.append(int(idx))
        else:
            tokens.append(part)
    return tokens


def _step_into(cur: Any, token: Any) -> Any:
    """Делает один шаг по объекту/списку. Бросает исключение при неудаче."""
    if isinstance(cur, dict):
        return cur[token]
    if isinstance(cur, list):
        if not isinstance(token, int):
            raise TypeError(f"ожидался индекс массива, получено '{token}'")
        return cur[token]
    raise TypeError(f"нельзя обратиться к '{token}' у значения {type(cur).__name__}")


def resolve_path(path: str, context: dict[str, Any]) -> Any:
    """Разрешает путь в контексте пайплайна (``args.*`` или ``steps.*``)."""
    tokens = _parse_path(path)
    ref = "{{" + path + "}}"
    if not tokens:
        raise TemplateResolveError(f"Не удалось разрешить ссылку: {ref} (пустой путь)")

    root = tokens[0]
    if root == "args":
        cur: Any = context.get("args", {})
        rest = tokens[1:]
    elif root == "steps":
        cur = context.get("steps", {})
        rest = tokens[1:]
    else:
        raise TemplateResolveError(f"Не удалось разрешить ссылку: {ref} (неизвестный корень '{root}')")

    try:
        for token in rest:
            cur = _step_into(cur, token)
    except (KeyError, IndexError, TypeError) as exc:
        raise TemplateResolveError(f"Не удалось разрешить ссылку: {ref} ({exc})") from exc
    return cur


def _stringify(value: Any) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, default=str)


def resolve_template(value: str, context: dict[str, Any]) -> Any:
    """Разрешает строку с шаблонами.

    Если строка целиком является одним шаблоном — возвращает сырое значение
    (dict/list/str), иначе подставляет строковые представления в текст.
    """
    full = _FULL_TEMPLATE_RE.match(value)
    if full:
        return resolve_path(full.group(1).strip(), context)

    def repl(match: re.Match) -> str:
        return _stringify(resolve_path(match.group(1).strip(), context))

    return _TEMPLATE_RE.sub(repl, value)


def resolve(value: Any, context: dict[str, Any]) -> Any:
    """Рекурсивно разрешает шаблоны в значении (str/dict/list)."""
    if isinstance(value, str):
        return resolve_template(value, context)
    if isinstance(value, dict):
        return {k: resolve(v, context) for k, v in value.items()}
    if isinstance(value, list):
        return [resolve(v, context) for v in value]
    return value
