"""Общий валидатор контракта Oracle Entity API."""

from __future__ import annotations

import re
from pathlib import Path

import yaml


SAFE_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


SAFE_SEGMENT = re.compile(r"^[A-Za-z0-9_-]+$")


class ContractMismatch(ValueError):
    """Ответ или описание API не соответствует контракту."""


def load_contract(path: str | Path) -> dict:
    raw = Path(path).read_bytes()

    if len(raw) > 16 * 1024:
        raise ContractMismatch("Файл контракта слишком большой")

    try:
        contract = yaml.safe_load(raw)
    except (yaml.YAMLError, UnicodeError):
        raise ContractMismatch(
            "Не удалось прочитать YAML контракта"
        ) from None

    required = {
        "source_entity",
        "records_path",
        "identity_field",
        "sequence_field",
        "response_items_field",
        "response_cursor_field",
    }

    if not isinstance(contract, dict):
        raise ContractMismatch("Контракт должен быть объектом")

    if set(contract) != required:
        raise ContractMismatch(
            "Неверный набор параметров контракта"
        )

    path_value = contract["records_path"]

    if not isinstance(path_value, str):
        raise ContractMismatch("Некорректный records_path")

    segments = path_value.split("/")

    if (
        not 1 <= len(path_value) <= 512
        or not path_value.startswith("/")
        or segments.count("{entity_id}") != 1
        or any(
            segment != "{entity_id}"
            and not SAFE_SEGMENT.fullmatch(segment)
            for segment in segments[1:]
        )
    ):
        raise ContractMismatch("Небезопасный records_path")

    for key in (
        "source_entity",
        "identity_field",
        "sequence_field",
        "response_items_field",
        "response_cursor_field",
    ):
        value = contract[key]

        if (
            not isinstance(value, str)
            or not SAFE_NAME.fullmatch(value)
        ):
            raise ContractMismatch(
                f"Некорректное имя параметра: {key}"
            )

    # Текущий Worker понимает только эту схему ответа.
    # Не допускаем ложного подтверждения другого формата.
    if (
        contract["response_items_field"] != "items"
        or contract["response_cursor_field"] != "next_cursor"
        or contract["sequence_field"] != "source_sequence"
    ):
        raise ContractMismatch(
            "Формат пока не поддерживается текущим Worker"
        )

    return contract
