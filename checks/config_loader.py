"""Загрузка строго валидированных конфигураций ресурсов из JSON/YAML.

Файл считается недоверенным: ограничение размера, без произвольных YAML-тегов,
алиасов, повторных ключей и повторяющихся идентификаторов ресурсов.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator

from shared.schemas import ResourceConfig


class _StrictSafeLoader(yaml.SafeLoader):
    def compose_node(self, parent, index):
        if self.check_event(yaml.AliasEvent):
            raise ValueError("YAML aliases запрещены")
        return super().compose_node(parent, index)

    def construct_mapping(self, node, deep=False):
        result = {}
        for key, value in node.value:
            key_obj = self.construct_object(key, deep=deep)
            if not isinstance(key_obj, str):
                raise ValueError("Ключи YAML должны быть строками")
            if key_obj in result:
                raise ValueError(f"Повтор ключа YAML: {key_obj}")
            result[key_obj] = self.construct_object(value, deep=deep)
        return result


def _unique_json_keys(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for k, v in items:
        if k in result:
            raise ValueError(f"Повтор ключа JSON: {k}")
        result[k] = v
    return result


class ResourceFile(BaseModel):
    model_config = ConfigDict(extra="forbid")
    resources: list[ResourceConfig] = Field(min_length=1, max_length=1000)

    @field_validator("resources")
    @classmethod
    def unique_ids(cls, resources: list[ResourceConfig]):
        ids = [r.id for r in resources]
        if len(ids) != len(set(ids)):
            raise ValueError("resource_id должен быть уникальным")
        return resources


def load_resources(path: str | Path, *, max_bytes: int = 1024 * 1024) -> ResourceFile:
    p = Path(path)
    if p.suffix.casefold() not in {".yaml", ".yml", ".json"}:
        raise ValueError("Только .json, .yaml или .yml")
    if not 1 <= max_bytes <= 10 * 1024 * 1024:
        raise ValueError("Неверный лимит размера файла")
    with p.open("rb") as source:
        raw = source.read(max_bytes + 1)
    if len(raw) > max_bytes:
        raise ValueError("Файл конфигурации превышает лимит")
    data = raw.decode("utf-8-sig")
    if p.suffix.casefold() == ".json":
        obj = json.loads(data, object_pairs_hook=_unique_json_keys,
                         parse_constant=lambda _: (_ for _ in ()).throw(ValueError("NaN/Infinity запрещены")))
    else:
        obj = yaml.load(data, Loader=_StrictSafeLoader)
    return ResourceFile.model_validate(obj)
