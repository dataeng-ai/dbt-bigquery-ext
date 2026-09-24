"""Helpers for parameterized BigQuery queries used by ``execute_ext``.

Scalar parameters are supported today. ARRAY and STRUCT type strings are
recognized so callers get a clear "not implemented yet" error instead of a
silent mis-inference — reserved for a future extension.
"""

from __future__ import annotations

import datetime
import decimal
from typing import Any, Dict, List, Mapping, Optional, Sequence

from google.cloud.bigquery import ArrayQueryParameter, ScalarQueryParameter, StructQueryParameter

from dbt_common.exceptions import DbtRuntimeError

# Auto parallelism when worker_pool_size == 0
AUTO_WORKER_POOL_SIZE = 16
# Hard ceiling to avoid accidental thread storms from bad Jinja inputs
MAX_WORKER_POOL_SIZE = 1024

_SCALAR_TYPES = frozenset(
    {
        "STRING",
        "BYTES",
        "INT64",
        "INTEGER",  # alias accepted by client library
        "FLOAT64",
        "FLOAT",
        "NUMERIC",
        "BIGNUMERIC",
        "BOOL",
        "BOOLEAN",
        "DATE",
        "DATETIME",
        "TIME",
        "TIMESTAMP",
        "GEOGRAPHY",
        "JSON",
        "INTERVAL",
    }
)

# Reserved for future support — do not infer or build these yet.
_COMPLEX_TYPE_PREFIXES = ("ARRAY", "STRUCT")


def resolve_worker_pool_size(worker_pool_size: Any, num_jobs: int) -> int:
    """Resolve pool size: 0=auto(16), -1=len(jobs), >0=explicit."""
    if isinstance(worker_pool_size, bool) or not isinstance(worker_pool_size, int):
        raise DbtRuntimeError(
            f"worker_pool_size must be an int (-1, 0, or >0); got {worker_pool_size!r} "
            f"({type(worker_pool_size).__name__})"
        )
    if worker_pool_size < -1:
        raise DbtRuntimeError(
            f"worker_pool_size must be -1 (unlimited), 0 (auto), or a positive int; "
            f"got {worker_pool_size}"
        )
    if num_jobs <= 0:
        return 1
    if worker_pool_size == 0:
        return AUTO_WORKER_POOL_SIZE
    if worker_pool_size == -1:
        return num_jobs
    if worker_pool_size > MAX_WORKER_POOL_SIZE:
        raise DbtRuntimeError(
            f"worker_pool_size={worker_pool_size} exceeds maximum allowed "
            f"({MAX_WORKER_POOL_SIZE})"
        )
    return worker_pool_size


def _normalize_type_name(type_name: str) -> str:
    return type_name.strip().upper()


def _is_complex_type(type_name: str) -> bool:
    normalized = _normalize_type_name(type_name)
    return normalized.startswith(_COMPLEX_TYPE_PREFIXES)


def _reject_complex_type(name: str, type_name: str) -> None:
    if _is_complex_type(type_name):
        raise DbtRuntimeError(
            f"Query parameter {name!r} has type {type_name!r}. "
            "ARRAY and STRUCT parameters are reserved for a future execute_ext "
            "implementation; only scalar types are supported currently."
        )


def infer_scalar_type(name: str, value: Any) -> str:
    """Infer a BigQuery scalar type string from a Python value."""
    if value is None:
        raise DbtRuntimeError(
            f"Cannot infer type for parameter {name!r}: value is null. "
            "Pass variable_set_types with an explicit type for this parameter."
        )
    if isinstance(value, bool):
        return "BOOL"
    if isinstance(value, int):
        return "INT64"
    if isinstance(value, float):
        return "FLOAT64"
    if isinstance(value, bytes):
        return "BYTES"
    if isinstance(value, str):
        return "STRING"
    if isinstance(value, datetime.datetime):
        return "TIMESTAMP"
    if isinstance(value, datetime.date):
        return "DATE"
    if isinstance(value, datetime.time):
        return "TIME"
    if isinstance(value, decimal.Decimal):
        return "NUMERIC"
    if isinstance(value, (list, tuple)):
        raise DbtRuntimeError(
            f"Parameter {name!r} looks like an ARRAY value; ARRAY parameters are "
            "not implemented yet. Pass a scalar or extend query_parameters.py."
        )
    if isinstance(value, dict):
        raise DbtRuntimeError(
            f"Parameter {name!r} looks like a STRUCT value; STRUCT parameters are "
            "not implemented yet. Pass a scalar or extend query_parameters.py."
        )
    raise DbtRuntimeError(
        f"Cannot infer BigQuery type for parameter {name!r} "
        f"with Python type {type(value).__name__}"
    )


def resolve_parameter_types(
    variable_set: Mapping[str, Any],
    variable_set_types: Optional[Mapping[str, str]],
) -> Dict[str, str]:
    """Merge explicit types with inference; validate nulls and reserved types."""
    types: Dict[str, str] = {}
    explicit = {
        str(k): _normalize_type_name(v) for k, v in (variable_set_types or {}).items()
    }

    for name, value in variable_set.items():
        key = str(name)
        if key in explicit:
            type_name = explicit[key]
            _reject_complex_type(key, type_name)
            if type_name not in _SCALAR_TYPES and not _is_complex_type(type_name):
                # Allow unknown scalars through to the client (e.g. future types),
                # but reject obviously complex forms above.
                pass
            if value is None:
                # BigQuery does not allow NULL query parameter values.
                raise DbtRuntimeError(
                    f"Query parameter {key!r} is null. BigQuery parameterized "
                    "queries do not support NULL parameter values."
                )
            types[key] = type_name
        else:
            if value is None:
                raise DbtRuntimeError(
                    f"Cannot infer type for parameter {key!r}: value is null. "
                    "Pass variable_set_types with an explicit type for this parameter."
                )
            types[key] = infer_scalar_type(key, value)

    # Explicit types for keys missing from this variable set are ignored per-row;
    # callers may share one type map across heterogeneous sets.
    return types


def build_query_parameters(
    variable_set: Mapping[str, Any],
    variable_set_types: Optional[Mapping[str, str]] = None,
) -> List[Any]:
    """Build google-cloud-bigquery query_parameters for one variable set."""
    if not isinstance(variable_set, Mapping):
        raise DbtRuntimeError(
            f"Each entry in variable_set_values must be a mapping of name->value; "
            f"got {type(variable_set).__name__}"
        )

    types = resolve_parameter_types(variable_set, variable_set_types)
    parameters: List[Any] = []

    for name, value in variable_set.items():
        key = str(name)
        type_name = types[key]
        _reject_complex_type(key, type_name)

        # Factory reserved for future ARRAY/STRUCT builders.
        parameters.append(_build_parameter(key, type_name, value))

    return parameters


def _build_parameter(name: str, type_name: str, value: Any) -> Any:
    """Construct a query parameter. Scalar only for now; hooks for ARRAY/STRUCT."""
    normalized = _normalize_type_name(type_name)
    if normalized.startswith("ARRAY"):
        return _build_array_parameter(name, normalized, value)
    if normalized.startswith("STRUCT"):
        return _build_struct_parameter(name, normalized, value)
    return ScalarQueryParameter(name, normalized, value)


def _build_array_parameter(name: str, type_name: str, value: Any) -> ArrayQueryParameter:
    raise DbtRuntimeError(
        f"ARRAY query parameters are not implemented yet (parameter {name!r}, "
        f"type {type_name!r})."
    )


def _build_struct_parameter(name: str, type_name: str, value: Any) -> StructQueryParameter:
    raise DbtRuntimeError(
        f"STRUCT query parameters are not implemented yet (parameter {name!r}, "
        f"type {type_name!r})."
    )


def validate_variable_set_values(variable_set_values: Any) -> List[Mapping[str, Any]]:
    if variable_set_values is None:
        return []
    if not isinstance(variable_set_values, Sequence) or isinstance(
        variable_set_values, (str, bytes)
    ):
        raise DbtRuntimeError(
            "variable_set_values must be a list of dicts "
            f"(got {type(variable_set_values).__name__})"
        )
    values: List[Mapping[str, Any]] = []
    for i, entry in enumerate(variable_set_values):
        if not isinstance(entry, Mapping):
            raise DbtRuntimeError(
                f"variable_set_values[{i}] must be a dict; got {type(entry).__name__}"
            )
        values.append(entry)
    return values


# BigQuery SchemaField.field_type → ScalarQueryParameter type string
_BQ_FIELD_TYPE_TO_PARAM: Mapping[str, str] = {
    "STRING": "STRING",
    "BYTES": "BYTES",
    "INTEGER": "INT64",
    "INT64": "INT64",
    "FLOAT": "FLOAT64",
    "FLOAT64": "FLOAT64",
    "NUMERIC": "NUMERIC",
    "BIGNUMERIC": "BIGNUMERIC",
    "BOOLEAN": "BOOL",
    "BOOL": "BOOL",
    "DATE": "DATE",
    "DATETIME": "DATETIME",
    "TIME": "TIME",
    "TIMESTAMP": "TIMESTAMP",
    "GEOGRAPHY": "GEOGRAPHY",
    "JSON": "JSON",
    "INTERVAL": "INTERVAL",
}


def bq_field_type_to_param_type(field_type: str) -> str:
    """Map a BigQuery schema field type to a query-parameter type string."""
    if field_type is None:
        raise DbtRuntimeError("Cannot map a null BigQuery field type to a query parameter")
    normalized = str(field_type).strip().upper()
    if normalized.startswith(_COMPLEX_TYPE_PREFIXES) or normalized in ("RECORD", "STRUCT"):
        raise DbtRuntimeError(
            f"Column type {field_type!r} is not supported for execute_ext variable sets "
            "(ARRAY/STRUCT/RECORD reserved). Use scalar columns only."
        )
    mapped = _BQ_FIELD_TYPE_TO_PARAM.get(normalized)
    if mapped is None:
        raise DbtRuntimeError(
            f"Unsupported BigQuery column type {field_type!r} for execute_ext "
            "variable_set_relation"
        )
    return mapped


def validate_variable_set_source(
    variable_set_values: Any,
    variable_set_relation: Any,
    variable_set_types: Any = None,
) -> None:
    """Ensure at most one of values / relation is provided; types only with values."""
    has_values = variable_set_values is not None
    has_relation = variable_set_relation is not None
    if has_values and has_relation:
        raise DbtRuntimeError(
            "execute_ext: pass only one of variable_set_values or variable_set_relation, "
            "not both"
        )
    if has_relation and variable_set_types is not None:
        raise DbtRuntimeError(
            "execute_ext: variable_set_types is not allowed with variable_set_relation "
            "(parameter types are read from the relation schema)"
        )


def render_variable_set_relation(relation: Any) -> str:
    """Render a dbt Relation (or string) to a SQL relation literal."""
    if relation is None:
        raise DbtRuntimeError("variable_set_relation is null")
    if isinstance(relation, str):
        rendered = relation.strip()
        if not rendered:
            raise DbtRuntimeError("variable_set_relation must be a non-empty relation")
        return rendered
    if hasattr(relation, "render") and callable(relation.render):
        rendered = str(relation.render()).strip()
        if not rendered:
            raise DbtRuntimeError("variable_set_relation.render() returned an empty string")
        return rendered
    rendered = str(relation).strip()
    if not rendered:
        raise DbtRuntimeError("variable_set_relation must be a non-empty relation")
    return rendered


def variable_sets_from_bq_rows(
    rows: Sequence[Any],
    schema: Sequence[Any],
) -> tuple[List[Dict[str, Any]], Dict[str, str]]:
    """Convert BigQuery row results + schema into variable_set_values and types.

    Each row becomes one dict (column name → value). Types come from the schema.
    NULL cell values raise — BigQuery query parameters cannot be NULL.
    """
    if schema is None:
        raise DbtRuntimeError(
            "variable_set_relation query returned no schema; cannot build parameter types"
        )

    types: Dict[str, str] = {}
    columns: List[str] = []
    for field in schema:
        name = getattr(field, "name", None)
        field_type = getattr(field, "field_type", None)
        if not name:
            raise DbtRuntimeError("variable_set_relation schema has a column with no name")
        mode = getattr(field, "mode", None)
        if mode and str(mode).upper() == "REPEATED":
            raise DbtRuntimeError(
                f"Column {name!r} is REPEATED (ARRAY); ARRAY parameters are not "
                "supported for execute_ext variable_set_relation"
            )
        types[str(name)] = bq_field_type_to_param_type(field_type)
        columns.append(str(name))

    values: List[Dict[str, Any]] = []
    for i, row in enumerate(rows):
        entry: Dict[str, Any] = {}
        for col in columns:
            try:
                # Row supports mapping access; fall back to getattr for plain objects.
                if hasattr(row, "get"):
                    value = row.get(col)
                elif isinstance(row, Mapping):
                    value = row[col]
                else:
                    value = row[col]
            except Exception as exc:  # noqa: BLE001 — normalize access errors
                raise DbtRuntimeError(
                    f"variable_set_relation row {i}: failed to read column {col!r}: {exc}"
                ) from exc
            if value is None:
                raise DbtRuntimeError(
                    f"variable_set_relation row {i}: column {col!r} is NULL. "
                    "BigQuery parameterized queries do not support NULL parameter values."
                )
            entry[col] = value
        values.append(entry)

    return values, types
