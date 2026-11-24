"""Safety checks applied to every query the model writes, before it runs.

The agent may only read from the governed Gold layer:
  * exactly one statement
  * a read-only query (SELECT / WITH / UNION), nothing that writes or changes state
  * every table must live in <catalog>.gold (CTE names are allowed)
  * a row limit is always enforced
"""
from dataclasses import dataclass

import sqlglot
from sqlglot import exp

DIALECT = "databricks"

# Any of these anywhere in the tree means the query is not read-only.
_FORBIDDEN = tuple(
    getattr(exp, name) for name in (
        "Insert", "Update", "Delete", "Merge", "Create", "Drop", "Alter", "AlterTable",
        "Command", "Grant", "Revoke", "TruncateTable", "Set", "Use", "Copy", "LoadData",
    ) if hasattr(exp, name)
)


class GuardrailError(ValueError):
    """Raised when generated SQL breaks a safety rule."""


@dataclass
class CheckedSQL:
    sql: str            # the SQL that will actually run (row limit applied)
    tables: list        # fully qualified tables the query reads


def check_sql(sql: str, catalog: str = "workspace", schema: str = "gold", row_limit: int = 1000) -> CheckedSQL:
    sql = (sql or "").strip().rstrip(";").strip()
    if not sql:
        raise GuardrailError("The model returned no SQL.")

    try:
        statements = [s for s in sqlglot.parse(sql, read=DIALECT) if s is not None]
    except sqlglot.errors.ParseError as e:
        raise GuardrailError(f"Could not parse the SQL: {str(e).splitlines()[0]}") from e
    if len(statements) != 1:
        raise GuardrailError("Only a single SQL statement is allowed.")
    tree = statements[0]

    if not isinstance(tree, (exp.Select, exp.Union, exp.Except, exp.Intersect)):
        raise GuardrailError(f"Only read-only SELECT queries are allowed (got {tree.key.upper()}).")
    bad = next(tree.find_all(*_FORBIDDEN), None) if _FORBIDDEN else None
    if bad is not None:
        raise GuardrailError(f"Statement type {bad.key.upper()} is not allowed.")

    cte_names = {c.alias_or_name.lower() for c in tree.find_all(exp.CTE)}
    tables = []
    for t in tree.find_all(exp.Table):
        name = t.name.lower()
        db = (t.db or "").lower()
        cat = (t.catalog or "").lower()
        if not db and name in cte_names:
            continue
        if not name:
            # table-valued functions such as explode(...) / range(...)
            continue
        if db != schema or cat not in ("", catalog):
            raise GuardrailError(
                f"Table '{t.sql(dialect=DIALECT)}' is outside {catalog}.{schema}. Only Gold tables can be queried.")
        tables.append(f"{catalog}.{schema}.{name}")

    if tree.args.get("limit") is None:
        if isinstance(tree, exp.Select):
            tree = tree.limit(row_limit)
            final = tree.sql(dialect=DIALECT)
        else:
            final = f"SELECT * FROM ({tree.sql(dialect=DIALECT)}) AS q LIMIT {row_limit}"
    else:
        final = tree.sql(dialect=DIALECT)

    return CheckedSQL(sql=final, tables=sorted(set(tables)))
