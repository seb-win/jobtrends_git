"""RunMetrics

Generische Helpers, um HTTP-Request-Metriken aus einem http_stats-Dict
in die Tabelle scrape_runs zu schreiben.

Design:
- compute ist rein-funktional (kein DB-Zugriff)
- update benötigt nur eine conn_factory (z.B. get_conn aus Templates.db_runs)
"""

from __future__ import annotations

from typing import Any, Callable, Dict, Mapping


__all__ = [
    "_compute_http_column_metrics",
    "_update_http_columns_in_db",
]


def _compute_http_column_metrics(http_stats: Mapping[str, Any] | None) -> Dict[str, int]:
    """
    Erwartetes (flexibles) Input-Schema:
      http_stats = {
        "requests_total": int,
        "requests_failed": int,
        "status_counts": {"200": 10, "403": 2, "429": 1, "500": 3, ...}
      }

    Output:
      Dict passend zu euren scrape_runs Spalten:
        http_requests_total, http_requests_failed, http_403_count, http_429_count, http_5xx_count
    """
    status_counts = (http_stats or {}).get("status_counts", {}) or {}

    def _get(code: str) -> int:
        try:
            return int(status_counts.get(str(code), 0) or 0)
        except Exception:
            return 0

    http_5xx = 0
    for k, v in status_counts.items():
        try:
            code_int = int(k)
        except Exception:
            continue
        if 500 <= code_int <= 599:
            try:
                http_5xx += int(v)
            except Exception:
                pass

    return {
        "http_requests_total": int((http_stats or {}).get("requests_total", 0) or 0),
        "http_requests_failed": int((http_stats or {}).get("requests_failed", 0) or 0),
        "http_403_count": _get("403"),
        "http_429_count": _get("429"),
        "http_5xx_count": int(http_5xx),
    }


def _update_http_columns_in_db(
    run_id: str,
    http_stats: Mapping[str, Any] | None,
    *,
    conn_factory: Callable[[], Any],
    table_name: str = "scrape_runs",
) -> None:
    """
    Schreibt die durch _compute_http_column_metrics berechneten Werte in die DB.

    Parameter:
      - run_id: PK/Identifier in scrape_runs
      - http_stats: siehe _compute_http_column_metrics
      - conn_factory: Callable ohne Args, liefert Context Manager (z.B. get_conn())
      - table_name: optional, default 'scrape_runs'

    Erwartet, dass die Ziel-Tabelle folgende Spalten hat:
      http_requests_total, http_requests_failed, http_403_count, http_429_count, http_5xx_count
    """
    metrics = _compute_http_column_metrics(http_stats)

    sql = f"""
    update {table_name}
       set http_requests_total = %s,
           http_requests_failed = %s,
           http_403_count = %s,
           http_429_count = %s,
           http_5xx_count = %s
     where run_id = %s;
    """

    with conn_factory() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    metrics["http_requests_total"],
                    metrics["http_requests_failed"],
                    metrics["http_403_count"],
                    metrics["http_429_count"],
                    metrics["http_5xx_count"],
                    run_id,
                ),
            )
        try:
            conn.commit()
        except Exception:
            # je nach Treiber/Connection-Handling kann autocommit aktiv sein
            pass
