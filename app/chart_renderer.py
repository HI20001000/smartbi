from __future__ import annotations

from pathlib import Path
import re

from app.chart_planner import ChartSpec, ROW_INDEX_X_KEY
from app.query_executor import QueryResult


_CJK_RE = re.compile(r"[\u3400-\u9fff]")


def _contains_cjk(text: str) -> bool:
    return bool(_CJK_RE.search(text or ""))


def _configure_matplotlib_cjk_font() -> bool:
    try:
        from matplotlib import font_manager as fm
        import matplotlib
    except Exception:
        return False

    preferred_fonts = [
        "Microsoft JhengHei",
        "Microsoft YaHei",
        "PMingLiU",
        "SimSun",
        "PingFang TC",
        "Noto Sans CJK TC",
        "Noto Sans CJK SC",
        "SimHei",
        "WenQuanYi Zen Hei",
        "Arial Unicode MS",
    ]

    available = {f.name for f in fm.fontManager.ttflist}
    for name in preferred_fonts:
        if name in available:
            matplotlib.rcParams["font.sans-serif"] = [name, "DejaVu Sans"]
            matplotlib.rcParams["axes.unicode_minus"] = False
            return True
    return False


def _safe_label(text: str, has_cjk_font: bool) -> str:
    raw = str(text or "")
    if _contains_cjk(raw) and not has_cjk_font:
        return "cjk_label"
    return raw


def render_chart(query_result: QueryResult, chart_spec: ChartSpec, output_path: str) -> str:
    """Render chart image to output_path and return absolute file path."""
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception as exc:  # pragma: no cover - environment dependent
        raise RuntimeError("matplotlib is required for chart rendering.") from exc

    has_cjk_font = _configure_matplotlib_cjk_font()

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.set_title(_safe_label(chart_spec.title, has_cjk_font))

    if chart_spec.chart_type == "line" and chart_spec.x and chart_spec.y:
        x_data = [row.get(chart_spec.x) for row in query_result.rows]
        y_col = chart_spec.y[0]
        y_data = [row.get(y_col) for row in query_result.rows]
        ax.plot(x_data, y_data, marker="o")

        # highlight imputed (zero-filled) points if query returns marker column
        imputed_key = "__imputed_zero_fill__"
        if any(imputed_key in row for row in query_result.rows):
            imputed_x = []
            imputed_y = []
            for row in query_result.rows:
                flag = row.get(imputed_key)
                if str(flag) in {"1", "True", "true"}:
                    imputed_x.append(row.get(chart_spec.x))
                    imputed_y.append(row.get(y_col))
            if imputed_x:
                ax.scatter(imputed_x, imputed_y, color="red", marker="x", s=55)
                annotation_text = "缺值補0" if has_cjk_font else "imputed_zero_fill"
                ax.annotate(annotation_text, (imputed_x[-1], imputed_y[-1]), textcoords="offset points", xytext=(8, 8), fontsize=9)

        # overlay a simple moving-average trend line when data points are enough
        numeric_y = [float(v) for v in y_data if isinstance(v, (int, float))]
        if len(numeric_y) >= 3 and len(numeric_y) == len(y_data):
            window = 3
            trend: list[float] = []
            for idx in range(len(numeric_y)):
                left = max(0, idx - window + 1)
                chunk = numeric_y[left : idx + 1]
                trend.append(sum(chunk) / len(chunk))
            ax.plot(x_data, trend, linestyle="--", linewidth=2, alpha=0.8)

        ax.set_xlabel(_safe_label(chart_spec.x, has_cjk_font))
        ax.set_ylabel(_safe_label(y_col, has_cjk_font))
        ax.tick_params(axis="x", rotation=35)
    elif chart_spec.chart_type == "bar" and chart_spec.x and chart_spec.y:
        y_col = chart_spec.y[0]
        y_data = [row.get(y_col) for row in query_result.rows]
        if chart_spec.x == ROW_INDEX_X_KEY:
            x_data = [str(i + 1) for i, _ in enumerate(query_result.rows)]
            x_label = "row_index"
        else:
            x_data = [_safe_label(str(row.get(chart_spec.x)), has_cjk_font) for row in query_result.rows]
            x_label = _safe_label(chart_spec.x, has_cjk_font)
        ax.bar(x_data, y_data)
        ax.set_xlabel(x_label)
        ax.set_ylabel(_safe_label(y_col, has_cjk_font))
        ax.tick_params(axis="x", rotation=35)
    elif chart_spec.chart_type == "pie" and chart_spec.x and chart_spec.y:
        y_col = chart_spec.y[0]
        labels = [_safe_label(str(row.get(chart_spec.x)), has_cjk_font) for row in query_result.rows]
        values = [float(row.get(y_col) or 0) for row in query_result.rows]
        ax.pie(values, labels=labels, autopct="%1.1f%%", startangle=90)
        ax.axis("equal")
    elif chart_spec.chart_type == "scatter" and chart_spec.x and chart_spec.y:
        x_col = chart_spec.x
        y_col = chart_spec.y[0]
        x_data = [float(row.get(x_col) or 0) for row in query_result.rows]
        y_data = [float(row.get(y_col) or 0) for row in query_result.rows]
        ax.scatter(x_data, y_data, alpha=0.8)
        ax.set_xlabel(_safe_label(x_col, has_cjk_font))
        ax.set_ylabel(_safe_label(y_col, has_cjk_font))
    else:
        ax.axis("off")
        preview = query_result.rows[:10]
        text = "\n".join(str(r) for r in preview) if preview else "No data"
        ax.text(0.02, 0.98, text, va="top", family="monospace")

    fig.tight_layout()
    fig.savefig(output, dpi=140)
    plt.close(fig)
    return str(output.resolve())
