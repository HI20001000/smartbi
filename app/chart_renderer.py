from __future__ import annotations

from pathlib import Path

from app.chart_planner import ChartSpec, ROW_INDEX_X_KEY
from app.query_executor import QueryResult


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

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.set_title(chart_spec.title)

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
                ax.annotate("缺值補0", (imputed_x[-1], imputed_y[-1]), textcoords="offset points", xytext=(8, 8), fontsize=9)

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

        ax.set_xlabel(chart_spec.x)
        ax.set_ylabel(y_col)
        ax.tick_params(axis="x", rotation=35)
    elif chart_spec.chart_type == "bar" and chart_spec.x and chart_spec.y:
        y_col = chart_spec.y[0]
        y_data = [row.get(y_col) for row in query_result.rows]
        if chart_spec.x == ROW_INDEX_X_KEY:
            x_data = [str(i + 1) for i, _ in enumerate(query_result.rows)]
            x_label = "row_index"
        else:
            x_data = [str(row.get(chart_spec.x)) for row in query_result.rows]
            x_label = chart_spec.x
        ax.bar(x_data, y_data)
        ax.set_xlabel(x_label)
        ax.set_ylabel(y_col)
        ax.tick_params(axis="x", rotation=35)
    else:
        ax.axis("off")
        preview = query_result.rows[:10]
        text = "\n".join(str(r) for r in preview) if preview else "No data"
        ax.text(0.02, 0.98, text, va="top", family="monospace")

    fig.tight_layout()
    fig.savefig(output, dpi=140)
    plt.close(fig)
    return str(output.resolve())
