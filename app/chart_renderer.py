from __future__ import annotations

from pathlib import Path

from app.chart_planner import ChartSpec
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
        ax.set_xlabel(chart_spec.x)
        ax.set_ylabel(y_col)
        ax.tick_params(axis="x", rotation=35)
    elif chart_spec.chart_type == "bar" and chart_spec.x and chart_spec.y:
        x_data = [str(row.get(chart_spec.x)) for row in query_result.rows]
        y_col = chart_spec.y[0]
        y_data = [row.get(y_col) for row in query_result.rows]
        ax.bar(x_data, y_data)
        ax.set_xlabel(chart_spec.x)
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
