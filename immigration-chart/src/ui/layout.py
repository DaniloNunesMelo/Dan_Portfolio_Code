"""
Main Gradio Blocks layout.
"""
import gradio as gr

from .callbacks import (
    update_metric_choices,
    update_groupby_choices,
    render_chart,
    render_pivot,
    render_history,
    refresh_data,
    get_source_info_md,
)
from .controls import (
    COUNTRIES,
    CHART_TYPES,
    GROUP_BY_OPTIONS,
    GENDER_OPTIONS,
    PIVOT_DIMENSIONS,
    METRICS_BY_COUNTRY,
    DEFAULT_YEAR_START,
    DEFAULT_YEAR_END,
    MIN_YEAR,
    MAX_YEAR,
)


def create_app() -> gr.Blocks:
    with gr.Blocks(title="Immigration Analysis Dashboard", theme=gr.themes.Soft()) as app:

        gr.Markdown(
            "# Immigration Analysis Dashboard\n"
            "**Italy & Canada** — Official data from OECD, Eurostat, and IRCC"
        )

        # ── Control row ──────────────────────────────────────────────
        with gr.Row():
            with gr.Column(scale=1):
                country_select = gr.CheckboxGroup(
                    choices=COUNTRIES,
                    value=["Italy"],
                    label="Destination Country",
                )
                metric_select = gr.Dropdown(
                    choices=METRICS_BY_COUNTRY["Italy"],
                    value="Inflows of Foreign Population",
                    label="Metric / Variable",
                    interactive=True,
                )
                with gr.Row():
                    year_start = gr.Slider(
                        minimum=MIN_YEAR, maximum=MAX_YEAR,
                        value=DEFAULT_YEAR_START, step=1, label="From Year",
                    )
                    year_end = gr.Slider(
                        minimum=MIN_YEAR, maximum=MAX_YEAR,
                        value=DEFAULT_YEAR_END, step=1, label="To Year",
                    )

            with gr.Column(scale=1):
                chart_type = gr.Radio(
                    choices=CHART_TYPES,
                    value="Line",
                    label="Chart Type",
                )
                group_by = gr.Dropdown(
                    choices=GROUP_BY_OPTIONS,
                    value="By Origin Country",
                    label="Group By",
                )

            with gr.Column(scale=1):
                top_n = gr.Slider(
                    minimum=5, maximum=50, value=15, step=5,
                    label="Top N Groups",
                )
                gender_filter = gr.Dropdown(
                    choices=GENDER_OPTIONS,
                    value="Total",
                    label="Gender",
                )
                refresh_btn = gr.Button("Refresh Live Data", variant="secondary")

        # ── Tabs ─────────────────────────────────────────────────────
        with gr.Tabs():

            with gr.Tab("Chart"):
                chart_output = gr.Plot(label="")
                chart_status = gr.Textbox(
                    label="Status", interactive=False, max_lines=1,
                    placeholder="Select countries to load data…",
                )

            with gr.Tab("Pivot Table"):
                with gr.Row():
                    pivot_rows = gr.Dropdown(
                        choices=PIVOT_DIMENSIONS,
                        value="counterpart_name",
                        label="Rows",
                    )
                    pivot_cols = gr.Dropdown(
                        choices=PIVOT_DIMENSIONS,
                        value="year",
                        label="Columns",
                    )
                pivot_output = gr.Dataframe(
                    interactive=False,
                    wrap=True,
                    label="Pivot Table",
                )

            with gr.Tab("History"):
                history_chart = gr.Plot(label="Data Coverage by Source")
                history_info = gr.Markdown("")

            with gr.Tab("Data Source Info"):
                source_info = gr.Markdown(
                    "Select countries and render a chart to see data source details."
                )

        # ── Inputs lists ─────────────────────────────────────────────
        chart_inputs = [
            country_select, metric_select,
            year_start, year_end,
            chart_type, group_by, top_n, gender_filter,
        ]
        pivot_inputs = [
            country_select, metric_select,
            year_start, year_end,
            pivot_rows, pivot_cols, gender_filter,
        ]

        # ── Event wiring ─────────────────────────────────────────────
        country_select.change(
            fn=update_metric_choices,
            inputs=[country_select],
            outputs=[metric_select],
        )
        country_select.change(
            fn=update_groupby_choices,
            inputs=[country_select],
            outputs=[group_by],
        )

        # Rerender chart on any control change
        for inp in chart_inputs:
            inp.change(
                fn=render_chart,
                inputs=chart_inputs,
                outputs=[chart_output, chart_status],
            )

        # Pivot table
        for inp in [pivot_rows, pivot_cols] + pivot_inputs:
            inp.change(
                fn=render_pivot,
                inputs=pivot_inputs,
                outputs=[pivot_output],
            )

        # History tab
        country_select.change(
            fn=render_history,
            inputs=[country_select],
            outputs=[history_chart, history_info],
        )

        # Source info tab
        country_select.change(
            fn=get_source_info_md,
            inputs=[country_select],
            outputs=[source_info],
        )

        # Refresh button
        refresh_btn.click(
            fn=refresh_data,
            inputs=[country_select],
            outputs=[chart_status],
        )
        refresh_btn.click(
            fn=render_chart,
            inputs=chart_inputs,
            outputs=[chart_output, chart_status],
        )

        # Load initial state
        app.load(
            fn=render_chart,
            inputs=chart_inputs,
            outputs=[chart_output, chart_status],
        )
        app.load(
            fn=render_history,
            inputs=[country_select],
            outputs=[history_chart, history_info],
        )
        app.load(
            fn=get_source_info_md,
            inputs=[country_select],
            outputs=[source_info],
        )

    return app
