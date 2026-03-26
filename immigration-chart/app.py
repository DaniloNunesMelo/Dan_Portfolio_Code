"""
Immigration Analysis Dashboard
Entry point: python app.py
"""
import sys
from pathlib import Path
import gradio as gr

# Ensure src/ is importable
sys.path.insert(0, str(Path(__file__).parent))

# Import chart builders so they register themselves
import src.charts.line      # noqa: F401
import src.charts.bar       # noqa: F401
import src.charts.heatmap   # noqa: F401
import src.charts.choropleth  # noqa: F401
import src.charts.pie       # noqa: F401
import src.charts.bubble    # noqa: F401

from src.ui.layout import create_app

if __name__ == "__main__":
    app = create_app()
    app.launch(
        server_name="0.0.0.0",
        server_port=7860,
        share=False,
        show_error=True,
        theme=gr.themes.Soft(),
    )
