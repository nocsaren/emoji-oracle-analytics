from jinja2 import Environment, FileSystemLoader
import pandas as pd
import plotly.express as px
import os
import numpy as np

def generate_report(df: pd.DataFrame, output_path: str):

    # --- 2. Generate text summaries ---
    overview_text = f"The dataset contains {len(df):,} rows and {len(df.columns)} columns."
    stats_text = df.describe().to_html(classes='dataframe', border=0)

    # --- 3. Render with Jinja2 ---
    env = Environment(loader=FileSystemLoader('.'))
    template = env.get_template("report_template.html")

    html_content = template.render(
        overview_text=overview_text,
        stats_text=stats_text
    )

    # --- 4. Write HTML file ---
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(f"✅ Report saved to {output_path}")





