from jinja2 import Environment, FileSystemLoader
import pandas as pd
import plotly.express as px
import os
import numpy as np

def generate_report(df, dict, kpis, context):

    # --- 1. Dataframes ---
    output_path = context["report_path"]
    df_by_ads = dict['by_ads']
    df_by_sessions = dict['by_sessions']
    df_by_users = dict['by_users']
    df_by_questions = dict['by_questions']
    df_by_date = dict['by_date']
    df_technical_events = dict['technical_events']

    # --- 2. Calculated content ---




    # --- 4. Visualizations ---

    # --- 5. Render with Jinja2 ---
    env = Environment(loader=FileSystemLoader('templates'))
    template = env.get_template("main_template.html")

    html_content = template.render(
        kpis=kpis,
        )
    
    
    # --- 6. Write HTML file ---
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(f"✅ Report saved to {output_path}")





