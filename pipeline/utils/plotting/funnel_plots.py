import plotly.graph_objects as go

from config.logging import get_logger

from pipeline.utils.plotting.plot_helpers import funnel_gradient
from config.plot_style import (DEFAULT_LAYOUT,
                               BAR_LAYOUT,
                               LINE_LAYOUT,
                               HIST_LAYOUT,
                               HEAT_LAYOUT,
                               PIE_LAYOUT,
                               FUNNEL_LAYOUT
)

def create_funnel_chart(title, df, stage_list, user_col='user_pseudo_id', version=None):
    """
    df: dataframe where each stage column is 0/1
    stage_list: ordered list of stages in the funnel
    user_col: column representing unique users
    """
    try:

        # Version Filtering
        if version is not None:
            df = df[df['start_version'] == version]
        # Values
        values = [df[user_col].nunique()] + [df[s].sum() for s in stage_list]
        labels = ["Total Installs"] + stage_list

        # Define colors (one per step)
        colors = funnel_gradient(len(labels))

        colors = colors[:len(labels)]  # just in case list is shorter than needed

        fig = go.Figure(
            go.Funnel(
                y=labels,
                x=values,
                textinfo="value+percent initial",
                marker={"color": colors}  # assign colors per step
            )
        )

        fig.update_layout(
            title = title,
            funnelmode="stack"
        )


        fig.update_layout(**DEFAULT_LAYOUT)
        fig.update_layout(**FUNNEL_LAYOUT)

        return fig.to_html(full_html=False,
                        include_plotlyjs='cdn',
                        config={'responsive': True})

    except Exception as e:
        return f"<p>Error: {e}</p>"
    


