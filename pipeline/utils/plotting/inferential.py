
import pandas as pd
import plotly.graph_objects as go

from pipeline.utils.inferential_helpers import compute_ci_counts

from config.plot_style import (DEFAULT_LAYOUT,
                               BAR_LAYOUT,
                               LINE_LAYOUT,
                               HIST_LAYOUT,
                               HEAT_LAYOUT,
                               PIE_LAYOUT,
)


def create_inferential_user_last_event_chart(df: pd.DataFrame):
    if df.empty:
        fig = go.Figure()
        fig.update_layout(title="No data")
        return fig.to_html(full_html=False, include_plotlyjs='cdn')

    counts = df['last_event_name'].value_counts()
    colors = [BAR_LAYOUT["colorway"][i % len(BAR_LAYOUT["colorway"])] for i in range(len(counts))]
    
    lower_err, upper_err = compute_ci_counts(counts)

    fig = go.Figure(
        data=go.Bar(
            x=counts.index,
            y=counts.values,
            marker=dict(color=colors),
            error_y=dict(
                type='data',
                symmetric=False,
                array=upper_err,
                arrayminus=lower_err
            )
        )
    )
    fig.update_layout(
        title='Last Events of Users',
        xaxis_title='Event Name',
        yaxis_title='Number of Sessions'
    )
    fig.update_layout(**DEFAULT_LAYOUT)
    fig.update_layout(**BAR_LAYOUT)
    return fig.to_html(full_html=False, include_plotlyjs='cdn', config={'responsive': True})


def create_inferential_session_last_event_chart(df: pd.DataFrame):
    if df.empty:
        fig = go.Figure()
        fig.update_layout(title="No data")
        return fig.to_html(full_html=False, include_plotlyjs='cdn', config={'responsive': True})

    counts = df['last_event_name'].value_counts()
    colors = [BAR_LAYOUT["colorway"][i % len(BAR_LAYOUT["colorway"])] for i in range(len(counts))]
    
    lower_err, upper_err = compute_ci_counts(counts)

    fig = go.Figure(
        data=go.Bar(
            x=counts.index,
            y=counts.values,
            marker=dict(color=colors),
            error_y=dict(
                type='data',
                symmetric=False,
                array=upper_err,
                arrayminus=lower_err
            )
        )
    )
    fig.update_layout(
        title='Last Event in Sessions',
        xaxis_title='Event Name',
        yaxis_title='Number of Sessions'
    )
    fig.update_layout(**DEFAULT_LAYOUT)
    fig.update_layout(**BAR_LAYOUT)
    return fig.to_html(full_html=False, include_plotlyjs='cdn', config={'responsive': True})
