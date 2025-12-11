
import pandas as pd
import numpy as np
import plotly.graph_objects as go

from pipeline.utils.inferential_helpers import (compute_ci_counts,
                                                binomial_count_ci)

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
    
    lower_err, upper_err = compute_ci_counts(counts, z=1.96)

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
    
    lower_err, upper_err = compute_ci_counts(counts, z=1.96)

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

def create_inferential_user_behaviour_per_day_chart(df: pd.DataFrame):

    df_sessions = (
        df[["user_pseudo_id", "event_params__ga_session_id", "session_duration_minutes"]]
        .drop_duplicates(subset=["user_pseudo_id", "event_params__ga_session_id"])
    )

    user_playtime = (
        df_sessions.groupby("user_pseudo_id", as_index=False)
                .agg(total_playtime_minutes=("session_duration_minutes", "sum"))
    )

    df = df.merge(user_playtime, on="user_pseudo_id", how="left")
    df["total_playtime_minutes"] = df["total_playtime_minutes"].fillna(0)

    df['install_flag'] = (df['event_name'] == 'First Open').astype(int)
    df['10_min_flag'] = (df['total_playtime_minutes'] >= 10).astype(int)
    df['tutorial_flag'] = (df['event_params__tutorial_video'] == 'tutorial_video').astype(int)
    df['game_end_flag'] = (df['event_name'] == 'Game Ended').astype(int)
    df['uninstall_flag'] = (df['event_name'] == 'App Removed').astype(int)

    daily_user = (
        df.groupby(['event_date', 'user_pseudo_id'], as_index=False)
        .agg({
            'install_flag': 'max',
            '10_min_flag': 'max',
            'tutorial_flag': 'max',
            'game_end_flag': 'max',
            'uninstall_flag': 'max'
        })
    )

    daily = daily_user.groupby('event_date').agg(
        installs=('install_flag', 'sum'),
        ten_min=('10_min_flag', 'sum'),
        tutorial=('tutorial_flag', 'sum'),
        game_end=('game_end_flag', 'sum'),
        uninstalls=('uninstall_flag', 'sum'),
        total_users=('user_pseudo_id', 'nunique')
    ).sort_index()

    fig = go.Figure()

    metrics = {
        'Installed': 'installs',
        'Passed 10 Minutes': 'ten_min',
        'Passed Tutorial': 'tutorial',
        'Completion': 'game_end',
        'Uninstalled': 'uninstalls'
    }

    for label, col in metrics.items():
        lower, upper = binomial_count_ci(daily[col], daily['total_users'], z=1.96)
        fig.add_trace(go.Scatter(
            x=daily.index,
            y=daily[col],
            mode='lines',
            name=label,
            error_y=dict(
                type='data',
                symmetric=False,
                array=upper,
                arrayminus=lower
            )
        ))

    fig.update_layout(
        title='Daily User Behaviour',
        xaxis_title='Date',
        yaxis_title='Count',
    )
    fig.update_layout(**DEFAULT_LAYOUT)
    fig.update_layout(**LINE_LAYOUT)
    return fig.to_html(full_html=False, include_plotlyjs='cdn', config={'responsive': True})
