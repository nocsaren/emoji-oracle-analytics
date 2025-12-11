import pandas as pd
import plotly.graph_objects as go

from config.plot_style import (DEFAULT_LAYOUT,
                               BAR_LAYOUT,
                               LINE_LAYOUT,
                               HIST_LAYOUT,
                               HEAT_LAYOUT,
                               PIE_LAYOUT,
)

                               

def create_user_behaviour_per_day_chart(df: pd.DataFrame):

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

    ).sort_index()

    
    # 5. Plot
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=daily.index,
        y=daily['installs'],
        mode='lines',
        name='Installed'
    ))

    fig.add_trace(go.Scatter(
        x=daily.index,
        y=daily['ten_min'],
        mode='lines',
        name='Passed 10 Minutes'
    ))

    fig.add_trace(go.Scatter(
        x=daily.index,
        y=daily['tutorial'],
        mode='lines',
        name='Passed Tutorial'
    ))

    fig.add_trace(go.Scatter(
        x=daily.index,
        y=daily['game_end'],
        mode='lines',
        name='Completion'
    ))

    fig.add_trace(go.Scatter(
        x=daily.index,
        y=daily['uninstalls'],
        mode='lines',
        name='Uninstalled'
    ))

    fig.update_layout(
        title='Daily User Behaviour',
        xaxis_title='Date',
        yaxis_title='Count',
    )
    fig.update_layout(**DEFAULT_LAYOUT)
    fig.update_layout(**LINE_LAYOUT)
    return fig.to_html(full_html=False, 
                       include_plotlyjs='cdn',
                       config={'responsive': True})


def create_user_last_event_chart(df: pd.DataFrame):

    if df.empty:
        # return an empty figure HTML if no data
        fig = go.Figure()
        fig.update_layout(title="No data")
        return fig.to_html(full_html=False, include_plotlyjs='cdn')

    # Count events (ordered by count)
    counts = df['last_event_name'].value_counts()
    colors = [BAR_LAYOUT["colorway"][i % len(BAR_LAYOUT["colorway"])] 
          for i in range(len(counts))]

    fig = go.Figure(
        data=go.Bar(
            x=counts.index,
            y=counts.values,
            marker=dict(color=colors)
        )
    )
    fig.update_layout(
        title=f'Last Events of Users',
        xaxis_title='Event Name',
        yaxis_title='Number of Sessions',
    )
    fig.update_layout(**DEFAULT_LAYOUT)
    fig.update_layout(**BAR_LAYOUT)
    return fig.to_html(full_html=False, 
                       include_plotlyjs='cdn',
                       config={'responsive': True})


def create_uninstall_last_event_chart(df: pd.DataFrame):

    uninstalled = df[df['App Removed'] == 1].copy()
    # Count events (ordered by count)
    counts = uninstalled['last_event_name'].value_counts()

    colors = [BAR_LAYOUT["colorway"][i % len(BAR_LAYOUT["colorway"])] 
          for i in range(len(counts))]

    fig = go.Figure(
        data=go.Bar(
            x=counts.index,
            y=counts.values,
            marker=dict(color=colors)
        )
    )
    fig.update_layout(
        title=f'Last Events of Users Before Uninstall',
        xaxis_title='Event Name',
        yaxis_title='Number of Sessions',
    )
    fig.update_layout(**DEFAULT_LAYOUT)
    fig.update_layout(**BAR_LAYOUT)
    return fig.to_html(full_html=False, 
                       include_plotlyjs='cdn',
                       config={'responsive': True})


def create_question_progress_histogram(df: pd.DataFrame):

    # Count events (ordered by count)
    counts = df['Question Completed'].value_counts()

    fig = go.Figure(
        data=go.Bar(
            x=counts.index,
            y=counts.values
        )
    )
    fig.update_layout(
        title=f'Questions Completed',
        xaxis_title='Question Completion',
        yaxis_title='Count of Question Completions',
    )
    fig.update_layout(**DEFAULT_LAYOUT)
    fig.update_layout(**HIST_LAYOUT)
    return fig.to_html(full_html=False, 
                       include_plotlyjs='cdn',
                       config={'responsive': True})


def create_character_progress_histogram(df: pd.DataFrame):

    # Count events (ordered by count)
    counts = df['total_characters_opened'].value_counts()

    fig = go.Figure(
        data=go.Bar(
            x=counts.index,
            y=counts.values
        )
    )
    fig.update_layout(
        title=f'Characters Opened',
        xaxis_title='Characters',
        yaxis_title='Count of Character Opens',
    )
    fig.update_layout(**DEFAULT_LAYOUT)
    fig.update_layout(**HIST_LAYOUT)
    return fig.to_html(full_html=False, 
                       include_plotlyjs='cdn',
                       config={'responsive': True})

def create_session_counts_histogram(df: pd.DataFrame):

    # Count events (ordered by count)
    counts = df['total_sessions'].value_counts()

    fig = go.Figure(
        data=go.Bar(
            x=counts.index,
            y=counts.values
        )
    )
    fig.update_layout(
        title=f'Number of Sessions Distribution',
        xaxis_title='Sessions',
        yaxis_title='Count of Sessions',
    )
    fig.update_layout(**DEFAULT_LAYOUT)
    fig.update_layout(**HIST_LAYOUT)
    return fig.to_html(full_html=False, 
                       include_plotlyjs='cdn',
                       config={'responsive': True})


def create_daily_install_uninstall_delta_chart(df: pd.DataFrame):
    """
    Create a bar chart showing the daily net change of installs minus uninstalls,
    with green/red coloring and numeric labels.
    """

    # Daily flags
    df['install_flag'] = (df['event_name'] == 'First Open').astype(int)
    df['uninstall_flag'] = (df['event_name'] == 'App Removed').astype(int)

    # Aggregate per day
    daily = df.groupby('event_date').agg(
        installs=('install_flag', 'sum'),
        uninstalls=('uninstall_flag', 'sum')
    ).sort_index()

    # Daily delta
    daily['delta'] = daily['installs'] - daily['uninstalls']

    # Colors: green for gains, red for losses, grey for zero
    colors = daily['delta'].apply(
        lambda x: 'green' if x > 0 else ('red' if x < 0 else 'lightgrey')
    )

    # Text labels, e.g. "+56" or "-77"
    text_labels = daily['delta'].apply(lambda x: f"{x:+d}")

    # Plot
    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=daily.index,
        y=daily['delta'],
        marker_color=colors,
        text=text_labels,
        textposition='outside',
        name='Daily Delta'
    ))

    fig.update_layout(
        title='Daily Install–Uninstall Delta',
        xaxis_title='Date',
        yaxis_title='Net Change',
        bargap=0.2,
        uniformtext_minsize=8,
        uniformtext_mode='hide',
    )
    fig.update_layout(**DEFAULT_LAYOUT)

    return fig.to_html(full_html=False, 
                       include_plotlyjs='cdn',
                       config={'responsive': True})

def create_users_per_day_chart(df: pd.DataFrame):
    """
    Create a line chart showing the number of unique users per day.
    """

    fig = go.Figure(
        data=go.Scatter(
            x=df['event_date'],
            y=df['unique_users'],
            mode='lines+markers',
            line=dict(color='blue'),
            marker=dict(size=6)
        )
    )
    fig.update_layout(
        title='Unique Users Per Day',
        xaxis_title='Date',
        yaxis_title='Number of Unique Users',
        xaxis=dict(tickformat='%Y-%m-%d'),
        yaxis=dict(dtick=1)
    )
    fig.update_layout(**DEFAULT_LAYOUT)
    fig.update_layout(**LINE_LAYOUT)

    return fig.to_html(full_html=False, 
                       include_plotlyjs='cdn',
                       config={'responsive': True})