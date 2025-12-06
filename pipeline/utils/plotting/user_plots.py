import pandas as pd
import plotly.graph_objects as go

def create_user_behaviour_per_day_chart(df: pd.DataFrame):
    """
    Create a line chart showing the number of new users per day.
    """

    df_sessions = (
        df[["user_pseudo_id", "event_params__ga_session_id", "session_duration_minutes"]]
            .drop_duplicates(subset=["user_pseudo_id", "event_params__ga_session_id"])
    )

    user_playtime = (
        df_sessions.groupby('user_pseudo_id', as_index=False)
                    .agg(total_playtime_minutes=("session_duration_minutes", "sum"))
    )
    print(user_playtime.head())
    
    # 1. Create install flag — unique users per day
    df['install_flag'] = (df['event_name'] == 'First Open').astype(int)
    df['10_min_flag'] = (user_playtime['total_playtime_minutes'] >= 10).astype(int)
    df['tutorial_flag'] = (df['event_params__tutorial_video'] == 'tutorial_video').astype(int)
    df['game_end_flag'] = (df['event_name'] == 'Game Ended').astype(int)
    df['uninstall_flag'] = (df['event_name'] == 'App Removed').astype(int)

    # 3. Aggregate per day
    daily = df.groupby('event_date').agg(
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

    return fig.to_html(full_html=False, include_plotlyjs='cdn')


def create_user_last_event_chart(df: pd.DataFrame):

    if df.empty:
        # return an empty figure HTML if no data
        fig = go.Figure()
        fig.update_layout(title="No data")
        return fig.to_html(full_html=False, include_plotlyjs='cdn')

    # Count events (ordered by count)
    counts = df['last_event_name'].value_counts()

    fig = go.Figure(
        data=go.Bar(
            x=counts.index,
            y=counts.values,
            marker=dict(color='magenta')
        )
    )
    fig.update_layout(
        title=f'Last Events of Users',
        xaxis_title='Event Name',
        yaxis_title='Number of Sessions'
    )
    return fig.to_html(full_html=False, include_plotlyjs='cdn')


def create_uninstall_last_event_chart(df: pd.DataFrame):

    uninstalled = df[df['App Removed'] == 1].copy()
    # Count events (ordered by count)
    counts = uninstalled['last_event_name'].value_counts()

    fig = go.Figure(
        data=go.Bar(
            x=counts.index,
            y=counts.values,
            marker=dict(color='magenta')
        )
    )
    fig.update_layout(
        title=f'Last Events of Users Before Uninstall',
        xaxis_title='Event Name',
        yaxis_title='Number of Sessions'
    )
    return fig.to_html(full_html=False, include_plotlyjs='cdn')


def create_question_progress_histogram(df: pd.DataFrame):

    # Count events (ordered by count)
    counts = df['Question Completed'].value_counts()

    fig = go.Figure(
        data=go.Bar(
            x=counts.index,
            y=counts.values,
            marker=dict(color='magenta')
        )
    )
    fig.update_layout(
        title=f'Questions Completed',
        xaxis_title='Question Completion',
        yaxis_title='Count of Question Completions'
    )
    return fig.to_html(full_html=False, include_plotlyjs='cdn')


def create_character_progress_histogram(df: pd.DataFrame):

    # Count events (ordered by count)
    counts = df['total_characters_opened'].value_counts()

    fig = go.Figure(
        data=go.Bar(
            x=counts.index,
            y=counts.values,
            marker=dict(color='magenta')
        )
    )
    fig.update_layout(
        title=f'Characters Opened',
        xaxis_title='Characters',
        yaxis_title='Count of Character Opens'
    )
    return fig.to_html(full_html=False, include_plotlyjs='cdn')

def create_session_counts_histogram(df: pd.DataFrame):

    # Count events (ordered by count)
    counts = df['total_sessions'].value_counts()

    fig = go.Figure(
        data=go.Bar(
            x=counts.index,
            y=counts.values,
            marker=dict(color='magenta')
        )
    )
    fig.update_layout(
        title=f'Number of Sessions Distribution',
        xaxis_title='Sessions',
        yaxis_title='Count of Sessions'
    )
    return fig.to_html(full_html=False, include_plotlyjs='cdn')