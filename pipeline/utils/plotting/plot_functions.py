import pandas as pd
import plotly.graph_objects as go


from config.logging import get_logger
from config.plot_style import (DEFAULT_LAYOUT,
                               BAR_LAYOUT,
                               LINE_LAYOUT,
                               HIST_LAYOUT,
                               HEAT_LAYOUT,
                               PIE_LAYOUT
)

import datetime as dt

logger = get_logger(__name__)

def create_wrong_answers_heatmap(df: pd.DataFrame):

    # --- constants controlling auto sizing ---
    row_px = 35          # pixels per row
    col_px = 25          # pixels per column
    header_px = 100      # space for x-axis labels
    left_px = 100        # space for y-axis labels


    # ------------------ aggregation ------------------
    heat = (
        df.groupby([
            'event_params__character_name',
            'event_params__current_tier',
            'event_params__current_question_index'
        ])['wrong_answer_ratio']
        .mean()
        .reset_index()
    )

    pivot = heat.pivot_table(
        index='event_params__character_name',
        columns=['event_params__current_tier', 'event_params__current_question_index'],
        values='wrong_answer_ratio',
        fill_value=0
    )

    pivot = pivot.sort_index(axis=1, level=[0, 1])
    pivot = pivot.reindex(sorted(pivot.index, key=lambda x: str(x).lower()), axis=0)

    # Build axis labels
    tiers = [f"{t}" for t, q in pivot.columns]
    questions = [f"{q}" for t, q in pivot.columns]

    # ------------------ auto figure size ------------------
    n_rows = len(pivot.index)
    n_cols = len(pivot.columns)

    # ------------------ figure ------------------
    fig = go.Figure(
        data=go.Heatmap(            
            z=pivot.values,
            x=[tiers, questions],
            y=pivot.index.tolist(),
            colorscale='OrRd',
            showscale=False,
            hovertemplate=(
                "Character: %{y}<br>"
                "Tier: %{x[0]}<br>"
                "Question: %{x[1]}<br>"
                "Wrong Ratio: %{z:.2f}<extra></extra>"
            )
        )
    )

    # force all y labels to show
    fig.update_yaxes(
        autorange='reversed',
        automargin=True,
        tickmode='array',
        tickvals=pivot.index.tolist(),
        ticktext=pivot.index.tolist(),
        tickfont=dict(size=12)
    )

    fig.update_layout(
        title = 'Wrong Answers per Question Heatmap',        
        xaxis=dict(type='multicategory'),
        yaxis=dict(title='Character'),
        xaxis_title='Tier - Question',
        autosize=True,
        margin=dict(l=left_px, r=40, t=160, b=80)
    )
    fig.update_layout(**DEFAULT_LAYOUT)
    fig.update_layout(**HEAT_LAYOUT)

    return fig.to_html(full_html=False, 
                       include_plotlyjs='cdn',
                       config={'responsive': True})




def create_cumulative_users_chart(df: pd.DataFrame):
    """
    Create an area chart showing cumulative unique users over time.
    """

    fig = go.Figure(
        data=go.Scatter(
            x=df['event_date'],
            y=df['unique_users'].cumsum(),
            mode='lines',
            fill='tozeroy',
            line=dict(color='green')
        )
    )
    fig.update_layout(
        title='Cumulative Unique Users Over Time',
        xaxis_title='Date',
        yaxis_title='Cumulative Unique Users',
        xaxis=dict(tickformat='%Y-%m-%d'),
        yaxis=dict(dtick=5)
    )
    fig.update_layout(**DEFAULT_LAYOUT)
    fig.update_layout(**LINE_LAYOUT)

    return fig.to_html(full_html=False, 
                       include_plotlyjs='cdn',
                       config={'responsive': True})


def create_cum_install_uninstall_chart(df: pd.DataFrame):

    # 1. Create install flag — unique users per day
    df['install_flag'] = (df['event_name'] == 'First Open').astype(int)

    # 2. Uninstall flag
    df['uninstall_flag'] = (df['event_name'] == 'App Removed').astype(int)

    # 3. Aggregate per day
    daily = df.groupby('event_date').agg(
        installs=('install_flag', 'sum'),
        uninstalls=('uninstall_flag', 'sum')
    ).sort_index()

    # 4. Compute cumulative
    daily['cum_installs'] = daily['installs'].cumsum()
    daily['cum_uninstalls'] = daily['uninstalls'].cumsum()

    # 5. Plot
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=daily.index,
        y=daily['cum_installs'],
        mode='lines',
        name='Cumulative Installs',
        fill='tozeroy'
    ))

    fig.add_trace(go.Scatter(
        x=daily.index,
        y=daily['cum_uninstalls'],
        mode='lines',
        name='Cumulative Uninstalls',
        fill='tozeroy'
    ))

    fig.update_layout(
        title='Cumulative Installs and Uninstalls',
        xaxis_title='Date',
        yaxis_title='Count',
    )
    fig.update_layout(**DEFAULT_LAYOUT)
    fig.update_layout(**LINE_LAYOUT)

    return fig.to_html(full_html=False, 
                       include_plotlyjs='cdn',
                       config={'responsive': True})



def create_sessions_per_day_chart(df: pd.DataFrame): 
    """
    Create a line chart showing the number of sessions per day.
    """

    fig = go.Figure(
        data=go.Scatter(
            x=df['event_date'],
            y=df['unique_sessions'],
            mode='lines+markers',
            line=dict(color='purple'),
            marker=dict(size=6)
        )
    )
    fig.update_layout(
        title='Sessions Per Day',
        xaxis_title='Date',
        yaxis_title='Number of Sessions',
        xaxis=dict(tickformat='%Y-%m-%d'),
        yaxis=dict(dtick=1),
    )
    fig.update_layout(**DEFAULT_LAYOUT)
    fig.update_layout(**LINE_LAYOUT)

    return fig.to_html(full_html=False, 
                       include_plotlyjs='cdn',
                       config={'responsive': True})

def create_session_duration_histogram(df: pd.DataFrame):
    """
    Create a histogram showing the distribution of session durations.
    """

    fig = go.Figure(
        data=go.Histogram(
            x=df['session_duration_seconds'] / 60
        )
    )
    fig.update_layout(
        title='Session Duration Distribution',
        xaxis_title='Session Duration (minutes)',
        yaxis_title='Count of Sessions'
    )
    fig.update_layout(**DEFAULT_LAYOUT)
    fig.update_layout(**HIST_LAYOUT)

    return fig.to_html(full_html=False, 
                       include_plotlyjs='cdn',
                       config={'responsive': True})

def create_total_playtime_histogram(df: pd.DataFrame):

    fig = go.Figure(
        data=go.Histogram(
            x=df['total_playtime_minutes'],
            xbins=dict(
                start=0,
                end=df['total_playtime_minutes'].max(),
                size=5   # adjust as you like
            )
        )
    )
    fig.update_layout(
        title='Total Playtime Distribution',
        xaxis_title='Total Playtime (minutes)',
        yaxis_title='Count of Playtime'
    )
    fig.update_layout(**DEFAULT_LAYOUT)
    fig.update_layout(**HIST_LAYOUT)

    return fig.to_html(full_html=False, 
                       include_plotlyjs='cdn',
                       config={'responsive': True})



def create_ads_per_question_heatmap(df: pd.DataFrame):

    # --- constants controlling auto sizing ---
    row_px = 35          # pixels per row
    col_px = 25          # pixels per column
    header_px = 100      # space for x-axis labels
    left_px = 100        # space for y-axis labels


    # ------------------ aggregation ------------------
    heat = (
        df.groupby([
            'event_params__character_name',
            'event_params__current_tier',
            'event_params__current_question_index'
        ])['ads_watch_ratio']
        .mean()
        .reset_index()
    )

    pivot = heat.pivot_table(
        index='event_params__character_name',
        columns=['event_params__current_tier', 'event_params__current_question_index'],
        values='ads_watch_ratio',
        fill_value=0
    )

    pivot = pivot.sort_index(axis=1, level=[0, 1])
    pivot = pivot.reindex(sorted(pivot.index, key=lambda x: str(x).lower()), axis=0)

    # Build axis labels
    tiers = [f"{t}" for t, q in pivot.columns]
    questions = [f"{q}" for t, q in pivot.columns]

    # ------------------ auto figure size ------------------
    n_rows = len(pivot.index)
    n_cols = len(pivot.columns)

    # ------------------ figure ------------------
    fig = go.Figure(
        data=go.Heatmap(
            z=pivot.values,
            x=[tiers, questions],
            y=pivot.index.tolist(),
            colorscale='Purples',
            showscale=False,
            hovertemplate=(
                "Character: %{y}<br>"
                "Tier: %{x[0]}<br>"
                "Question: %{x[1]}<br>"
                "Ad Watch Ratio: %{z:.2f}<extra></extra>"
            )
        )
    )

    # force all y labels to show
    fig.update_yaxes(
        autorange='reversed',
        automargin=True,
        tickmode='array',
        tickvals=pivot.index.tolist(),
        ticktext=pivot.index.tolist(),
        tickfont=dict(size=12)
    )

    fig.update_layout(
        title = 'Ads Watched per Question Heatmap',
        xaxis=dict(type='multicategory'),
        yaxis=dict(title='Character'),
        xaxis_title='Tier - Question',
        autosize=True,
        margin=dict(l=left_px, r=40, t=160, b=80)
    )

    fig.update_layout(**DEFAULT_LAYOUT)
    fig.update_layout(**HEAT_LAYOUT)


    return fig.to_html(full_html=False, 
                       include_plotlyjs='cdn',
                       config={'responsive': True})


def create_item_per_question_heatmap(item, df: pd.DataFrame):
    item_ratio = item + '_use_ratio'
    # --- constants controlling auto sizing ---
    row_px = 35          # pixels per row
    col_px = 25          # pixels per column
    header_px = 100      # space for x-axis labels
    left_px = 100        # space for y-axis labels


    # ------------------ aggregation ------------------
    heat = (
        df.groupby([
            'event_params__character_name',
            'event_params__current_tier',
            'event_params__current_question_index'
        ])[item_ratio]
        .mean()
        .reset_index()
    )

    pivot = heat.pivot_table(
        index='event_params__character_name',
        columns=['event_params__current_tier', 'event_params__current_question_index'],
        values=item_ratio,
        fill_value=0
    )

    pivot = pivot.sort_index(axis=1, level=[0, 1])
    pivot = pivot.reindex(sorted(pivot.index, key=lambda x: str(x).lower()), axis=0)

    # Build axis labels
    tiers = [f"{t}" for t, q in pivot.columns]
    questions = [f"{q}" for t, q in pivot.columns]

    # ------------------ auto figure size ------------------
    n_rows = len(pivot.index)
    n_cols = len(pivot.columns)

    # ------------------ figure ------------------
    fig = go.Figure(
        data=go.Heatmap(
            z=pivot.values,
            x=[tiers, questions],
            y=pivot.index.tolist(),
            colorscale='Blues',
            showscale=False,
            hovertemplate=(
                "Character: %{y}<br>"
                "Tier: %{x[0]}<br>"
                "Question: %{x[1]}<br>"
                f"{item_ratio}: "
                "%{z:.2f}<extra></extra>"
            )
        )
    )

    # force all y labels to show
    fig.update_yaxes(
        autorange='reversed',
        automargin=True,
        tickmode='array',
        tickvals=pivot.index.tolist(),
        ticktext=pivot.index.tolist(),
        tickfont=dict(size=12)
    )

    fig.update_layout(
        title = f'{item.capitalize()} Use per Question Heatmap',
        xaxis=dict(type='multicategory'),
        yaxis=dict(title='Character'),
        xaxis_title='Tier - Question',
        autosize=True,
        margin=dict(l=left_px, r=40, t=160, b=80)
    )
    
    fig.update_layout(**DEFAULT_LAYOUT)
    fig.update_layout(**HEAT_LAYOUT)

    

    return fig.to_html(full_html=False, 
                       include_plotlyjs='cdn',
                       config={'responsive': True})

def create_ads_per_day_chart(df: pd.DataFrame):
    """
    Create a line chart showing the number of ads viewed per day.
    """

    fig = go.Figure(
        data=go.Scatter(
            x=df['event_date'],
            y=df['ads_watched'],
            mode='lines+markers',
            line=dict(color='red'),
            marker=dict(size=6)
        )
    )
    fig.update_layout(
        title='Ads Viewed Per Day',
        xaxis_title='Date',
        yaxis_title='Number of Ads Viewed',
        xaxis=dict(tickformat='%Y-%m-%d'),
        yaxis=dict(dtick=1),
    )
    fig.update_layout(**DEFAULT_LAYOUT)
    fig.update_layout(**LINE_LAYOUT)

    return fig.to_html(full_html=False, 
                       include_plotlyjs='cdn',
                       config={'responsive': True})

import datetime as dt
import pandas as pd
import plotly.graph_objects as go

def create_user_last_event_chart(df: pd.DataFrame, threshold: int = 7):
    """
    Create a histogram of the last event users generated, for users inactive > threshold days.
    """


    if df.empty:
        # return an empty figure HTML if no data
        fig = go.Figure()
        fig.update_layout(title="No data")
        return fig.to_html(full_html=False, 
                       include_plotlyjs='cdn',
                       config={'responsive': True})

    # Ensure last_event_date is datetime
    last_dates = pd.to_datetime(df['last_event_date'], errors='coerce')

    # make everything UTC
    last_dates = last_dates.dt.tz_convert('UTC')

    now = pd.Timestamp.now(tz='UTC').normalize()

    # compute age
    age = now - last_dates.dt.normalize()


    # Mask for users inactive more than threshold days
    mask = age > pd.Timedelta(days=threshold)
    df = df.loc[mask].copy()

    if df.empty:
        fig = go.Figure()
        fig.update_layout(title=f'No users inactive for more than {threshold} days')
        return fig.to_html(full_html=False, 
                       include_plotlyjs='cdn',
                       config={'responsive': True})

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
        title=f'Last Event Generated by Users Inactive for More Than {threshold} Days',
        xaxis_title='Event Name',
        yaxis_title='Number of Users',
    )
    fig.update_layout(**DEFAULT_LAYOUT)
    fig.update_layout(**BAR_LAYOUT)
    return fig.to_html(full_html=False, 
                       include_plotlyjs='cdn',
                       config={'responsive': True})

def create_session_last_event_chart(df: pd.DataFrame):

    if df.empty:
        # return an empty figure HTML if no data
        fig = go.Figure()
        fig.update_layout(title="No data")
        return fig.to_html(full_html=False, 
                       include_plotlyjs='cdn',
                       config={'responsive': True})

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
        title=f'Last Event in Sessions',
        xaxis_title='Event Name',
        yaxis_title='Number of Sessions',
    )
    fig.update_layout(**DEFAULT_LAYOUT)
    fig.update_layout(**BAR_LAYOUT)
    return fig.to_html(full_html=False, 
                       include_plotlyjs='cdn',
                       config={'responsive': True})




