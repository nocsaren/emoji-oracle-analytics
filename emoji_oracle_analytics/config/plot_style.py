DEFAULT_LAYOUT = {
    # Figure size
#    'height': 768,
#    'width': 2048,

    # Theme
    'template': 'seaborn',

    # Titles and fonts
    'title_font': {'size': 20, 'family': 'Arial', 'color': 'black'},
    'xaxis_title_font': {'size': 16, 'family': 'Arial', 'color': 'black'},
    'yaxis_title_font': {'size': 16, 'family': 'Arial', 'color': 'black'},
    'xaxis_tickfont': {'size': 12, 'family': 'Arial', 'color': 'black'},
    'yaxis_tickfont': {'size': 12, 'family': 'Arial', 'color': 'black'},

    # Margins
    'margin': {'l': 60, 'r': 50, 't': 80, 'b': 60},

    # Axis grid and lines
    'xaxis': {
        'showgrid': True,
        'gridcolor': 'lightgray',
        'zeroline': False,
        'showline': True,
        'linecolor': 'black',
    },
    'yaxis': {
        'showgrid': True,
        'gridcolor': 'lightgray',
        'zeroline': False,
        'showline': True,
        'linecolor': 'black',
    },

    # Hover label formatting
    'hoverlabel': {
        'bgcolor': 'white',
        'font_size': 12,
        'font_family': 'Arial'
    },

    # Legend defaults
    'legend': {'orientation': 'h', 'y': -0.2, 'x': 0.5, 'xanchor': 'center', 'font': {'size': 12}},

    # Default color sequence (can be overridden per chart)
    'colorway': ['#636EFA', '#EF553B', '#00CC96', '#AB63FA', '#FFA15A', '#19D3F3'],
}


LINE_LAYOUT = {
    # Line chart style
    'hoverlabel': {
        'bgcolor': 'white',
        'font_size': 12,
        'font_family': 'Arial',
    }, 
    # Legend tweaks
    'legend': {'orientation': 'h', 'y': -0.2, 'x': 0.5, 'xanchor': 'center', 'font': {'size': 12}},

    # Optional: default color sequence for lines
    'colorway': ['#636EFA', '#EF553B', '#00CC96', '#AB63FA', '#FFA15A', '#19D3F3'],
}


HIST_LAYOUT = {
    # Bar/Histogram style
    'barmode': 'overlay',             # overlay or group for multiple histograms
    'barnorm': None,                  # can be 'percent' or None
    'bargap': 0.2,                    # gap between bars
    'bargroupgap': 0.1,               # gap between groups if multiple series

    # Hover formatting
    'hovermode': 'x unified',
    'hoverlabel': {
        'bgcolor': 'white',
        'font_size': 12,
        'font_family': 'Arial'
    },

    # Legend positioning
    'legend': {'orientation': 'h', 'y': -0.2, 'x': 0.5, 'xanchor': 'center', 'font': {'size': 12}},

    # Optional cumulative histogram defaults
    'barnorm': None,                 # can be 'percent', 'probability', etc.
    'colorway': ['#636EFA', '#EF553B', '#00CC96', '#AB63FA', '#FFA15A', '#19D3F3'],
}


HEAT_LAYOUT = {


    # Heatmap-specific axes
    'xaxis': {
        'showgrid': False,
        'tickangle': -45,
        'side': 'bottom',
    },
    'yaxis': {
        'showgrid': False,
        'autorange': 'reversed',  # y-axis top-to-bottom
    },

    # Hover
    'hovermode': 'closest',
    'hoverlabel': {
        'bgcolor': 'white',
        'font_size': 12,
        'font_family': 'Arial'
    },

    # Colorbar defaults
    'coloraxis_colorbar': {
        'title': 'Value',
        'ticks': 'outside',
        'tickfont': {'size': 12, 'family': 'Arial'},
    },

    # Optional: margins
    'margin': {'l': 80, 'r': 50, 't': 80, 'b': 80},

    # Optional: legend tweaks (though heatmaps usually use colorbar)
    'legend': {'orientation': 'h', 'y': -0.2, 'x': 0.5, 'xanchor': 'center', 'font': {'size': 12}},
}


PIE_LAYOUT = {
    # Pie chart specific settings
    'showlegend': True,
    
    # Legend placement
    'legend': {
        'orientation': 'v',   # vertical legend is common for pies
        'y': 0.5,
        'x': 1.05,
        'yanchor': 'middle',
        'xanchor': 'left',
        'font': {'size': 12, 'family': 'Arial'}
    },

    # Hover formatting
    'hovermode': 'closest',
    'hoverlabel': {
        'bgcolor': 'white',
        'font_size': 12,
        'font_family': 'Arial'
    },

    # Margins (give space for legend)
    'margin': {'l': 50, 'r': 150, 't': 80, 'b': 50},

    # Optional: color sequence for slices (can override per trace)
    'colorway': ['#636EFA', '#EF553B', '#00CC96', '#AB63FA', '#FFA15A', '#19D3F3'],
}

BAR_LAYOUT = {
    # Optional: show values on bars automatically
    'barmode': 'group',        # default grouping of bars
    'barnorm': None,           # don't normalize
    'uniformtext_minsize': 12, # minimum font size for bar text
    'uniformtext_mode': 'show',# always show text if added via text=...

    # Bar spacing
    'bargap': 0.2,             # gap between bars
    'bargroupgap': 0.1,        # gap between groups of bars

    # Hover formatting
    'hoverlabel': {
        'bgcolor': 'white',
        'font_size': 12,
        'font_family': 'Arial'
    },

    # Optional legend tweaks for bar charts
    'legend': {'orientation': 'h', 'y': -0.25, 'x': 0.5, 'xanchor': 'center', 'font': {'size': 12}},

    # Optional: default bar colors if not specified per trace
    'colorway': ['#636EFA', '#EF553B', '#00CC96', '#AB63FA', '#FFA15A', '#19D3F3'],
}


FUNNEL_LAYOUT = {

    'height': 768,

    # Funnel chart style
    'showlegend': False,
    
    # Legend placement
    'legend': {
        'orientation': 'v',   # vertical legend is common
        'y': 0.5,
        'x': 1.05,
        'yanchor': 'middle',
        'xanchor': 'left',
        'font': {'size': 12, 'family': 'Arial'}
    },

    # Hover formatting
    'hovermode': 'closest',
    'hoverlabel': {
        'bgcolor': 'white',
        'font_size': 12,
        'font_family': 'Arial'
    },

    # Margins
    'margin': {'l': 40, 'r': 40, 't': 80, 'b': 20},

    # Optional: default color sequence for funnel steps
    'colorway': ['#636EFA', '#EF553B', '#00CC96', '#AB63FA', '#FFA15A', '#19D3F3'],

    # Optional: text position defaults
    'uniformtext_minsize': 12,
    'uniformtext_mode': 'show'
}