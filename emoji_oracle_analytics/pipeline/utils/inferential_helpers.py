import numpy as np

def compute_ci_counts(counts, z = 1.96):
    """
    Compute approximate confidence intervals for counts.
    Uses normal approximation for binomial proportions.
    Returns lower and upper error bar lengths for plotly.
    """
    total = counts.sum()
    # Proportion of each count
    p = counts / total
    # Standard error for proportion
    se = np.sqrt(p * (1 - p) / total)
    # Convert to counts scale
    lower_err = (p - z * se) * total
    upper_err = (z * se + p) * total - counts
    lower_err = counts - lower_err
    return lower_err, upper_err


def binomial_count_ci(counts, totals, z = 1.96):
    """
    counts: Series of event counts (e.g., installs per day)
    totals: Series of total users per day
    Returns lower_err, upper_err for Plotly (asymmetrical error bars)
    """
    p = counts / totals
    se = (p * (1 - p) / totals).pow(0.5)

    lower = (counts - z * se * totals).clip(lower=0)
    upper = (counts + z * se * totals)

    arrayminus = counts - lower
    arrayplus = upper - counts
    return arrayminus, arrayplus
