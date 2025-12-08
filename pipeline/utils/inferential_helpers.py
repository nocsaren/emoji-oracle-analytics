import numpy as np

def compute_ci_counts(counts, confidence=0.95):
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
    # Z-score for two-sided confidence
    z = 1.96 if confidence == 0.95 else 1.645  # 95% or 90% CI approx
    # Convert to counts scale
    lower_err = (p - z * se) * total
    upper_err = (z * se + p) * total - counts
    lower_err = counts - lower_err
    return lower_err, upper_err
