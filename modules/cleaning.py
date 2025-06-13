import pandas as pd




def apply_value_maps(df, map_of_maps, keep_unmapped=True):
    """
    Applies value mapping dictionaries to specified DataFrame columns.

    Parameters:
        df (pd.DataFrame): The DataFrame to modify.
        map_of_maps (dict): A dictionary of column names to value-mapping dictionaries.
        keep_unmapped (bool): If True, keeps original values when no match is found.

    Returns:
        pd.DataFrame: A new DataFrame with mapped values.
    """
    df_copy = df.copy()
    
    for col, value_map in map_of_maps.items():
        if col in df_copy.columns:
            if keep_unmapped:
                df_copy[col] = df_copy[col].map(value_map).fillna(df_copy[col])
            else:
                df_copy[col] = df_copy[col].map(value_map)
        else:
            print(f"⚠️ Column '{col}' not found in DataFrame.")
    
    return df_copy


def safe_select_and_rename(df: pd.DataFrame, rename_dict: dict) -> pd.DataFrame:
    """
    Selects and renames columns in a DataFrame based on a rename dictionary.
    
    - Continues even if some columns are missing.
    - Prints a message for each missing column.
    
    Parameters:
        df (pd.DataFrame): The input DataFrame.
        rename_dict (dict): Dictionary of {original_column: new_name}.
    
    Returns:
        pd.DataFrame: A new DataFrame with selected and renamed columns.
    """
    existing_cols = []
    for col in rename_dict:
        if col in df.columns:
            existing_cols.append(col)
        else:
            print(f"[Warning] Column not found: '{col}'")
    
    return df[existing_cols].rename(columns=rename_dict)
