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


def safe_rename(df: pd.DataFrame, rename_dict: dict) -> pd.DataFrame:
    """
    Renames columns in a DataFrame based on a rename dictionary.
    
    - Keeps all original columns.
    - Only renames those that exist in the DataFrame.
    - Prints a warning for each missing column in rename_dict.

    Parameters:
        df (pd.DataFrame): The input DataFrame.
        rename_dict (dict): Dictionary of {original_column: new_name}.

    Returns:
        pd.DataFrame: DataFrame with renamed columns.
    """
    existing_keys = df.columns.intersection(rename_dict.keys())
    missing_keys = set(rename_dict.keys()) - set(existing_keys)

    for col in missing_keys:
        print(f"[Warning] Column to rename not found: '{col}'")

    return df.rename(columns={k: rename_dict[k] for k in existing_keys})
