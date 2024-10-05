# %%
import warnings
from typing import Any, Dict

import pandas as pd
import streamlit as st
from pandas.api.types import is_datetime64_any_dtype, is_numeric_dtype, is_object_dtype

warnings.simplefilter(action="ignore", category=UserWarning)


# %%
def dataframe_explorer(df: pd.DataFrame, case: bool = True) -> pd.DataFrame:
    """
    Adds a UI on top of a dataframe to let viewers filter columns

    Args:
        df (pd.DataFrame): Original dataframe
        case (bool, optional): If True, text inputs will be case sensitive. Defaults to True.

    Returns:
        pd.DataFrame: Filtered dataframe
    """

    df = df.copy()

    # Try to convert datetimes into standard format (datetime, no timezone)
    for col in df.columns:
        if is_object_dtype(df[col]):
            try:
                df[col] = pd.to_datetime(df[col])
            except Exception:
                pass

        if is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.tz_localize(None)

    modification_container = st.container()

    with modification_container:
        to_filter_columns = st.multiselect(
            "数据筛选",
            df.columns,
        )
        filters: Dict[str, Any] = dict()
        for column in to_filter_columns:
            left, right = st.columns((1, 20))
            # Treat columns with < 10 unique values as categorical
            if isinstance(df[column].dtype, pd.CategoricalDtype) or df[column].nunique() < 10:
                left.write("↳")
                filters[column] = right.multiselect(
                    f"{column}选项",
                    df[column].unique(),
                    default=list(df[column].unique()),
                    # key=f"{random_key_base}_{column}",
                )
                df = df[df[column].isin(filters[column])].copy()
            elif is_numeric_dtype(df[column]):
                left.write("↳")
                _min = float(df[column].min())
                _max = float(df[column].max())
                step = (_max - _min) / 100
                filters[column] = right.slider(
                    f"{column}范围",
                    _min,
                    _max,
                    (_min, _max),
                    step=step,
                    # key=f"{random_key_base}_{column}",
                )
                df = df[df[column].between(*filters[column])].copy()
            elif is_datetime64_any_dtype(df[column]):
                left.write("↳")
                filters[column] = right.date_input(
                    f"{column}范围",
                    value=(
                        df[column].min(),
                        df[column].max(),
                    ),
                    # key=f"{random_key_base}_{column}",
                )
                if len(filters[column]) == 2:
                    filters[column] = tuple(map(pd.to_datetime, filters[column]))
                    start_date, end_date = filters[column]
                    df = df.loc[df[column].between(start_date, end_date)].copy()
            else:
                left.write("↳")
                filters[column] = right.text_input(
                    f"{column}中包含文字",
                    # key=f"{random_key_base}_{column}",
                )
                if filters[column]:
                    df = df[df[column].str.contains(filters[column], case=case)].copy()

    return df


# %%
def style_metric_key_cards(
    key: str,
    background_color: str = "#FFF",
    border_size_px: int = 1,
    border_color: str = "#CCC",
    border_radius_px: int = 5,
    border_left_color: str = "#9AD8E1",
    box_shadow: bool = True,
) -> None:
    """
    Applies a custom style to st.metrics in the page

    Args:
        background_color (str, optional): Background color. Defaults to "#FFF".
        border_size_px (int, optional): Border size in pixels. Defaults to 1.
        border_color (str, optional): Border color. Defaults to "#CCC".
        border_radius_px (int, optional): Border radius in pixels. Defaults to 5.
        border_left_color (str, optional): Borfer left color. Defaults to "#9AD8E1".
        box_shadow (bool, optional): Whether a box shadow is applied. Defaults to True.
    """

    box_shadow_str = (
        "box-shadow: 0 0.15rem 1.75rem 0 rgba(58, 59, 69, 0.15) !important;"
        if box_shadow
        else "box-shadow: none !important;"
    )
    st.markdown(
        f"""
        <style>
            .st-key-{key} {{
                background-color: {background_color};
                border: {border_size_px}px solid {border_color};
                padding: 5% 5% 5% 10%;
                border-radius: {border_radius_px}px;
                border-left: 0.5rem solid {border_left_color} !important;
                {box_shadow_str}
            }}
        </style>
        """,
        unsafe_allow_html=True,
    )
