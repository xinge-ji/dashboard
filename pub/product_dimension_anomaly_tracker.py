import random
from datetime import datetime, timedelta

import duckdb
import numpy as np
import pandas as pd
import streamlit as st
from streamlit_extras.metric_cards import style_metric_cards

###############################################################################
# 生成数据
###############################################################################


# 定义随机日期生成函数
def random_date(start, end):
    return start + timedelta(days=random.randint(0, int((end - start).days)))


@st.cache_data
def get_data(sql_query):
    # 设置随机种子
    np.random.seed(42)

    # 设置数据量
    n = 1000000

    # 商品包装
    packaging_options = ["小包装", "中包装", "大包装"]

    # 保管分区
    storage_areas = [
        "常温散件分区",
        "常温整件分区",
        "冷藏药品散件分区",
        "冷藏药品整件分区",
        "冷藏器械散件分区",
        "冷藏器械整件分区",
    ]

    # 随机生成日期
    start_date = datetime(2024, 1, 1)
    end_date = datetime.today()
    last_inbound_date = [random_date(start_date, end_date) for _ in range(n)]

    # 创建DataFrame
    df = pd.DataFrame(
        {
            "商品id": np.arange(1, n + 1),
            "商品包装": np.random.choice(packaging_options, n),
            "保管分区": np.random.choice(storage_areas, n),
            "上次入库时间": last_inbound_date,
            "当前仓库存在": np.random.choice([True, False], n),
            "长宽高缺失": np.random.choice([True, False], n),
            "长宽高存在0": np.random.choice([True, False], n),
            "长宽高相等": np.random.choice([True, False], n),
        }
    )

    return duckdb.query(sql_query).df()


###############################################################################
# 看板设计
###############################################################################

st.header("商品包装长宽高数据异常监控")

with st.expander("说明"):
    st.write("""
             本看板旨在展示商品包装的长宽高尺寸异常情况，帮助管理人员及时识别和处理潜在问题。异常类型包括：
             
             - 长宽高缺失：监控物体的尺寸数据，自动识别缺失任意一个或多个维度的数据，确保每个物体都有完整的尺寸信息。
             - 长宽高存在0值：检测到任何物体的长、宽、高中存在0值，标识出可能的录入错误或数据缺失，确保数据的准确性和可靠性。
             - 长宽高相等：识别出长、宽、高相等的物体，这可能表示数据录入错误或特定的商品类别，以便于后续核实和处理。
             """)


col1, col2 = st.columns([0.3, 0.7])
with col1:
    with st.container(border=True):
        storage_zone = st.multiselect(
            "保管分区",
            [
                "常温散件分区",
                "常温整件分区",
                "冷藏药品散件分区",
                "冷藏药品整件分区",
                "冷藏器械散件分区",
                "冷藏器械整件分区",
            ],
            [
                "常温散件分区",
                "常温整件分区",
                "冷藏药品散件分区",
                "冷藏药品整件分区",
                "冷藏器械散件分区",
                "冷藏器械整件分区",
            ],
        )
        day_since_last_inbound = st.number_input("距离上次入库的时间不超过（天）", min_value=1, value=30, step=1)
        stock_availability = st.selectbox("当前仓库商品或所有商品", ("当前仓库商品", "所有商品"))

sql_statistics = f"""
WITH FilteredData AS (
    SELECT 
        *,
        CASE WHEN 上次入库时间 >= CURRENT_DATE - INTERVAL '30 days' THEN 1 ELSE 0 END AS within_30_days
    FROM 
        df
    WHERE 
        保管分区 IN ({','.join(["'" + i + "'" for i in storage_zone])})
        AND 上次入库时间 >= CURRENT_DATE - INTERVAL '{day_since_last_inbound} days'
        {"AND 当前仓库存在 = TRUE" if stock_availability == "当前仓库商品" else ""}
)
SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN 长宽高缺失 = TRUE THEN 1 ELSE 0 END) AS missing_dimensions_count,
    SUM(CASE WHEN 长宽高存在0 = TRUE THEN 1 ELSE 0 END) AS zero_dimensions_count,
    SUM(CASE WHEN 长宽高相等 = TRUE THEN 1 ELSE 0 END) AS equal_dimensions_count
FROM 
    FilteredData
"""

df_statistics = get_data(sql_statistics)

sql_missing_dimensions = f"""
SELECT
    *
FROM
    df
WHERE
    保管分区 IN ({','.join(["'" + i + "'" for i in storage_zone])})
    AND 上次入库时间 >= CURRENT_DATE - INTERVAL '{day_since_last_inbound} days'
    {"AND 当前仓库存在 = TRUE" if stock_availability == "当前仓库商品" else ""}
    AND 长宽高缺失 = TRUE
"""

df_missing_dimensions = get_data(sql_missing_dimensions)

sql_zero_dimensions = f"""
SELECT
    *
FROM
    df
WHERE
    保管分区 IN ({','.join(["'" + i + "'" for i in storage_zone])})
    AND 上次入库时间 >= CURRENT_DATE - INTERVAL '{day_since_last_inbound} days'
    {"AND 当前仓库存在 = TRUE" if stock_availability == "当前仓库商品" else ""}
    AND 长宽高存在0 = TRUE
"""

df_zero_dimensions = get_data(sql_zero_dimensions)

sql_equal_dimensions = f"""
SELECT
    *
FROM
    df
WHERE
    保管分区 IN ({','.join(["'" + i + "'" for i in storage_zone])})
    AND 上次入库时间 >= CURRENT_DATE - INTERVAL '{day_since_last_inbound} days'
    {"AND 当前仓库存在 = TRUE" if stock_availability == "当前仓库商品" else ""}
    AND 长宽高相等 = TRUE
"""

df_equal_dimensions = get_data(sql_equal_dimensions)

with col2:
    sub_col1, sub_col2, sub_col3, sub_col4 = st.columns(4)
    sub_col1.metric(label="所有商品数量", value=f"{int(df_statistics['total_rows']):,}")
    sub_col2.metric(
        label="长宽高缺失商品数量",
        value=f"{
                    int(df_statistics['missing_dimensions_count']):,}",
    )
    sub_col3.metric(
        label="长宽高存在0值数量",
        value=f"{
                    int(df_statistics['zero_dimensions_count']):,}",
    )
    sub_col4.metric(
        label="长宽高相等商品数量",
        value=f"{
                    int(df_statistics['equal_dimensions_count']):,}",
    )
    style_metric_cards()

    tab1, tab2, tab3 = st.tabs(["📙 长宽高缺失 ", "📘 长宽高存在0值 ", "📗 长宽高相等 "])

    with tab1:
        st.dataframe(df_missing_dimensions)

    with tab2:
        st.dataframe(df_zero_dimensions)

    with tab3:
        st.dataframe(df_equal_dimensions)
