import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import duckdb
from streamlit_extras.metric_cards import style_metric_cards
import plotly.express as px

# Helper function to generate random date
def random_date(start, end):
    return start + timedelta(days=random.randint(0, int((end - start).days)))

def generate_random_datetime_in_shifts(n, start_date, end_date):
    random_times = []

    for _ in range(n):
        # 随机生成一个在 start_date 和 end_date 之间的日期
        random_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))

        # 随机选择班次：早班, 午班, 晚班
        shift = random.choice(['早班', '午班', '晚班'])

        if shift == '早班':
            # 08:00 到 12:00 的时间段
            random_hour = random.randint(8, 11)
        elif shift == '午班':
            # 12:00 到 18:00 的时间段
            random_hour = random.randint(12, 17)
        else:  # 晚班
            # 18:00 到 24:00 的时间段
            random_hour = random.randint(18, 23)

        random_minute = random.randint(0, 59)
        random_second = random.randint(0, 59)

        # 生成完整的随机时间（包括年月日、时分秒）
        random_time = datetime.combine(random_date, datetime.min.time()) + timedelta(
            hours=random_hour, minutes=random_minute, seconds=random_second
        )

        random_times.append(random_time)

    return random_times

def get_data(sql_query):
    return con.query(sql_query).df()


# 设置随机种子
np.random.seed(42)

# 设置数据量
n = 1000000


@st.cache_data
def generate_shelving_data(n):
    in_out_type_options = [0, 1]  # 0: 上架 (即入库), 1: 下架 (即出库)
    in_out_type_names = {0: "上架 (即入库)", 1: "下架 (即出库)"}
    task_status_options = [1, 2, 3]  # 1: 不发送, 2: 可发送 (待上架), 3: 执行完毕 (上架完成)
    task_status_names = {1: "不发送", 2: "可发送 (待上架)", 3: "执行完毕 (上架完成)"}
    zone_names = ["药业食品散件分区", "药业口服注射散件分区", "药业外用散件分区", "药业局部用药分区", "药业配件分区"]
    storage_names = ["药业散件库区", "大药房中药库区", "口服药品库区", "注射用药库区"]

    # 随机生成字段
    start_date = datetime(2023, 1, 1)
    end_date = datetime.today()

    completion_dates = generate_random_datetime_in_shifts(n, start_date, end_date)  # 任务完成时间
    executor_ids = np.random.randint(100, 200, n)  # 任务执行人id
    executors = [f"执行人_{i}" for i in executor_ids]  # 任务执行人
    in_out_type_ids = np.random.choice(in_out_type_options, n)  # 出入库类型id
    in_out_type_names = [in_out_type_names[type_id] for type_id in in_out_type_ids]  # 出入库类型名称
    task_status_ids = np.random.choice(task_status_options, n)  # 任务状态id
    task_status_names = [task_status_names[status_id] for status_id in task_status_ids]  # 任务状态名称
    zone_names_random = np.random.choice(zone_names, n)  # 上架分区名称
    storage_names_random = np.random.choice(storage_names, n)  # 上架库区名称

    # 创建DataFrame
    df_shelving = pd.DataFrame({
        '来源单据ID': np.arange(1, n+1),
        '出入库ID': np.arange(1, n+1),
        '任务完成时间': completion_dates,
        '任务执行人ID': executor_ids,
        '任务执行人': executors,
        '出入库类型ID': in_out_type_ids,
        '出入库类型名称': in_out_type_names,
        '任务状态ID': task_status_ids,
        '任务状态名称': task_status_names,
        '上架分区名称': zone_names_random,
        '上架库区名称': storage_names_random
    })

    return df_shelving

df_shelving = generate_shelving_data(n)

con = duckdb.connect()  # 创建 DuckDB 连接
con.execute("CREATE TABLE 上架表 AS SELECT * FROM df_shelving")

st.title("上架板块展示")
current_year = datetime.now().year
col1, col2 = st.columns([0.3, 0.7])
with col1:
    with st.container(border=True):
        year = st.selectbox("选择年份", list(range(2023, current_year + 1)))
        month = st.selectbox("选择月份", list(range(1, 13)), format_func=lambda x: f"{x}月")

selected_date = f"{year}-{str(month).zfill(2)}"

shelving_data_sql = f"""
SELECT 
    COUNT(出入库ID) AS 上架条目数
FROM
    上架表
WHERE
    任务状态ID = 3 
    AND STRFTIME('%Y-%m', 任务完成时间) = '{selected_date}'
"""

with col2:
    st.metric(label="已上架条目数", value=f"{int(get_data(shelving_data_sql)['上架条目数']):,}")
    style_metric_cards()

shelving_sql_by_zone = f"""
SELECT
    上架分区名称, 
    COUNT(出入库ID) AS 上架条目数
FROM
    上架表
WHERE
    任务状态ID = 3 
    AND STRFTIME('%Y-%m', 任务完成时间) = '{selected_date}'
GROUP BY 上架分区名称
ORDER BY 上架条目数
"""

shelving_sql_by_storage = f"""
SELECT
    上架库区名称,
    COUNT(出入库ID) AS 上架条目数
FROM
    上架表
WHERE
    任务状态ID = 3 
    AND STRFTIME('%Y-%m', 任务完成时间) = '{selected_date}'
GROUP BY 上架库区名称
ORDER BY 上架条目数
"""
col3, col4 = st.columns([0.5, 0.5])
with col3:
    data = get_data(shelving_sql_by_zone)
    fig = px.bar(y=data['上架分区名称'], x=data['上架条目数'], labels={'y': '上架分区名称', 'x': '上架条目数'})
    st.plotly_chart(fig)

with col4:
    data = get_data(shelving_sql_by_storage)
    fig = px.bar(y=data['上架库区名称'], x=data['上架条目数'], labels={'y': '上架库区名称', 'x': '上架条目数'})
    st.plotly_chart(fig)
