from datetime import datetime, timedelta
from pathlib import Path
import altair as alt
import duckdb
import numpy as np
import pandas as pd
import streamlit as st
from chinese_calendar import get_workdays, is_holiday
from dateutil.relativedelta import relativedelta
from streamlit_extras.chart_container import chart_container
from streamlit_extras.stylable_container import stylable_container
import random
from streamlit_extras.metric_cards import style_metric_cards

def generate_random_datetime_in_shifts(n):
    now = datetime.now()  # 获取当前时间
    yesterday_8am = now.replace(hour=8, minute=0, second=0, microsecond=0) - timedelta(days=1)  # 昨天早上8点

    random_times = []

    for _ in range(n):
        # 在昨天早上8点到今天现在时间之间生成一个随机日期时间
        random_date = yesterday_8am + timedelta(seconds=random.randint(0, int((now - yesterday_8am).total_seconds())))

        # 随机选择班次：早班, 午班, 晚班
        shift = random.choice(['早班', '午班', '晚班'])

        if shift == '早班':
            # 08:00 到 12:00 的时间段
            random_hour = random.randint(1, 11)
        elif shift == '午班':
            # 12:00 到 18:00 的时间段
            random_hour = random.randint(12, 17)
        else:  # 晚班
            # 18:00 到当前时间或24:00之间的时间段（晚班结束时间取决于当前时间）
            latest_hour = min(now.hour, 23) if random_date.date() == now.date() else 23
            random_hour = random.randint(18, 23)

        random_minute = random.randint(0, 59)
        random_second = random.randint(0, 59)

        # 生成完整的随机时间（包括年月日、时分秒）
        random_time = random_date.replace(hour=random_hour, minute=random_minute, second=random_second)

        random_times.append(random_time)
    
    if n == 1:
        return random_times[0]

    return random_times


np.random.seed(42)

n = 500

@st.cache_data
def generate_data(n):
    orderstatus = [1,2,3,4,5,6,7,10]
    
    logisticNo = np.arange(1, n+1)
    
    goodspeerNo = np.arange(1,n+1)
    
    goods_owner_ids = [1, 2, 6, 64, 284, 550, 682, 9100]
    goods_owner_dict = {
        1: "鹭燕医药股份有限公司",
        2: "厦门鹭燕医疗器械有限公司",
        6: "厦门鹭燕大药房有限公司",
        64: "厦门燕来福制药有限公司",
        284: "泉州鹭燕大药房有限公司",
        550: "厦门鹭燕海峡两岸药材贸易有限公司",
        682: "厦门鹭燕药业有限公司",
        9100: "第三方"
    }
    
    sendCompanyId = np.random.choice(goods_owner_ids, n)
    sendCompany = [goods_owner_dict[goods_id] for goods_id in sendCompanyId]
    
    driverId = np.random.choice(np.arange(1,11),n)
    
    classesName = [f"班次_{i}" for i in np.random.randint(1, 23, n)]
    
    LineType = np.random.choice(['同城','干线'], n)
    
    varietyAttribute = np.random.choice(['常温','冷链','麻醉'],n)
    
    whole_quantities = np.random.randint(1, 50, n)
    scatter_quantities = np.random.randint(1, 500, n)
    
    order_dates = generate_random_datetime_in_shifts(n)

    df_delivery = pd.DataFrame({
        '建单时间': order_dates,
        '调度时间': order_dates,
        '装车时间': order_dates,
        '发车时间': order_dates,
        '签收时间': order_dates,
        '运单状态id': np.random.choice(orderstatus, n),
        '运单号': logisticNo,
        '随货同行单号': goodspeerNo,
        '货主id': sendCompanyId,
        '货主名称': sendCompany,
        '司机id': driverId,
        '班次名称': classesName,
        '线路类型': LineType,
        '品种属性': varietyAttribute,
        '整件数': whole_quantities,
        '散件数': scatter_quantities,
        '运输天数': np.random.choice([1,2,3], n),
        '客户': [f"客户_{i}" for i in np.random.randint(1, 8, n)],
        '车牌号': np.random.randint(1, 15, n),
        '线路': np.random.randint(1, 23, n)
    })
    
    return df_delivery

df_delivery= generate_data(n)

con = duckdb.connect()  # 创建 DuckDB 连接
con.execute("CREATE TABLE 运单表 AS SELECT * FROM df_delivery") 
    

def get_data(sql_query):
    return con.query(sql_query).df()


delivery_waitlist_sql = f"""
SELECT COUNT(随货同行单号) AS 待派条目数
FROM 运单表
WHERE 运单状态id IN (1,2,3)
"""

delivery_waitlist_sql_company = f"""
SELECT COUNT(随货同行单号) AS 待派条目数, 货主名称
FROM 运单表
WHERE 运单状态id IN (1,2,3)
GROUP BY 货主名称
"""

delivery_wait_sql = f"""
SELECT COUNT(随货同行单号) AS 待运条目数
FROM 运单表
WHERE 运单状态id = 1
"""

delivery_wait_sql_company = f"""
SELECT COUNT(随货同行单号) AS 待运条目数, 客户
FROM 运单表
WHERE 运单状态id  = 1
GROUP BY 客户
"""

truck_num_sql = f"""
SELECT COUNT(DISTINCT 车牌号) AS 车辆数量
FROM 运单表
WHERE 运单状态id = 1
"""

class_num_sql = f"""
SELECT COUNT(DISTINCT 班次名称) AS 班次数量
FROM 运单表
WHERE 运单状态id = 1
"""

line_num_sql = f"""
SELECT COUNT(随货同行单号) AS 待运条目数, 线路类型
FROM 运单表
WHERE 运单状态id  = 1
GROUP BY 线路类型
"""

linetype_sql = f"""
WITH line_type_tb AS (
    SELECT *, CASE WHEN 线路  IN (13,14,15,16) THEN '东线' WHEN 线路 IN (17,18,19,20) THEN '西线' ELSE '外省' END AS 东西干线
    FROM 运单表
    WHERE 线路 IN (13,14,15,16,17,18,19,20,21,22)
)

SELECT COUNT(随货同行单号) AS 待运条目数, 东西干线
FROM line_type_tb
WHERE 运单状态id  = 1
GROUP BY 东西干线
"""

col1, col2 = st.columns(2)

with col1:
    st.metric(label="待派条目数", value=f"{int(get_data(delivery_waitlist_sql)['待派条目数']):,}")
    st.metric(label="车辆数", value=f"{int(get_data(truck_num_sql)['车辆数量']):,}")
    style_metric_cards()
    
    data1 = get_data(delivery_waitlist_sql_company)
        
    chart_auto = alt.Chart(data1, title=alt.TitleParams("待派条目数", anchor="middle")).mark_bar().encode(
    x=alt.X("待派条目数", title="待派条目数"),
    y=alt.Y('货主名称', title="货主")
    )
    st.altair_chart(chart_auto, use_container_width=True)
    

with col2:
    st.metric(label="待运条目数", value=f"{int(get_data(delivery_wait_sql)['待运条目数']):,}")
    st.metric(label="班次数", value=f"{int(get_data(class_num_sql)['班次数量']):,}")
    style_metric_cards()
    
    sub_col1, sub_col2 = col2.tabs(['客户', '线路'])
    
    data1 = get_data(delivery_wait_sql_company)
        
    chart_auto = alt.Chart(data1, title=alt.TitleParams("待运条目数", anchor="middle")).mark_bar().encode(
    x=alt.X("待运条目数", title="待运条目数"),
    y=alt.Y('客户', title="客户")
    )
    sub_col1.altair_chart(chart_auto, use_container_width=True)
    
    data2 = get_data(line_num_sql)
        
    chart_auto = alt.Chart(data2).mark_bar().encode(
    x=alt.X("待运条目数", title="待运条目数"),
    y=alt.Y('线路类型', title="线路类型")
    )
    sub_col2.altair_chart(chart_auto, use_container_width=True)
    
    data3 = get_data(linetype_sql)
        
    chart_auto = alt.Chart(data3).mark_arc().encode(
    theta=alt.Theta("待运条目数", type="quantitative", title="待运条目数"),
    color=alt.Color("东西干线", title="东西干线")
    )
    sub_col2.altair_chart(chart_auto, use_container_width=True)
    
    

delivery_sql = f"""
SELECT COUNT(随货同行单号) AS 已派条目数
FROM 运单表
WHERE 运单状态id IN (4,5,6,7)
"""

delivery_time_sql = f"""
SELECT COUNT(随货同行单号) AS 已派条目数, STRFTIME('%H:%M', 签收时间) AS 时间
FROM 运单表
WHERE 运单状态id IN (4,5,6,7)
GROUP BY 时间
"""



col3, col4 = st.columns(2)
with col3:
    st.metric(label="已派条目数", value=f"{int(get_data(delivery_sql)['已派条目数']):,}")
    style_metric_cards()
    
    data1 = get_data(delivery_time_sql)
        
    chart_auto = alt.Chart(data1, title=alt.TitleParams("已派条目数", anchor="middle")).mark_bar().encode(
    y=alt.Y("已派条目数", title="已派条目数"),
    x=alt.X('时间', title="时间")
    )
    st.altair_chart(chart_auto, use_container_width=True)
    
