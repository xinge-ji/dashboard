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
    
    if n == 1:
        return random_times[0]

    return random_times

np.random.seed(42)

n = 1000000

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
    
    LineType = np.random.choice([1,2], n)
    
    varietyAttribute = np.random.choice(['常温','冷链','麻醉'],n)
    
    whole_quantities = np.random.randint(1, 50, n)
    scatter_quantities = np.random.randint(1, 500, n)
    
    start_date = datetime(2023, 1, 1)
    end_date = datetime.today()
    order_dates = generate_random_datetime_in_shifts(n, start_date, end_date)

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
        '货主名称': [f"客户_{i}" for i in np.random.randint(1, 8, n)],
        '司机id': driverId,
        '班次名称': classesName,
        '线路类型id': LineType,
        '品种属性': varietyAttribute,
        '整件数': whole_quantities,
        '散件数': scatter_quantities,
        '运输天数': np.random.choice([1,2,3], n)
    })
    
    licensePlate = np.random.randint(1, 15, n)
    
    mileage = np.random.randint(50, 100, n)
    
    optime = order_dates
    
    workHours = np.random.randint(1,9, n)
    
    repairfee = np.random.randint(50, 150, n)
    
    repairtime= np.random.choice([1,2,3], n)
    
    deliveryTime = generate_random_datetime_in_shifts(n, start_date, end_date)
    
    df_repair = pd.DataFrame({
        '随货同行单号': goodspeerNo,
        '车牌号': licensePlate,
        '里程': mileage,
        '加油时间': optime,
        '工作时间': workHours,
        '修理费': repairfee,
        '修理时间': repairtime,
        '完成时间': deliveryTime,
        '司机': [f"司机_{i}" for i in np.random.randint(1, 10, n)],
        '运行时间': np.random.randint(5,10, n)
    })
    
    return df_delivery, df_repair

df_delivery, df_repair = generate_data(n)

con = duckdb.connect()  # 创建 DuckDB 连接
con.execute("CREATE TABLE 运单表 AS SELECT * FROM df_delivery") 
con.execute("CREATE TABLE 车辆表 AS SELECT * FROM df_repair")
    

def get_data(sql_query):
    return con.query(sql_query).df()



with stylable_container(
    key="container_with_border",
    css_styles="""
            {
                border: 1px solid rgba(49, 51, 63, 0.2);
                border-radius: 0.5rem;
                padding: calc(1em - 1px)
            }
            """,
):
    col1, col2, col3, col4 = st.columns(4, vertical_alignment="bottom")

    time_selection = col1.radio(
        "选择周期",
        ["日报", "周报", "月报"],
        index=0,
        horizontal=True,
    )
    time_dict = {"日报": "每日", "周报": "每周", "月报": "每月"}

    if time_selection == "日报":
        default_start_time = datetime.now() - timedelta(days=30)
    elif time_selection == "周报":
        default_start_time = datetime.now() - timedelta(weeks=26) - timedelta(days=datetime.now().weekday())
    elif time_selection == "月报":
        default_start_time = (datetime.now() - relativedelta(months=25)).replace(day=1)

    start_time = col2.date_input("起始时间", value=default_start_time)

    if time_selection == "日报":
        default_end_time = datetime.now() - timedelta(days=1)
    elif time_selection == "周报":
        default_end_time = datetime.now() - timedelta(days=datetime.now().weekday() + 1)
    elif time_selection == "月报":
        default_end_time = datetime.now().replace(day=1) - timedelta(days=1)

    end_time = col3.date_input("结束时间", value=default_end_time)

    include_holiday = col4.toggle("包括节假日", value=True)    
    
    
tab1, tab2 = st.tabs(['配送情况', '车辆信息'])    

with tab1:
    subtab1, subtab2, subtab3 = tab1.tabs(["整体", "客户", "品种属性"])
    
delivery_sql_day = f"""
SELECT COUNT(随货同行单号) AS 已派条目数, STRFTIME('%Y-%m-%d', 签收时间) AS 天数
FROM 运单表
WHERE 运单状态id IN (4,5,6,7) AND STRFTIME('%Y-%m-%d', 签收时间) BETWEEN '{start_time}' AND '{end_time}'
GROUP BY STRFTIME('%Y-%m-%d', 签收时间)
"""

delivery_sql_week = f"""
SELECT date_add(签收时间, INTERVAL (-WEEKDAY(签收时间)+1) DAY) AS 周数, 
       COUNT(随货同行单号) AS 已派条目数
FROM 运单表
WHERE 运单状态id IN (4,5,6,7) 
  AND STRFTIME('%Y-%m-%d', 签收时间) BETWEEN '{start_time}' AND '{end_time}'
GROUP BY 周数
ORDER BY 周数
"""

delivery_sql_month = f"""
SELECT STRFTIME('%Y-%m', 签收时间) AS 月份, 
       COUNT(随货同行单号) AS 已派条目数
FROM 运单表
WHERE 运单状态id IN (4,5,6,7) 
  AND STRFTIME('%Y-%m-%d', 签收时间) BETWEEN '{start_time}' AND '{end_time}'
GROUP BY 月份
ORDER BY 月份
"""

delivery_sql_class1 = f"""
WITH table_with_shift AS (
    SELECT 
    *,
    CASE 
        WHEN EXTRACT(HOUR FROM 发车时间) BETWEEN 12 AND 15 THEN '午班'
        WHEN EXTRACT(HOUR FROM 发车时间) BETWEEN 16 AND 18 THEN '晚班'
        ELSE '早班'
    END AS shift
    FROM 运单表
)
SELECT COUNT(随货同行单号) AS 已派条目数, STRFTIME('%Y-%m-%d', 签收时间) AS 天数, shift
FROM table_with_shift
WHERE 运单状态id IN (4,5,6,7) AND STRFTIME('%Y-%m-%d', 签收时间) BETWEEN '{start_time}' AND '{end_time}'
GROUP BY STRFTIME('%Y-%m-%d', 签收时间), shift
"""

delivery_sql_class2 = f"""
WITH table_with_shift AS (
    SELECT 
    *,
    CASE 
        WHEN EXTRACT(HOUR FROM 发车时间) BETWEEN 12 AND 15 THEN '午班'
        WHEN EXTRACT(HOUR FROM 发车时间) BETWEEN 16 AND 18 THEN '晚班'
        ELSE '早班'
    END AS shift
    FROM 运单表
)
SELECT date_add(签收时间, INTERVAL (-WEEKDAY(签收时间)+1) DAY) AS 周数, 
       COUNT(随货同行单号) AS 已派条目数,
       shift
FROM table_with_shift
WHERE 运单状态id IN (4,5,6,7) 
  AND STRFTIME('%Y-%m-%d', 签收时间) BETWEEN '{start_time}' AND '{end_time}'
GROUP BY 周数, shift
ORDER BY 周数
"""

delivery_sql_class3 = f"""
WITH table_with_shift AS (
    SELECT 
    *,
    CASE 
        WHEN EXTRACT(HOUR FROM 发车时间) BETWEEN 12 AND 15 THEN '午班'
        WHEN EXTRACT(HOUR FROM 发车时间) BETWEEN 16 AND 18 THEN '晚班'
        ELSE '早班'
    END AS shift
    FROM 运单表
)
SELECT STRFTIME('%Y-%m', 签收时间) AS 月份, 
       COUNT(随货同行单号) AS 已派条目数,
       shift
FROM table_with_shift
WHERE 运单状态id IN (4,5,6,7) 
  AND STRFTIME('%Y-%m-%d', 签收时间) BETWEEN '{start_time}' AND '{end_time}'
GROUP BY 月份, shift
ORDER BY 月份
"""

return_sql_day = f"""
SELECT COUNT(随货同行单号) AS 取回条目数, STRFTIME('%Y-%m-%d', 签收时间) AS 天数
FROM 运单表
WHERE 运单状态id IN (4,5,6,7) AND STRFTIME('%Y-%m-%d', 签收时间) BETWEEN '{start_time}' AND '{end_time}'
GROUP BY STRFTIME('%Y-%m-%d', 签收时间)
"""

company_sql_day = f"""
SELECT COUNT(随货同行单号) AS 已派条目数, STRFTIME('%Y-%m-%d', 签收时间) AS 天数, 货主名称
FROM 运单表
WHERE 运单状态id IN (4,5,6,7) AND STRFTIME('%Y-%m-%d', 签收时间) BETWEEN '{start_time}' AND '{end_time}'
GROUP BY STRFTIME('%Y-%m-%d', 签收时间), 货主名称
"""

company_sql_week = f"""
SELECT COUNT(随货同行单号) AS 已派条目数, date_add(签收时间, INTERVAL (-WEEKDAY(签收时间)+1) DAY) AS 周数, 货主名称
FROM 运单表
WHERE 运单状态id IN (4,5,6,7) AND STRFTIME('%Y-%m-%d', 签收时间) BETWEEN '{start_time}' AND '{end_time}'
GROUP BY 周数, 货主名称
"""

company_sql_month = f"""
SELECT COUNT(随货同行单号) AS 已派条目数, STRFTIME('%Y-%m', 签收时间) AS 月份, 货主名称
FROM 运单表
WHERE 运单状态id IN (4,5,6,7) AND STRFTIME('%Y-%m-%d', 签收时间) BETWEEN '{start_time}' AND '{end_time}'
GROUP BY 月份, 货主名称
"""

delivery_time_sql = f"""
SELECT AVG(运输天数) AS 平均到货天数, STRFTIME('%Y-%m-%d', 签收时间) AS 天数
FROM 运单表
WHERE 运单状态id IN (4,5,6,7) AND STRFTIME('%Y-%m-%d', 签收时间) BETWEEN '{start_time}' AND '{end_time}'
GROUP BY STRFTIME('%Y-%m-%d', 签收时间)
"""

item_num_sql = f"""
SELECT SUM(整件数) AS whole, SUM(散件数) AS scatter, STRFTIME('%Y-%m-%d', 签收时间) AS 天数
FROM 运单表
WHERE 运单状态id IN (4,5,6,7) AND STRFTIME('%Y-%m-%d', 签收时间) BETWEEN '{start_time}' AND '{end_time}'
GROUP BY STRFTIME('%Y-%m-%d', 签收时间)
"""

delivery_time_sql_company = f"""
SELECT AVG(运输天数) AS 平均到货天数, STRFTIME('%Y-%m-%d', 签收时间) AS 天数, 货主名称
FROM 运单表
WHERE 运单状态id IN (4,5,6,7) AND STRFTIME('%Y-%m-%d', 签收时间) BETWEEN '{start_time}' AND '{end_time}'
GROUP BY STRFTIME('%Y-%m-%d', 签收时间), 货主名称
"""

type_sql = f"""
SELECT COUNT(随货同行单号) AS 已派条目数, STRFTIME('%Y-%m-%d', 签收时间) AS 天数, 品种属性
FROM 运单表
WHERE 运单状态id IN (4,5,6,7) AND STRFTIME('%Y-%m-%d', 签收时间) BETWEEN '{start_time}' AND '{end_time}'
GROUP BY STRFTIME('%Y-%m-%d', 签收时间), 品种属性
"""

truck_sql_day = f"""
SELECT COUNT(车辆表.随货同行单号) AS 已派条目数, STRFTIME('%Y-%m-%d', 签收时间) AS 天数, 车牌号
FROM 运单表 join 车辆表 on 运单表.随货同行单号 = 车辆表.随货同行单号
WHERE 运单状态id IN (4,5,6,7) AND STRFTIME('%Y-%m-%d', 签收时间) BETWEEN '{start_time}' AND '{end_time}'
GROUP BY STRFTIME('%Y-%m-%d', 签收时间), 车牌号
"""

driver_sql_day = f"""
SELECT COUNT(车辆表.随货同行单号) AS 已派条目数, STRFTIME('%Y-%m-%d', 签收时间) AS 天数, 司机
FROM 运单表 join 车辆表 on 运单表.随货同行单号 = 车辆表.随货同行单号
WHERE 运单状态id IN (4,5,6,7) AND STRFTIME('%Y-%m-%d', 签收时间) BETWEEN '{start_time}' AND '{end_time}'
GROUP BY STRFTIME('%Y-%m-%d', 签收时间), 司机
"""
driver_time_sql_company = f"""
SELECT AVG(运行时间) AS 平均工作时长, STRFTIME('%Y-%m-%d', 签收时间) AS 天数, 司机
FROM 运单表 join 车辆表 on 运单表.随货同行单号 = 车辆表.随货同行单号
WHERE 运单状态id IN (4,5,6,7) AND STRFTIME('%Y-%m-%d', 签收时间) BETWEEN '{start_time}' AND '{end_time}'
GROUP BY STRFTIME('%Y-%m-%d', 签收时间), 司机
"""

mileage_sql = f"""
SELECT SUM(里程) AS 里程数, STRFTIME('%Y-%m-%d', 签收时间) AS 天数, 车牌号
FROM 运单表 join 车辆表 on 运单表.随货同行单号 = 车辆表.随货同行单号
WHERE 运单状态id IN (4,5,6,7) AND STRFTIME('%Y-%m-%d', 签收时间) BETWEEN '{start_time}' AND '{end_time}'
GROUP BY STRFTIME('%Y-%m-%d', 签收时间), 车牌号
"""


with subtab1:
    if time_selection == '日报':
        data1 = get_data(delivery_sql_day)
        
        chart_auto = alt.Chart(data1, title=alt.TitleParams("已派条目数", anchor="middle")).mark_bar().encode(
        x=alt.X("yearmonthdate(天数):O", title="签收时间"),
        y=alt.Y('已派条目数', title="已派条目数")
        )

        line_auto = alt.Chart(data1).mark_line(color="red").transform_window(
                    rolling_mean="mean(已派条目数)",
                    frame=[-9, 0],
                ).encode(x="yearmonthdate(天数):O", y="rolling_mean:Q")
        subtab1.altair_chart(chart_auto + line_auto, use_container_width=True)
        data2 = get_data(delivery_sql_class1)
        subtab1.altair_chart(
                alt.Chart(data2)
                .mark_line(point=True)
                .encode(
                    x=alt.X("yearmonthdate(天数):O", title="发车时间"),
                    y=alt.Y("已派条目数", title="已派条目数"),
                    color="shift",
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )
        
        data3 = get_data(item_num_sql)
        subtab1_col1, subtab1_col2 = subtab1.columns(2)
        subtab1_col1.altair_chart(
                alt.Chart(data3, title=alt.TitleParams("整件货品数", anchor="middle"))
                .mark_bar()
                .encode(
                    x=alt.X("yearmonthdate(天数):O", title="签收时间"),
                    y=alt.Y("whole", title="整件货品数"),
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )
        subtab1_col2.altair_chart(
                alt.Chart(data3, title=alt.TitleParams("散件货品数", anchor="middle"))
                .mark_bar()
                .encode(
                    x=alt.X("yearmonthdate(天数):O", title="签收时间"),
                    y=alt.Y("scatter", title="散件货品数"),
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )
        
        data4 = get_data(delivery_time_sql)
        subtab1.altair_chart(
                alt.Chart(data4, title=alt.TitleParams("平均到货天数", anchor="middle"))
                .mark_line(point=True)
                .encode(
                    x=alt.X("yearmonthdate(天数):O", title="签收时间"),
                    y=alt.Y("平均到货天数", title="平均到货天数", scale=alt.Scale(domain=[1.6, 2.2])),
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )
        
        
        
        

    if time_selection == '周报':
        data1 = get_data(delivery_sql_week)
        
        chart_auto = alt.Chart(data1).mark_bar().encode(
        x=alt.X("yearmonthdate(周数):O", title="签收时间"),
        y=alt.Y('已派条目数', title="已派条目数")
        )

        line_auto = alt.Chart(data1, title=alt.TitleParams("已派条目数", anchor="middle")).mark_line(color="red").transform_window(
                    rolling_mean="mean(已派条目数)",
                    frame=[-9, 0],
                ).encode(x="yearmonthdate(周数):O", y="rolling_mean:Q")
        subtab1.altair_chart(chart_auto + line_auto, use_container_width=True)
        data2 = get_data(delivery_sql_class2)
        subtab1.altair_chart(
                alt.Chart(data2)
                .mark_line(point=True)
                .encode(
                    x=alt.X("yearmonthdate(周数):O", title="发车时间"),
                    y=alt.Y("已派条目数", title="已派条目数"),
                    color="shift",
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )
        
        data3 = get_data(item_num_sql)
        subtab1_col1, subtab1_col2 = subtab1.columns(2)
        subtab1_col1.altair_chart(
                alt.Chart(data3, title=alt.TitleParams("整件货品数", anchor="middle"))
                .mark_bar()
                .encode(
                    x=alt.X("yearmonthdate(天数):O", title="签收时间"),
                    y=alt.Y("whole", title="整件货品数"),
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )
        subtab1_col2.altair_chart(
                alt.Chart(data3, title=alt.TitleParams("散件货品数", anchor="middle"))
                .mark_bar()
                .encode(
                    x=alt.X("yearmonthdate(天数):O", title="签收时间"),
                    y=alt.Y("scatter", title="散件货品数"),
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )
        
        data4 = get_data(delivery_time_sql)
        subtab1.altair_chart(
                alt.Chart(data4, title=alt.TitleParams("平均到货天数", anchor="middle"))
                .mark_line(point=True)
                .encode(
                    x=alt.X("yearmonthdate(天数):O", title="签收时间"),
                    y=alt.Y("平均到货天数", title="平均到货天数", scale=alt.Scale(domain=[1.6, 2.2])),
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )
        
    
    if time_selection == '月报':
        data1 = get_data(delivery_sql_month)
        
        chart_auto = alt.Chart(data1, title=alt.TitleParams("已派条目数", anchor="middle")).mark_bar().encode(
        x=alt.X("yearmonthdate(月份):O", title="签收时间"),
        y=alt.Y('已派条目数', title="已派条目数")
        )

        line_auto = alt.Chart(data1).mark_line(color="red").transform_window(
                    rolling_mean="mean(已派条目数)",
                    frame=[-9, 0],
                ).encode(x="yearmonthdate(月份):O", y="rolling_mean:Q")
        subtab1.altair_chart(chart_auto + line_auto, use_container_width=True)
        data2 = get_data(delivery_sql_class3)
        subtab1.altair_chart(
                alt.Chart(data2)
                .mark_line(point=True)
                .encode(
                    x=alt.X("yearmonthdate(月份):O", title="发车时间"),
                    y=alt.Y("已派条目数", title="已派条目数"),
                    color="shift",
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )
        
        data3 = get_data(item_num_sql)
        subtab1_col1, subtab1_col2 = subtab1.columns(2)
        subtab1_col1.altair_chart(
                alt.Chart(data3, title=alt.TitleParams("整件货品数", anchor="middle"))
                .mark_bar()
                .encode(
                    x=alt.X("yearmonthdate(天数):O", title="签收时间"),
                    y=alt.Y("whole", title="整件货品数"),
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )
        subtab1_col2.altair_chart(
                alt.Chart(data3, title=alt.TitleParams("散件货品数", anchor="middle"))
                .mark_bar()
                .encode(
                    x=alt.X("yearmonthdate(天数):O", title="签收时间"),
                    y=alt.Y("scatter", title="散件货品数"),
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )
        
        data4 = get_data(delivery_time_sql)
        subtab1.altair_chart(
                alt.Chart(data4, title=alt.TitleParams("平均到货天数", anchor="middle"))
                .mark_line(point=True)
                .encode(
                    x=alt.X("yearmonthdate(天数):O", title="签收时间"),
                    y=alt.Y("平均到货天数", title="平均到货天数", scale=alt.Scale(domain=[1.6, 2.2])),
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )
        
        
with subtab2:
    if time_selection == '日报':
        data1 = get_data(company_sql_day)
        subtab2.altair_chart(
                alt.Chart(data1, title=alt.TitleParams("已派条目数", anchor="middle"))
                .mark_line(point=True)
                .encode(
                    x=alt.X("yearmonthdate(天数):O", title="签收时间"),
                    y=alt.Y("已派条目数", title="已派条目数", scale=alt.Scale(domain=[60, 130])),
                    color="货主名称",
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )
        data2 = get_data(delivery_time_sql_company)
        subtab2.altair_chart(
                alt.Chart(data2, title=alt.TitleParams("平均到货天数", anchor="middle"))
                .mark_line(point=True)
                .encode(
                    x=alt.X("yearmonthdate(天数):O", title="签收时间"),
                    y=alt.Y("平均到货天数", title="平均到货天数", scale=alt.Scale(domain=[1.6, 2.2])),
                    color="货主名称",
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )
        

    if time_selection == '周报':
        data1 = get_data(company_sql_week)
        subtab2.altair_chart(
                alt.Chart(data1)
                .mark_line(point=True)
                .encode(
                    x=alt.X("yearmonthdate(周数):O", title="签收时间"),
                    y=alt.Y("已派条目数", title="已派条目数",scale=alt.Scale(domain=[60, 130])),
                    color="货主名称",
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )
    
    if time_selection == '月报':
        data1 = get_data(company_sql_day)
        subtab2.altair_chart(
                alt.Chart(data1)
                .mark_line(point=True)
                .encode(
                    x=alt.X("yearmonthdate(月份):O", title="签收时间"),
                    y=alt.Y("已派条目数", title="已派条目数", scale=alt.Scale(domain=[60, 130])),
                    color="货主名称",
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )

with subtab3:
    if time_selection == '日报':
        data1 = get_data(type_sql)
        subtab3.altair_chart(
                alt.Chart(data1, title=alt.TitleParams("已派条目数", anchor="middle"))
                .mark_line(point=True)
                .encode(
                    x=alt.X("yearmonthdate(天数):O", title="签收时间"),
                    y=alt.Y("已派条目数", title="已派条目数", scale=alt.Scale(domain=[180, 310])),
                    color="品种属性",
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )
        

    if time_selection == '周报':
        data1 = get_data(type_sql)
        subtab3.altair_chart(
                alt.Chart(data1, title=alt.TitleParams("已派条目数", anchor="middle"))
                .mark_line(point=True)
                .encode(
                    x=alt.X("yearmonthdate(天数):O", title="签收时间"),
                    y=alt.Y("已派条目数", title="已派条目数", scale=alt.Scale(domain=[180, 310])),
                    color="品种属性",
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )
    
    if time_selection == '月报':
        data1 = get_data(type_sql)
        subtab3.altair_chart(
                alt.Chart(data1, title=alt.TitleParams("已派条目数", anchor="middle"))
                .mark_line(point=True)
                .encode(
                    x=alt.X("yearmonthdate(天数):O", title="签收时间"),
                    y=alt.Y("已派条目数", title="已派条目数", scale=alt.Scale(domain=[180, 310])),
                    color="品种属性",
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )
        
repair_sql = f"""
SELECT AVG(修理时间) AS 维修时间, AVG(修理费) AS 维修费用, STRFTIME('%Y-%m-%d', 签收时间) AS 天数, 车牌号
FROM 运单表 join 车辆表 on 运单表.随货同行单号 = 车辆表.随货同行单号
WHERE 运单状态id IN (4,5,6,7) AND STRFTIME('%Y-%m-%d', 签收时间) BETWEEN '{start_time}' AND '{end_time}'
GROUP BY STRFTIME('%Y-%m-%d', 签收时间), 车牌号
"""

with tab2:
    subtab1, subtab2 = tab2.tabs(["整体", '维修'])
    if time_selection == '日报':
        data1 = get_data(truck_sql_day)
        subtab1.altair_chart(
                alt.Chart(data1, title=alt.TitleParams("已派条目数", anchor="middle"))
                .mark_line(point=True)
                .encode(
                    x=alt.X("yearmonthdate(天数):O", title="签收时间"),
                    y=alt.Y("已派条目数", title="已派条目数"),
                    color="车牌号",
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )
        
        data4 = get_data(mileage_sql)
        subtab1.altair_chart(
                alt.Chart(data4, title=alt.TitleParams("里程数", anchor="middle"))
                .mark_line(point=True)
                .encode(
                    x=alt.X("yearmonthdate(天数):O", title="签收时间"),
                    y=alt.Y("里程数", title="里程数"),
                    color="车牌号",
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )
        
        data2 = get_data(driver_sql_day)
        subtab1.altair_chart(
                alt.Chart(data2, title=alt.TitleParams("已派条目数", anchor="middle"))
                .mark_line(point=True)
                .encode(
                    x=alt.X("yearmonthdate(天数):O", title="签收时间"),
                    y=alt.Y("已派条目数", title="已派条目数"),
                    color="司机",
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )
        
        data3 = get_data(driver_time_sql_company)
        subtab1.altair_chart(
                alt.Chart(data3, title=alt.TitleParams("平均工作时长", anchor="middle"))
                .mark_line(point=True)
                .encode(
                    x=alt.X("yearmonthdate(天数):O", title="签收时间"),
                    y=alt.Y("平均工作时长", title="平均工作时长", scale=alt.Scale(domain=[6.2, 7.7])),
                    color="司机",
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )
        
        data5 = get_data(repair_sql)
        subtab2.altair_chart(
                alt.Chart(data5, title=alt.TitleParams("维修时长", anchor="middle"))
                .mark_line(point=True)
                .encode(
                    x=alt.X("yearmonthdate(天数):O", title="日期"),
                    y=alt.Y("维修时间", title="维修时长", scale=alt.Scale(domain=[1.5, 2.5])),
                    color="车牌号",
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )
        
        subtab2.altair_chart(
                alt.Chart(data5, title=alt.TitleParams("维修费用", anchor="middle"))
                .mark_line(point=True)
                .encode(
                    x=alt.X("yearmonthdate(天数):O", title="日期"),
                    y=alt.Y("维修费用", title="维修费用", scale=alt.Scale(domain=[80, 120])),
                    color="车牌号",
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )
        

    if time_selection == '周报':
        data1 = get_data(truck_sql_day)
        subtab1.altair_chart(
                alt.Chart(data1, title=alt.TitleParams("已派条目数", anchor="middle"))
                .mark_line(point=True)
                .encode(
                    x=alt.X("yearmonthdate(天数):O", title="签收时间"),
                    y=alt.Y("已派条目数", title="已派条目数", scale=alt.Scale(domain=[180, 310])),
                    color="车牌号",
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )
    if time_selection == '月报':
        data1 = get_data(truck_sql_day)
        subtab1.altair_chart(
                alt.Chart(data1, title=alt.TitleParams("已派条目数", anchor="middle"))
                .mark_line(point=True)
                .encode(
                    x=alt.X("yearmonthdate(天数):O", title="签收时间"),
                    y=alt.Y("已派条目数", title="已派条目数", scale=alt.Scale(domain=[180, 310])),
                    color="车牌号",
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )
      