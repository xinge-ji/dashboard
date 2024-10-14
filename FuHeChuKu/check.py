import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import duckdb
from streamlit_extras.metric_cards import style_metric_cards
import matplotlib.pyplot as plt
import plotly.express as px
import altair as alt

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
    
    if n == 1:
        return random_times[0]

    return random_times


np.random.seed(42)

n = 1000000

@st.cache_data
def generate_data(n):
    goods_owner_ids = [1, 2, 6, 64, 284, 550, 682,
                       9102, 9103, 9104, 9105, 9108, 9110, 9111, 9112, 9113, 9114,
                       9115, 9116, 9118, 9119, 9120, 9122, 9123, 9124, 9125, 9126,
                       9128, 9129, 9130, 9131, 9132, 9134, 9135, 9138, 9140, 9141,
                       9142, 9145, 9146, 9150, 9151, 9152, 9158, 9160, 9162, 9164]
    goods_owner_dict = {
        1: "鹭燕医药股份有限公司",
        2: "厦门鹭燕医疗器械有限公司",
        6: "厦门鹭燕大药房有限公司",
        64: "厦门燕来福制药有限公司",
        284: "泉州鹭燕大药房有限公司",
        550: "厦门鹭燕海峡两岸药材贸易有限公司",
        682: "厦门鹭燕药业有限公司",
        9102: "福建一善药业有限公司",
        9103: "厦门共鑫科技有限公司",
        9104: "福建天泉药业股份有限公司",
        9105: "福建省联康药业连锁有限公司",
        9108: "福建省厚正科技有限公司",
        9110: "厦门慕仁医疗科技有限公司",
        9111: "厦门康瑞电子科技有限公司",
        9112: "厦门医闽通医疗科技有限公司",
        9113: "北京泰德制药股份有限公司",
        9114: "厦门世通泰康医疗器械有限公司",
        9115: "厦门超来医疗设备有限公司",
        9116: "厦门海辰天泽仪器有限公司",
        9118: "厦门长松生物科技有限公司",
        9119: "厦门康泰贝瑞生物科技有限公司",
        9120: "厦门朗致生物科技有限公司",
        9122: "厦门优智源商贸有限公司",
        9123: "厦门昱轩科技有限公司",
        9124: "厦门致道医疗器械有限公司",
        9125: "善春医疗科技有限公司",
        9126: "合普（中国）医疗科技股份有限公司",
        9128: "厦门中福华康科技发展有限公司",
        9129: "厦门润泽康医药科技有限公司",
        9130: "厦门科迈医疗器械有限公司",
        9131: "泉州市万祺医疗科技有限公司",
        9132: "创信(厦门)融资租赁有限公司",
        9134: "厦门市艾宝医疗科技有限公司",
        9135: "中科裕丞（厦门）生物科技有限公司",
        9138: "厦门桓嘉医疗科技有限公司",
        9140: "泉州市浩鑫城医疗器械有限公司",
        9141: "厦门懿铭科技有限公司",
        9142: "厦门绿雅堂药业有限公司",
        9145: "厦门驰鹭贸易有限公司",
        9146: "厦门昱柏贸易有限公司",
        9150: "厦门伶新商贸有限公司",
        9151: "厦门市标旭医疗科技有限公司",
        9152: "厦门依帆达医疗科技有限公司",
        9158: "厦门万涵海科技有限公司",
        9160: "厦门加木医疗科技有限公司",
        9162: "厦门多实医疗器械有限公司",
        9164: "厦门泓宇正元医疗器械有限公司"
    }

    sent_order_status_ids = [-1,0,1,2,3,4,5,6,7]
    sent_order_status_dict = {-1: "库存不足",0: "取消",1: "下单",2: "待拣货",3: "待复核",4: "已出货",5: "已抵达",6: "等待补货", 7: "出库取消" }

    waveid = [random.randint(0, 10000) if random.random() > 0.2 else None for _ in range(n)]

    business_type_ids = [11,12,18,21,25,43,73]
    business_type_dict = {11: "销售",12: "进货退出",18: "移库出",21: "报损",25: "赠品出库",43: "配送出",73: "领料出库"}

    start_date = datetime(2023, 1, 1)
    end_date = datetime.today()
    order_dates = generate_random_datetime_in_shifts(n, start_date, end_date)

    random_goods_owner_ids = np.random.choice(goods_owner_ids, n)
    goods_owner_names = [goods_owner_dict[goods_id] for goods_id in random_goods_owner_ids]

    random_sent_order_status_ids = np.random.choice(sent_order_status_ids, n)
    sent_order_status_names = [sent_order_status_dict[order_id] for order_id in random_sent_order_status_ids]

    random_business_type_ids = np.random.choice(business_type_ids, n)
    random_business_type_names = [business_type_dict[business_id] for business_id in random_business_type_ids]

    sub_order_status_id = [0,1,2,5]
    sub_order_status_dict = {0:'取消', 1:'待配货',2:'处理中',5:'已出库'}
    random_sub_order_status_ids = np.random.choice(sub_order_status_id, n)
    random_sub_order_status_names = [sub_order_status_dict[sub_order_id] for sub_order_id in random_sub_order_status_ids]

    sent_order_num = np.random.randint(100,1000, n)
    actual_sent_order_num = [np.random.randint(90, num) for num in sent_order_num]

    df_sale = pd.DataFrame({
        '物流出库订单id': np.arange(1, n+1),
        '出库订单细单id': np.arange(1, n+1),
        '货主id': random_goods_owner_ids,
        '货主名称': goods_owner_names,
        '订单接收时间': order_dates,
        '出货状态id': random_sent_order_status_ids,
        '出货状态名称': sent_order_status_names,
        '波次id': waveid,
        '业务类型id': random_business_type_ids,
        '业务类型名称': random_business_type_names,
        '细单状态id': random_sub_order_status_ids,
        '细单状态': random_sub_order_status_names,
        '出货数量': sent_order_num,
        '实际出货数量': actual_sent_order_num
    })


    excute_status_ids = [0,1,2,3,4,5]
    excute_status_dict = {1: "初始",2: "释放",3: "完成",4: "锁定",0: "取消",5: "转单处理中" }
    random_excute_status_ids = np.random.choice(excute_status_ids, n)
    random_excute_status_names = [excute_status_dict[excute_id] for excute_id in random_excute_status_ids]


    df_wave_management = pd.DataFrame({
        '执行时间': order_dates,
        '执行状态id': random_excute_status_ids,
        '执行状态': random_excute_status_names,
        '波次id': waveid
    })

    df_check = pd.DataFrame({
        '记账日期': order_dates,
        '出入库id': np.arange(1,n+1),
        '出入库类型id': np.random.choice([0,1], n),
        '任务状态id': np.random.choice([1,2,3],n),
        '出库数量': sent_order_num,
        '实际业务数量': actual_sent_order_num,
        '库区名称': np.random.choice(['自动库', '平库', '冷库', '中药库', '第三方'], n),
        '复核状态id': np.random.choice([1,2,3], n),
        '任务完成时间': [generate_random_datetime_in_shifts(1, start_date, end_date) if random.random() > 0.3 else None for _ in range(n)],
        '复核数量': np.random.randint(2, 5, n),
        '记账人': [f"记账人_{i}" for i in np.random.randint(1, 1001, n)],
        '任务执行人': [f"拣货人_{i}" for i in np.random.randint(1, 1001, n)],
        '复核人A': [f"复核人_{i}" for i in np.random.randint(1, 1001, n)]
    })

    return df_sale, df_wave_management, df_check


df_sale, df_wave_management, df_check = generate_data(n)

con = duckdb.connect()  # 创建 DuckDB 连接
con.execute("CREATE TABLE 销售表 AS SELECT * FROM df_sale") 
con.execute("CREATE TABLE 波次管理表 AS SELECT * FROM df_wave_management")
con.execute("CREATE TABLE 出入库明细查询表 AS SELECT * FROM df_check")


def get_data(sql_query):
    return con.query(sql_query).df()

st.title("出库复核展示")
defalut_start_date = datetime.now() - timedelta(days=30)
default_end_time = datetime.now() - timedelta(days=1)
current_year = datetime.now().year

col1, col2 = st.columns((2))
with st.container(border=True):
    with col1:
        start_time = st.date_input("起始日期", value=defalut_start_date)
    with col2:
        end_time = st.date_input("结束日期", value=default_end_time)



third_party_owners = (9102, 9103, 9104, 9105, 9108, 9110, 9111, 9112, 9113, 9114,
                       9115, 9116, 9118, 9119, 9120, 9122, 9123, 9124, 9125, 9126,
                       9128, 9129, 9130, 9131, 9132, 9134, 9135, 9138, 9140, 9141,
                       9142, 9145, 9146, 9150, 9151, 9152, 9158, 9160, 9162, 9164)

xiamen_owners = (1,2,6,64,284,550,682)

order_num_by_owner_sql_all = f"""
SELECT
    货主id,
    货主名称,
    COUNT(物流出库订单id) AS 销售条目数
FROM 
    销售表
WHERE 
    细单状态 = '已出库'
    AND STRFTIME('%Y-%m-%d', 订单接收时间) BETWEEN '{start_time}' AND '{end_time}'
GROUP BY 货主id, 货主名称
ORDER BY 销售条目数 desc
"""

order_num_by_owner_sql_xiamen = f"""
SELECT
    货主id,
    货主名称,
    COUNT(物流出库订单id) AS 销售条目数
FROM 
    销售表
WHERE 
    细单状态 = '已出库'
    AND 货主id IN {xiamen_owners}
    AND STRFTIME('%Y-%m-%d', 订单接收时间) BETWEEN '{start_time}' AND '{end_time}'
GROUP BY 货主id, 货主名称
ORDER BY 销售条目数 desc
"""

order_num_by_owner_sql_third_party = f"""
SELECT
    货主id,
    货主名称,
    COUNT(物流出库订单id) AS 销售条目数
FROM 
    销售表
WHERE 
    细单状态 = '已出库'
    AND 货主id IN {third_party_owners}
    AND STRFTIME('%Y-%m-%d', 订单接收时间) BETWEEN '{start_time}' AND '{end_time}'
GROUP BY 货主id, 货主名称
ORDER BY 销售条目数 desc
"""

total_order_sql_all = f"""
SELECT
    COUNT(物流出库订单id) AS 销售条目数
FROM 
    销售表
WHERE 
    细单状态 = '已出库'
    AND STRFTIME('%Y-%m-%d', 订单接收时间) BETWEEN '{start_time}' AND '{end_time}'
"""

total_order_sql_xiamen = f"""
SELECT
    COUNT(物流出库订单id) AS 销售条目数
FROM 
    销售表
WHERE 
    细单状态 = '已出库'
    AND 货主id IN {xiamen_owners}
    AND STRFTIME('%Y-%m-%d', 订单接收时间) BETWEEN '{start_time}' AND '{end_time}'
"""

total_order_sql_third_party = f"""
SELECT
    COUNT(物流出库订单id) AS 销售条目数
FROM 
    销售表
WHERE 
    细单状态 = '已出库'
    AND 货主id IN {third_party_owners}
    AND STRFTIME('%Y-%m-%d', 订单接收时间) BETWEEN '{start_time}' AND '{end_time}'
"""

col_3, col_4 = st.columns([0.3, 0.7])
with col_3:
    with st.container(border=True):
        owners_type = st.selectbox("请选择货主类型", ["所有货主", "厦门货主", "第三方货主"])

    if owners_type == '所有货主':
        st.metric(label="总销售条目数", value=f"{int(get_data(total_order_sql_all)['销售条目数']):,}")
        style_metric_cards()
    elif owners_type == "厦门货主":
        st.metric(label="总销售条目数", value=f"{int(get_data(total_order_sql_xiamen)['销售条目数']):,}")
        style_metric_cards()
    elif owners_type == "第三方货主":
        st.metric(label="总销售条目数", value=f"{int(get_data(total_order_sql_third_party)['销售条目数']):,}")
        style_metric_cards()

with col_4:
    if owners_type == "所有货主":
        st.dataframe(get_data(order_num_by_owner_sql_all))
    elif owners_type == "厦门货主":
        st.dataframe(get_data(order_num_by_owner_sql_xiamen))
    elif owners_type == "第三方货主":
        st.dataframe(get_data(order_num_by_owner_sql_third_party))


sales_num_by_month_sql = f"""
SELECT
    STRFTIME('%Y-%m-%d', 订单接收时间) AS 订单接收时间,
    COUNT(物流出库订单id) AS 销售条目数
FROM 
    销售表
WHERE 
    细单状态 = '已出库'
    AND STRFTIME('%Y-%m-%d', 订单接收时间) BETWEEN '{start_time}' AND '{end_time}'
GROUP BY
    STRFTIME('%Y-%m-%d', 订单接收时间)
"""
with st.expander("销售数量趋势"):
    plot_data_sales = get_data(sales_num_by_month_sql)

    chart = alt.Chart(plot_data_sales).mark_bar().encode(
    x=alt.X("yearmonthdate(订单接收时间):O", title="订单接收时间"),
    y='销售条目数'
    )

    line = (
            alt.Chart(plot_data_sales)
            .mark_line(color="red")
            .transform_window(
                rolling_mean="mean(销售条目数)",
                frame=[-9, 0],
            )
            .encode(x="yearmonthdate(订单接收时间):O", y="rolling_mean:Q")
        )

    st.altair_chart(chart+line, use_container_width=True)

    #fig, ax = plt.subplots(figsize=(10, 5))
    #ax.bar(plot_data_sales['订单接收时间'], plot_data_sales['销售条目数'])
    #ax.plot(plot_data_sales['订单接收时间'], plot_data_sales['销售条目数'], marker='o', linestyle='-', color='grey')
    #ax.set_xlabel('Month')
    #ax.set_ylabel('Sales')

    #st.pyplot(fig)


st.markdown('---')

with st.container(border=True):
    tab1, tab2, tab3, tab4, tab5 = st.tabs(['自动库', '平库', '冷库', '中药库', '第三方'])

classification_num_sql = f"""
SELECT
    库区名称,
    COUNT(出入库id) AS 已分拣数,
    COUNT(DISTINCT 任务执行人) AS 分拣人数,
    SUM(复核数量) AS 复核数量,
    COUNT(DISTINCT 复核人A) AS 复核人数
FROM
    出入库明细查询表
WHERE
    任务状态id = 3
    AND STRFTIME('%Y-%m-%d', 任务完成时间) BETWEEN '{start_time}' AND '{end_time}'
GROUP BY
    库区名称
"""
df_classification_stat = get_data(classification_num_sql)

classification_trend_sql_auto = f"""
SELECT
    COUNT(出入库id) AS 已分拣数,
    STRFTIME('%Y-%m-%d', 任务完成时间) AS 任务完成时间,

FROM
    出入库明细查询表
WHERE
    任务状态id = 3
    AND 库区名称 = '自动库'
    AND STRFTIME('%Y-%m-%d', 任务完成时间) BETWEEN '{start_time}' AND '{end_time}'
GROUP BY
    STRFTIME('%Y-%m-%d', 任务完成时间)
"""

classification_trend_sql_ping = f"""
SELECT
    COUNT(出入库id) AS 已分拣数,
    STRFTIME('%Y-%m-%d', 任务完成时间) AS 任务完成时间,

FROM
    出入库明细查询表
WHERE
    任务状态id = 3
    AND 库区名称 = '平库'
    AND STRFTIME('%Y-%m-%d', 任务完成时间) BETWEEN '{start_time}' AND '{end_time}'
GROUP BY
    STRFTIME('%Y-%m-%d', 任务完成时间)
"""

classification_trend_sql_cold = f"""
SELECT
    COUNT(出入库id) AS 已分拣数,
    STRFTIME('%Y-%m-%d', 任务完成时间) AS 任务完成时间,

FROM
    出入库明细查询表
WHERE
    任务状态id = 3
    AND 库区名称 = '冷库'
    AND STRFTIME('%Y-%m-%d', 任务完成时间) BETWEEN '{start_time}' AND '{end_time}'
GROUP BY
    STRFTIME('%Y-%m-%d', 任务完成时间)
"""

classification_trend_sql_third_party = f"""
SELECT
    COUNT(出入库id) AS 已分拣数,
    STRFTIME('%Y-%m-%d', 任务完成时间) AS 任务完成时间,

FROM
    出入库明细查询表
WHERE
    任务状态id = 3
    AND 库区名称 = '第三方'
    AND STRFTIME('%Y-%m-%d', 任务完成时间) BETWEEN '{start_time}' AND '{end_time}'
GROUP BY
    STRFTIME('%Y-%m-%d', 任务完成时间)
"""

classification_trend_sql_zhongyao = f"""
SELECT
    COUNT(出入库id) AS 已分拣数,
    STRFTIME('%Y-%m-%d', 任务完成时间) AS 任务完成时间,

FROM
    出入库明细查询表
WHERE
    任务状态id = 3
    AND 库区名称 = '中药库'
    AND STRFTIME('%Y-%m-%d', 任务完成时间) BETWEEN '{start_time}' AND '{end_time}'
GROUP BY
    STRFTIME('%Y-%m-%d', 任务完成时间)
"""

double_check_trend_sql_auto = f"""
SELECT
    SUM(复核数量) AS 复核数量,
    STRFTIME('%Y-%m-%d', 任务完成时间) AS 任务完成时间,

FROM
    出入库明细查询表
WHERE
    任务状态id = 3
    AND 库区名称 = '自动库'
    AND STRFTIME('%Y-%m-%d', 任务完成时间) BETWEEN '{start_time}' AND '{end_time}'
GROUP BY
    STRFTIME('%Y-%m-%d', 任务完成时间)
"""
double_check_trend_sql_ping = f"""
SELECT
    SUM(复核数量) AS 复核数量,
    STRFTIME('%Y-%m-%d', 任务完成时间) AS 任务完成时间,

FROM
    出入库明细查询表
WHERE
    任务状态id = 3
    AND 库区名称 = '平库'
    AND STRFTIME('%Y-%m-%d', 任务完成时间) BETWEEN '{start_time}' AND '{end_time}'
GROUP BY
    STRFTIME('%Y-%m-%d', 任务完成时间)
"""

double_check_trend_sql_cold = f"""
SELECT
    SUM(复核数量) AS 复核数量,
    STRFTIME('%Y-%m-%d', 任务完成时间) AS 任务完成时间,

FROM
    出入库明细查询表
WHERE
    任务状态id = 3
    AND 库区名称 = '冷库'
    AND STRFTIME('%Y-%m-%d', 任务完成时间) BETWEEN '{start_time}' AND '{end_time}'
GROUP BY
    STRFTIME('%Y-%m-%d', 任务完成时间)
"""

double_check_trend_sql_zhongyao = f"""
SELECT
    SUM(复核数量) AS 复核数量,
    STRFTIME('%Y-%m-%d', 任务完成时间) AS 任务完成时间,

FROM
    出入库明细查询表
WHERE
    任务状态id = 3
    AND 库区名称 = '中药库'
    AND STRFTIME('%Y-%m-%d', 任务完成时间) BETWEEN '{start_time}' AND '{end_time}'
GROUP BY
    STRFTIME('%Y-%m-%d', 任务完成时间)
"""

double_check_trend_sql_third_party = f"""
SELECT
    SUM(复核数量) AS 复核数量,
    STRFTIME('%Y-%m-%d', 任务完成时间) AS 任务完成时间,

FROM
    出入库明细查询表
WHERE
    任务状态id = 3
    AND 库区名称 = '第三方'
    AND STRFTIME('%Y-%m-%d', 任务完成时间) BETWEEN '{start_time}' AND '{end_time}'
GROUP BY
    STRFTIME('%Y-%m-%d', 任务完成时间)
"""

data1_sql = f"""
SELECT
    库区名称,
    STRFTIME('%Y-%m-%d', 任务完成时间) AS 任务完成时间,
    COUNT(出入库id) AS 已分拣数,
    COUNT(DISTINCT 任务执行人) AS 分拣人数,
    SUM(复核数量) AS 复核数量,
    COUNT(DISTINCT 复核人A) AS 复核人数
FROM
    出入库明细查询表
WHERE
    任务状态id = 3
    AND STRFTIME('%Y-%m-%d', 任务完成时间) BETWEEN '{start_time}' AND '{end_time}'
GROUP BY
    库区名称, STRFTIME('%Y-%m-%d', 任务完成时间)
"""

days = end_time - start_time
with tab1:
    sub_c11, sub_c12, sub_c13, sub_c14 = st.columns(4)
    sub_c11.metric(label="自动库分拣数", value=f"{int(df_classification_stat.loc[df_classification_stat['库区名称'] == '自动库', '已分拣数']):,}")    
    sub_c11.metric(label="分拣速度(件/天)", value=f"{round(int(df_classification_stat.loc[df_classification_stat['库区名称'] == '自动库', '已分拣数'])/days.days):,}")
    sub_c12.metric(label="分拣人员数", value=f"{int(df_classification_stat.loc[df_classification_stat['库区名称'] == '自动库', '分拣人数']):,}")
    sub_c12.metric(label="分拣速度(件/人)", value=f"{round(int(df_classification_stat.loc[df_classification_stat['库区名称'] == '自动库', '已分拣数'])/int(df_classification_stat.loc[df_classification_stat['库区名称'] == '自动库', '分拣人数'])):,}")    
    sub_c13.metric(label="复核数量", value=f"{int(df_classification_stat.loc[df_classification_stat['库区名称'] == '自动库', '复核数量']):,}")
    sub_c13.metric(label="复核速度(件/天)", value=f"{round(int(df_classification_stat.loc[df_classification_stat['库区名称'] == '自动库', '复核数量'])/days.days):,}")
    sub_c14.metric(label="复核人员数", value=f"{int(df_classification_stat.loc[df_classification_stat['库区名称'] == '自动库', '复核人数']):,}")
    sub_c14.metric(label="复核速度(件/人)", value=f"{round(int(df_classification_stat.loc[df_classification_stat['库区名称'] == '自动库', '复核数量'])/int(df_classification_stat.loc[df_classification_stat['库区名称'] == '自动库', '复核人数'])):,}")
    style_metric_cards()

    plot_data_classification_auto = get_data(classification_trend_sql_auto)

    chart_auto = alt.Chart(plot_data_classification_auto).mark_bar().encode(
    x=alt.X("yearmonthdate(任务完成时间):O", title="任务完成时间"),
    y='已分拣数'
    )

    line_auto = alt.Chart(plot_data_classification_auto).mark_line(color="red").transform_window(
                rolling_mean="mean(已分拣数)",
                frame=[-9, 0],
            ).encode(x="yearmonthdate(任务完成时间):O", y="rolling_mean:Q")

    st.altair_chart(chart_auto + line_auto, use_container_width=True)

    plot_data_doublecheck_auto = get_data(double_check_trend_sql_auto)

    chart_auto_d = alt.Chart(plot_data_doublecheck_auto).mark_bar().encode(
    x=alt.X("yearmonthdate(任务完成时间):O", title="任务完成时间"),
    y='复核数量'
    )
    line_auto_d = alt.Chart(plot_data_doublecheck_auto).mark_line(color="red").transform_window(
                rolling_mean="mean(复核数量)",
                frame=[-9, 0],
            ).encode(x="yearmonthdate(任务完成时间):O", y="rolling_mean:Q")

    st.altair_chart(chart_auto_d + line_auto_d, use_container_width=True)





with tab2:
    sub_c21, sub_c22, sub_c23, sub_c24 = st.columns(4)
    sub_c21.metric(label="平库分拣数", value=f"{int(df_classification_stat.loc[df_classification_stat['库区名称'] == '平库', '已分拣数']):,}")    
    sub_c21.metric(label="分拣速度(件/天)", value=f"{round(int(df_classification_stat.loc[df_classification_stat['库区名称'] == '平库', '已分拣数'])/days.days):,}")
    sub_c22.metric(label="分拣人员数", value=f"{int(df_classification_stat.loc[df_classification_stat['库区名称'] == '平库', '分拣人数']):,}")    
    sub_c22.metric(label="分拣速度(件/人)", value=f"{round(int(df_classification_stat.loc[df_classification_stat['库区名称'] == '平库', '已分拣数'])/int(df_classification_stat.loc[df_classification_stat['库区名称'] == '平库', '分拣人数'])):,}")
    sub_c23.metric(label="复核数量", value=f"{int(df_classification_stat.loc[df_classification_stat['库区名称'] == '平库', '复核数量']):,}")
    sub_c23.metric(label="复核速度(件/天)", value=f"{round(int(df_classification_stat.loc[df_classification_stat['库区名称'] == '平库', '复核数量'])/days.days):,}")
    sub_c24.metric(label="复核人员数", value=f"{int(df_classification_stat.loc[df_classification_stat['库区名称'] == '平库', '复核人数']):,}")   
    sub_c24.metric(label="复核速度(件/人)", value=f"{round(int(df_classification_stat.loc[df_classification_stat['库区名称'] == '平库', '复核数量'])/int(df_classification_stat.loc[df_classification_stat['库区名称'] == '平库', '复核人数'])):,}")
    style_metric_cards()

    plot_data_classification_ping = get_data(classification_trend_sql_ping)

    chart_ping = alt.Chart(plot_data_classification_ping).mark_bar().encode(
    x=alt.X("yearmonthdate(任务完成时间):O", title="任务完成时间"),
    y='已分拣数'
    )

    line_ping = alt.Chart(plot_data_classification_ping).mark_line(color="red").transform_window(
                rolling_mean="mean(已分拣数)",
                frame=[-9, 0],
            ).encode(x="yearmonthdate(任务完成时间):O", y="rolling_mean:Q")
    st.altair_chart(chart_ping + line_ping, use_container_width=True)

    plot_data_doublecheck_ping = get_data(double_check_trend_sql_ping)

    chart_ping_d = alt.Chart(plot_data_doublecheck_ping).mark_bar().encode(
    x=alt.X("yearmonthdate(任务完成时间):O", title="任务完成时间"),
    y='复核数量'
    )
    line_ping_d = alt.Chart(plot_data_doublecheck_ping).mark_line(color="red").transform_window(
                rolling_mean="mean(复核数量)",
                frame=[-9, 0],
            ).encode(x="yearmonthdate(任务完成时间):O", y="rolling_mean:Q")

    st.altair_chart(chart_ping_d + line_ping_d, use_container_width=True)

with tab3:
    sub_c31, sub_c32, sub_c33, sub_c34 = st.columns(4)
    sub_c31.metric(label="冷库分拣数", value=f"{int(df_classification_stat.loc[df_classification_stat['库区名称'] == '冷库', '已分拣数']):,}")    
    sub_c31.metric(label="分拣速度(件/天)", value=f"{round(int(df_classification_stat.loc[df_classification_stat['库区名称'] == '冷库', '已分拣数'])/days.days):,}")
    sub_c32.metric(label="分拣人员数", value=f"{int(df_classification_stat.loc[df_classification_stat['库区名称'] == '冷库', '分拣人数']):,}")    
    sub_c32.metric(label="分拣速度(件/人)", value=f"{round(int(df_classification_stat.loc[df_classification_stat['库区名称'] == '冷库', '已分拣数'])/int(df_classification_stat.loc[df_classification_stat['库区名称'] == '冷库', '分拣人数'])):,}")
    sub_c33.metric(label="复核数量", value=f"{int(df_classification_stat.loc[df_classification_stat['库区名称'] == '冷库', '复核数量']):,}")
    sub_c33.metric(label="复核速度(件/天)", value=f"{round(int(df_classification_stat.loc[df_classification_stat['库区名称'] == '冷库', '复核数量'])/days.days):,}")
    sub_c34.metric(label="复核人员数", value=f"{int(df_classification_stat.loc[df_classification_stat['库区名称'] == '冷库', '复核人数']):,}")   
    sub_c34.metric(label="复核速度(件/人)", value=f"{round(int(df_classification_stat.loc[df_classification_stat['库区名称'] == '冷库', '复核数量'])/int(df_classification_stat.loc[df_classification_stat['库区名称'] == '冷库', '复核人数'])):,}")
    style_metric_cards()

    plot_data_classification_cold = get_data(classification_trend_sql_cold)

    chart_cold = alt.Chart(plot_data_classification_cold).mark_bar().encode(
    x=alt.X("yearmonthdate(任务完成时间):O", title="任务完成时间"),
    y='已分拣数'
    )
    line_cold = alt.Chart(plot_data_classification_cold).mark_line(color="red").transform_window(
                rolling_mean="mean(已分拣数)",
                frame=[-9, 0],
            ).encode(x="yearmonthdate(任务完成时间):O", y="rolling_mean:Q")

    st.altair_chart(chart_cold + line_cold, use_container_width=True)

    plot_data_doublecheck_cold = get_data(double_check_trend_sql_cold)

    chart_cold_d = alt.Chart(plot_data_doublecheck_cold).mark_bar().encode(
    x=alt.X("yearmonthdate(任务完成时间):O", title="任务完成时间"),
    y='复核数量'
    )
    line_cold_d = alt.Chart(plot_data_doublecheck_cold).mark_line(color="red").transform_window(
                rolling_mean="mean(复核数量)",
                frame=[-9, 0],
            ).encode(x="yearmonthdate(任务完成时间):O", y="rolling_mean:Q")

    st.altair_chart(chart_cold_d + line_cold_d, use_container_width=True)

with tab4:
    sub_c41, sub_c42, sub_c43, sub_c44 = st.columns(4)
    sub_c41.metric(label="中药库分拣数", value=f"{int(df_classification_stat.loc[df_classification_stat['库区名称'] == '中药库', '已分拣数']):,}")    
    sub_c41.metric(label="分拣速度(件/天)", value=f"{round(int(df_classification_stat.loc[df_classification_stat['库区名称'] == '中药库', '已分拣数'])/days.days):,}")
    sub_c42.metric(label="分拣人员数", value=f"{int(df_classification_stat.loc[df_classification_stat['库区名称'] == '中药库', '分拣人数']):,}")    
    sub_c42.metric(label="分拣速度(件/人)", value=f"{round(int(df_classification_stat.loc[df_classification_stat['库区名称'] == '中药库', '已分拣数'])/int(df_classification_stat.loc[df_classification_stat['库区名称'] == '中药库', '分拣人数'])):,}")
    sub_c43.metric(label="复核数量", value=f"{int(df_classification_stat.loc[df_classification_stat['库区名称'] == '中药库', '复核数量']):,}")
    sub_c43.metric(label="复核速度(件/天)", value=f"{round(int(df_classification_stat.loc[df_classification_stat['库区名称'] == '中药库', '复核数量'])/days.days):,}")
    sub_c44.metric(label="复核人员数", value=f"{int(df_classification_stat.loc[df_classification_stat['库区名称'] == '中药库', '复核人数']):,}")   
    sub_c44.metric(label="复核速度(件/人)", value=f"{round(int(df_classification_stat.loc[df_classification_stat['库区名称'] == '中药库', '复核数量'])/int(df_classification_stat.loc[df_classification_stat['库区名称'] == '中药库', '复核人数'])):,}")
    style_metric_cards()

    plot_data_classification_zhongyao = get_data(classification_trend_sql_zhongyao)

    chart_zhongyao = alt.Chart(plot_data_classification_zhongyao).mark_bar().encode(
    x=alt.X("yearmonthdate(任务完成时间):O", title="任务完成时间"),
    y='已分拣数'
    )
    line_zy = alt.Chart(plot_data_classification_zhongyao).mark_line(color="red").transform_window(
                rolling_mean="mean(已分拣数)",
                frame=[-9, 0],
            ).encode(x="yearmonthdate(任务完成时间):O", y="rolling_mean:Q")

    st.altair_chart(chart_zhongyao+line_zy, use_container_width=True)

    plot_data_doublecheck_zhongyao = get_data(double_check_trend_sql_zhongyao)

    chart_zhongyao_d = alt.Chart(plot_data_doublecheck_zhongyao).mark_bar().encode(
    x=alt.X("yearmonthdate(任务完成时间):O", title="任务完成时间"),
    y='复核数量'
    )
    line_zy_d = alt.Chart(plot_data_doublecheck_zhongyao).mark_line(color="red").transform_window(
                rolling_mean="mean(复核数量)",
                frame=[-9, 0],
            ).encode(x="yearmonthdate(任务完成时间):O", y="rolling_mean:Q")

    st.altair_chart(chart_zhongyao_d+line_zy_d, use_container_width=True)

with tab5:
    sub_c51, sub_c52, sub_c53, sub_c54 = st.columns(4)
    sub_c51.metric(label="第三方分拣数", value=f"{int(df_classification_stat.loc[df_classification_stat['库区名称'] == '第三方', '已分拣数']):,}")    
    sub_c51.metric(label="分拣速度(件/天)", value=f"{round(int(df_classification_stat.loc[df_classification_stat['库区名称'] == '第三方', '已分拣数'])/days.days):,}")
    sub_c52.metric(label="分拣人员数", value=f"{int(df_classification_stat.loc[df_classification_stat['库区名称'] == '第三方', '分拣人数']):,}")    
    sub_c52.metric(label="分拣速度(件/人)", value=f"{round(int(df_classification_stat.loc[df_classification_stat['库区名称'] == '第三方', '已分拣数'])/int(df_classification_stat.loc[df_classification_stat['库区名称'] == '第三方', '分拣人数'])):,}")
    sub_c53.metric(label="复核数量", value=f"{int(df_classification_stat.loc[df_classification_stat['库区名称'] == '第三方', '复核数量']):,}")
    sub_c53.metric(label="复核速度(件/天)", value=f"{round(int(df_classification_stat.loc[df_classification_stat['库区名称'] == '第三方', '复核数量'])/days.days):,}")
    sub_c54.metric(label="复核人员数", value=f"{int(df_classification_stat.loc[df_classification_stat['库区名称'] == '第三方', '复核人数']):,}")   
    sub_c54.metric(label="复核速度(件/人)", value=f"{round(int(df_classification_stat.loc[df_classification_stat['库区名称'] == '第三方', '复核数量'])/int(df_classification_stat.loc[df_classification_stat['库区名称'] == '第三方', '复核人数'])):,}")
    style_metric_cards()

    plot_data_classification_third_party = get_data(classification_trend_sql_third_party)

    chart_third_party = alt.Chart(plot_data_classification_third_party).mark_bar().encode(
    x=alt.X("yearmonthdate(任务完成时间):O", title="任务完成时间"),
    y='已分拣数'
    )
    line_third = alt.Chart(plot_data_classification_third_party).mark_line(color="red").transform_window(
                rolling_mean="mean(已分拣数)",
                frame=[-9, 0],
            ).encode(x="yearmonthdate(任务完成时间):O", y="rolling_mean:Q")

    st.altair_chart(chart_third_party+line_third, use_container_width=True)

    plot_data_doublecheck_third_party = get_data(double_check_trend_sql_third_party)

    chart_third_d = alt.Chart(plot_data_doublecheck_third_party).mark_bar().encode(
    x=alt.X("yearmonthdate(任务完成时间):O", title="任务完成时间"),
    y='复核数量'
    )
    line_third_d = alt.Chart(plot_data_doublecheck_third_party).mark_line(color="red").transform_window(
                rolling_mean="mean(复核数量)",
                frame=[-9, 0],
            ).encode(x="yearmonthdate(任务完成时间):O", y="rolling_mean:Q")

    st.altair_chart(chart_third_d+ line_third_d, use_container_width=True)

data1 = get_data(data1_sql)
chart1 = alt.Chart(data1).mark_line().encode(
    x=alt.X("yearmonthdate(任务完成时间):O", title="任务完成时间"),  # T for temporal data type to treat as date
    y='已分拣数',  # Q for quantitative data type
    color='库区名称',  # N for nominal, to color by category
    tooltip=['任务完成时间', '库区名称', '已分拣数']
).interactive()  # Enables zoom and pan

# Display the chart in Streamlit
st.altair_chart(chart1, use_container_width=True)

chart2 = alt.Chart(data1).mark_line().encode(
    x=alt.X("yearmonthdate(任务完成时间):O", title="任务完成时间"),  # T for temporal data type to treat as date
    y='复核数量',  # Q for quantitative data type
    color='库区名称',  # N for nominal, to color by category
    tooltip=['任务完成时间', '库区名称', '复核数量']
).interactive()  # Enables zoom and pan

# Display the chart in Streamlit
st.altair_chart(chart2, use_container_width=True)