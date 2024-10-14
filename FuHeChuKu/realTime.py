import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import duckdb
from streamlit_extras.metric_cards import style_metric_cards
import plotly.express as px
import matplotlib.pyplot as plt


def random_date(start, end):
    return start + timedelta(days=random.randint(0, int((end - start).days)))


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

# 设置随机种子
np.random.seed(42)

# 设置数据量
n = 100000

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
    order_dates = generate_random_datetime_in_shifts(n)

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
        '任务完成时间': [generate_random_datetime_in_shifts(1) if random.random() > 0.3 else None for _ in range(n)],
        '复核数量': np.random.randint(2, 10, n),
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

yesterday = datetime.now() - timedelta(days=1)

yesterday_str = yesterday.strftime('%Y-%m')

today = datetime.now()
today2 = today.strftime('%Y-%m-%d %H:%M:%S')
today_str = today.strftime('%Y-%m-%d')

st.title("实时数据展示")

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
    AND STRFTIME('%Y-%m-%d', 订单接收时间) = '{today_str}'
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
    AND STRFTIME('%Y-%m-%d', 订单接收时间) = '{today_str}'
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
    AND STRFTIME('%Y-%m-%d', 订单接收时间) = '{today_str}'
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
    AND STRFTIME('%Y-%m-%d', 订单接收时间) = '{today_str}'
"""

total_order_sql_xiamen = f"""
SELECT
    COUNT(物流出库订单id) AS 销售条目数
FROM 
    销售表
WHERE 
    细单状态 = '已出库'
    AND 货主id IN {xiamen_owners}
    AND STRFTIME('%Y-%m-%d', 订单接收时间) = '{today_str}'
"""

total_order_sql_third_party = f"""
SELECT
    COUNT(物流出库订单id) AS 销售条目数
FROM 
    销售表
WHERE 
    细单状态 = '已出库'
    AND 货主id IN {third_party_owners}
    AND STRFTIME('%Y-%m-%d', 订单接收时间) = '{today_str}'
"""

total_sub_order_sql = f"""
SELECT
    COUNT(出库订单细单id) AS 细单数
FROM 
    销售表
WHERE 
    细单状态 = '已出库'
    AND STRFTIME('%Y-%m-%d', 订单接收时间) = '{today_str}'
"""

total_sub_order_sql_xiamen = f"""
SELECT
    COUNT(出库订单细单id) AS 细单数
FROM 
    销售表
WHERE 
    细单状态 = '已出库'
    AND 货主id IN {xiamen_owners}
    AND STRFTIME('%Y-%m-%d', 订单接收时间) = '{today_str}'
"""

total_sub_order_sql_third_party = f"""
SELECT
    COUNT(出库订单细单id) AS 细单数
FROM 
    销售表
WHERE 
    细单状态 = '已出库'
    AND 货主id IN {third_party_owners}
    AND STRFTIME('%Y-%m-%d', 订单接收时间) = '{today_str}'
"""


col1, col2 = st.columns([0.3, 0.7])
with col1:
    if st.button('刷新'):
        new_n = np.random.randint(100, 500)
        df_sale_new, df_wave_management_new, df_check_new = generate_data(new_n)
        con.execute("INSERT INTO 销售表 SELECT * FROM df_sale_new")
        con.execute("INSERT INTO 波次管理表 SELECT * FROM df_wave_management_new")
        con.execute("INSERT INTO 出入库明细查询表 SELECT * FROM df_check_new")

with col2:
    st.markdown(f'数据截至时间:{today2}')


col_3, col_4 = st.columns([0.3, 0.7])
with col_3:
    with st.container(border=True):
        owners_type = st.selectbox("请选择货主类型", ["所有货主", "厦门货主", "第三方货主"])

    if owners_type == '所有货主':
        st.metric(label="总销售条目总单数", value=f"{int(get_data(total_order_sql_all)['销售条目数']):,}")
        st.metric(label='总销售条目细单数', value=f"{int(get_data(total_sub_order_sql)['细单数']):,}")
        style_metric_cards()
    elif owners_type == "厦门货主":
        st.metric(label="总销售条目总单数", value=f"{int(get_data(total_order_sql_xiamen)['销售条目数']):,}")
        st.metric(label='总销售条目细单数', value=f"{int(get_data(total_sub_order_sql_xiamen)['细单数']):,}")
        style_metric_cards()
    elif owners_type == "第三方货主":
        st.metric(label="总销售条目总单数", value=f"{int(get_data(total_order_sql_third_party)['销售条目数']):,}")
        st.metric(label='总销售条目细单数', value=f"{int(get_data(total_sub_order_sql_third_party)['细单数']):,}")
        style_metric_cards()

with col_4:
    if owners_type == "所有货主":
        st.dataframe(get_data(order_num_by_owner_sql_all))
    elif owners_type == "厦门货主":
        st.dataframe(get_data(order_num_by_owner_sql_xiamen))
    elif owners_type == "第三方货主":
        st.dataframe(get_data(order_num_by_owner_sql_third_party))


wave_num_sql = f"""
SELECT
    COUNT(波次id) AS 已发波数
FROM 
    波次管理表
WHERE 
    执行状态 != '初始'
    AND STRFTIME('%Y-%m-%d', 执行时间) = '{today_str}'
"""

unsent_wave_num_sql = f"""
SELECT
    COUNT(波次id) AS 未发波数
FROM 
    波次管理表
WHERE 
    执行状态 = '初始'
    AND STRFTIME('%Y-%m-%d', 执行时间) = '{today_str}'
"""

col5, col6 = st.columns(2)
with col5:
    st.metric(label="已发波数", value=f"{int(get_data(wave_num_sql)['已发波数']):,}")
    style_metric_cards()

with col6:
    st.metric(label="未发波数", value=f"{int(get_data(unsent_wave_num_sql)['未发波数']):,}")
    style_metric_cards()

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
    AND STRFTIME('%Y-%m-%d', 任务完成时间) = '{today_str}'
GROUP BY
    库区名称
"""

classification_num_wait_sql = f"""
SELECT
    库区名称,
    COUNT(出入库id) AS 待分拣数
FROM
    出入库明细查询表
WHERE
    任务状态id = 2
    AND 任务完成时间 IS NULL
GROUP BY
    库区名称
"""

df_classification_stat = get_data(classification_num_sql)
df_classification_wait_stat = get_data(classification_num_wait_sql)

color_palettes = ["#1F77B4", "#FF7F0E", "#2CA02C"]

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

with tab1:
    sub_c11, sub_c12, sub_c13, sub_c14 = st.columns(4)
    sub_c11.metric(label="自动库已分拣数", value=f"{int(df_classification_stat.loc[df_classification_stat['库区名称'] == '自动库', '已分拣数']):,}")    
    sub_c12.metric(label="分拣速度(件/小时)", value=f"{round(int(df_classification_stat.loc[df_classification_stat['库区名称'] == '自动库', '已分拣数'])/10):,}")
    sub_c12.metric(label="分拣人员数", value=f"{int(df_classification_stat.loc[df_classification_stat['库区名称'] == '自动库', '分拣人数']):,}")
    sub_c11.metric(label="待分拣数", value=f"{int(df_classification_wait_stat.loc[df_classification_wait_stat['库区名称'] == '自动库', '待分拣数']):,}")    
    sub_c13.metric(label="复核数量", value=f"{int(df_classification_stat.loc[df_classification_stat['库区名称'] == '自动库', '复核数量']):,}")
    sub_c13.metric(label="复核速度(件/小时)", value=f"{round(int(df_classification_stat.loc[df_classification_stat['库区名称'] == '自动库', '复核数量'])/10):,}")
    sub_c14.metric(label="复核人员数", value=f"{int(df_classification_stat.loc[df_classification_stat['库区名称'] == '自动库', '复核人数']):,}")
    sub_c14.metric(label="复核速度(件/人)", value=f"{round(int(df_classification_stat.loc[df_classification_stat['库区名称'] == '自动库', '复核数量'])/int(df_classification_stat.loc[df_classification_stat['库区名称'] == '自动库', '复核人数'])):,}")
    style_metric_cards()

with tab2:
    sub_c21, sub_c22, sub_c23, sub_c24 = st.columns(4)
    sub_c21.metric(label="平库已分拣数", value=f"{int(df_classification_stat.loc[df_classification_stat['库区名称'] == '平库', '已分拣数']):,}")    
    sub_c22.metric(label="分拣速度(件/小时)", value=f"{round(int(df_classification_stat.loc[df_classification_stat['库区名称'] == '平库', '已分拣数'])/10):,}")
    sub_c22.metric(label="分拣人员数", value=f"{int(df_classification_stat.loc[df_classification_stat['库区名称'] == '平库', '分拣人数']):,}")    
    sub_c21.metric(label="待分拣数", value=f"{int(df_classification_wait_stat.loc[df_classification_wait_stat['库区名称'] == '平库', '待分拣数']):,}")
    sub_c23.metric(label="复核数量", value=f"{int(df_classification_stat.loc[df_classification_stat['库区名称'] == '平库', '复核数量']):,}")
    sub_c23.metric(label="复核速度(件/小时)", value=f"{round(int(df_classification_stat.loc[df_classification_stat['库区名称'] == '平库', '复核数量'])/10):,}")
    sub_c24.metric(label="复核人员数", value=f"{int(df_classification_stat.loc[df_classification_stat['库区名称'] == '平库', '复核人数']):,}")   
    sub_c24.metric(label="复核速度(件/人)", value=f"{round(int(df_classification_stat.loc[df_classification_stat['库区名称'] == '平库', '复核数量'])/int(df_classification_stat.loc[df_classification_stat['库区名称'] == '平库', '复核人数'])):,}")
    style_metric_cards()

with tab3:
    sub_c31, sub_c32, sub_c33, sub_c34 = st.columns(4)
    sub_c31.metric(label="冷库已分拣数", value=f"{int(df_classification_stat.loc[df_classification_stat['库区名称'] == '冷库', '已分拣数']):,}")    
    sub_c32.metric(label="分拣速度(件/小时)", value=f"{round(int(df_classification_stat.loc[df_classification_stat['库区名称'] == '冷库', '已分拣数'])/23):,}")
    sub_c32.metric(label="分拣人员数", value=f"{int(df_classification_stat.loc[df_classification_stat['库区名称'] == '冷库', '分拣人数']):,}")    
    sub_c31.metric(label="待分拣数", value=f"{int(df_classification_wait_stat.loc[df_classification_wait_stat['库区名称'] == '冷库', '待分拣数']):,}")
    sub_c33.metric(label="复核数量", value=f"{int(df_classification_stat.loc[df_classification_stat['库区名称'] == '冷库', '复核数量']):,}")
    sub_c33.metric(label="复核速度(件/小时)", value=f"{round(int(df_classification_stat.loc[df_classification_stat['库区名称'] == '冷库', '复核数量'])/10):,}")
    sub_c34.metric(label="复核人员数", value=f"{int(df_classification_stat.loc[df_classification_stat['库区名称'] == '冷库', '复核人数']):,}")   
    sub_c34.metric(label="复核速度(件/人)", value=f"{round(int(df_classification_stat.loc[df_classification_stat['库区名称'] == '冷库', '复核数量'])/int(df_classification_stat.loc[df_classification_stat['库区名称'] == '冷库', '复核人数'])):,}")
    style_metric_cards()

with tab4:
    sub_c41, sub_c42, sub_c43, sub_c44 = st.columns(4)
    sub_c41.metric(label="中药库已分拣数", value=f"{int(df_classification_stat.loc[df_classification_stat['库区名称'] == '中药库', '已分拣数']):,}")    
    sub_c42.metric(label="分拣速度(件/小时)", value=f"{round(int(df_classification_stat.loc[df_classification_stat['库区名称'] == '中药库', '已分拣数'])/10):,}")
    sub_c42.metric(label="分拣人员数", value=f"{int(df_classification_stat.loc[df_classification_stat['库区名称'] == '中药库', '分拣人数']):,}")    
    sub_c41.metric(label="待分拣数", value=f"{int(df_classification_wait_stat.loc[df_classification_wait_stat['库区名称'] == '中药库', '待分拣数']):,}")
    sub_c43.metric(label="复核数量", value=f"{int(df_classification_stat.loc[df_classification_stat['库区名称'] == '中药库', '复核数量']):,}")
    sub_c43.metric(label="复核速度(件/小时)", value=f"{round(int(df_classification_stat.loc[df_classification_stat['库区名称'] == '中药库', '复核数量'])/10):,}")
    sub_c44.metric(label="复核人员数", value=f"{int(df_classification_stat.loc[df_classification_stat['库区名称'] == '中药库', '复核人数']):,}")   
    sub_c44.metric(label="复核速度(件/人)", value=f"{round(int(df_classification_stat.loc[df_classification_stat['库区名称'] == '中药库', '复核数量'])/int(df_classification_stat.loc[df_classification_stat['库区名称'] == '中药库', '复核人数'])):,}")
    style_metric_cards()

with tab5:
    sub_c51, sub_c52, sub_c53, sub_c54 = st.columns(4)
    sub_c51.metric(label="第三方已分拣数", value=f"{int(df_classification_stat.loc[df_classification_stat['库区名称'] == '第三方', '已分拣数']):,}")    
    sub_c52.metric(label="分拣速度(件/小时)", value=f"{round(int(df_classification_stat.loc[df_classification_stat['库区名称'] == '第三方', '已分拣数'])/10):,}")
    sub_c52.metric(label="分拣人员数", value=f"{int(df_classification_stat.loc[df_classification_stat['库区名称'] == '第三方', '分拣人数']):,}")    
    sub_c51.metric(label="待分拣数", value=f"{int(df_classification_wait_stat.loc[df_classification_wait_stat['库区名称'] == '第三方', '待分拣数']):,}")
    sub_c53.metric(label="复核数量", value=f"{int(df_classification_stat.loc[df_classification_stat['库区名称'] == '第三方', '复核数量']):,}")
    sub_c53.metric(label="复核速度(件/小时)", value=f"{round(int(df_classification_stat.loc[df_classification_stat['库区名称'] == '第三方', '复核数量'])/10):,}")
    sub_c54.metric(label="复核人员数", value=f"{int(df_classification_stat.loc[df_classification_stat['库区名称'] == '第三方', '复核人数']):,}")   
    sub_c54.metric(label="复核速度(件/人)", value=f"{round(int(df_classification_stat.loc[df_classification_stat['库区名称'] == '第三方', '复核数量'])/int(df_classification_stat.loc[df_classification_stat['库区名称'] == '第三方', '复核人数'])):,}")
    style_metric_cards()

