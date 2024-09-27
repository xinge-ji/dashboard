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


# 设置随机种子
np.random.seed(42)

# 设置数据量
n = 1000000


# 采购表字段
@st.cache_data
def generate_purchase_data(n):
    purchase_status_options = [0, 1, 2, 3, 4]
    purchase_status_names = {0: "取消", 1: "下单", 2: "处理中", 3: "完成", 4: "挂起"}

    business_type_options = [1, 7, 8, 18, 22, 23, 25, 42, 71, 75]
    business_type_names = {1: "进货", 7: '销退', 8: "销售", 18: '移库出', 22: '报溢', 23: "赠品入库", 25: '赠品出库', 42: "收配退",
                           71: '产成品入库', 75: "退料入库"}

    logistics_centers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

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

    # 随机生成日期
    start_date = datetime(2023, 1, 1)
    end_date = datetime.today()
    order_dates = generate_random_datetime_in_shifts(n, start_date, end_date)

    random_purchase_status_ids = np.random.choice(purchase_status_options, n)
    purchase_status = [purchase_status_names[status] for status in random_purchase_status_ids]

    random_business_type_ids = np.random.choice(business_type_options, n)
    business_type = [business_type_names[type_id] for type_id in random_business_type_ids]

    random_goods_owner_ids = np.random.choice(goods_owner_ids, n)
    goods_owner_names = [goods_owner_dict[goods_id] for goods_id in random_goods_owner_ids]

    # 创建采购表
    df_purchase = pd.DataFrame({
        '制单日期': order_dates,
        '采购员': [f"采购员_{i}" for i in np.random.randint(1, 10, n)],
        '入库订单id': np.arange(1, n + 1),
        '物流中心id': np.random.choice(logistics_centers, n),
        '采购总单状态id': random_purchase_status_ids,
        '采购总单状态名称': purchase_status,
        '业务类型id': random_business_type_ids,
        '业务类型名称': business_type,
        '入库细单id': np.arange(1, n + 1),
        '货主id': random_goods_owner_ids,
        '货主名称': goods_owner_names
    })

    return df_purchase


@st.cache_data
def generate_receipt_data(n):
    transport_conditions = ["常温运输", "冷藏运输", "冷冻运输", "恒温运输"]
    order_quantities = np.random.randint(500, 10000, n)
    received_quantities = [np.random.randint(10, order_qty + 1) for order_qty in order_quantities]
    start_date = datetime(2023, 1, 1)
    end_date = datetime.today()
    order_dates = generate_random_datetime_in_shifts(n, start_date, end_date)
    # 创建收货表
    df_receipt = pd.DataFrame({
        '入库细单id': np.arange(1, n + 1),
        '入库订单id': np.arange(1, n + 1),
        '收货人id': np.random.randint(1, 51, n),
        '收货人': [f"收货人_{i}" for i in np.random.randint(1, 51, n)],
        '原单数量': order_quantities,
        '收货数量': received_quantities,
        '收货日期': order_dates,
        '运输条件': np.random.choice(transport_conditions, n)
    })

    return df_receipt


@st.cache_data
def generate_inspection_data(n):
    start_date = datetime(2023, 1, 1)
    end_date = datetime.today()
    document_status_options = [0, 1, 2, 3, 4, 5]
    document_status_names = {0: "取消", 1: "下单", 2: "处理中", 3: "完成", 4: "挂起", 5: "上架完成"}
    random_document_status_ids = np.random.choice(document_status_options, n)
    receipt_status_options = [0, 1]
    receipt_status_names = {0: "未收货", 1: "收货完成"}
    random_receipt_status_ids = np.random.choice(receipt_status_options, n)
    # 细单状态
    xd_status_options = [1, 2, 3, 4]
    xd_status_names = {1: '临时', 2: '收货完成', 3: '准备上架', 4: '上架完成'}
    random_xd_status = np.random.choice(xd_status_options, n)
    # 验收标志
    receive_label_options = [1, 2, 3]
    receive_label_names = {1: '未验收', 2: '已验收', 3: '拟合格'}
    random_receive_label = np.random.choice(receive_label_options, n)
    # 货品数量
    whole_quantities = np.random.randint(1, 50, n)
    scatter_quantities = np.random.randint(1, 500, n)
    package_size = np.random.choice([99, 999, 9999], n)
    goods_qty = whole_quantities * package_size + scatter_quantities
    # 拟合格
    fake_complete_options = [0, 1, 2, 3, 4]  # 0: 作废, 1: 临时, 2: 确认, 3: 完成, 4: 待处理
    fake_complete_names = {0: "作废", 1: "临时", 2: "确认", 3: "完成", 4: "待处理"}
    random_fake_complete = np.random.choice(fake_complete_options, n)

    # 创建验收表
    df_inspection = pd.DataFrame({
        '收货明细id': np.arange(1, n + 1),
        '入库细单id': np.arange(1, n + 1),
        '单据状态id': random_document_status_ids,
        '单据状态名称': [document_status_names[status] for status in random_document_status_ids],
        '收货状态id': random_receipt_status_ids,
        '收货状态名称': [receipt_status_names[status] for status in random_receipt_status_ids],
        '细单状态id': random_xd_status,
        '细单状态名称': [xd_status_names[status] for status in random_xd_status],
        '验收标志id': random_receive_label,
        '验收标志名称': [receive_label_names[label] for label in random_receive_label],
        '货品状态': np.random.choice(['不合格', '合格', '无实物合格', '拟合格', '业务停销', '到货不足', '质量停销'], n),
        '验收时间': generate_random_datetime_in_shifts(n, start_date, end_date),
        '验收员': [f"验收员_{i}" for i in np.random.randint(1, 100, n)],
        '电子监管码': np.random.randint(10000, 99999, n),
        '货品数量': goods_qty,
        '整件数量': whole_quantities,
        '散件数量': scatter_quantities,
        '包装单位ID': np.random.choice([1, 2, 3], n),
        '包装大小': package_size,
        '拟合格源入库订单id': np.arange(1, n + 1),
        '拟合格源出入库id': np.arange(1, n + 1),
        '拟合格状态结束时间': generate_random_datetime_in_shifts(n, start_date, end_date),
        '拟合格单据状态id': random_fake_complete,
        '拟合格单据状态名称': [fake_complete_names[status] for status in random_fake_complete]
    })

    return df_inspection


df_purchase = generate_purchase_data(n)
df_receipt = generate_receipt_data(n)
df_inspection = generate_inspection_data(n)

# 连接 DuckDB，并将生成的数据导入到 DuckDB 中
con = duckdb.connect()  # 创建 DuckDB 连接
con.execute("CREATE TABLE 采购表 AS SELECT * FROM df_purchase")  # 将 DataFrame 数据导入到 DuckDB
con.execute("CREATE TABLE 收货表 AS SELECT * FROM df_receipt")
con.execute("CREATE TABLE 验收表 AS SELECT * FROM df_inspection")


# SQL 查询函数
def get_data(sql_query):
    return con.query(sql_query).df()


st.title("验收板块展示")
current_year = datetime.now().year
col1, col2 = st.columns([0.3, 0.7])
with col1:
    with st.container(border=True):
        year = st.selectbox("选择年份", list(range(2023, current_year + 1)))
        month = st.selectbox("选择月份", list(range(1, 13)), format_func=lambda x: f"{x}月")

selected_date = f"{year}-{str(month).zfill(2)}"
# 查询按钮
# if st.button("查询数据"):
# query = f"SELECT * FROM 采购表 WHERE 采购总单状态id = {status_id} LIMIT 100"
# result_df = con.query(query).df()

# 展示查询结果
# st.write(f"查询结果：采购状态为 '{status_filter}' 的记录")
# st.dataframe(result_df)

# 入库细单数
xd_num_sql = f"""
SELECT 
    COUNT(验收表.入库细单id) AS 入库细单数
FROM 
    验收表 join 采购表 ON 验收表.入库细单id = 采购表.入库细单id
WHERE 
    收货状态id = 1 
    AND (单据状态id = 1 OR (单据状态id = 2 AND 验收标志id = 3))
    AND 业务类型id IN (1,8,23,42,22,18)
    AND STRFTIME('%Y-%m', 制单日期) = '{selected_date}'
"""
# 入库订单数
order_num_sql = f"""
SELECT 
    COUNT(采购表.入库订单id) AS 入库订单数
FROM 
    验收表 join 采购表 ON 验收表.入库细单id = 采购表.入库细单id
WHERE 
    收货状态id = 1 
    AND (单据状态id = 1 OR (单据状态id = 2 AND 验收标志id = 3))
    AND 业务类型id IN (1,8,23,42,22,18)
    AND STRFTIME('%Y-%m', 制单日期) = '{selected_date}'
"""

# 各货主到货待验条目数
order_num_by_owner_sql_all = f"""
SELECT
    采购表.货主id,
    采购表.货主名称,
    COUNT(采购表.入库订单id) AS 入库订单数
FROM 
    验收表 join 采购表 ON 验收表.入库细单id = 采购表.入库细单id
WHERE 
    收货状态id = 1 
    AND (单据状态id = 1 OR (单据状态id = 2 AND 验收标志id = 3))
    AND 业务类型id IN (1,8,23,42,22,18)
    AND STRFTIME('%Y-%m', 制单日期) = '{selected_date}'
GROUP BY 采购表.货主id, 采购表.货主名称
ORDER BY 入库订单数 desc
"""
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
third_party_owners = [k for k in goods_owner_dict if str(k).startswith('91')]
xiamen_owners = [k for k in goods_owner_dict if k not in third_party_owners]

order_num_by_owner_sql_xiamen = f"""
SELECT
    采购表.货主id,
    采购表.货主名称,
    COUNT(采购表.入库订单id) AS 入库订单数
FROM 
    验收表 join 采购表 ON 验收表.入库细单id = 采购表.入库细单id
WHERE 
    收货状态id = 1 
    AND (单据状态id = 1 OR (单据状态id = 2 AND 验收标志id = 3))
    AND 业务类型id IN (1,8,23,42,22,18)
    AND 采购表.货主id IN {xiamen_owners}
    AND STRFTIME('%Y-%m', 制单日期) = '{selected_date}'
GROUP BY 采购表.货主id, 采购表.货主名称
ORDER BY 入库订单数 desc
"""

order_num_by_owner_sql_third_party = f"""
SELECT
    采购表.货主id,
    采购表.货主名称,
    COUNT(采购表.入库订单id) AS 入库订单数
FROM 
    验收表 join 采购表 ON 验收表.入库细单id = 采购表.入库细单id
WHERE 
    收货状态id = 1 
    AND (单据状态id = 1 OR (单据状态id = 2 AND 验收标志id = 3))
    AND 业务类型id IN (1,8,23,42,22,18)
    AND 采购表.货主id IN {third_party_owners}
    AND STRFTIME('%Y-%m', 制单日期) = '{selected_date}'
GROUP BY 采购表.货主id, 采购表.货主名称
ORDER BY 入库订单数 desc
"""
# 待验电子监管码数
encode_num_sql = f"""
SELECT
    COUNT(DISTINCT 电子监管码) AS '扫码数'
FROM 验收表
WHERE 
    (单据状态id = 1 OR (单据状态id = 2 AND 验收标志id = 3)) AND (电子监管码 != 0)
"""

# 已验收数
inspection_finished_num_sql = f"""
SELECT 
    COUNT(验收表.入库细单id) AS 已验收数
FROM 
    验收表 join 采购表 ON 验收表.入库细单id = 采购表.入库细单id
WHERE 
    收货状态id = 1 
    AND 验收标志id != 1
    AND 业务类型id IN (1,8,23,42,22,18)
    AND 货品状态 != '无实物合格'
    AND (STRFTIME('%Y-%m', 验收时间) = '{selected_date}' OR STRFTIME('%Y-%m', 拟合格状态结束时间) = '{selected_date}')
"""

# 电子监管码数

# 销退条目数
xt_num_sql = f"""
SELECT 
    COUNT(验收表.入库细单id) AS 销退条目数
FROM 
    验收表 join 采购表 ON 验收表.入库细单id = 采购表.入库细单id
WHERE 
    收货状态id = 1 
    AND (单据状态id = 1 OR (单据状态id = 2 AND 验收标志id = 3))
    AND 业务类型id = 7
    AND STRFTIME('%Y-%m', 制单日期) = '{selected_date}'
"""

with col2:
    #st.metric(label="入库细单数", value=f"{int(get_data(xd_num_sql)['入库细单数']):,}")
    st.metric(label="入库订单数", value=f"{int(get_data(order_num_sql)['入库订单数']):,}")
    st.metric(label="销退条目数", value=f"{int(get_data(xt_num_sql)['销退条目数']):,}")
    st.metric(label="已验收数", value=f"{int(get_data(inspection_finished_num_sql)['已验收数']):,}")
    st.metric(label="电子监管码数", value=f"{int(get_data(encode_num_sql)['扫码数']):,}")
    style_metric_cards()
# 验收人员数
#st.metric(label="入库细单数", value=f"{int(df_stat['入库细单数']):,}")
#st.metric(label="入库订单数", value=f"{int(get_data(order_num_sql)['入库订单数']):,}")
#st.metric(label="已验收数", value=f"{int(get_data(inspection_finished_num_sql)['已验收数']):,}")
#st.metric(label="电子监管码数", value=f"{int(get_data(encode_num_sql)['扫码数']):,}")
#style_metric_cards()

col_3, col_4, col_5 = st.columns([0.3, 0.4, 0.3])
with col_3:
    with st.container(border=True):
        owners_type = st.selectbox("请选择货主类型", ["所有货主", "厦门货主", "第三方货主"])

with col_4:
    if owners_type == "所有货主":
        st.dataframe(get_data(order_num_by_owner_sql_all))
    elif owners_type == "厦门货主":
        st.dataframe(get_data(order_num_by_owner_sql_xiamen))
    elif owners_type == "第三方货主":
        st.dataframe(get_data(order_num_by_owner_sql_third_party))

with col_5:
    if owners_type == "所有货主":
        data = get_data(order_num_by_owner_sql_all)
        fig = px.bar(x=data['货主名称'], y=data['入库订单数'], labels={'x': '货主名称', 'y': '入库订单数'})
        st.plotly_chart(fig)
    elif owners_type == "厦门货主":
        data = get_data(order_num_by_owner_sql_xiamen)
        fig = px.bar(x=data['货主名称'], y=data['入库订单数'], labels={'x': '货主名称', 'y': '入库订单数'})
        st.plotly_chart(fig)
    elif owners_type == "第三方货主":
        data = get_data(order_num_by_owner_sql_third_party)
        fig = px.bar(x=data['货主名称'], y=data['入库订单数'], labels={'x': '货主名称', 'y': '入库订单数'})
        st.plotly_chart(fig)



#st.dataframe(get_data(order_num_by_owner_sql))
