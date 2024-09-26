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

    return random_times

# 设置随机种子
np.random.seed(42)

# 设置数据量
n = 100000


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
    order_dates = generate_random_datetime_in_shifts(n)

    random_purchase_status_ids = np.random.choice(purchase_status_options, n)
    purchase_status = [purchase_status_names[status] for status in random_purchase_status_ids]

    random_business_type_ids = np.random.choice(business_type_options, n)
    business_type = [business_type_names[type_id] for type_id in random_business_type_ids]

    random_goods_owner_ids = np.random.choice(goods_owner_ids, n)
    goods_owner_names = [goods_owner_dict[goods_id] for goods_id in random_goods_owner_ids]

    # 创建采购表
    df_purchase = pd.DataFrame({
        '制单日期': generate_random_datetime_in_shifts(n),
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
    order_dates = generate_random_datetime_in_shifts(n)
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
        '验收时间': generate_random_datetime_in_shifts(n),
        '验收员': [f"验收员_{i}" for i in np.random.randint(1, 100, n)],
        '电子监管码': np.random.randint(10000, 99999, n),
        '货品数量': goods_qty,
        '整件数量': whole_quantities,
        '散件数量': scatter_quantities,
        '包装单位ID': np.random.choice([1, 2, 3], n),
        '包装大小': package_size,
        '拟合格源入库订单id': np.arange(1, n + 1),
        '拟合格源出入库id': np.arange(1, n + 1),
        '拟合格状态结束时间': generate_random_datetime_in_shifts(n),
        '拟合格单据状态id': random_fake_complete,
        '拟合格单据状态名称': [fake_complete_names[status] for status in random_fake_complete]
    })

    return df_inspection


@st.cache_data
def generate_shelving_data(n):
    in_out_type_options = [0, 1]  # 0: 上架 (即入库), 1: 下架 (即出库)
    in_out_type_names = {0: "上架 (即入库)", 1: "下架 (即出库)"}
    task_status_options = [1, 2, 3]  # 1: 不发送, 2: 可发送 (待上架), 3: 执行完毕 (上架完成)
    task_status_names = {1: "不发送", 2: "可发送 (待上架)", 3: "执行完毕 (上架完成)"}
    zone_names = ["药业食品散件分区", "药业口服注射散件分区", "药业外用散件分区", "药业局部用药分区", "药业配件分区"]
    storage_names = ["药业散件库区", "大药房中药库区", "口服药品库区", "注射用药库区"]

    completion_dates = generate_random_datetime_in_shifts(n)  # 任务完成时间
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

df_purchase = generate_purchase_data(n)
df_receipt = generate_receipt_data(n)
df_inspection = generate_inspection_data(n)
df_shelving = generate_shelving_data(100000)

con = duckdb.connect()  # 创建 DuckDB 连接
con.execute("CREATE TABLE 采购表 AS SELECT * FROM df_purchase")
con.execute("CREATE TABLE 收货表 AS SELECT * FROM df_receipt")
con.execute("CREATE TABLE 验收表 AS SELECT * FROM df_inspection")
con.execute("CREATE TABLE 上架表 AS SELECT * FROM df_shelving")

def get_data(sql_query):
    return con.query(sql_query).df()

yesterday = datetime.now() - timedelta(days=1)

yesterday_str = yesterday.strftime('%Y-%m')

today = datetime.now()
today2 = today.strftime('%Y-%m-%d %H:%M:%S')
today_str = today.strftime('%Y-%m-%d')

# 昨日待验条目数
yesterday_order_num_sql = f"""
SELECT 
    COUNT(采购表.入库订单id) AS 昨日待验条目数
FROM 
    验收表 join 采购表 ON 验收表.入库细单id = 采购表.入库细单id
WHERE 
    收货状态id = 1 
    AND (单据状态id = 1 OR (单据状态id = 2 AND 验收标志id = 3))
    AND 业务类型id IN (1,8,23,42,22,18)
    AND STRFTIME('%Y-%m', 制单日期) = {yesterday_str}
"""

# 昨日验收条目数
yesterday_order_num_finished_sql = f"""
SELECT 
    COUNT(收货明细id) AS 昨日验收条目数
FROM 
    验收表
WHERE 
    验收标志id IN (2,3)
    AND 货品状态 != '无实物合格'
    AND (STRFTIME('%Y-%m', 验收时间) = '{yesterday_str}' OR STRFTIME('%Y-%m', 拟合格状态结束时间) = '{yesterday_str}')
"""

# 今天待验条目数
today_order_num_sql = f"""
SELECT 
    COUNT(采购表.入库订单id) AS 今日待验条目数
FROM 
    验收表 join 采购表 ON 验收表.入库细单id = 采购表.入库细单id
WHERE 
    收货状态id = 1 
    AND (单据状态id = 1 OR (单据状态id = 2 AND 验收标志id = 3))
    AND 业务类型id IN (1,8,23,42,22,18)
    AND STRFTIME('%Y-%m-%d',制单日期) = '{today_str}'
"""

# 今天已验收条目数
today_order_num_finished_sql = f"""
SELECT 
    COUNT(收货明细id) AS 今日验收条目数
FROM 
    验收表 join 采购表 ON 验收表.入库细单id = 采购表.入库细单id
WHERE
    收货状态id = 1
    AND 验收标志id IN (2,3)
    AND 业务类型id IN (1,8,23,42,22,18)
    AND 货品状态 != '无实物合格'
    AND (STRFTIME('%Y-%m-%d', 验收时间) = '{today_str}' OR STRFTIME('%Y-%m-%d', 拟合格状态结束时间) = '{today_str}')
"""

# 各单据状态值
status_num_sql = f"""
SELECT 
    SUM(CASE WHEN 单据状态id = 1 THEN 1 ELSE 0 END) AS '下单数',
    SUM(CASE WHEN 单据状态id = 2 AND 验收标志id = 3 THEN 1 ELSE 0 END) AS '处理中且拟合格数'
FROM 
    验收表 
JOIN 
    采购表 ON 验收表.入库细单id = 采购表.入库细单id
WHERE 
    收货状态id = 1 
    AND 业务类型id IN (1, 8, 23, 42, 22, 18)
    AND STRFTIME('%Y-%m-%d', 制单日期) = '{today_str}'
"""

# 销退条目数
xt_num_sql = f"""
SELECT 
    COUNT(验收表.入库细单id) AS 今日销退条目数
FROM 
    验收表 join 采购表 ON 验收表.入库细单id = 采购表.入库细单id
WHERE 
    收货状态id = 1 
    AND (单据状态id = 1 OR (单据状态id = 2 AND 验收标志id = 3))
    AND 业务类型id = 7
    AND STRFTIME('%Y-%m-%d', 制单日期) = '{today_str}'
"""

st.title('实时数据展示')
col1, col2 = st.columns([0.3, 0.7])
with col1:
    if st.button('刷新'):
        new_n = np.random.randint(100, 500)
        df_purchase_new = generate_purchase_data(new_n)
        df_receipt_new = generate_receipt_data(new_n)
        df_inspection_new = generate_inspection_data(new_n)
        df_shelving_new = generate_shelving_data(new_n)
        con.execute("INSERT INTO 验收表 SELECT * FROM df_inspection_new")
        con.execute("INSERT INTO 采购表 SELECT * FROM df_purchase_new")
        con.execute("INSERT INTO 收货表 SELECT * FROM df_receipt_new")
        con.execute("INSERT INTO 上架表 SELECT * FROM df_shelving_new")

with col2:
    st.markdown(f'数据截止时间:{today2}')

col3, col4 = st.columns([0.5, 0.5])
with col3:
    st.metric(label="今日待验条目数", value=f"{int(get_data(today_order_num_sql)['今日待验条目数']):,}")
    style_metric_cards()
with col4:
    st.markdown('蓝色：下单数       橘色：处理中且拟合格数')
    status_df = get_data(status_num_sql)
    fig, ax = plt.subplots()
    ax.pie(status_df.iloc[0], autopct='%1.1f%%', startangle=90,
           wedgeprops=dict(width=0.3))
    #status_df.columns.tolist()
    centre_circle = plt.Circle((0, 0), 0.70, fc='white')
    fig.gca().add_artist(centre_circle)

    # 确保饼图是圆形的
    ax.axis('equal')

    # 在 Streamlit 中显示图表
    st.pyplot(fig)

col5, col6 = st.columns([0.5, 0.5])
with col5:
    st.metric(label="今日验收条目数", value=f"{int(get_data(today_order_num_finished_sql)['今日验收条目数']):,}")
    style_metric_cards()
with col6:
    st.metric(label="今日销退条目数", value=f"{int(get_data(xt_num_sql)['今日销退条目数']):,}")
    style_metric_cards()

st.markdown("---")

today_shelving_data_sql = f"""
SELECT 
    COUNT(出入库ID) AS 今日待上架条目数
FROM
    上架表
WHERE
    任务状态ID = 2 
    AND STRFTIME('%Y-%m-%d', 任务完成时间) = '{today_str}'
"""

today_shelving_data_completed_sql = f"""
SELECT 
    COUNT(出入库ID) AS 今日已上架条目数
FROM
    上架表
WHERE
    任务状态ID = 3 
    AND STRFTIME('%Y-%m-%d', 任务完成时间) = '{today_str}'
"""

col7, col8 = st.columns([0.5, 0.5])
with col7:
    st.metric(label="今日待上架条目数", value=f"{int(get_data(today_shelving_data_sql)['今日待上架条目数']):,}")

with col8:
    st.metric(label="今日已上架条目数", value=f"{int(get_data(today_shelving_data_completed_sql)['今日已上架条目数']):,}")

#st.dataframe(get_data(status_num_sql))