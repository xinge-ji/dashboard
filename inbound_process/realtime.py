# %%
import sys
from datetime import datetime, time, timedelta
from pathlib import Path

import altair as alt

# import adbc_driver_flightsql.dbapi as flight_sql
# import adbc_driver_manager
import duckdb
import numpy as np
import pandas as pd
import schedule
import streamlit as st

sys.path.append(str(Path(sys.argv[0]).resolve().parent.parent))
from dashboard.util.util import dataframe_explorer, style_metric_key_cards

# ---------------------------------------------------------------------------------------------------------------------
# 连接数据库 - Doris (实际使用)
# ---------------------------------------------------------------------------------------------------------------------
# %%
# db_uri = "grpc://0.0.0.0:`fe.conf_arrow_flight_sql_port`"
# db_kwargs = {
#     adbc_driver_manager.DatabaseOptions.USERNAME.value: "root",
#     adbc_driver_manager.DatabaseOptions.PASSWORD.value: "",
# }


# @st.cache_resource
# def db_connection():
#     conn = flight_sql.connect(uri=db_uri, db_kwargs=db_kwargs)
#     return conn.cursor()


# cursor = db_connection()


# ---------------------------------------------------------------------------------------------------------------------
# 连接数据库 - duckdb (测试使用)
# ---------------------------------------------------------------------------------------------------------------------
# %%
conn = duckdb.connect()
conn.execute("""
    CREATE TABLE in_data (
        in_id INT,
        create_datetime DATETIME,
        operation_type TEXT,
        order_status TEXT,
        goodsowner_name TEXT,
        purchaser_name VARCHAR,
        receiver_name TEXT,
        receive_id INT,
        gsp_flag TEXT,
        receive_status TEXT,
        inspector_name TEXT,
        receive_datetime DATETIME,
        inspect_datetime DATETIME,
        iwcs_datetime DATETIME,
        inspect_status TEXT,
        goods_status TEXT,
        whole_qty INT,
        scatter_qty INT,
        ecode_flag INT,
        inout_id INT,
        rf_datetime DATETIME,
        rf_flag TEXT,
        section_name TEXT,
        area_name TEXT,
        rfman_name TEXT
    );""")
cursor = conn.cursor()


def generate_data():
    # 检查是否有数据
    table_rows = conn.execute("SELECT COUNT(*), FROM in_data").df().iloc[0, 0]

    if table_rows > 0:
        # 如果已经有数据，新的随机数据从上次最后数据时间开始
        last_data = conn.execute(
            "SELECT MAX(rf_datetime), MAX(in_id), MAX(receive_id), MAX(inout_id) FROM in_data"
        ).df()
        last_rf_datetime, last_in_id, last_receive_id, last_inout_id = (
            last_data.iloc[0, 0],
            last_data.iloc[0, 1],
            last_data.iloc[0, 2],
            last_data.iloc[0, 3],
        )
        last_rf_datetime = pd.to_datetime(last_rf_datetime)

        start_time = last_rf_datetime
        num_in_ids = 100
    else:
        # 如果没有数据，新的随机数据从早上6点开始
        today = datetime.now().date()
        start_time = datetime.combine(today, time(6, 0))
        last_in_id, last_receive_id, last_inout_id = 0, 0, 0
        num_in_ids = 1000

    operation_types = [
        "进货",
        "销退",
        "销售",
        "移库出",
        "报溢",
        "赠品入库",
        "赠品出库",
        "收配退",
        "产成品入库",
        "退料入库",
    ]
    goods_owners = [
        "鹭燕医药股份有限公司",
        "厦门鹭燕医疗器械有限公司",
        "厦门鹭燕大药房有限公司",
        "厦门燕来福制药有限公司",
        "泉州鹭燕大药房有限公司",
        "厦门鹭燕海峡两岸药材贸易有限公司",
        "厦门鹭燕药业有限公司",
        "福建一善药业有限公司",
        "厦门共鑫科技有限公司",
        "福建天泉药业股份有限公司",
        "福建省联康药业连锁有限公司",
    ]
    order_status_types = ["取消", "下单", "处理中", "挂起", "上架完成"]
    purchaser_names = ["梅姬幸", "邵珊佳", "邹标", "巫律业", "严旭钰", "仲翌丁", "郁爱娟", "姬妃迅", "杜远谦", "贺澄瑾"]
    receiver_names = ["宣彤京", "金寒", "邓美桐", "卢亚唯", "陶朔桦", "裴迅昭", "鲍岳", "胡棋翔", "秋国遥", "戚诗"]
    inspector_names = ["董婕雪", "邓迅", "伍情", "焦云", "汤珑旦", "娄非非", "阮崴榕", "娄谦锁", "徐杉珊", "施衡"]
    rfman_names = ["吴超璇", "盛庭风", "房昭中", "武杨红", "嵇晨忆", "崔列灵", "时良", "昌奎献", "郝奇晓", "乌昭"]
    gsp_types = ["未验收", "已验收", "拟合格"]
    receive_status_types = ["未收货", "收货完成"]
    inspect_status_types = ["取消", "下单", "处理中", "挂起", "上架完成"]
    goods_status_types = ["不合格", "合格", "无实物合格", "拟合格", "业务停销", "到货不足", "质量停销"]
    rf_types = ["不发送", "可发送", "执行完毕"]
    section_names = [
        "药业食品散件分区",
        "药业口服注射散件分区",
        "药业外用散件分区",
        "药业食品整件分区",
        "药业口服注射整件分区",
        "药业外用整件分区",
    ]
    area_names = ["药业散件库区", "药业整件库区", "大药房西药库区", "大药房中药库区", "大药房食品库区"]

    data1 = pd.DataFrame(
        {
            "in_id": np.arange(last_in_id + 1, last_in_id + num_in_ids + 1),
            "create_datetime": [
                pd.to_datetime(
                    np.random.randint(
                        (start_time - timedelta(days=5)).timestamp(), (datetime.now() + timedelta(hours=8)).timestamp()
                    ),
                    unit="s",
                )
                for _ in range(num_in_ids)
            ],
            "operation_type": np.random.choice(operation_types, num_in_ids),
            "order_status": np.random.choice(order_status_types, num_in_ids),
            "goodsowner_name": np.random.choice(goods_owners, num_in_ids),
            "purchaser_name": np.random.choice(purchaser_names, num_in_ids),
            "receiver_name": np.random.choice(receiver_names, num_in_ids),
        }
    )

    data2 = {
        "receive_id": [],
        "in_id": [],
        "receive_datetime": [],
        "inspect_datetime": [],
        "iwcs_datetime": [],
    }

    for _, row in data1.iterrows():
        num_receive = np.random.randint(1, 3)
        for _ in range(num_receive):
            last_receive_id += 1
            data2["receive_id"].append(last_receive_id)
            data2["in_id"].append(row["in_id"])
            data2["receive_datetime"].append(
                pd.to_datetime(
                    np.random.randint(
                        (row["create_datetime"]).timestamp(),
                        (datetime.now() + timedelta(hours=8)).timestamp(),
                    ),
                    unit="s",
                )
            )
            data2["inspect_datetime"].append(
                pd.to_datetime(
                    np.random.randint(
                        (data2["receive_datetime"][-1]).timestamp(),
                        (datetime.now() + timedelta(hours=8)).timestamp(),
                    ),
                    unit="s",
                )
            )
            data2["iwcs_datetime"].append(
                pd.to_datetime(
                    np.random.randint(
                        (data2["inspect_datetime"][-1]).timestamp(),
                        (datetime.now() + timedelta(hours=8)).timestamp(),
                    ),
                    unit="s",
                )
            )

    data2 = pd.DataFrame(data2)
    data2["gsp_flag"] = np.random.choice(gsp_types, data2.shape[0])
    data2["receive_status"] = np.random.choice(receive_status_types, data2.shape[0])
    data2["inspector_name"] = np.random.choice(inspector_names, data2.shape[0])
    data2["inspect_status"] = np.random.choice(inspect_status_types, data2.shape[0])
    data2["goods_status"] = np.random.choice(goods_status_types, data2.shape[0])
    data2["whole_qty"] = np.random.randint(1, 100, data2.shape[0])
    data2["scatter_qty"] = np.random.randint(1, 100, data2.shape[0])
    data2["ecode_flag"] = np.random.randint(0, 2, data2.shape[0])

    data3 = {"inout_id": [], "receive_id": [], "rf_datetime": []}

    for _, row in data2.iterrows():
        num_inout_id = np.random.randint(1, 6)
        for _ in range(num_inout_id):
            last_inout_id += 1
            data3["inout_id"].append(last_inout_id)
            data3["receive_id"].append(row["receive_id"])
            data3["rf_datetime"].append(
                pd.to_datetime(
                    np.random.randint(
                        (row["inspect_datetime"]).timestamp(),
                        (datetime.now() + timedelta(hours=8)).timestamp(),
                    ),
                    unit="s",
                )
            )

    data3 = pd.DataFrame(data3)
    data3["rf_flag"] = np.random.choice(rf_types, data3.shape[0])
    data3["section_name"] = np.random.choice(section_names, data3.shape[0])
    data3["area_name"] = np.random.choice(area_names, data3.shape[0])
    data3["rfman_name"] = np.random.choice(rfman_names, data3.shape[0])

    df = data1.merge(data2, how="inner", on="in_id").merge(data3, how="inner", on="receive_id")

    # Insert back into DuckDB
    conn.execute(
        "INSERT INTO in_data SELECT in_id, create_datetime, operation_type, order_status, goodsowner_name, purchaser_name, receiver_name, receive_id, gsp_flag, receive_status, inspector_name, receive_datetime, inspect_datetime, iwcs_datetime, inspect_status, goods_status, whole_qty, scatter_qty, ecode_flag, inout_id, rf_datetime, rf_flag, section_name, area_name, rfman_name FROM df"
    )


# ---------------------------------------------------------------------------------------------------------------------
# 获取数据
# ---------------------------------------------------------------------------------------------------------------------
# %%


# 未收货订单
@st.cache_data(ttl=500)  # Cache data for 5 minutes
def get_unreceived_order_data():
    sql = """
    SELECT DISTINCT in_id, create_datetime, operation_type, goodsowner_name, purchaser_name, DATE_DIFF('day', create_datetime, current_date) AS days_since_creation
    FROM in_data
    WHERE (receive_status is NULL OR receive_status = '未收货')
    AND order_status IN ('下单', '处理中')
    GROUP BY in_id, create_datetime, operation_type, goodsowner_name, purchaser_name, DATE_DIFF('day', create_datetime, current_date)
    """
    cursor.execute(sql)
    return cursor.fetch_df()


# 今日收货订单
@st.cache_data(ttl=500)  # Cache data for 5 minutes
def get_received_order_data():
    sql = """
    SELECT DISTINCT in_id, receive_datetime, operation_type, goodsowner_name, purchaser_name, receiver_name
    FROM in_data
    WHERE receive_status = '收货完成' AND receive_datetime >= TODAY()
    GROUP BY in_id, receive_datetime, operation_type, goodsowner_name, purchaser_name, receiver_name
    """
    cursor.execute(sql)
    return cursor.fetch_df()


# 待验收明细
@st.cache_data(ttl=500)  # Cache data for 5 minutes
def get_uninspect_data():
    sql = """
    SELECT DISTINCT receive_id, in_id, gsp_flag, receive_datetime 
    FROM in_data
    WHERE inspect_status in ('下单', '处理中') AND goods_status != '无实物合格'
    GROUP BY receive_id, in_id, gsp_flag, receive_datetime 
    """
    cursor.execute(sql)
    return cursor.fetch_df()


# 今日验收明细
@st.cache_data(ttl=500)  # Cache data for 5 minutes
def get_inspected_data():
    sql = """
    SELECT receive_id, gsp_flag, inspect_datetime, date_diff('minute', receive_datetime, inspect_datetime) as inspect_duration_min, inspector_name, goods_status, whole_qty, scatter_qty, ecode_flag, 'inspect' as type
    FROM in_data
    WHERE gsp_flag in ('已验收', '拟合格') AND goods_status != '无实物合格' AND inspect_datetime >= TODAY()
    GROUP BY receive_id, gsp_flag, receive_datetime, inspect_datetime, inspector_name, goods_status, whole_qty, scatter_qty, ecode_flag
    UNION ALL
    SELECT receive_id, gsp_flag, iwcs_datetime as inspect_datetime, date_diff('minute', receive_datetime, iwcs_datetime) as inspect_duration_min, inspector_name, goods_status, whole_qty, scatter_qty, ecode_flag, 'iwcs' as type
    FROM in_data
    WHERE gsp_flag in ('已验收', '拟合格') AND goods_status != '无实物合格' AND iwcs_datetime >= TODAY()
    GROUP BY receive_id, gsp_flag, receive_datetime, iwcs_datetime, inspector_name, goods_status, whole_qty, scatter_qty, ecode_flag
    """
    cursor.execute(sql)
    return cursor.fetch_df()


# 待上架明细
@st.cache_data(ttl=500)  # Cache data for 5 minutes
def get_unshelf_data():
    sql = """
    SELECT inout_id, rf_flag, section_name, area_name, inspect_datetime
    FROM in_data
    WHERE rf_flag = '可发送'
    """
    cursor.execute(sql)
    return cursor.fetch_df()


# 今日上架明细
@st.cache_data(ttl=500)  # Cache data for 5 minutes
def get_shelved_data():
    sql = """
    SELECT inout_id, rf_flag, section_name, area_name, rfman_name, rf_datetime, date_diff('minute', inspect_datetime, rf_datetime) as shelf_duration_min
    FROM in_data
    WHERE rf_flag = '执行完毕' AND rf_datetime >= TODAY()
    """
    cursor.execute(sql)
    return cursor.fetch_df()


# ---------------------------------------------------------------------------------------------------------------------
# 数据看板
# ---------------------------------------------------------------------------------------------------------------------
# %%
# 外观参数
color_palettes = ["#1F77B4", "#FF7F0E", "#2CA02C"]

# 指标排列
columns_placeholder = st.columns(3)
receive_column = columns_placeholder[0].empty()
inspect_column = columns_placeholder[1].empty()
shelf_column = columns_placeholder[2].empty()


# 收货
def update_receive_metrics():
    unreceived_order_df = get_unreceived_order_data()
    received_order_df = get_received_order_data()
    receive_container = receive_column.container()

    receive_container.container(key="receivemetric1").metric(
        label="总未收货订单数", value=len(unreceived_order_df["in_id"].unique())
    )
    style_metric_key_cards(key="receivemetric1", border_left_color=color_palettes[0])
    receive_container.container(key="receivemetric2").metric(
        label="等待超2天未收货订单数",
        value=len(unreceived_order_df[unreceived_order_df["days_since_creation"] > 2]["in_id"].unique()),
    )
    style_metric_key_cards(key="receivemetric2", border_left_color=color_palettes[0])
    receive_container.container(key="receivemetric3").metric(
        label="今日收货订单数", value=len(received_order_df["in_id"].unique())
    )
    style_metric_key_cards(key="receivemetric3", border_left_color=color_palettes[0])
    tab1, tab2, tab3, tab4 = receive_container.tabs(["分时数量", "货品货主", "收货员", "订单类型"])
    tab1.altair_chart(
        alt.Chart(
            received_order_df.groupby(pd.Grouper(key="receive_datetime", freq="15min"))
            .size()
            .reset_index(name="in_id_count")
        )
        .mark_bar()
        .encode(
            x=alt.X("receive_datetime", title="今日收货时间", axis=alt.Axis(format="%H:%M")),
            y=alt.Y("in_id_count", title="今日收货订单数"),
            color=alt.value(color_palettes[0]),
        ),
        use_container_width=True,
    )
    tab2.altair_chart(
        alt.Chart(
            received_order_df.groupby(pd.Grouper(key="goodsowner_name"))
            .size()
            .reset_index(name="goodsowner_count")
            .sort_values(by="goodsowner_count", ascending=False)
        )
        .mark_bar()
        .encode(
            y=alt.Y("goodsowner_name", sort=alt.SortField(field="goodsowner_count", order="descending"), title=""),
            x=alt.X("goodsowner_count", title="今日收货订单数"),
            color=alt.value(color_palettes[0]),
        )
        .configure_axis(labelLimit=400),
        use_container_width=True,
    )
    tab3.altair_chart(
        alt.Chart(
            received_order_df.groupby(pd.Grouper(key="receiver_name"))
            .size()
            .reset_index(name="receiver_count")
            .sort_values(by="receiver_count", ascending=False)
        )
        .mark_bar()
        .encode(
            y=alt.Y("receiver_name", sort=alt.SortField(field="receiver_count", order="descending"), title=""),
            x=alt.X("receiver_count", title="今日收货订单数"),
            color=alt.value(color_palettes[0]),
        )
        .configure_axis(labelLimit=400),
        use_container_width=True,
    )
    tab4.altair_chart(
        alt.Chart(
            received_order_df.groupby(pd.Grouper(key="operation_type"))
            .size()
            .reset_index(name="operation_count")
            .sort_values(by="operation_count", ascending=False)
        )
        .mark_bar()
        .encode(
            y=alt.Y("operation_type", sort=alt.SortField(field="operation_count", order="descending"), title=""),
            x=alt.X("operation_count", title="今日收货订单数"),
            color=alt.value(color_palettes[0]),
        )
        .configure_axis(labelLimit=400),
        use_container_width=True,
    )


# 验收
def update_inspect_metrics():
    uninspect_order_df = get_uninspect_data()
    inspected_order_df = get_inspected_data()
    inspect_container = inspect_column.container()

    inspect_container.container(key="inspectmetric1").metric(
        label="待验收的收货订单数",
        value=len(uninspect_order_df["in_id"].unique()),
    )
    style_metric_key_cards(key="inspectmetric1", border_left_color=color_palettes[1])
    inspect_container.container(key="inspectmetric2").metric(
        label="待验收的验收明细数",
        value=len(uninspect_order_df["receive_id"].unique()),
        help="一个收货订单可被拆分为多个验货明细。",
    )
    style_metric_key_cards(key="inspectmetric2", border_left_color=color_palettes[1])
    inspect_container.container(key="inspectmetric3").metric(
        label="今日验收的验收明细数",
        value=len(inspected_order_df["receive_id"].unique()),
        help="验收标志转为“已验收”或“拟合格”的明细单，或“拟合格”状态转为“完成”的明细单。去除“无实物收货”单据。",
    )
    style_metric_key_cards(key="inspectmetric3", border_left_color=color_palettes[1])

    tab1, tab2, tab3, tab4, tab5 = inspect_container.tabs(
        ["验收明细数", "整件货物数", "散件货物数", "电子监管码", "收货验货用时"]
    )
    with tab1:
        subtab1, subtab2, subtab3 = tab1.tabs(["分时数量", "验货员", "货品状态"])
        subtab1.altair_chart(
            alt.Chart(
                inspected_order_df.groupby(["gsp_flag", pd.Grouper(key="inspect_datetime", freq="15min")])
                .size()
                .reset_index(name="receive_id_count")
            )
            .mark_bar()
            .encode(
                x=alt.X("inspect_datetime", title="今日验货时间", axis=alt.Axis(format="%H:%M")),
                y=alt.Y("receive_id_count", title="今日验收的验收明细数"),
                color=alt.Color("gsp_flag"),
            ),
            use_container_width=True,
        )
        subtab2.altair_chart(
            alt.Chart(
                inspected_order_df.groupby(["gsp_flag", "inspector_name"])
                .size()
                .reset_index(name="receive_id_count")
                .assign(total_by_inspector=lambda x: x.groupby("inspector_name")["receive_id_count"].transform("sum"))
                .sort_values(by="total_by_inspector", ascending=False)
            )
            .mark_bar()
            .encode(
                y=alt.Y("inspector_name", sort=alt.SortField(field="total_by_inspector", order="descending"), title=""),
                x=alt.X("receive_id_count", title="今日验收的验收明细数"),
                color=alt.Color("gsp_flag"),
            )
            .configure_axis(labelLimit=400),
            use_container_width=True,
        )
        subtab3.altair_chart(
            alt.Chart(
                inspected_order_df.groupby(["gsp_flag", "goods_status"])
                .size()
                .reset_index(name="receive_id_count")
                .assign(total_by_goods_status=lambda x: x.groupby("goods_status")["receive_id_count"].transform("sum"))
                .sort_values(by="total_by_goods_status", ascending=False)
            )
            .mark_bar()
            .encode(
                y=alt.Y(
                    "goods_status", sort=alt.SortField(field="total_by_goods_status", order="descending"), title=""
                ),
                x=alt.X("receive_id_count", title="今日验收的验收明细数"),
                color=alt.Color("gsp_flag"),
            )
            .configure_axis(labelLimit=400),
            use_container_width=True,
        )
    with tab2:
        subtab1, subtab2, subtab3 = tab2.tabs(["分时数量", "验货员", "货品状态"])
        subtab1.altair_chart(
            alt.Chart(
                inspected_order_df.groupby(["gsp_flag", pd.Grouper(key="inspect_datetime", freq="15min")])["whole_qty"]
                .sum()
                .reset_index(name="whole_qty_sum")
            )
            .mark_bar()
            .encode(
                x=alt.X("inspect_datetime", title="今日验货时间", axis=alt.Axis(format="%H:%M")),
                y=alt.Y("whole_qty_sum", title="今日验收的整件货品数"),
                color=alt.Color("gsp_flag"),
            ),
            use_container_width=True,
        )
        subtab2.altair_chart(
            alt.Chart(
                inspected_order_df.groupby(["gsp_flag", "inspector_name"])["whole_qty"]
                .sum()
                .reset_index(name="whole_qty_sum")
                .assign(total_by_inspector=lambda x: x.groupby("inspector_name")["whole_qty_sum"].transform("sum"))
                .sort_values(by="total_by_inspector", ascending=False)
            )
            .mark_bar()
            .encode(
                y=alt.Y("inspector_name", sort=alt.SortField(field="total_by_inspector", order="descending"), title=""),
                x=alt.X("whole_qty_sum", title="今日验收的整件货品数"),
                color=alt.Color("gsp_flag"),
            )
            .configure_axis(labelLimit=400),
            use_container_width=True,
        )
        subtab3.altair_chart(
            alt.Chart(
                inspected_order_df.groupby(["gsp_flag", "goods_status"])["whole_qty"]
                .sum()
                .reset_index(name="whole_qty_sum")
                .assign(total_by_goods_status=lambda x: x.groupby("goods_status")["whole_qty_sum"].transform("sum"))
                .sort_values(by="total_by_goods_status", ascending=False)
            )
            .mark_bar()
            .encode(
                y=alt.Y(
                    "goods_status", sort=alt.SortField(field="total_by_goods_status", order="descending"), title=""
                ),
                x=alt.X("whole_qty_sum", title="今日验收的整件货品数"),
                color=alt.Color("gsp_flag"),
            )
            .configure_axis(labelLimit=400),
            use_container_width=True,
        )
    with tab3:
        subtab1, subtab2, subtab3 = tab3.tabs(["分时数量", "验货员", "货品状态"])
        subtab1.altair_chart(
            alt.Chart(
                inspected_order_df.groupby(["gsp_flag", pd.Grouper(key="inspect_datetime", freq="15min")])[
                    "scatter_qty"
                ]
                .sum()
                .reset_index(name="scatter_qty_sum")
            )
            .mark_bar()
            .encode(
                x=alt.X("inspect_datetime", title="今日验货时间", axis=alt.Axis(format="%H:%M")),
                y=alt.Y("scatter_qty_sum", title="今日验收的散件货品数"),
                color=alt.Color("gsp_flag"),
            ),
            use_container_width=True,
        )
        subtab2.altair_chart(
            alt.Chart(
                inspected_order_df.groupby(["gsp_flag", "inspector_name"])["scatter_qty"]
                .sum()
                .reset_index(name="scatter_qty_sum")
                .assign(total_by_inspector=lambda x: x.groupby("inspector_name")["scatter_qty_sum"].transform("sum"))
                .sort_values(by="total_by_inspector", ascending=False)
            )
            .mark_bar()
            .encode(
                y=alt.Y("inspector_name", sort=alt.SortField(field="total_by_inspector", order="descending"), title=""),
                x=alt.X("scatter_qty_sum", title="今日验收的散件货品数"),
                color=alt.Color("gsp_flag"),
            )
            .configure_axis(labelLimit=400),
            use_container_width=True,
        )
        subtab3.altair_chart(
            alt.Chart(
                inspected_order_df.groupby(["gsp_flag", "goods_status"])["scatter_qty"]
                .sum()
                .reset_index(name="scatter_qty_sum")
                .assign(total_by_goods_status=lambda x: x.groupby("goods_status")["scatter_qty_sum"].transform("sum"))
                .sort_values(by="total_by_goods_status", ascending=False)
            )
            .mark_bar()
            .encode(
                y=alt.Y(
                    "goods_status", sort=alt.SortField(field="total_by_goods_status", order="descending"), title=""
                ),
                x=alt.X("scatter_qty_sum", title="今日验收的散件货品数"),
                color=alt.Color("gsp_flag"),
            )
            .configure_axis(labelLimit=400),
            use_container_width=True,
        )
    with tab4:
        subtab1, subtab2, subtab3 = tab4.tabs(["分时数量", "验货员", "货品状态"])
        subtab1.altair_chart(
            alt.Chart(
                inspected_order_df[(inspected_order_df["ecode_flag"] == 1) & (inspected_order_df["type"] == "inspect")]
                .assign(ecode_qty=lambda x: x["whole_qty"] + x["scatter_qty"])
                .groupby(["gsp_flag", pd.Grouper(key="inspect_datetime", freq="15min")])["ecode_qty"]
                .sum()
                .reset_index(name="ecode_qty_sum")
            )
            .mark_bar()
            .encode(
                x=alt.X("inspect_datetime", title="今日验货时间", axis=alt.Axis(format="%H:%M")),
                y=alt.Y("ecode_qty_sum", title="今日验收的电子监管码数"),
                color=alt.Color("gsp_flag"),
            ),
            use_container_width=True,
        )
        subtab2.altair_chart(
            alt.Chart(
                inspected_order_df[(inspected_order_df["ecode_flag"] == 1) & (inspected_order_df["type"] == "inspect")]
                .assign(ecode_qty=lambda x: x["whole_qty"] + x["scatter_qty"])
                .groupby(["gsp_flag", "inspector_name"])["ecode_qty"]
                .sum()
                .reset_index(name="ecode_qty_sum")
                .assign(total_by_inspector=lambda x: x.groupby("inspector_name")["ecode_qty_sum"].transform("sum"))
                .sort_values(by="total_by_inspector", ascending=False)
            )
            .mark_bar()
            .encode(
                y=alt.Y("inspector_name", sort=alt.SortField(field="total_by_inspector", order="descending"), title=""),
                x=alt.X("ecode_qty_sum", title="今日验收的电子监管码数"),
                color=alt.Color("gsp_flag"),
            )
            .configure_axis(labelLimit=400),
            use_container_width=True,
        )
        subtab3.altair_chart(
            alt.Chart(
                inspected_order_df[(inspected_order_df["ecode_flag"] == 1) & (inspected_order_df["type"] == "inspect")]
                .assign(ecode_qty=lambda x: x["whole_qty"] + x["scatter_qty"])
                .groupby(["gsp_flag", "goods_status"])["ecode_qty"]
                .sum()
                .reset_index(name="ecode_qty_sum")
                .assign(total_by_goods_status=lambda x: x.groupby("goods_status")["ecode_qty_sum"].transform("sum"))
                .sort_values(by="total_by_goods_status", ascending=False)
            )
            .mark_bar()
            .encode(
                y=alt.Y(
                    "goods_status", sort=alt.SortField(field="total_by_goods_status", order="descending"), title=""
                ),
                x=alt.X("ecode_qty_sum", title="今日验收的电子监管码数"),
                color=alt.Color("gsp_flag"),
            )
            .configure_axis(labelLimit=400),
            use_container_width=True,
        )
    with tab5:
        subtab1, subtab2 = tab5.tabs(["验货员", "货品状态"])
        subtab1.altair_chart(
            alt.Chart(
                inspected_order_df.groupby(["gsp_flag", "inspector_name"])["inspect_duration_min"]
                .mean()
                .reset_index(name="inspect_duration_min_mean")
            )
            .mark_bar()
            .encode(
                y=alt.Y("gsp_flag", title=""),
                x=alt.X("inspect_duration_min_mean", title="今日验收细单平均验收到收货时间（分钟）"),
                color="gsp_flag",
                row=alt.Row(
                    "inspector_name",
                    title="",
                    sort=alt.SortField(field="inspect_duration_min_mean"),
                ),
            ),
        )
        subtab2.altair_chart(
            alt.Chart(
                inspected_order_df.groupby(["gsp_flag", "goods_status"])["inspect_duration_min"]
                .mean()
                .reset_index(name="inspect_duration_min_mean")
            )
            .mark_bar()
            .encode(
                y=alt.Y("gsp_flag", title=""),
                x=alt.X("inspect_duration_min_mean", title="今日验收细单平均验收到收货时间（分钟）"),
                color="gsp_flag",
                row=alt.Row(
                    "goods_status",
                    title="",
                    sort=alt.SortField(field="inspect_duration_min_mean"),
                ),
            ),
        )


# 上架
def update_shelf_metrics():
    unshelf_order_df = get_unshelf_data()
    shelved_order_df = get_shelved_data()
    shelf_container = shelf_column.container()

    shelf_container.container(key="shelfmetric1").metric(
        label="待上架的上架细单数",
        value=len(unshelf_order_df["inout_id"].unique()),
    )
    style_metric_key_cards(key="shelfmetric1", border_left_color=color_palettes[2])
    shelf_container.container(key="shelfmetric2").metric(
        label="今日上架的上架细单数",
        value=len(shelved_order_df["inout_id"].unique()),
    )
    style_metric_key_cards(key="shelfmetric2", border_left_color=color_palettes[2])
    shelf_container.container(key="shelfmetric3").metric(
        label="今日验收到上架的平均时间（分钟）",
        value=f'{np.mean(shelved_order_df["shelf_duration_min"]):.1f}',
    )
    style_metric_key_cards(key="shelfmetric3", border_left_color=color_palettes[2])

    tab1, tab2 = shelf_container.tabs(["上架细单数", "验收上架用时"])
    with tab1:
        subtab1, subtab2, subtab3, subtab4 = tab1.tabs(["分时数量", "上架员", "库区", "分区"])
        subtab1.altair_chart(
            alt.Chart(
                shelved_order_df.groupby(pd.Grouper(key="rf_datetime", freq="15min"))
                .size()
                .reset_index(name="inout_id_count")
            )
            .mark_bar()
            .encode(
                x=alt.X("rf_datetime", title="今日收货时间", axis=alt.Axis(format="%H:%M")),
                y=alt.Y("inout_id_count", title="今日上架的上架细单数"),
                color=alt.value(color_palettes[2]),
            ),
            use_container_width=True,
        )
        subtab2.altair_chart(
            alt.Chart(shelved_order_df.groupby(pd.Grouper(key="rfman_name")).size().reset_index(name="inout_id_count"))
            .mark_bar()
            .encode(
                y=alt.Y("rfman_name", sort=alt.SortField(field="inout_id_count", order="descending"), title=""),
                x=alt.X("inout_id_count", title="今日上架的上架细单数"),
                color=alt.value(color_palettes[2]),
            ),
            use_container_width=True,
        )
        subtab3.altair_chart(
            alt.Chart(shelved_order_df.groupby(pd.Grouper(key="area_name")).size().reset_index(name="inout_id_count"))
            .mark_bar()
            .encode(
                y=alt.Y("area_name", sort=alt.SortField(field="inout_id_count", order="descending"), title=""),
                x=alt.X("inout_id_count", title="今日上架的上架细单数"),
                color=alt.value(color_palettes[2]),
            )
            .configure_axis(labelLimit=400),
            use_container_width=True,
        )
        subtab4.altair_chart(
            alt.Chart(
                shelved_order_df.groupby(pd.Grouper(key="section_name")).size().reset_index(name="inout_id_count")
            )
            .mark_bar()
            .encode(
                y=alt.Y("section_name", sort=alt.SortField(field="inout_id_count", order="descending"), title=""),
                x=alt.X("inout_id_count", title="今日上架的上架细单数"),
                color=alt.value(color_palettes[2]),
            )
            .configure_axis(labelLimit=400),
            use_container_width=True,
        )
    with tab2:
        subtab1, subtab2, subtab3 = tab2.tabs(["上架员", "库区", "分区"])
        subtab1.altair_chart(
            alt.Chart(
                shelved_order_df.groupby(pd.Grouper(key="rfman_name"))["shelf_duration_min"]
                .mean()
                .reset_index(name="shelf_duration_min_mean")
            )
            .mark_bar()
            .encode(
                y=alt.Y("rfman_name", sort=alt.SortField(field="shelf_duration_min_mean"), title=""),
                x=alt.X("shelf_duration_min_mean", title="今日上架的上架细单平均验收到上架时间（分钟）"),
                color=alt.value(color_palettes[2]),
            ),
            use_container_width=True,
        )
        subtab2.altair_chart(
            alt.Chart(
                shelved_order_df.groupby(pd.Grouper(key="area_name"))["shelf_duration_min"]
                .mean()
                .reset_index(name="shelf_duration_min_mean")
            )
            .mark_bar()
            .encode(
                y=alt.Y("area_name", sort=alt.SortField(field="shelf_duration_min_mean"), title=""),
                x=alt.X("shelf_duration_min_mean", title="今日上架的上架细单平均验收到上架时间（分钟）"),
                color=alt.value(color_palettes[2]),
            )
            .configure_axis(labelLimit=400),
            use_container_width=True,
        )
        subtab3.altair_chart(
            alt.Chart(
                shelved_order_df.groupby(pd.Grouper(key="section_name"))["shelf_duration_min"]
                .mean()
                .reset_index(name="shelf_duration_min_mean")
            )
            .mark_bar()
            .encode(
                y=alt.Y("section_name", sort=alt.SortField(field="shelf_duration_min_mean"), title=""),
                x=alt.X("shelf_duration_min_mean", title="今日上架的上架细单平均验收到上架时间（分钟）"),
                color=alt.value(color_palettes[2]),
            )
            .configure_axis(labelLimit=400),
            use_container_width=True,
        )


@st.fragment
def lookup_data():
    with st.expander("查看未收货订单详情"):
        df = get_unreceived_order_data()
        filtered_df = dataframe_explorer(df, case=False)
        st.dataframe(filtered_df, use_container_width=True)

    with st.expander("查看未验收订单详情"):
        df = get_uninspect_data()
        filtered_df = dataframe_explorer(df, case=False)
        st.dataframe(filtered_df, use_container_width=True)

    with st.expander("查看未上架订单详情"):
        df = get_unshelf_data()
        filtered_df = dataframe_explorer(df, case=False)
        st.dataframe(filtered_df, use_container_width=True)


# 数据更新
def update_data():
    update_receive_metrics()
    update_inspect_metrics()
    update_shelf_metrics()


# 定时运行
generate_data()
lookup_data()
update_data()
schedule.every(1).minutes.do(update_data)
