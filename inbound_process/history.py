# %%
import sys
from datetime import datetime, timedelta
from pathlib import Path

import altair as alt

# import adbc_driver_flightsql.dbapi as flight_sql
# import adbc_driver_manager
import duckdb
import numpy as np
import pandas as pd
import streamlit as st
from chinese_calendar import get_workdays, is_holiday
from dateutil.relativedelta import relativedelta
from streamlit_extras.chart_container import chart_container
from streamlit_extras.stylable_container import stylable_container

sys.path.append(str(Path(sys.argv[0]).resolve().parent.parent))


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
        rfman_name TEXT,
        create_datetime_is_holiday BOOLEAN, 
        receive_datetime_is_holiday BOOLEAN, 
        inspect_datetime_is_holiday BOOLEAN, 
        iwcs_datetime_is_holiday BOOLEAN, 
        rf_datetime_is_holiday BOOLEAN,
        create_datetime_shift TEXT, 
        receive_datetime_shift TEXT, 
        inspect_datetime_shift TEXT, 
        iwcs_datetime_shift TEXT, 
        rf_datetime_shift TEXT,
        create_receive_natural_day INTEGER, 
        create_receive_working_day INTEGER, 
        create_receive_working_min INTEGER,
        receive_inspect_natural_day INTEGER, 
        receive_inspect_working_day INTEGER, 
        receive_inspect_working_min INTEGER,
        inspect_shelf_natural_day INTEGER, 
        inspect_shelf_working_day INTEGER, 
        inspect_shelf_working_min INTEGER,
        inspect_iwcs_natural_day INTEGER, 
        inspect_iwcs_working_day INTEGER, 
        inspect_iwcs_working_min INTEGER
    );""")
cursor = conn.cursor()


def get_working_days(start_time, end_time):
    workdays = get_workdays(start_time, end_time)  # 包括开始和结束日期
    if workdays:
        return len(workdays) - 1
    else:
        return 0


def get_working_minutes(start_time, end_time):
    # 定义工作起始时间
    work_start = 8  # 8 AM
    work_end = 22  # 10 PM

    # 获取工作日
    workdays = get_workdays(start_time, end_time)

    total_minutes = 0

    # 计算每个工作日工作时间
    for workday in workdays:
        workday_start = datetime.combine(workday, datetime.min.time()) + timedelta(hours=work_start)
        workday_end = datetime.combine(workday, datetime.min.time()) + timedelta(hours=work_end)

        effective_start = max(start_time, workday_start)
        effective_end = min(end_time, workday_end)

        if effective_start < effective_end:
            total_minutes += (effective_end - effective_start).total_seconds() / 60

    return total_minutes


def get_shift(input_time):
    if input_time.hour >= 8 and input_time.hour < 13:
        return "上午"
    elif input_time.hour >= 13 and input_time.hour < 18:
        return "下午"
    else:
        return "晚上"


def generate_data():
    start_time = datetime.now() - timedelta(days=400)
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

    df["create_datetime_is_holiday"] = df["create_datetime"].apply(is_holiday)
    df["receive_datetime_is_holiday"] = df["receive_datetime"].apply(is_holiday)
    df["inspect_datetime_is_holiday"] = df["inspect_datetime"].apply(is_holiday)
    df["iwcs_datetime_is_holiday"] = df["iwcs_datetime"].apply(is_holiday)
    df["rf_datetime_is_holiday"] = df["rf_datetime"].apply(is_holiday)

    df["create_datetime_shift"] = df["create_datetime"].apply(get_shift)
    df["receive_datetime_shift"] = df["receive_datetime"].apply(get_shift)
    df["inspect_datetime_shift"] = df["inspect_datetime"].apply(get_shift)
    df["iwcs_datetime_shift"] = df["iwcs_datetime"].apply(get_shift)
    df["rf_datetime_shift"] = df["rf_datetime"].apply(get_shift)

    df["create_receive_natural_day"] = (
        df["receive_datetime"].dt.floor("D") - df["create_datetime"].dt.floor("D")
    ).dt.days + 1
    df["create_receive_working_day"] = df.apply(
        lambda x: get_working_days(x["create_datetime"], x["receive_datetime"]), axis=1
    )
    df["create_receive_working_min"] = df.apply(
        lambda x: get_working_minutes(x["create_datetime"], x["receive_datetime"]), axis=1
    )

    df["receive_inspect_natural_day"] = (
        df["inspect_datetime"].dt.floor("D") - df["receive_datetime"].dt.floor("D")
    ).dt.days + 1
    df["receive_inspect_working_day"] = df.apply(
        lambda x: get_working_days(x["receive_datetime"], x["inspect_datetime"]), axis=1
    )
    df["receive_inspect_working_min"] = df.apply(
        lambda x: get_working_minutes(x["receive_datetime"], x["inspect_datetime"]), axis=1
    )

    df["inspect_shelf_natural_day"] = (
        df["rf_datetime"].dt.floor("D") - df["inspect_datetime"].dt.floor("D")
    ).dt.days + 1
    df["inspect_shelf_working_day"] = df.apply(
        lambda x: get_working_days(x["inspect_datetime"], x["rf_datetime"]), axis=1
    )
    df["inspect_shelf_working_min"] = df.apply(
        lambda x: get_working_minutes(x["inspect_datetime"], x["rf_datetime"]), axis=1
    )

    df["inspect_iwcs_natural_day"] = (
        df["iwcs_datetime"].dt.floor("D") - df["inspect_datetime"].dt.floor("D")
    ).dt.days + 1
    df["inspect_iwcs_working_day"] = df.apply(
        lambda x: get_working_days(x["inspect_datetime"], x["iwcs_datetime"]), axis=1
    )
    df["inspect_iwcs_working_min"] = df.apply(
        lambda x: get_working_minutes(x["inspect_datetime"], x["iwcs_datetime"]), axis=1
    )

    df.loc[np.random.choice(df.index, int(num_in_ids * 0.2), replace=False), "iwcs_datetime"] = np.nan
    df.loc[df["iwcs_datetime"].isna(), "inspect_iwcs_natural_day"] = np.nan
    df.loc[df["iwcs_datetime"].isna(), "inspect_iwcs_working_dayy"] = np.nan
    df.loc[df["iwcs_datetime"].isna(), "inspect_iwcs_working_min"] = np.nan

    # Insert back into DuckDB
    conn.execute(
        """
        INSERT INTO in_data 
        SELECT in_id, create_datetime, operation_type, order_status, goodsowner_name, purchaser_name, receiver_name, 
               receive_id, gsp_flag, receive_status, inspector_name, receive_datetime, inspect_datetime, iwcs_datetime, inspect_status, goods_status, whole_qty, scatter_qty, ecode_flag, 
               inout_id, rf_datetime, rf_flag, section_name, area_name, rfman_name,
               create_datetime_is_holiday, receive_datetime_is_holiday, inspect_datetime_is_holiday, iwcs_datetime_is_holiday, rf_datetime_is_holiday,
               create_datetime_shift, receive_datetime_shift, inspect_datetime_shift, iwcs_datetime_shift, rf_datetime_shift,
               create_receive_natural_day, create_receive_working_day, create_receive_working_min,
               receive_inspect_natural_day, receive_inspect_working_day, receive_inspect_working_min,
               inspect_shelf_natural_day, inspect_shelf_working_day, inspect_shelf_working_min,
               inspect_iwcs_natural_day, inspect_iwcs_working_day, inspect_iwcs_working_min
        FROM df
        """
    )


# ---------------------------------------------------------------------------------------------------------------------
# 获取数据
# ---------------------------------------------------------------------------------------------------------------------
# %%
def get_data(sql):
    cursor.execute(sql)
    return cursor.fetch_df()


generate_data()


# ---------------------------------------------------------------------------------------------------------------------
# 数据看板
# ---------------------------------------------------------------------------------------------------------------------
# %%
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


# %%
tab1, tab2, tab3 = st.tabs(["收货", "验收", "上架"])


def receive_data_query(dimension):
    sql_select = ""
    additional_field = ""
    holiday_field = "AND receive_datetime_is_holiday is false" if not include_holiday else ""

    if dimension == "整体":
        additional_field = ", receive_datetime_shift as 班次"
        sql_select = ", receive_datetime_shift"
    elif dimension == "货品货主":
        additional_field = f", goodsowner_name as {dimension}"
        sql_select = ", goodsowner_name"
    elif dimension == "收货员":
        additional_field = f", receiver_name as {dimension}, receive_datetime_shift as 班次"
        sql_select = ", receiver_name, receive_datetime_shift"
    elif dimension == "订单类型":
        additional_field = f", operation_type as {dimension}"
        sql_select = ", operation_type"

    if time_selection == "日报":
        sql = f"""
        WITH receive_data (in_id, receive_datetime, create_receive_working_day{sql_select}) AS (
            SELECT DISTINCT in_id, DATETRUNC('day', receive_datetime), create_receive_working_day{sql_select}
            FROM in_data
            WHERE receive_datetime >= CAST('{start_time}' AS DATE) 
              AND receive_datetime <= CAST('{end_time}' AS DATE)
              {holiday_field}
        )
        SELECT COUNT(in_id) as 收货订单数, 
               receive_datetime as 收货日期,
               MEAN(create_receive_working_day) as 采购收货天数{additional_field}
        FROM receive_data
        GROUP BY receive_datetime{sql_select}
        """
    elif time_selection == "周报":
        sql = f"""
        WITH receive_data (in_id, receive_datetime, create_receive_working_day{sql_select}) AS (
            SELECT DISTINCT in_id, DATETRUNC('day', receive_datetime), create_receive_working_day{sql_select}
            FROM in_data
            WHERE receive_datetime >= CAST('{start_time}' AS DATE) 
              AND receive_datetime <= CAST('{end_time}' AS DATE)
              {holiday_field}
        )
        SELECT COUNT(in_id) as 收货订单数, 
               date_add(receive_datetime, INTERVAL (-WEEKDAY(receive_datetime)+1) DAY) as 收货日期,
               MEAN(create_receive_working_day) as 采购收货天数{additional_field}
        FROM receive_data
        GROUP BY date_add(receive_datetime, INTERVAL (-WEEKDAY(receive_datetime)+1) DAY){sql_select}
        """
    elif time_selection == "月报":
        sql = f"""
        WITH receive_data (in_id, receive_datetime, create_receive_working_day{sql_select}) AS (
            SELECT DISTINCT in_id, DATETRUNC('day', receive_datetime), create_receive_working_day{sql_select}
            FROM in_data
            WHERE receive_datetime >= CAST('{start_time}' AS DATE) 
              AND receive_datetime <= CAST('{end_time}' AS DATE)
              {holiday_field}
        )
        SELECT COUNT(in_id) as 收货订单数, 
               date_add(receive_datetime, INTERVAL (-DAY(receive_datetime)+1) DAY) as 收货日期,
               MEAN(create_receive_working_day) as 采购收货天数{additional_field}
        FROM receive_data
        GROUP BY date_add(receive_datetime, INTERVAL (-DAY(receive_datetime)+1) DAY){sql_select}
        """
    return sql


def update_receive():
    with tab1:
        subtab1, subtab2, subtab3, subtab4 = tab1.tabs(["整体", "货品货主", "收货员", "订单类型"])
        with subtab1:
            df = get_data(receive_data_query("整体"))
            bar = (
                alt.Chart(
                    df.groupby("收货日期")["收货订单数"].sum().reset_index(name="收货订单数"),
                    title=alt.TitleParams("收货订单数", anchor="middle"),
                )
                .mark_bar()
                .encode(
                    x=alt.X("yearmonthdate(收货日期):O", title="收货时间"),
                    y=alt.Y("收货订单数", title="收货订单数"),
                )
            )
            line = (
                alt.Chart(df.groupby("收货日期")["收货订单数"].sum().reset_index(name="收货订单数"))
                .mark_line(color="red")
                .transform_window(
                    rolling_mean="mean(收货订单数)",
                    frame=[-9, 0],
                )
                .encode(x="yearmonthdate(收货日期):O", y="rolling_mean:Q")
            )
            subtab1.altair_chart(
                bar + line,
                use_container_width=True,
            )
            subtab1.altair_chart(
                alt.Chart(df)
                .mark_line(point=True)
                .encode(
                    x=alt.X("yearmonthdate(收货日期):O", title="收货时间"),
                    y=alt.Y("收货订单数", title="收货订单数"),
                    color="班次",
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )
        with subtab2:
            df = get_data(receive_data_query("货品货主"))
            subtab2_col1, subtab2_col2 = subtab2.columns(2)
            subtab2_col1.subheader("收货订单数")
            subtab2_col1.altair_chart(
                alt.Chart(df)
                .mark_line(point=True)
                .encode(
                    x=alt.X("yearmonthdate(收货日期):O", title="收货时间"),
                    y=alt.Y("收货订单数", title="收货订单数"),
                    color="货品货主",
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )
            subtab2_col1.altair_chart(
                alt.Chart(df.groupby("货品货主")["收货订单数"].mean().reset_index(name="平均收货订单数"))
                .mark_bar()
                .encode(
                    y=alt.Y("货品货主", title="", sort=alt.SortField(field="平均收货订单数", order="descending")),
                    x=alt.X("平均收货订单数", title=f"平均{time_dict[time_selection]}收货订单数"),
                    tooltip=["货品货主", alt.Tooltip(f"平均{time_dict[time_selection]}收货订单数:Q", format=".1f")],
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )
            subtab2_col2.subheader("采购到收货时间")
            subtab2_col2.altair_chart(
                alt.Chart(df.groupby("货品货主")["采购收货天数"].mean().reset_index(name="平均采购收货工作日天数"))
                .mark_bar()
                .encode(
                    y=alt.Y(
                        "货品货主", title="", sort=alt.SortField(field="平均采购收货工作日天数", order="descending")
                    ),
                    x=alt.X("平均采购收货工作日天数", title="平均采购收货工作日天数"),
                    tooltip=["货品货主", alt.Tooltip("平均采购收货工作日天数:Q", format=".1f")],
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )
        with subtab3:
            subtab3.subheader("收货订单数")
            df = get_data(receive_data_query("收货员"))
            subtab3.altair_chart(
                alt.Chart(df.groupby(["收货员", "收货日期"])["收货订单数"].sum().reset_index(name="收货订单数"))
                .mark_line(point=True)
                .encode(
                    x=alt.X("yearmonthdate(收货日期):O", title="收货时间"),
                    y=alt.Y("收货订单数", title="收货订单数"),
                    color="收货员",
                ),
                use_container_width=True,
            )
            subtab3_col1, subtab3_col2, subtab3_col3, subtab3_col4 = subtab3.columns(4)
            subtab3_col1.altair_chart(
                alt.Chart(
                    df.groupby("收货员")["收货订单数"].mean().reset_index(name="平均收货订单数"),
                    title=alt.TitleParams("全天", anchor="middle"),
                )
                .mark_bar()
                .encode(
                    y=alt.Y("收货员", title="", sort=alt.SortField(field="平均收货订单数", order="descending")),
                    x=alt.X("平均收货订单数", title=f"平均{time_dict[time_selection]}收货订单数"),
                    tooltip=["收货员", alt.Tooltip(f"平均{time_dict[time_selection]}收货订单数:Q", format=".1f")],
                ),
                use_container_width=True,
            )
            subtab3_col2.altair_chart(
                alt.Chart(
                    df[df["班次"] == "上午"].groupby("收货员")["收货订单数"].mean().reset_index(name="平均收货订单数"),
                    title=alt.TitleParams("上午", anchor="middle"),
                )
                .mark_bar()
                .encode(
                    y=alt.Y("收货员", title="", sort=alt.SortField(field="平均收货订单数", order="descending")),
                    x=alt.X("平均收货订单数", title=f"平均{time_dict[time_selection]}收货订单数"),
                    tooltip=["收货员", alt.Tooltip(f"平均{time_dict[time_selection]}收货订单数:Q", format=".1f")],
                ),
                use_container_width=True,
            )
            subtab3_col3.altair_chart(
                alt.Chart(
                    df[df["班次"] == "下午"].groupby("收货员")["收货订单数"].mean().reset_index(name="平均收货订单数"),
                    title=alt.TitleParams("下午", anchor="middle"),
                )
                .mark_bar()
                .encode(
                    y=alt.Y("收货员", title="", sort=alt.SortField(field="平均收货订单数", order="descending")),
                    x=alt.X("平均收货订单数", title=f"平均{time_dict[time_selection]}收货订单数"),
                    tooltip=["收货员", alt.Tooltip(f"平均{time_dict[time_selection]}收货订单数:Q", format=".1f")],
                ),
                use_container_width=True,
            )
            subtab3_col4.altair_chart(
                alt.Chart(
                    df[df["班次"] == "晚上"].groupby("收货员")["收货订单数"].mean().reset_index(name="平均收货订单数"),
                    title=alt.TitleParams("晚上", anchor="middle"),
                )
                .mark_bar()
                .encode(
                    y=alt.Y("收货员", title="", sort=alt.SortField(field="平均收货订单数", order="descending")),
                    x=alt.X("平均收货订单数", title=f"平均{time_dict[time_selection]}收货订单数"),
                    tooltip=["收货员", alt.Tooltip(f"平均{time_dict[time_selection]}收货订单数:Q", format=".1f")],
                ),
                use_container_width=True,
            )
        with subtab4:
            df = get_data(receive_data_query("订单类型"))
            subtab4_col1, subtab4_col2 = subtab4.columns(2)
            subtab4_col1.subheader("收货订单数")
            subtab4_col1.altair_chart(
                alt.Chart(df)
                .mark_line(point=True)
                .encode(
                    x=alt.X("yearmonthdate(收货日期):O", title="收货时间"),
                    y=alt.Y("收货订单数", title="收货订单数"),
                    color="订单类型",
                ),
                use_container_width=True,
            )
            subtab4_col1.altair_chart(
                alt.Chart(df.groupby("订单类型")["收货订单数"].mean().reset_index(name="平均收货订单数"))
                .mark_bar()
                .encode(
                    y=alt.Y("订单类型", title="", sort=alt.SortField(field="平均收货订单数", order="descending")),
                    x=alt.X("平均收货订单数", title=f"平均{time_dict[time_selection]}收货订单数"),
                    tooltip=["订单类型", alt.Tooltip(f"平均{time_dict[time_selection]}收货订单数:Q", format=".1f")],
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )
            subtab4_col2.subheader("采购到收货时间")
            subtab4_col2.altair_chart(
                alt.Chart(df.groupby("订单类型")["采购收货天数"].mean().reset_index(name="平均采购收货工作日天数"))
                .mark_bar()
                .encode(
                    y=alt.Y(
                        "订单类型", title="", sort=alt.SortField(field="平均采购收货工作日天数", order="descending")
                    ),
                    x=alt.X("平均采购收货工作日天数", title="平均采购收货工作日天数"),
                    tooltip=["订单类型", alt.Tooltip("平均采购收货工作日天数:Q", format=".1f")],
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )


# %%
def inspect_data_query(dimension):
    sql_select = ""
    additional_field = ""
    holiday_field = "AND inspect_datetime_is_holiday is false" if not include_holiday else ""

    if dimension == "整体":
        additional_field = ", inspect_datetime_shift as 班次"
        sql_select = ", inspect_datetime_shift"
    elif dimension == "验收员":
        additional_field = f", inspector_name as {dimension}, inspect_datetime_shift as 班次"
        sql_select = ", inspector_name, inspect_datetime_shift"
    elif dimension == "货品状态":
        additional_field = f", goods_status as {dimension}, goodsowner_name as 货品货主"
        sql_select = ", goods_status, goodsowner_name"

    if time_selection == "日报":
        sql = f"""
        WITH inspect_data (receive_id, inspect_datetime, inspect_type, whole_qty, scatter_qty, ecode_flag, receive_inspect_working_day, inspect_iwcs_working_day{sql_select}) AS (
            SELECT DISTINCT receive_id, DATETRUNC('day', inspect_datetime) as inspect_datetime, CASE WHEN gsp_flag='拟合格' OR iwcs_datetime NOT NULL THEN '拟合格验收' ELSE '直接验收' END AS inspect_type, whole_qty, scatter_qty, ecode_flag, receive_inspect_working_day, inspect_iwcs_working_day{sql_select}
            FROM in_data
            WHERE inspect_datetime >= CAST('{start_time}' AS DATE) 
              AND inspect_datetime <= CAST('{end_time}' AS DATE)
              AND goods_status != '无实物合格'
              {holiday_field}
        )
        SELECT COUNT(receive_id) as 验收明细数, 
               inspect_datetime as 验收日期,
               inspect_type as 验收类型,
               SUM(whole_qty) as 整件货物数,
               SUM(scatter_qty) as 散件货物数,
               SUM(CASE WHEN ecode_flag IS TRUE THEN whole_qty+scatter_qty ELSE 0 END) as 电子监管码数,
               MEAN(receive_inspect_working_day) as 收货验货天数,
               MEAN(inspect_iwcs_working_day) as 拟合格验货天数{additional_field}
        FROM inspect_data
        GROUP BY inspect_datetime, inspect_type{sql_select}
        """
    elif time_selection == "周报":
        sql = f"""
        WITH inspect_data (receive_id, inspect_datetime, inspect_type, whole_qty, scatter_qty, ecode_flag, receive_inspect_working_day, inspect_iwcs_working_day{sql_select}) AS (
            SELECT DISTINCT receive_id, DATETRUNC('day', inspect_datetime) as inspect_datetime, CASE WHEN gsp_flag='拟合格' OR iwcs_datetime NOT NULL THEN '拟合格验收' ELSE '直接验收' END AS inspect_type, whole_qty, scatter_qty, ecode_flag, receive_inspect_working_day, inspect_iwcs_working_day{sql_select}
            FROM in_data
            WHERE inspect_datetime >= CAST('{start_time}' AS DATE) 
              AND inspect_datetime <= CAST('{end_time}' AS DATE)
              AND goods_status != '无实物合格'
              {holiday_field}
        )
        SELECT COUNT(receive_id) as 验收明细数, 
               date_add(inspect_datetime, INTERVAL (-WEEKDAY(inspect_datetime)+1) DAY) as 验收日期,
               inspect_type as 验收类型,
               SUM(whole_qty) as 整件货物数,
               SUM(scatter_qty) as 散件货物数,
               SUM(CASE WHEN ecode_flag IS TRUE THEN whole_qty+scatter_qty ELSE 0 END) as 电子监管码数,
               MEAN(receive_inspect_working_day) as 收货验货天数,
               MEAN(inspect_iwcs_working_day) as 拟合格验货天数{additional_field}
        FROM inspect_data
        GROUP BY date_add(inspect_datetime, INTERVAL (-WEEKDAY(inspect_datetime)+1) DAY), inspect_type{sql_select}
        """
    elif time_selection == "月报":
        sql = f"""
        WITH inspect_data (receive_id, inspect_datetime, inspect_type, whole_qty, scatter_qty, ecode_flag, receive_inspect_working_day, inspect_iwcs_working_day{sql_select}) AS (
            SELECT DISTINCT receive_id, DATETRUNC('day', inspect_datetime) as inspect_datetime, CASE WHEN gsp_flag='拟合格' OR iwcs_datetime NOT NULL THEN '拟合格验收' ELSE '直接验收' END AS inspect_type, whole_qty, scatter_qty, ecode_flag, receive_inspect_working_day, inspect_iwcs_working_day{sql_select}
            FROM in_data
            WHERE inspect_datetime >= CAST('{start_time}' AS DATE) 
              AND inspect_datetime <= CAST('{end_time}' AS DATE)
              AND goods_status != '无实物合格'
              {holiday_field}
        )
        SELECT COUNT(receive_id) as 验收明细数, 
               date_add(inspect_datetime, INTERVAL (-DAY(inspect_datetime)+1) DAY) as 验收日期,
               inspect_type as 验收类型,
               SUM(whole_qty) as 整件货物数,
               SUM(scatter_qty) as 散件货物数,
               SUM(CASE WHEN ecode_flag IS TRUE THEN whole_qty+scatter_qty ELSE 0 END) as 电子监管码数,
               MEAN(receive_inspect_working_day) as 收货验货天数,
               MEAN(inspect_iwcs_working_day) as 拟合格验货天数{additional_field}
        FROM inspect_data
        GROUP BY date_add(inspect_datetime, INTERVAL (-DAY(inspect_datetime)+1) DAY), inspect_type{sql_select}
        """
    return sql


# %%
def update_inspect():
    with tab2:
        subtab1, subtab2, subtab3 = tab2.tabs(["整体", "验收员", "货品状态"])
        with subtab1:
            df = get_data(inspect_data_query("整体"))
            subtab1_col1, subtab1_col2 = subtab1.columns(2)
            bar = (
                alt.Chart(
                    df.groupby(["验收日期", "验收类型"])["验收明细数"].sum().reset_index(name="验收明细数"),
                    title=alt.TitleParams("验收明细数", anchor="middle"),
                )
                .mark_bar()
                .encode(
                    x=alt.X("yearmonthdate(验收日期):O", title="验收时间"),
                    y=alt.Y("验收明细数", title="验收明细数"),
                    color="验收类型",
                )
            )
            line = (
                alt.Chart(df.groupby("验收日期")["验收明细数"].sum().reset_index(name="验收明细数"))
                .mark_line(color="red")
                .transform_window(
                    rolling_mean="mean(验收明细数)",
                    frame=[-9, 0],
                )
                .encode(x="yearmonthdate(验收日期):O", y="rolling_mean:Q")
            )
            subtab1_col1.altair_chart(
                bar + line,
                use_container_width=True,
            )
            subtab1_col2.altair_chart(
                alt.Chart(
                    df[df["验收类型"] == "拟合格验收"],
                    title=alt.TitleParams("验收明细数", anchor="middle"),
                )
                .mark_line(point=True)
                .encode(
                    x=alt.X("yearmonthdate(验收日期):O", title="验收时间"),
                    y=alt.Y("验收明细数", title="验收明细数"),
                    color="班次",
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )
            subtab1_col3, subtab1_col4, subtab1_col5 = subtab1.columns(3)
            subtab1_col3.altair_chart(
                alt.Chart(
                    df.groupby(["验收日期", "验收类型"])["整件货物数"].sum().reset_index(name="整件货物数"),
                    title=alt.TitleParams("整件货物数", anchor="middle"),
                )
                .mark_bar()
                .encode(
                    x=alt.X("yearmonthdate(验收日期):O", title="验收时间"),
                    y=alt.Y("整件货物数", title="整件货物数"),
                    color="验收类型",
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )
            subtab1_col4.altair_chart(
                alt.Chart(
                    df.groupby(["验收日期", "验收类型"])["散件货物数"].sum().reset_index(name="散件货物数"),
                    title=alt.TitleParams("散件货物数", anchor="middle"),
                )
                .mark_bar()
                .encode(
                    x=alt.X("yearmonthdate(验收日期):O", title="验收时间"),
                    y=alt.Y("散件货物数", title="散件货物数"),
                    color="验收类型",
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )
            subtab1_col5.altair_chart(
                alt.Chart(
                    df.groupby(["验收日期", "验收类型"])["电子监管码数"].sum().reset_index(name="电子监管码数"),
                    title=alt.TitleParams("电子监管码数", anchor="middle"),
                )
                .mark_bar()
                .encode(
                    x=alt.X("yearmonthdate(验收日期):O", title="验收时间"),
                    y=alt.Y("电子监管码数", title="电子监管码数"),
                    color="验收类型",
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )
            subtab1_col6, subtab1_col7 = subtab1.columns(2)
            subtab1_col6.altair_chart(
                alt.Chart(
                    df.groupby(["验收日期", "验收类型"])["收货验货天数"].mean().reset_index(name="平均收货验货天数"),
                    title=alt.TitleParams("平均收货验货天数", anchor="middle"),
                )
                .mark_line(point=True)
                .encode(
                    x=alt.X("yearmonthdate(验收日期):O", title="验收时间"),
                    y=alt.Y("平均收货验货天数", title="平均收货验货天数"),
                    color="验收类型",
                    tooltip=[
                        "验收日期",
                        "验收类型",
                        alt.Tooltip("平均收货验货天数:Q", format=".1f"),
                    ],
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )
            subtab1_col7.altair_chart(
                alt.Chart(
                    df[df["验收类型"] == "拟合格验收"]
                    .groupby("验收日期")["拟合格验货天数"]
                    .mean()
                    .reset_index(name="平均拟合格验货天数"),
                    title=alt.TitleParams("平均拟合格验货天数", anchor="middle"),
                )
                .mark_line(point=True)
                .encode(
                    x=alt.X("yearmonthdate(验收日期):O", title="验收时间"),
                    y=alt.Y("平均拟合格验货天数", title="平均拟合格验货天数"),
                    tooltip=[
                        "验收日期",
                        alt.Tooltip("平均拟合格验货天数:Q", format=".1f"),
                    ],
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )
        with subtab2:
            df = get_data(inspect_data_query("验收员"))
            subtab2_col1, subtab2_col2, subtab2_col3, subtab2_col4 = subtab2.columns(4)
            subtab2_col1.subheader("平均验收明细数")
            subtab2_col1.altair_chart(
                alt.Chart(
                    df.groupby(["验收员", "验收类型"])["验收明细数"].mean().reset_index(name="平均验收明细数"),
                    title=alt.TitleParams("全天", anchor="middle"),
                )
                .mark_bar()
                .encode(
                    y=alt.Y("验收员", title="", sort=alt.SortField(field="平均验收明细数", order="descending")),
                    x=alt.X("平均验收明细数", title=f"平均{time_dict[time_selection]}验收明细数"),
                    color="验收类型",
                    tooltip=["验收员", alt.Tooltip(f"平均{time_dict[time_selection]}验收明细数:Q", format=".1f")],
                ),
                use_container_width=True,
            )
            subtab2_col1.altair_chart(
                alt.Chart(
                    df[df["班次"] == "上午"]
                    .groupby(["验收员", "验收类型"])["验收明细数"]
                    .mean()
                    .reset_index(name="平均验收明细数"),
                    title=alt.TitleParams("上午", anchor="middle"),
                )
                .mark_bar()
                .encode(
                    y=alt.Y("验收员", title="", sort=alt.SortField(field="平均验收明细数", order="descending")),
                    x=alt.X("平均验收明细数", title=f"平均{time_dict[time_selection]}验收明细数"),
                    color="验收类型",
                    tooltip=["验收员", alt.Tooltip(f"平均{time_dict[time_selection]}验收明细数:Q", format=".1f")],
                ),
                use_container_width=True,
            )
            subtab2_col1.altair_chart(
                alt.Chart(
                    df[df["班次"] == "下午"]
                    .groupby(["验收员", "验收类型"])["验收明细数"]
                    .mean()
                    .reset_index(name="平均验收明细数"),
                    title=alt.TitleParams("下午", anchor="middle"),
                )
                .mark_bar()
                .encode(
                    y=alt.Y("验收员", title="", sort=alt.SortField(field="平均验收明细数", order="descending")),
                    x=alt.X("平均验收明细数", title=f"平均{time_dict[time_selection]}验收明细数"),
                    color="验收类型",
                    tooltip=["验收员", alt.Tooltip(f"平均{time_dict[time_selection]}验收明细数:Q", format=".1f")],
                ),
                use_container_width=True,
            )
            subtab2_col1.altair_chart(
                alt.Chart(
                    df[df["班次"] == "晚上"]
                    .groupby(["验收员", "验收类型"])["验收明细数"]
                    .mean()
                    .reset_index(name="平均验收明细数"),
                    title=alt.TitleParams("晚上", anchor="middle"),
                )
                .mark_bar()
                .encode(
                    y=alt.Y("验收员", title="", sort=alt.SortField(field="平均验收明细数", order="descending")),
                    x=alt.X("平均验收明细数", title=f"平均{time_dict[time_selection]}验收明细数"),
                    color="验收类型",
                    tooltip=["验收员", alt.Tooltip(f"平均{time_dict[time_selection]}验收明细数:Q", format=".1f")],
                ),
                use_container_width=True,
            )
            subtab2_col2.subheader("平均整件货物数")
            subtab2_col2.altair_chart(
                alt.Chart(
                    df.groupby(["验收员", "验收类型"])["整件货物数"].mean().reset_index(name="平均整件货物数"),
                    title=alt.TitleParams("全天", anchor="middle"),
                )
                .mark_bar()
                .encode(
                    y=alt.Y("验收员", title="", sort=alt.SortField(field="平均整件货物数", order="descending")),
                    x=alt.X("平均整件货物数", title=f"平均{time_dict[time_selection]}整件货物数"),
                    color="验收类型",
                    tooltip=["验收员", alt.Tooltip(f"平均{time_dict[time_selection]}整件货物数:Q", format=".1f")],
                ),
                use_container_width=True,
            )
            subtab2_col2.altair_chart(
                alt.Chart(
                    df[df["班次"] == "上午"]
                    .groupby(["验收员", "验收类型"])["整件货物数"]
                    .mean()
                    .reset_index(name="平均整件货物数"),
                    title=alt.TitleParams("上午", anchor="middle"),
                )
                .mark_bar()
                .encode(
                    y=alt.Y("验收员", title="", sort=alt.SortField(field="平均整件货物数", order="descending")),
                    x=alt.X("平均整件货物数", title=f"平均{time_dict[time_selection]}整件货物数"),
                    color="验收类型",
                    tooltip=["验收员", alt.Tooltip(f"平均{time_dict[time_selection]}整件货物数:Q", format=".1f")],
                ),
                use_container_width=True,
            )
            subtab2_col2.altair_chart(
                alt.Chart(
                    df[df["班次"] == "下午"]
                    .groupby(["验收员", "验收类型"])["整件货物数"]
                    .mean()
                    .reset_index(name="平均整件货物数"),
                    title=alt.TitleParams("下午", anchor="middle"),
                )
                .mark_bar()
                .encode(
                    y=alt.Y("验收员", title="", sort=alt.SortField(field="平均整件货物数", order="descending")),
                    x=alt.X("平均整件货物数", title=f"平均{time_dict[time_selection]}整件货物数"),
                    color="验收类型",
                    tooltip=["验收员", alt.Tooltip(f"平均{time_dict[time_selection]}整件货物数:Q", format=".1f")],
                ),
                use_container_width=True,
            )
            subtab2_col2.altair_chart(
                alt.Chart(
                    df[df["班次"] == "晚上"]
                    .groupby(["验收员", "验收类型"])["整件货物数"]
                    .mean()
                    .reset_index(name="平均整件货物数"),
                    title=alt.TitleParams("晚上", anchor="middle"),
                )
                .mark_bar()
                .encode(
                    y=alt.Y("验收员", title="", sort=alt.SortField(field="平均整件货物数", order="descending")),
                    x=alt.X("平均整件货物数", title=f"平均{time_dict[time_selection]}整件货物数"),
                    color="验收类型",
                    tooltip=["验收员", alt.Tooltip(f"平均{time_dict[time_selection]}整件货物数:Q", format=".1f")],
                ),
                use_container_width=True,
            )
            subtab2_col3.subheader("平均散件货物数")
            subtab2_col3.altair_chart(
                alt.Chart(
                    df.groupby(["验收员", "验收类型"])["散件货物数"].mean().reset_index(name="平均散件货物数"),
                    title=alt.TitleParams("全天", anchor="middle"),
                )
                .mark_bar()
                .encode(
                    y=alt.Y("验收员", title="", sort=alt.SortField(field="平均散件货物数", order="descending")),
                    x=alt.X("平均散件货物数", title=f"平均{time_dict[time_selection]}散件货物数"),
                    color="验收类型",
                    tooltip=["验收员", alt.Tooltip(f"平均{time_dict[time_selection]}散件货物数:Q", format=".1f")],
                ),
                use_container_width=True,
            )
            subtab2_col3.altair_chart(
                alt.Chart(
                    df[df["班次"] == "上午"]
                    .groupby(["验收员", "验收类型"])["散件货物数"]
                    .mean()
                    .reset_index(name="平均散件货物数"),
                    title=alt.TitleParams("上午", anchor="middle"),
                )
                .mark_bar()
                .encode(
                    y=alt.Y("验收员", title="", sort=alt.SortField(field="平均散件货物数", order="descending")),
                    x=alt.X("平均散件货物数", title=f"平均{time_dict[time_selection]}散件货物数"),
                    color="验收类型",
                    tooltip=["验收员", alt.Tooltip(f"平均{time_dict[time_selection]}散件货物数:Q", format=".1f")],
                ),
                use_container_width=True,
            )
            subtab2_col3.altair_chart(
                alt.Chart(
                    df[df["班次"] == "下午"]
                    .groupby(["验收员", "验收类型"])["散件货物数"]
                    .mean()
                    .reset_index(name="平均散件货物数"),
                    title=alt.TitleParams("下午", anchor="middle"),
                )
                .mark_bar()
                .encode(
                    y=alt.Y("验收员", title="", sort=alt.SortField(field="平均散件货物数", order="descending")),
                    x=alt.X("平均散件货物数", title=f"平均{time_dict[time_selection]}散件货物数"),
                    color="验收类型",
                    tooltip=["验收员", alt.Tooltip(f"平均{time_dict[time_selection]}散件货物数:Q", format=".1f")],
                ),
                use_container_width=True,
            )
            subtab2_col3.altair_chart(
                alt.Chart(
                    df[df["班次"] == "晚上"]
                    .groupby(["验收员", "验收类型"])["散件货物数"]
                    .mean()
                    .reset_index(name="平均散件货物数"),
                    title=alt.TitleParams("晚上", anchor="middle"),
                )
                .mark_bar()
                .encode(
                    y=alt.Y("验收员", title="", sort=alt.SortField(field="平均散件货物数", order="descending")),
                    x=alt.X("平均散件货物数", title=f"平均{time_dict[time_selection]}散件货物数"),
                    color="验收类型",
                    tooltip=["验收员", alt.Tooltip(f"平均{time_dict[time_selection]}散件货物数:Q", format=".1f")],
                ),
                use_container_width=True,
            )
            subtab2_col4.subheader("平均电子监管码数")
            subtab2_col4.altair_chart(
                alt.Chart(
                    df.groupby(["验收员", "验收类型"])["电子监管码数"].mean().reset_index(name="平均电子监管码数"),
                    title=alt.TitleParams("全天", anchor="middle"),
                )
                .mark_bar()
                .encode(
                    y=alt.Y("验收员", title="", sort=alt.SortField(field="平均电子监管码数", order="descending")),
                    x=alt.X("平均电子监管码数", title=f"平均{time_dict[time_selection]}电子监管码数"),
                    color="验收类型",
                    tooltip=["验收员", alt.Tooltip(f"平均{time_dict[time_selection]}电子监管码数:Q", format=".1f")],
                ),
                use_container_width=True,
            )
            subtab2_col4.altair_chart(
                alt.Chart(
                    df[df["班次"] == "上午"]
                    .groupby(["验收员", "验收类型"])["电子监管码数"]
                    .mean()
                    .reset_index(name="平均电子监管码数"),
                    title=alt.TitleParams("上午", anchor="middle"),
                )
                .mark_bar()
                .encode(
                    y=alt.Y("验收员", title="", sort=alt.SortField(field="平均电子监管码数", order="descending")),
                    x=alt.X("平均电子监管码数", title=f"平均{time_dict[time_selection]}电子监管码数"),
                    color="验收类型",
                    tooltip=["验收员", alt.Tooltip(f"平均{time_dict[time_selection]}电子监管码数:Q", format=".1f")],
                ),
                use_container_width=True,
            )
            subtab2_col4.altair_chart(
                alt.Chart(
                    df[df["班次"] == "下午"]
                    .groupby(["验收员", "验收类型"])["电子监管码数"]
                    .mean()
                    .reset_index(name="平均电子监管码数"),
                    title=alt.TitleParams("下午", anchor="middle"),
                )
                .mark_bar()
                .encode(
                    y=alt.Y("验收员", title="", sort=alt.SortField(field="平均电子监管码数", order="descending")),
                    x=alt.X("平均电子监管码数", title=f"平均{time_dict[time_selection]}电子监管码数"),
                    color="验收类型",
                    tooltip=["验收员", alt.Tooltip(f"平均{time_dict[time_selection]}电子监管码数:Q", format=".1f")],
                ),
                use_container_width=True,
            )
            subtab2_col4.altair_chart(
                alt.Chart(
                    df[df["班次"] == "晚上"]
                    .groupby(["验收员", "验收类型"])["电子监管码数"]
                    .mean()
                    .reset_index(name="平均电子监管码数"),
                    title=alt.TitleParams("晚上", anchor="middle"),
                )
                .mark_bar()
                .encode(
                    y=alt.Y("验收员", title="", sort=alt.SortField(field="平均电子监管码数", order="descending")),
                    x=alt.X("平均电子监管码数", title=f"平均{time_dict[time_selection]}电子监管码数"),
                    color="验收类型",
                    tooltip=["验收员", alt.Tooltip(f"平均{time_dict[time_selection]}电子监管码数:Q", format=".1f")],
                ),
                use_container_width=True,
            )
        with subtab3:
            df = get_data(inspect_data_query("货品状态"))
            subtab3_col1, subtab3_col2 = subtab3.columns(2)
            subtab3_col1.altair_chart(
                alt.Chart(
                    df.groupby(["货品状态", "验收类型"])["验收明细数"].mean().reset_index(name="平均验收明细数"),
                    title=alt.TitleParams("平均验收明细数", anchor="middle"),
                )
                .mark_bar()
                .encode(
                    y=alt.Y("货品状态", title="", sort=alt.SortField(field="平均验收明细数", order="descending")),
                    x=alt.X("平均验收明细数", title=f"平均{time_dict[time_selection]}验收明细数"),
                    color="验收类型",
                    tooltip=["货品状态", alt.Tooltip(f"平均{time_dict[time_selection]}验收明细数:Q", format=".1f")],
                ),
                use_container_width=True,
            )
            subtab3_col2.altair_chart(
                alt.Chart(
                    df.groupby(["货品状态", "验收类型"])["整件货物数"].mean().reset_index(name="平均整件货物数"),
                    title=alt.TitleParams("平均整件货物数", anchor="middle"),
                )
                .mark_bar()
                .encode(
                    y=alt.Y("货品状态", title="", sort=alt.SortField(field="平均整件货物数", order="descending")),
                    x=alt.X("平均整件货物数", title=f"平均{time_dict[time_selection]}整件货物数"),
                    color="验收类型",
                    tooltip=["货品状态", alt.Tooltip(f"平均{time_dict[time_selection]}整件货物数:Q", format=".1f")],
                ),
                use_container_width=True,
            )
            subtab3_col1.altair_chart(
                alt.Chart(
                    df.groupby(["货品状态", "验收类型"])["散件货物数"].mean().reset_index(name="平均散件货物数"),
                    title=alt.TitleParams("平均散件货物", anchor="middle"),
                )
                .mark_bar()
                .encode(
                    y=alt.Y("货品状态", title="", sort=alt.SortField(field="平均散件货物数", order="descending")),
                    x=alt.X("平均散件货物数", title=f"平均{time_dict[time_selection]}散件货物数"),
                    color="验收类型",
                    tooltip=["货品状态", alt.Tooltip(f"平均{time_dict[time_selection]}散件货物数:Q", format=".1f")],
                ),
                use_container_width=True,
            )
            subtab3_col2.altair_chart(
                alt.Chart(
                    df.groupby(["货品状态", "验收类型"])["电子监管码数"].mean().reset_index(name="平均电子监管码数"),
                    title=alt.TitleParams("平均电子监管码数", anchor="middle"),
                )
                .mark_bar()
                .encode(
                    y=alt.Y("货品状态", title="", sort=alt.SortField(field="平均电子监管码数", order="descending")),
                    x=alt.X("平均电子监管码数", title=f"平均{time_dict[time_selection]}电子监管码数"),
                    color="验收类型",
                    tooltip=["货品状态", alt.Tooltip(f"平均{time_dict[time_selection]}电子监管码数:Q", format=".1f")],
                ),
                use_container_width=True,
            )
            sub_df = (
                df.groupby("货品货主")
                .apply(lambda x: x["货品状态"].isin(["合格", "拟合格"]).sum() / len(x) * 100)
                .reset_index(name="合格比例")
            )
            with chart_container(sub_df):
                subtab3.altair_chart(
                    alt.Chart(
                        sub_df,
                        title=alt.TitleParams("各货品货主总验收明细数合格/拟合格明细占比", anchor="middle"),
                    )
                    .mark_bar()
                    .encode(
                        y=alt.Y("货品货主", title="", sort=alt.SortField(field="合格比例", order="descending")),
                        x=alt.X("合格比例", title="比例（%）"),
                        tooltip=[
                            "货品货主",
                            alt.Tooltip("合格比例:Q", format=".1f"),
                        ],
                    ),
                    use_container_width=True,
                )


# %%
def shelf_data_query(dimension):
    sql_select = ""
    additional_field = ""
    holiday_field = "AND rf_datetime_is_holiday is false" if not include_holiday else ""

    if dimension == "整体":
        additional_field = ", rf_datetime_shift as 班次"
        sql_select = ", rf_datetime_shift"
    elif dimension == "上架员":
        additional_field = f", rfman_name as {dimension}, rf_datetime_shift as 班次"
        sql_select = ", rfman_name, rf_datetime_shift"
    elif dimension == "库区":
        additional_field = f", area_name as {dimension}"
        sql_select = ", area_name"
    elif dimension == "分区":
        additional_field = f", section_name as {dimension}"
        sql_select = ", section_name"

    if time_selection == "日报":
        sql = f"""
        WITH shelf_data (inout_id, rf_datetime, inspect_shelf_working_day{sql_select}) AS (
            SELECT DISTINCT inout_id, DATETRUNC('day', rf_datetime) as rf_datetime, inspect_shelf_working_day{sql_select}
            FROM in_data
            WHERE rf_datetime >= CAST('{start_time}' AS DATE) 
              AND rf_datetime <= CAST('{end_time}' AS DATE)
              {holiday_field}
        )
        SELECT COUNT(inout_id) as 上架明细数, 
               rf_datetime as 上架日期,
               MEAN(inspect_shelf_working_day) as 验货上架天数{additional_field}
        FROM shelf_data
        GROUP BY rf_datetime{sql_select}
        """
    elif time_selection == "周报":
        sql = f"""
        WITH shelf_data (inout_id, rf_datetime, inspect_shelf_working_day{sql_select}) AS (
            SELECT DISTINCT inout_id, DATETRUNC('day', rf_datetime) as rf_datetime, inspect_shelf_working_day{sql_select}
            FROM in_data
            WHERE rf_datetime >= CAST('{start_time}' AS DATE) 
              AND rf_datetime <= CAST('{end_time}' AS DATE)
              {holiday_field}
        )
        SELECT COUNT(inout_id) as 上架明细数, 
               date_add(rf_datetime, INTERVAL (-WEEKDAY(rf_datetime)+1) DAY) as 上架日期,
               MEAN(inspect_shelf_working_day) as 验货上架天数{additional_field}
        FROM shelf_data
        GROUP BY date_add(rf_datetime, INTERVAL (-WEEKDAY(rf_datetime)+1) DAY){sql_select}
        """
    elif time_selection == "月报":
        sql = f"""
        WITH shelf_data (inout_id, rf_datetime, inspect_shelf_working_day{sql_select}) AS (
            SELECT DISTINCT inout_id, DATETRUNC('day', rf_datetime) as rf_datetime, inspect_shelf_working_day{sql_select}
            FROM in_data
            WHERE rf_datetime >= CAST('{start_time}' AS DATE) 
              AND rf_datetime <= CAST('{end_time}' AS DATE)
              {holiday_field}
        )
        SELECT COUNT(inout_id) as 上架明细数, 
               date_add(rf_datetime, INTERVAL (-DAY(rf_datetime)+1) DAY) as 上架日期,
               MEAN(inspect_shelf_working_day) as 验货上架天数{additional_field}
        FROM shelf_data
        GROUP BY date_add(rf_datetime, INTERVAL (-DAY(rf_datetime)+1) DAY){sql_select}
        """
    return sql


# %%
def update_shelf():
    with tab3:
        subtab1, subtab2, subtab3, subtab4 = tab3.tabs(["整体", "上架员", "库区", "分区"])
        with subtab1:
            df = get_data(shelf_data_query("整体"))
            bar = (
                alt.Chart(
                    df.groupby("上架日期")["上架明细数"].sum().reset_index(name="上架明细数"),
                    title=alt.TitleParams("上架明细数", anchor="middle"),
                )
                .mark_bar()
                .encode(
                    x=alt.X("yearmonthdate(上架日期):O", title="上架时间"),
                    y=alt.Y("上架明细数", title="上架明细数"),
                )
            )
            line = (
                alt.Chart(df.groupby("上架日期")["上架明细数"].sum().reset_index(name="上架明细数"))
                .mark_line(color="red")
                .transform_window(
                    rolling_mean="mean(上架明细数)",
                    frame=[-9, 0],
                )
                .encode(x="yearmonthdate(上架日期):O", y="rolling_mean:Q")
            )
            subtab1.altair_chart(
                bar + line,
                use_container_width=True,
            )
            subtab1.altair_chart(
                alt.Chart(df)
                .mark_line(point=True)
                .encode(
                    x=alt.X("yearmonthdate(上架日期):O", title="上架时间"),
                    y=alt.Y("上架明细数", title="上架明细数"),
                    color="班次",
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )
            subtab1.altair_chart(
                alt.Chart(
                    df.groupby("上架日期")["验货上架天数"].mean().reset_index(name="平均验货上架天数"),
                    title=alt.TitleParams("平均验货上架天数", anchor="middle"),
                )
                .mark_line(point=True)
                .encode(
                    x=alt.X("yearmonthdate(上架日期):O", title="上架时间"),
                    y=alt.Y("平均验货上架天数", title="平均验货上架天数数"),
                    tooltip=[
                        "上架日期",
                        alt.Tooltip("平均验货上架天数:Q", format=".1f"),
                    ],
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )
        with subtab2:
            df = get_data(shelf_data_query("上架员"))
            subtab2.altair_chart(
                alt.Chart(
                    df.groupby(["上架员", "上架日期"])["上架明细数"].sum().reset_index(name="上架明细数"),
                    title=alt.TitleParams("上架明细数", anchor="middle"),
                )
                .mark_line(point=True)
                .encode(
                    x=alt.X("yearmonthdate(上架日期):O", title="上架时间"),
                    y=alt.Y("上架明细数", title="上架明细数"),
                    color="上架员",
                ),
                use_container_width=True,
            )
            subtab2_col1, subtab2_col2, subtab2_col3, subtab2_col4 = subtab2.columns(4)
            subtab2_col1.altair_chart(
                alt.Chart(
                    df.groupby("上架员")["上架明细数"].mean().reset_index(name="平均上架明细数"),
                    title=alt.TitleParams("全天", anchor="middle"),
                )
                .mark_bar()
                .encode(
                    y=alt.Y("上架员", title="", sort=alt.SortField(field="平均上架明细数", order="descending")),
                    x=alt.X("平均上架明细数", title=f"平均{time_dict[time_selection]}上架明细数"),
                    tooltip=["上架员", alt.Tooltip(f"平均{time_dict[time_selection]}上架明细数:Q", format=".1f")],
                ),
                use_container_width=True,
            )
            subtab2_col2.altair_chart(
                alt.Chart(
                    df[df["班次"] == "上午"].groupby("上架员")["上架明细数"].mean().reset_index(name="平均上架明细数"),
                    title=alt.TitleParams("上午", anchor="middle"),
                )
                .mark_bar()
                .encode(
                    y=alt.Y("上架员", title="", sort=alt.SortField(field="平均上架明细数", order="descending")),
                    x=alt.X("平均上架明细数", title=f"平均{time_dict[time_selection]}上架明细数"),
                    tooltip=["上架员", alt.Tooltip(f"平均{time_dict[time_selection]}上架明细数:Q", format=".1f")],
                ),
                use_container_width=True,
            )
            subtab2_col3.altair_chart(
                alt.Chart(
                    df[df["班次"] == "下午"].groupby("上架员")["上架明细数"].mean().reset_index(name="平均上架明细数"),
                    title=alt.TitleParams("下午", anchor="middle"),
                )
                .mark_bar()
                .encode(
                    y=alt.Y("上架员", title="", sort=alt.SortField(field="平均上架明细数", order="descending")),
                    x=alt.X("平均上架明细数", title=f"平均{time_dict[time_selection]}上架明细数"),
                    tooltip=["上架员", alt.Tooltip(f"平均{time_dict[time_selection]}上架明细数:Q", format=".1f")],
                ),
                use_container_width=True,
            )
            subtab2_col4.altair_chart(
                alt.Chart(
                    df[df["班次"] == "晚上"].groupby("上架员")["上架明细数"].mean().reset_index(name="平均上架明细数"),
                    title=alt.TitleParams("晚上", anchor="middle"),
                )
                .mark_bar()
                .encode(
                    y=alt.Y("上架员", title="", sort=alt.SortField(field="平均上架明细数", order="descending")),
                    x=alt.X("平均上架明细数", title=f"平均{time_dict[time_selection]}上架明细数"),
                    tooltip=["上架员", alt.Tooltip(f"平均{time_dict[time_selection]}上架明细数:Q", format=".1f")],
                ),
                use_container_width=True,
            )
        with subtab3:
            df = get_data(shelf_data_query("库区"))
            subtab3.altair_chart(
                alt.Chart(
                    df.groupby(["库区", "上架日期"])["上架明细数"].sum().reset_index(name="上架明细数"),
                    title=alt.TitleParams("上架明细数", anchor="middle"),
                )
                .mark_line(point=True)
                .encode(
                    x=alt.X("yearmonthdate(上架日期):O", title="上架时间"),
                    y=alt.Y("上架明细数", title="上架明细数"),
                    color="库区",
                ),
                use_container_width=True,
            )
            subtab3.altair_chart(
                alt.Chart(
                    df.groupby(["库区", "上架日期"])["验货上架天数"].mean().reset_index(name="平均验货上架天数"),
                    title=alt.TitleParams("平均验货上架天数", anchor="middle"),
                )
                .mark_line(point=True)
                .encode(
                    x=alt.X("yearmonthdate(上架日期):O", title="上架时间"),
                    y=alt.Y("平均验货上架天数", title="平均验货上架天数数"),
                    color="库区",
                    tooltip=[
                        "上架日期",
                        "库区",
                        alt.Tooltip("平均验货上架天数:Q", format=".1f"),
                    ],
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )
        with subtab4:
            df = get_data(shelf_data_query("分区"))
            subtab4.altair_chart(
                alt.Chart(
                    df.groupby(["分区", "上架日期"])["上架明细数"].sum().reset_index(name="上架明细数"),
                    title=alt.TitleParams("上架明细数", anchor="middle"),
                )
                .mark_line(point=True)
                .encode(
                    x=alt.X("yearmonthdate(上架日期):O", title="上架时间"),
                    y=alt.Y("上架明细数", title="上架明细数"),
                    color="分区",
                ),
                use_container_width=True,
            )
            subtab4.altair_chart(
                alt.Chart(
                    df.groupby(["分区", "上架日期"])["验货上架天数"].mean().reset_index(name="平均验货上架天数"),
                    title=alt.TitleParams("平均验货上架天数", anchor="middle"),
                )
                .mark_line(point=True)
                .encode(
                    x=alt.X("yearmonthdate(上架日期):O", title="上架时间"),
                    y=alt.Y("平均验货上架天数", title="平均验货上架天数数"),
                    color="分区",
                    tooltip=[
                        "上架日期",
                        "分区",
                        alt.Tooltip("平均验货上架天数:Q", format=".1f"),
                    ],
                )
                .configure_axis(labelLimit=400),
                use_container_width=True,
            )


update_receive()
update_inspect()
update_shelf()
