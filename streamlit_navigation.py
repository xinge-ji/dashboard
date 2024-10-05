import altair as alt
import streamlit as st
import streamlit_analytics2 as streamlit_analytics

inbound_realtime_page = st.Page("inbound_process/realtime.py", title="入库实时看板")


inbound_history_page = st.Page("inbound_process/history.py", title="入库历史看板")


def page3():
    st.write(st.session_state.foo)


def page4():
    st.write(st.session_state.foo)


def page5():
    st.write(st.session_state.foo)


def page6():
    st.write(st.session_state.foo)


def page7():
    st.write(st.session_state.foo)


pages = {
    "": [st.Page("index.py", title="首页", icon="🏠")],
    "仓库入库": [inbound_realtime_page, inbound_history_page],
    "仓库出库": [st.Page(page4, title="Learn about us"), st.Page(page5, title="Try it out")],
    "物流配送": [st.Page(page6, title="车辆装载率"), st.Page(page7, title="司机工作量")],
    "基础数据": [
        st.Page("pub/product_dimension_anomaly_tracker.py", title="商品包装长宽高数据异常监控", icon="📐"),
        st.Page(page3, title="客户登记配送地点与GPS显示送货地点差异监控", icon="📍"),
    ],
}

pg = st.navigation(pages)
st.set_page_config(page_title="物流数据看板", page_icon=":bus:", layout="wide")

# 隐藏右上角的deploy按钮和mainmenu，向上移动整体container
st.markdown(
    """
    <style>
        .stMainBlockContainer {
            margin-top: -3em;
        }
        #MainMenu {visibility: hidden;}
        .stAppDeployButton {display:none;}
        footer {visibility: hidden;}
        #stDecoration {display:none;}
    </style>
""",
    unsafe_allow_html=True,
)


# Chart主题
def my_theme():
    return {
        "config": {
            "view": {"continuousHeight": 450, "continuousWidth": 600},
            "range": {"category": {"scheme": "category10"}},
        }
    }


alt.themes.register("my_theme", my_theme)
alt.themes.enable("my_theme")


# 运行
def main():
    pg.run()


with streamlit_analytics.track():
    main()
