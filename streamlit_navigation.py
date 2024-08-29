import streamlit as st


def page1():
    st.write(st.session_state.foo)

def page2():
    st.write(st.session_state.foo)
    
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
    "基础数据": [
        st.Page("pub/product_dimension_anomaly_tracker.py", title="商品包装长宽高数据异常监控", icon="📐"),
        st.Page(page1, title="客户登记配送地点与GPS显示送货地点差异监控", icon="📍")
    ],
    "仓库入库": [
        st.Page(page2, title="Learn about us"),
        st.Page(page3, title="Try it out")
    ],
    "仓库出库": [
        st.Page(page4, title="Learn about us"),
        st.Page(page5, title="Try it out")
    ],
    "物流配送": [
        st.Page(page6, title="车辆装载率"),
        st.Page(page7, title="司机工作量")
    ],
}

pg = st.navigation(pages)
st.set_page_config(page_title="物流数据看板", page_icon=":bus:", layout="wide")
pg.run()
