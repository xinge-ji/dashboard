import streamlit as st

pages = {
    "实时数据展示": [
        st.Page("realTime.py", title="复核出库")
    ],
    "业务环节": [
        st.Page("check.py", title="复核出库")
    ]
}

pg = st.navigation(pages)
st.set_page_config(page_title="复核出库数据看板", page_icon=":bus:", layout="wide")
pg.run()