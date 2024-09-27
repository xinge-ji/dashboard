import streamlit as st

pages = {
    "实时业务数据": [
        st.Page("realTime.py", title="验收上架", icon="🕙")
    ],
    "业务环节": [
        st.Page("inspection.py", title="验收板块", icon='🔍'),
        st.Page('shelving.py', title="上架板块", icon='🪜')
    ]
}

pg = st.navigation(pages)
st.set_page_config(page_title="验收上架数据看板", page_icon=":bus:", layout="wide")
pg.run()