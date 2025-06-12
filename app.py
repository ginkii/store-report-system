import streamlit as st
import pandas as pd
import io

# 页面设置
st.set_page_config(page_title="门店报表查询系统", page_icon="📊")

# 主标题
st.title("📊 门店报表查询系统")

# 初始化状态
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
    st.session_state.store_name = ""

# 侧边栏文件上传
st.sidebar.title("📁 文件上传")

permissions_file = st.sidebar.file_uploader(
    "上传门店权限表", 
    type=['xlsx', 'xls']
)

reports_file = st.sidebar.file_uploader(
    "上传财务报表", 
    type=['xlsx', 'xls']
)

# 登出按钮
if st.session_state.logged_in:
    if st.sidebar.button("退出登录"):
        st.session_state.logged_in = False
        st.session_state.store_name = ""
        st.experimental_rerun()

# 主界面
if not st.session_state.logged_in:
    st.subheader("🔐 用户登录")
    
    if permissions_file:
        # 读取权限表
        permissions_df = pd.read_excel(permissions_file)
        
        if len(permissions_df.columns) >= 2:
            store_column = permissions_df.columns[0]
            id_column = permissions_df.columns[1]
            
            stores = permissions_df[store_column].unique().tolist()
            
            selected_store = st.selectbox("选择门店", stores)
            user_id = st.text_input("输入人员编号")
            
            if st.button("登录"):
                if selected_store and user_id:
                    # 检查权限
                    user_check = permissions_df[
                        (permissions_df[store_column] == selected_store) & 
                        (permissions_df[id_column] == user_id)
                    ]
                    
                    if len(user_check) > 0:
                        st.session_state.logged_in = True
                        st.session_state.store_name = selected_store
                        st.success("登录成功！")
                        st.experimental_rerun()
                    else:
                        st.error("门店或人员编号错误！")
        else:
            st.error("权限表格式错误")
    else:
        st.info("请先上传门店权限表")

else:
    # 已登录状态
    st.success(f"当前门店：{st.session_state.store_name}")
    
    if reports_file:
        # 读取报表
        excel_file = pd.ExcelFile(reports_file)
        
        # 找到对应门店的sheet
        target_sheet = None
        for sheet in excel_file.sheet_names:
            if st.session_state.store_name in sheet:
                target_sheet = sheet
                break
        
        if target_sheet:
            df = pd.read_excel(reports_file, sheet_name=target_sheet)
            
            st.subheader(f"{st.session_state.store_name} 财务报表")
            st.dataframe(df)
            
            # 下载按钮
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                df.to_excel(writer, index=False)
            
            st.download_button(
                label="下载报表",
                data=buffer.getvalue(),
                file_name=f"{st.session_state.store_name}_报表.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.error(f"未找到 {st.session_state.store_name} 的报表")
    else:
        st.info("请上传财务报表文件")