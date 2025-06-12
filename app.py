import streamlit as st
import pandas as pd
import io
from datetime import datetime

# 页面配置
st.set_page_config(
    page_title="门店报表查询系统",
    page_icon="📊",
    layout="wide"
)

# 标题
st.title("📊 门店报表查询系统")

# 初始化session state
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False
    st.session_state.current_store = None

# 侧边栏
with st.sidebar:
    st.header("📁 数据上传")
    
    permissions_file = st.file_uploader(
        "上传门店权限表",
        type=['xlsx', 'xls'],
        help="Excel文件：门店名称、人员编号"
    )
    
    reports_file = st.file_uploader(
        "上传财务报表总表", 
        type=['xlsx', 'xls'],
        help="Excel文件：每个门店一个sheet"
    )
    
    # 登出按钮
    if st.session_state.authenticated:
        if st.button("🚪 退出登录"):
            st.session_state.authenticated = False
            st.session_state.current_store = None
            st.rerun()

# 主要功能
if not st.session_state.authenticated:
    # 登录页面
    st.subheader("🔐 用户登录")
    
    if permissions_file is not None:
        try:
            # 读取权限表
            permissions_df = pd.read_excel(permissions_file)
            
            # 显示门店选择
            if len(permissions_df.columns) >= 2:
                store_col = permissions_df.columns[0]  # 第一列作为门店名称
                id_col = permissions_df.columns[1]     # 第二列作为人员编号
                
                store_names = sorted(permissions_df[store_col].unique().tolist())
                
                col1, col2 = st.columns(2)
                with col1:
                    selected_store = st.selectbox("选择门店", store_names)
                with col2:
                    employee_id = st.text_input("人员编号")
                
                if st.button("🔑 登录", type="primary"):
                    if selected_store and employee_id:
                        # 验证用户
                        user_exists = permissions_df[
                            (permissions_df[store_col] == selected_store) & 
                            (permissions_df[id_col] == employee_id)
                        ]
                        
                        if not user_exists.empty:
                            st.session_state.authenticated = True
                            st.session_state.current_store = selected_store
                            st.success(f"✅ 登录成功！欢迎来到 {selected_store}")
                            st.rerun()
                        else:
                            st.error("❌ 门店和人员编号不匹配！")
                    else:
                        st.warning("⚠️ 请输入完整信息！")
            else:
                st.error("❌ 权限表格式错误，请确保至少有两列：门店名称、人员编号")
                
        except Exception as e:
            st.error(f"❌ 读取权限表出错：{str(e)}")
    else:
        st.info("📋 请先上传门店权限表")

else:
    # 已登录状态
    current_store = st.session_state.current_store
    
    st.success(f"✅ 已登录：{current_store}")
    st.subheader(f"📊 {current_store} 财务报表")
    
    if reports_file is not None:
        try:
            # 读取报表文件
            excel_file = pd.ExcelFile(reports_file)
            
            # 查找对应门店的sheet
            store_sheet = None
            for sheet_name in excel_file.sheet_names:
                if current_store in sheet_name:
                    store_sheet = sheet_name
                    break
            
            if store_sheet:
                # 读取并显示报表
                df = pd.read_excel(reports_file, sheet_name=store_sheet)
                st.dataframe(df, use_container_width=True)
                
                # 下载功能
                st.subheader("💾 导出报表")
                
                # 创建下载文件
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    df.to_excel(writer, sheet_name=current_store, index=False)
                
                excel_data = output.getvalue()
                
                st.download_button(
                    label=f"📥 下载 {current_store} 报表",
                    data=excel_data,
                    file_name=f"{current_store}_财务报表_{datetime.now().strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            else:
                st.error(f"❌ 未找到门店 '{current_store}' 的报表数据")
                st.info("💡 请确保Excel文件中有包含门店名称的sheet页")
                
        except Exception as e:
            st.error(f"❌ 读取报表文件出错：{str(e)}")
    else:
        st.info("📋 请上传财务报表总表")

# 底部信息
st.markdown("---")
st.markdown("**系统说明：** 上传权限表和报表文件，选择门店并输入人员编号即可查看对应的财务报表。")
