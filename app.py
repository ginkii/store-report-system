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
st.title("📊 门店报表查询系统（简化版）")

# 初始化会话状态
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
    st.session_state.store_name = ""

# 侧边栏
with st.sidebar:
    st.title("⚙️ 系统设置")
    
    # 文件上传
    st.subheader("📁 文件上传")
    
    permissions_file = st.file_uploader(
        "上传门店权限表", 
        type=['xlsx', 'xls']
    )
    
    reports_file = st.file_uploader(
        "上传财务报表", 
        type=['xlsx', 'xls']
    )
    
    # 登录状态
    if st.session_state.logged_in:
        st.success(f"✅ 已登录：{st.session_state.store_name}")
        if st.button("退出登录"):
            st.session_state.logged_in = False
            st.session_state.store_name = ""
            st.rerun()

# 主界面
if not st.session_state.logged_in:
    # 登录界面
    st.subheader("🔐 用户登录")
    
    if permissions_file:
        try:
            # 读取权限表
            permissions_df = pd.read_excel(permissions_file)
            
            if len(permissions_df.columns) >= 2:
                store_column = permissions_df.columns[0]
                id_column = permissions_df.columns[1]
                
                stores = permissions_df[store_column].unique().tolist()
                
                col1, col2, col3 = st.columns([1,2,1])
                with col2:
                    selected_store = st.selectbox("选择门店", stores)
                    user_id = st.text_input("输入人员编号")
                    
                    if st.button("登录", use_container_width=True):
                        if selected_store and user_id:
                            user_check = permissions_df[
                                (permissions_df[store_column] == selected_store) & 
                                (permissions_df[id_column] == user_id)
                            ]
                            
                            if len(user_check) > 0:
                                st.session_state.logged_in = True
                                st.session_state.store_name = selected_store
                                st.success("登录成功！")
                                st.rerun()
                            else:
                                st.error("门店或人员编号错误！")
                        else:
                            st.warning("请填写完整信息")
        except Exception as e:
            st.error(f"读取权限表出错：{str(e)}")
    else:
        st.info("请先上传门店权限表")

else:
    # 已登录状态
    st.success(f"当前门店：{st.session_state.store_name}")
    
    if reports_file:
        try:
            # 获取所有sheet名称
            excel_file = pd.ExcelFile(reports_file)
            sheet_names = excel_file.sheet_names
            
            # 查找匹配的sheet
            matching_sheets = []
            for sheet in sheet_names:
                if (st.session_state.store_name in sheet or 
                    sheet in st.session_state.store_name):
                    matching_sheets.append(sheet)
            
            if matching_sheets:
                selected_sheet = st.selectbox("选择报表", matching_sheets)
                
                # 读取数据的不同方式
                read_method = st.radio(
                    "选择读取方式（如果显示错误，请尝试其他方式）",
                    ["自动", "单行表头", "双行表头", "原始数据"]
                )
                
                try:
                    if read_method == "自动":
                        # 尝试自动检测
                        df = pd.read_excel(reports_file, sheet_name=selected_sheet)
                        # 处理重复列名
                        if df.columns.duplicated().any():
                            cols = list(df.columns)
                            new_cols = []
                            counts = {}
                            for col in cols:
                                if col in counts:
                                    counts[col] += 1
                                    new_cols.append(f"{col}_{counts[col]}")
                                else:
                                    counts[col] = 0
                                    new_cols.append(col)
                            df.columns = new_cols
                    
                    elif read_method == "单行表头":
                        df = pd.read_excel(reports_file, sheet_name=selected_sheet, header=0)
                    
                    elif read_method == "双行表头":
                        # 读取原始数据
                        df_raw = pd.read_excel(reports_file, sheet_name=selected_sheet, header=None)
                        # 合并前两行作为列名
                        if len(df_raw) >= 2:
                            new_cols = []
                            for i in range(len(df_raw.columns)):
                                col1 = str(df_raw.iloc[0, i]) if pd.notna(df_raw.iloc[0, i]) else ""
                                col2 = str(df_raw.iloc[1, i]) if pd.notna(df_raw.iloc[1, i]) else ""
                                if col2 and col2 != 'nan':
                                    new_cols.append(f"{col1}_{col2}")
                                else:
                                    new_cols.append(col1 if col1 != 'nan' else f"列{i}")
                            df = df_raw.iloc[2:].copy()
                            df.columns = new_cols
                            df.reset_index(drop=True, inplace=True)
                    
                    else:  # 原始数据
                        df = pd.read_excel(reports_file, sheet_name=selected_sheet, header=None)
                    
                    # 显示数据信息
                    st.info(f"数据形状：{df.shape[0]} 行 × {df.shape[1]} 列")
                    
                    # 搜索功能
                    search_term = st.text_input("🔍 搜索内容")
                    if search_term:
                        mask = df.astype(str).apply(lambda x: x.str.contains(search_term, case=False, na=False)).any(axis=1)
                        df_display = df[mask]
                    else:
                        df_display = df
                    
                    # 显示数据
                    st.dataframe(df_display, use_container_width=True)
                    
                    # 下载功能
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        # Excel下载
                        buffer = io.BytesIO()
                        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                            df_display.to_excel(writer, index=False)
                        
                        st.download_button(
                            label="📥 下载Excel",
                            data=buffer.getvalue(),
                            file_name=f"{st.session_state.store_name}_报表_{datetime.now().strftime('%Y%m%d')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                    
                    with col2:
                        # CSV下载
                        csv = df_display.to_csv(index=False, encoding='utf-8-sig')
                        st.download_button(
                            label="📥 下载CSV",
                            data=csv,
                            file_name=f"{st.session_state.store_name}_报表_{datetime.now().strftime('%Y%m%d')}.csv",
                            mime="text/csv"
                        )
                    
                except Exception as e:
                    st.error(f"读取报表出错：{str(e)}")
                    st.info("请尝试切换不同的读取方式")
                    
                    # 显示错误详情
                    with st.expander("查看错误详情"):
                        st.code(str(e))
                        
            else:
                st.error(f"未找到门店 '{st.session_state.store_name}' 的报表")
                st.info("可用的报表：")
                for sheet in sheet_names:
                    st.write(f"- {sheet}")
                    
        except Exception as e:
            st.error(f"处理文件时出错：{str(e)}")
    else:
        st.info("请上传财务报表文件")

# 页脚
st.divider()
st.markdown("门店报表查询系统 v2.1 (简化版) | 技术支持：IT部门")
