import streamlit as st
import pandas as pd
import io
import os
from datetime import datetime
import plotly.graph_objects as go

# 页面配置
st.set_page_config(
    page_title="门店报表查询系统", 
    page_icon="📊",
    layout="wide"
)

# 管理员密码（建议修改）
ADMIN_PASSWORD = "admin123"  # 请修改为您的密码

# 自定义CSS样式
st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        padding: 1rem 0;
    }
    .store-info {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
    }
    .admin-panel {
        background-color: #fff3cd;
        padding: 1rem;
        border-radius: 0.5rem;
        border: 1px solid #ffeaa7;
        margin: 1rem 0;
    }
    </style>
""", unsafe_allow_html=True)

# 初始化会话状态
def init_session_state():
    if 'logged_in' not in st.session_state:
        st.session_state.logged_in = False
        st.session_state.store_name = ""
        st.session_state.user_id = ""
        st.session_state.login_time = None
        st.session_state.is_admin = False
    
    # 持久化存储上传的文件
    if 'permissions_data' not in st.session_state:
        st.session_state.permissions_data = None
    if 'reports_data' not in st.session_state:
        st.session_state.reports_data = {}

init_session_state()

# 主标题
st.markdown('<h1 class="main-header">📊 门店报表查询系统</h1>', unsafe_allow_html=True)

# 侧边栏 - 根据用户类型显示不同内容
with st.sidebar:
    st.title("⚙️ 系统功能")
    
    # 用户类型选择
    user_type = st.radio(
        "选择用户类型",
        ["普通用户", "管理员"],
        help="管理员可上传文件，普通用户只能查询"
    )
    
    # 管理员功能
    if user_type == "管理员":
        st.divider()
        st.subheader("🔐 管理员登录")
        
        admin_password = st.text_input("管理员密码", type="password")
        
        if st.button("验证管理员身份"):
            if admin_password == ADMIN_PASSWORD:
                st.session_state.is_admin = True
                st.success("✅ 管理员验证成功！")
                st.rerun()
            else:
                st.error("❌ 密码错误！")
                st.session_state.is_admin = False
        
        # 管理员已登录
        if st.session_state.is_admin:
            st.divider()
            st.subheader("📁 文件管理")
            
            # 上传权限表
            permissions_file = st.file_uploader(
                "上传门店权限表", 
                type=['xlsx', 'xls'],
                help="包含门店名称和人员编号"
            )
            
            if permissions_file:
                try:
                    df = pd.read_excel(permissions_file)
                    st.session_state.permissions_data = df
                    st.success(f"✅ 权限表已上传：{len(df)} 条记录")
                except Exception as e:
                    st.error(f"读取权限表失败：{str(e)}")
            
            # 显示当前权限表状态
            if st.session_state.permissions_data is not None:
                st.info(f"📋 当前权限表：{len(st.session_state.permissions_data)} 条记录")
                if st.checkbox("查看权限表"):
                    st.dataframe(st.session_state.permissions_data)
            
            st.divider()
            
            # 上传财务报表
            reports_file = st.file_uploader(
                "上传财务报表", 
                type=['xlsx', 'xls'],
                help="包含多个门店Sheet的报表"
            )
            
            if reports_file:
                try:
                    excel_file = pd.ExcelFile(reports_file)
                    sheets = excel_file.sheet_names
                    
                    # 保存所有sheet数据
                    for sheet in sheets:
                        df = pd.read_excel(reports_file, sheet_name=sheet)
                        st.session_state.reports_data[sheet] = df
                    
                    st.success(f"✅ 报表已上传：{len(sheets)} 个门店")
                    st.info("包含的门店：" + ", ".join(sheets))
                except Exception as e:
                    st.error(f"读取报表失败：{str(e)}")
            
            # 显示当前报表状态
            if st.session_state.reports_data:
                st.info(f"📊 当前报表：{len(st.session_state.reports_data)} 个门店")
                if st.checkbox("查看已上传的门店"):
                    for store in st.session_state.reports_data.keys():
                        st.write(f"- {store}")
            
            st.divider()
            
            # 管理功能
            st.subheader("🛠️ 管理功能")
            
            if st.button("🗑️ 清空所有数据", type="secondary"):
                st.session_state.permissions_data = None
                st.session_state.reports_data = {}
                st.success("已清空所有数据")
                st.rerun()
            
            if st.button("🚪 退出管理员", type="secondary"):
                st.session_state.is_admin = False
                st.rerun()
    
    # 普通用户登录状态
    else:
        if st.session_state.logged_in:
            st.divider()
            st.subheader("👤 当前登录")
            st.info(f"门店：{st.session_state.store_name}")
            st.info(f"编号：{st.session_state.user_id}")
            if st.session_state.login_time:
                st.info(f"时间：{st.session_state.login_time}")
            
            if st.button("🚪 退出登录", use_container_width=True):
                st.session_state.logged_in = False
                st.session_state.store_name = ""
                st.session_state.user_id = ""
                st.session_state.login_time = None
                st.rerun()

# 主界面内容
if user_type == "管理员" and st.session_state.is_admin:
    # 管理员界面
    st.markdown("""
        <div class="admin-panel">
            <h3>👨‍💼 管理员控制面板</h3>
            <p>您可以在左侧边栏上传和管理文件</p>
        </div>
    """, unsafe_allow_html=True)
    
    # 显示系统状态
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric(
            "权限表状态", 
            "已上传" if st.session_state.permissions_data is not None else "未上传",
            len(st.session_state.permissions_data) if st.session_state.permissions_data is not None else 0
        )
    
    with col2:
        st.metric(
            "报表数量", 
            f"{len(st.session_state.reports_data)} 个门店",
            "已就绪" if st.session_state.reports_data else "未上传"
        )
    
    # 使用说明
    with st.expander("📖 管理员操作指南"):
        st.markdown("""
        ### 管理员操作步骤：
        
        1. **上传权限表**
           - Excel文件，包含两列：门店名称、人员编号
           - 每行代表一个有权限的用户
        
        2. **上传财务报表**
           - Excel文件，每个Sheet代表一个门店
           - Sheet名称应与权限表中的门店名称对应
        
        3. **查看状态**
           - 确认文件上传成功
           - 检查门店数量是否正确
        
        4. **通知用户**
           - 告知门店用户可以登录查询
           - 提供门店名称和人员编号
        
        ### 注意事项：
        - 上传新文件会覆盖旧文件
        - 建议定期更新报表数据
        - 请妥善保管管理员密码
        """)

elif user_type == "管理员" and not st.session_state.is_admin:
    # 提示输入管理员密码
    st.info("👈 请在左侧边栏输入管理员密码")

else:
    # 普通用户界面
    if not st.session_state.logged_in:
        # 登录界面
        st.subheader("🔐 用户登录")
        
        # 检查是否有权限数据
        if st.session_state.permissions_data is None:
            st.warning("⚠️ 系统暂无数据，请联系管理员上传文件")
        else:
            permissions_df = st.session_state.permissions_data
            
            if len(permissions_df.columns) >= 2:
                store_column = permissions_df.columns[0]
                id_column = permissions_df.columns[1]
                
                # 转换数据类型
                permissions_df[store_column] = permissions_df[store_column].astype(str)
                permissions_df[id_column] = permissions_df[id_column].astype(str)
                
                # 获取门店列表
                stores = sorted(permissions_df[store_column].unique().tolist())
                
                # 登录表单
                col1, col2, col3 = st.columns([1, 2, 1])
                
                with col2:
                    with st.form("login_form"):
                        selected_store = st.selectbox(
                            "选择门店", 
                            stores,
                            help="请选择您所属的门店"
                        )
                        
                        user_id = st.text_input(
                            "人员编号", 
                            placeholder="请输入您的人员编号",
                            help="请输入您的人员编号"
                        )
                        
                        submit = st.form_submit_button("登录", use_container_width=True)
                        
                        if submit:
                            if selected_store and user_id:
                                # 验证权限
                                user_check = permissions_df[
                                    (permissions_df[store_column] == selected_store) & 
                                    (permissions_df[id_column] == str(user_id))
                                ]
                                
                                if len(user_check) > 0:
                                    st.session_state.logged_in = True
                                    st.session_state.store_name = selected_store
                                    st.session_state.user_id = user_id
                                    st.session_state.login_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                    st.success("✅ 登录成功！")
                                    st.balloons()
                                    st.rerun()
                                else:
                                    st.error("❌ 门店或人员编号错误！请检查后重试。")
                            else:
                                st.warning("⚠️ 请填写完整的登录信息")
    
    else:
        # 已登录 - 显示报表
        st.markdown(f"""
            <div class="store-info">
                <h3>当前门店：{st.session_state.store_name}</h3>
                <p>操作员：{st.session_state.user_id} | 登录时间：{st.session_state.login_time}</p>
            </div>
        """, unsafe_allow_html=True)
        
        # 查找对应的报表
        matching_sheets = []
        for sheet_name in st.session_state.reports_data.keys():
            if (st.session_state.store_name in sheet_name or 
                sheet_name in st.session_state.store_name):
                matching_sheets.append(sheet_name)
        
        if matching_sheets:
            # 如果有多个匹配的sheet，让用户选择
            if len(matching_sheets) > 1:
                selected_sheet = st.selectbox(
                    "找到多个相关报表，请选择：", 
                    matching_sheets
                )
            else:
                selected_sheet = matching_sheets[0]
            
            # 获取报表数据
            df = st.session_state.reports_data[selected_sheet]
            
            # 报表显示和操作
            st.subheader(f"📊 {st.session_state.store_name} - 财务报表")
            
            # 搜索功能
            col1, col2 = st.columns([3, 1])
            with col1:
                search_term = st.text_input("🔍 搜索报表内容", placeholder="输入关键词搜索...")
            
            with col2:
                n_rows = st.selectbox("显示行数", [10, 25, 50, 100, "全部"])
            
            # 应用搜索过滤
            if search_term:
                mask = df.astype(str).apply(lambda x: x.str.contains(search_term, case=False, na=False)).any(axis=1)
                filtered_df = df[mask]
            else:
                filtered_df = df
            
            # 显示数据统计
            st.info(f"📈 共 {len(filtered_df)} 条记录")
            
            # 显示数据表
            if n_rows == "全部":
                st.dataframe(filtered_df, use_container_width=True)
            else:
                st.dataframe(filtered_df.head(n_rows), use_container_width=True)
            
            # 下载功能
            col1, col2, col3 = st.columns(3)
            
            with col1:
                # 下载完整报表
                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False, sheet_name=st.session_state.store_name)
                
                st.download_button(
                    label="📥 下载完整报表",
                    data=buffer.getvalue(),
                    file_name=f"{st.session_state.store_name}_财务报表_{datetime.now().strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
            
            with col2:
                # 下载筛选后的数据
                if search_term and len(filtered_df) > 0:
                    buffer_filtered = io.BytesIO()
                    with pd.ExcelWriter(buffer_filtered, engine='openpyxl') as writer:
                        filtered_df.to_excel(writer, index=False, sheet_name=st.session_state.store_name)
                    
                    st.download_button(
                        label="📥 下载筛选结果",
                        data=buffer_filtered.getvalue(),
                        file_name=f"{st.session_state.store_name}_筛选报表_{datetime.now().strftime('%Y%m%d')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
            
            with col3:
                # 下载CSV格式
                csv = df.to_csv(index=False, encoding='utf-8-sig')
                st.download_button(
                    label="📥 下载CSV格式",
                    data=csv,
                    file_name=f"{st.session_state.store_name}_报表_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
            
            # 简单的数据分析（可选）
            if st.checkbox("📊 显示基础统计"):
                try:
                    # 找出数值列
                    numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
                    if len(numeric_cols) > 0:
                        st.write("### 数值列统计")
                        st.dataframe(df[numeric_cols].describe())
                except:
                    st.info("无法生成统计信息")
        
        else:
            st.error(f"❌ 未找到门店 '{st.session_state.store_name}' 的报表")
            st.info("请联系管理员确认报表是否已上传")

# 页脚
st.divider()
st.markdown("""
    <div style="text-align: center; color: #888; font-size: 0.8rem;">
        门店报表查询系统 v3.0 - 权限分离版 | 技术支持：IT部门
    </div>
""", unsafe_allow_html=True)
