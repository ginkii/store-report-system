import streamlit as st
import pandas as pd
import io
import os
import json
from datetime import datetime
import plotly.graph_objects as go
import plotly.express as px

# 页面配置
st.set_page_config(
    page_title="门店报表查询系统", 
    page_icon="📊",
    layout="wide"
)

# 管理员密码（建议修改）
ADMIN_PASSWORD = "admin123"  # 请修改为您的密码

# 数据文件路径（用于持久化存储）
DATA_DIR = "report_data"
PERMISSIONS_FILE = os.path.join(DATA_DIR, "permissions.json")
REPORTS_FILE = os.path.join(DATA_DIR, "reports.json")

# 创建数据目录
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

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
    .metric-card {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 0.5rem;
        border: 1px solid #dee2e6;
        margin: 0.5rem 0;
    }
    </style>
""", unsafe_allow_html=True)

# 数据持久化函数
def save_permissions_data(data):
    """保存权限数据到文件"""
    if data is not None:
        data_dict = data.to_dict()
        with open(PERMISSIONS_FILE, 'w', encoding='utf-8') as f:
            json.dump(data_dict, f, ensure_ascii=False)

def load_permissions_data():
    """从文件加载权限数据"""
    if os.path.exists(PERMISSIONS_FILE):
        try:
            with open(PERMISSIONS_FILE, 'r', encoding='utf-8') as f:
                data_dict = json.load(f)
            return pd.DataFrame(data_dict)
        except:
            return None
    return None

def save_reports_data(data):
    """保存报表数据到文件"""
    if data:
        # 将DataFrame转换为可JSON序列化的格式
        data_dict = {}
        for sheet_name, df in data.items():
            data_dict[sheet_name] = df.to_dict()
        with open(REPORTS_FILE, 'w', encoding='utf-8') as f:
            json.dump(data_dict, f, ensure_ascii=False)

def load_reports_data():
    """从文件加载报表数据"""
    if os.path.exists(REPORTS_FILE):
        try:
            with open(REPORTS_FILE, 'r', encoding='utf-8') as f:
                data_dict = json.load(f)
            # 将字典转换回DataFrame
            reports_data = {}
            for sheet_name, sheet_dict in data_dict.items():
                reports_data[sheet_name] = pd.DataFrame(sheet_dict)
            return reports_data
        except:
            return {}
    return {}

# 初始化会话状态
def init_session_state():
    if 'logged_in' not in st.session_state:
        st.session_state.logged_in = False
        st.session_state.store_name = ""
        st.session_state.user_id = ""
        st.session_state.login_time = None
        st.session_state.is_admin = False
    
    # 从文件加载数据
    if 'permissions_data' not in st.session_state:
        st.session_state.permissions_data = load_permissions_data()
    if 'reports_data' not in st.session_state:
        st.session_state.reports_data = load_reports_data()

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
                    save_permissions_data(df)  # 保存到文件
                    st.success(f"✅ 权限表已上传并保存：{len(df)} 条记录")
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
                    reports_data = {}
                    for sheet in sheets:
                        df = pd.read_excel(reports_file, sheet_name=sheet)
                        reports_data[sheet] = df
                    
                    st.session_state.reports_data = reports_data
                    save_reports_data(reports_data)  # 保存到文件
                    
                    st.success(f"✅ 报表已上传并保存：{len(sheets)} 个门店")
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
                # 删除文件
                if os.path.exists(PERMISSIONS_FILE):
                    os.remove(PERMISSIONS_FILE)
                if os.path.exists(REPORTS_FILE):
                    os.remove(REPORTS_FILE)
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
        
        3. **数据持久化**
           - 上传的数据会自动保存
           - 其他电脑的用户可以直接访问
        
        4. **通知用户**
           - 告知门店用户可以登录查询
           - 提供门店名称和人员编号
        
        ### 注意事项：
        - 上传新文件会覆盖旧文件
        - 建议定期更新报表数据
        - 请妥善保管管理员密码
        - 数据会保存在服务器上，所有用户共享
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
            
            # 数据分析和可视化
            if st.checkbox("📊 显示数据分析和可视化"):
                try:
                    st.write("### 财务指标分析")
                    
                    # 查找关键财务指标
                    first_col = df.columns[0] if len(df.columns) > 0 else None
                    
                    if first_col:
                        # 月份列表
                        months = ['1月', '2月', '3月', '4月', '5月', '6月', 
                                 '7月', '8月', '9月', '10月', '11月', '12月']
                        
                        # 显示列信息帮助调试
                        with st.expander("📋 查看报表列结构"):
                            col_info = []
                            for i, col in enumerate(df.columns):
                                col_info.append(f"第{i+1}列: {col}")
                            st.write("\n".join(col_info[:20]))  # 显示前20列
                        
                        # 初始化数据存储
                        monthly_data = {
                            '毛利-线上': {},
                            '净利润': {},
                            '应收-收取金额': {},
                            '已分润款': {},
                            '应收-未收额': {}
                        }
                        
                        # 用于存储应收-未收额的合计值
                        uncollected_total = 0
                        uncollected_row_found = False
                        
                        # 查找指标行
                        for idx, row in df.iterrows():
                            row_name = str(row[first_col]) if pd.notna(row[first_col]) else ""
                            
                            # 查找四个主要指标
                            metric_mapping = {
                                '三. 毛利-线上': '毛利-线上',
                                '五. 净利润': '净利润',
                                '应收-收取金额': '应收-收取金额',
                                '已分润款': '已分润款'
                            }
                            
                            # 处理月度数据指标
                            for key, metric_name in metric_mapping.items():
                                if key in row_name:
                                    # 遍历所有列，根据列名识别月份
                                    for col in df.columns[1:]:
                                        col_str = str(col)
                                        
                                        # 跳过合计列
                                        if '合计' in col_str:
                                            continue
                                        
                                        # 识别月份
                                        for month in months:
                                            if month in col_str:
                                                try:
                                                    val = row[col]
                                                    if pd.notna(val):
                                                        # 处理各种数值格式
                                                        val_str = str(val).replace(',', '').replace(' ', '')
                                                        if val_str.replace('.', '').replace('-', '').isdigit():
                                                            num_val = float(val_str)
                                                            monthly_data[metric_name][month] = num_val
                                                except:
                                                    pass
                                                break
                                    break
                            
                            # 特别处理应收-未收额（从合计列获取）
                            if '应收-未收额' in row_name or '应收未收额' in row_name:
                                uncollected_row_found = True
                                # 查找合计列
                                for col in df.columns[1:]:
                                    col_str = str(col)
                                    if '合计' in col_str:
                                        try:
                                            val = row[col]
                                            if pd.notna(val):
                                                val_str = str(val).replace(',', '').replace(' ', '')
                                                if val_str.replace('.', '').replace('-', '').isdigit():
                                                    uncollected_total = float(val_str)
                                        except:
                                            pass
                                        break
                        
                        # 如果没有找到应收-未收额行，计算每月的应收-未收额
                        for month in months:
                            receivable = monthly_data['应收-收取金额'].get(month, 0)
                            distributed = monthly_data['已分润款'].get(month, 0)
                            if receivable != 0 or distributed != 0:
                                monthly_data['应收-未收额'][month] = receivable - distributed
                        
                        # 创建数据框用于可视化
                        viz_data = []
                        
                        # 只包含前四个指标的月度数据
                        display_metrics = ['毛利-线上', '净利润', '应收-收取金额', '已分润款']
                        for metric in display_metrics:
                            data = monthly_data[metric]
                            for month, value in data.items():
                                if value != 0:  # 只包含有数据的月份
                                    month_num = months.index(month) + 1 if month in months else int(month.replace('月', ''))
                                    viz_data.append({
                                        '月份': f"{month_num:02d}月",
                                        '指标': metric,
                                        '金额': value
                                    })
                        
                        # 应收-未收额单独处理
                        uncollected_viz_data = []
                        for month, value in monthly_data['应收-未收额'].items():
                            if value != 0:
                                month_num = months.index(month) + 1 if month in months else int(month.replace('月', ''))
                                uncollected_viz_data.append({
                                    '月份': f"{month_num:02d}月",
                                    '指标': '应收-未收额(计算值)',
                                    '金额': value
                                })
                        
                        if viz_data:
                            viz_df = pd.DataFrame(viz_data)
                            
                            # 创建选项卡
                            tab1, tab2, tab3, tab4 = st.tabs(["📊 月度指标", "📈 趋势分析", "💰 应收未收额", "📋 数据表"])
                            
                            with tab1:
                                # 柱状图 - 四个主要指标
                                fig = px.bar(
                                    viz_df, 
                                    x='月份', 
                                    y='金额', 
                                    color='指标',
                                    title='月度财务指标对比（毛利、净利润、应收、已分润）',
                                    labels={'金额': '金额 (元)'},
                                    barmode='group'
                                )
                                fig.update_layout(height=500)
                                st.plotly_chart(fig, use_container_width=True)
                            
                            with tab2:
                                # 折线图 - 趋势分析
                                fig2 = px.line(
                                    viz_df, 
                                    x='月份', 
                                    y='金额', 
                                    color='指标',
                                    title='月度财务指标趋势',
                                    labels={'金额': '金额 (元)'},
                                    markers=True
                                )
                                fig2.update_layout(height=500)
                                st.plotly_chart(fig2, use_container_width=True)
                            
                            with tab3:
                                # 应收未收额分析
                                st.write("#### 应收-未收额分析")
                                
                                # 显示合计值
                                if uncollected_total != 0:
                                    col1, col2 = st.columns([1, 2])
                                    with col1:
                                        if uncollected_total < 0:
                                            st.metric("应收-未收额(合计列)", f"¥{uncollected_total:,.2f}", "门店应收款", delta_color="inverse")
                                        else:
                                            st.metric("应收-未收额(合计列)", f"¥{uncollected_total:,.2f}", "门店应付款")
                                    with col2:
                                        st.info("此数据来自报表的'合计'列中的'应收-未收额'行")
                                
                                # 显示月度计算值
                                if uncollected_viz_data:
                                    st.write("##### 月度应收-未收额（计算值）")
                                    uncollected_df = pd.DataFrame(uncollected_viz_data)
                                    
                                    # 柱状图
                                    fig3 = px.bar(
                                        uncollected_df,
                                        x='月份',
                                        y='金额',
                                        title='月度应收-未收额（应收-收取金额 减 已分润款）',
                                        labels={'金额': '金额 (元)'},
                                        color='金额',
                                        color_continuous_scale=['red', 'yellow', 'green'],
                                        color_continuous_midpoint=0
                                    )
                                    fig3.update_layout(height=400)
                                    st.plotly_chart(fig3, use_container_width=True)
                                    
                                    # 月度明细表
                                    monthly_uncollected = pd.DataFrame([
                                        {
                                            '月份': month,
                                            '应收-收取金额': monthly_data['应收-收取金额'].get(month, 0),
                                            '已分润款': monthly_data['已分润款'].get(month, 0),
                                            '应收-未收额': monthly_data['应收-未收额'].get(month, 0)
                                        }
                                        for month in months
                                        if month in monthly_data['应收-收取金额'] or month in monthly_data['已分润款']
                                    ])
                                    
                                    if not monthly_uncollected.empty:
                                        monthly_uncollected['应收-收取金额'] = monthly_uncollected['应收-收取金额'].apply(lambda x: f"¥{x:,.2f}")
                                        monthly_uncollected['已分润款'] = monthly_uncollected['已分润款'].apply(lambda x: f"¥{x:,.2f}")
                                        monthly_uncollected['应收-未收额'] = monthly_uncollected['应收-未收额'].apply(lambda x: f"¥{x:,.2f}")
                                        st.dataframe(monthly_uncollected, use_container_width=True)
                            
                            with tab4:
                                # 数据透视表
                                pivot_df = viz_df.pivot(index='月份', columns='指标', values='金额').fillna(0)
                                
                                # 格式化显示
                                formatted_df = pivot_df.applymap(lambda x: f"¥{x:,.2f}")
                                st.dataframe(formatted_df, use_container_width=True)
                                
                                # 汇总统计
                                st.write("#### 📊 汇总统计")
                                summary_data = []
                                
                                # 只统计四个主要指标
                                for metric in display_metrics:
                                    values = list(monthly_data[metric].values())
                                    if values:
                                        summary_data.append({
                                            '指标': metric,
                                            '总计': sum(values),
                                            '平均值': sum(values) / len(values),
                                            '最大值': max(values),
                                            '最小值': min(values)
                                        })
                                
                                if summary_data:
                                    summary_df = pd.DataFrame(summary_data)
                                    summary_df['总计'] = summary_df['总计'].apply(lambda x: f"¥{x:,.2f}")
                                    summary_df['平均值'] = summary_df['平均值'].apply(lambda x: f"¥{x:,.2f}")
                                    summary_df['最大值'] = summary_df['最大值'].apply(lambda x: f"¥{x:,.2f}")
                                    summary_df['最小值'] = summary_df['最小值'].apply(lambda x: f"¥{x:,.2f}")
                                    
                                    st.dataframe(summary_df, use_container_width=True)
                                
                                # 显示数据提取说明
                                with st.expander("💡 数据提取说明"):
                                    st.markdown("""
                                    - **月份识别**：通过列名中的"1月"、"2月"等文字识别
                                    - **四个主要指标**：从对应行提取月度数据
                                      - 三. 毛利-线上
                                      - 五. 净利润
                                      - 应收-收取金额
                                      - 已分润款
                                    - **应收-未收额**：
                                      - 合计值：从"合计"列的"应收-未收额"行提取
                                      - 月度值：通过"应收-收取金额"减"已分润款"计算
                                    - **负值说明**：应收-未收额为负表示门店应收款
                                    """)
                        else:
                            st.info("未找到可分析的月度数据，请检查报表格式是否包含月份列和指定的财务指标")
                        
                except Exception as e:
                    st.error(f"分析时出错：{str(e)}")
                    st.info("提示：请确保报表格式正确，包含月份列")
        
        else:
            st.error(f"❌ 未找到门店 '{st.session_state.store_name}' 的报表")
            st.info("请联系管理员确认报表是否已上传")

# 页脚
st.divider()
st.markdown("""
    <div style="text-align: center; color: #888; font-size: 0.8rem;">
        门店报表查询系统 v4.0 - 数据持久化增强版 | 技术支持：IT部门
    </div>
""", unsafe_allow_html=True)
