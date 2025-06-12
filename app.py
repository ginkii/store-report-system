import streamlit as st
import pandas as pd
import io
import hashlib
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go

# 页面配置
st.set_page_config(
    page_title="门店报表查询系统", 
    page_icon="📊",
    layout="wide"
)

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
    .metric-highlight {
        background-color: #ffe6e6;
        padding: 0.5rem;
        border-radius: 0.3rem;
        border-left: 4px solid #ff4444;
    }
    </style>
""", unsafe_allow_html=True)

# 辅助函数：自定义指标分析
def generic_custom_analysis(df, selected_items):
    """分析用户选择的财务指标"""
    # 准备月份数据
    months_data = {}
    month_order = ['1月', '2月', '3月', '4月', '5月', '6月', '7月', '8月', '9月', '10月', '11月', '12月']
    
    # 识别月份列
    for col in df.columns[1:]:
        col_str = str(col)
        if any(keyword in col_str.lower() for keyword in ['合计', '总计', 'total']):
            continue
        for month in month_order:
            if month in col_str:
                if month not in months_data:
                    months_data[month] = []
                months_data[month].append(col)
                break
    
    sorted_months = [m for m in month_order if m in months_data]
    
    if sorted_months:
        fig = go.Figure()
        colors = ['lightblue', 'lightgreen', 'lightcoral', 'lightyellow', 'lightpink']
        
        for idx, item in enumerate(selected_items):
            # 查找该项目所在的行
            item_row = None
            first_col = df.columns[0]
            for i, row in df.iterrows():
                if str(row[first_col]) == item:
                    item_row = i
                    break
            
            if item_row is not None:
                values = []
                for month in sorted_months:
                    month_value = 0
                    for col in months_data[month]:
                        try:
                            val = df.loc[item_row, col]
                            if pd.notna(val) and str(val).strip() not in ['', '-']:
                                val_str = str(val).replace(',', '').replace(' ', '')
                                month_value += float(val_str)
                        except:
                            pass
                    values.append(month_value)
                
                fig.add_trace(go.Scatter(
                    x=sorted_months,
                    y=values,
                    name=item,
                    mode='lines+markers',
                    line=dict(width=3),
                    marker=dict(size=10),
                ))
        
        fig.update_layout(
            title='自定义财务指标趋势',
            xaxis_title='月份',
            yaxis_title='金额（元）',
            hovermode='x unified',
            height=500
        )
        
        st.plotly_chart(fig, use_container_width=True)

# 辅助函数：通用趋势分析
def generic_trend_analysis(df, month_cols):
    """当找不到特定财务指标时的通用分析"""
    selected_month_col = st.selectbox("选择月份列", month_cols)
    
    # 找出所有数值列
    numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns.tolist()
    
    if numeric_cols:
        # 让用户选择要分析的指标
        selected_metrics = st.multiselect(
            "选择要分析的财务指标", 
            numeric_cols,
            default=numeric_cols[:2] if len(numeric_cols) >= 2 else numeric_cols
        )
        
        if selected_metrics:
            # 准备数据
            month_list = []
            for col in df.columns:
                if col != df.columns[0]:  # 排除第一列（通常是指标名称）
                    col_str = str(col).lower()
                    if not any(exclude in col_str for exclude in ['合计', '总计', 'total', 'sum']):
                        month_list.append(col)
            
            # 创建图表
            fig = go.Figure()
            
            colors = ['lightblue', 'lightgreen', 'lightcoral', 'lightyellow', 'lightpink']
            
            for i, metric in enumerate(selected_metrics):
                values = []
                for month in month_list:
                    try:
                        value = float(df[month][df[df.columns[0]] == metric].values[0])
                    except:
                        value = 0
                    values.append(value)
                
                fig.add_trace(go.Scatter(
                    x=month_list,
                    y=values,
                    name=metric,
                    mode='lines+markers',
                    line=dict(width=2),
                    marker=dict(size=8),
                ))
            
            fig.update_layout(
                title=f'{st.session_state.store_name} - 财务指标趋势',
                xaxis_title='月份',
                yaxis_title='金额（元）',
                hovermode='x unified',
                height=500
            )
            
            st.plotly_chart(fig, use_container_width=True)

# 初始化会话状态
def init_session_state():
    if 'logged_in' not in st.session_state:
        st.session_state.logged_in = False
        st.session_state.store_name = ""
        st.session_state.user_id = ""
        st.session_state.login_time = None

init_session_state()

# 主标题
st.markdown('<h1 class="main-header">📊 门店报表查询系统</h1>', unsafe_allow_html=True)

# 侧边栏配置
with st.sidebar:
    st.title("⚙️ 系统配置")
    
    # 管理员密码验证（可选功能）
    admin_mode = st.checkbox("管理员模式")
    if admin_mode:
        admin_password = st.text_input("管理员密码", type="password")
        # 这里使用简单的密码验证，实际应用中应该使用更安全的方式
        if admin_password == "admin123":  # 请修改为实际密码
            st.success("管理员模式已启用")
        else:
            admin_mode = False
    
    st.divider()
    
    # 文件上传区域
    st.subheader("📁 文件上传")
    
    # 权限表上传
    permissions_file = st.file_uploader(
        "上传门店权限表", 
        type=['xlsx', 'xls', 'csv'],
        help="请上传包含门店名称和人员编号的权限表"
    )
    
    # 报表文件上传
    reports_file = st.file_uploader(
        "上传财务报表", 
        type=['xlsx', 'xls'],
        help="请上传包含多个门店Sheet的财务报表"
    )
    
    st.divider()
    
    # 登录状态显示
    if st.session_state.logged_in:
        st.subheader("👤 当前登录")
        st.info(f"门店：{st.session_state.store_name}")
        st.info(f"编号：{st.session_state.user_id}")
        if st.session_state.login_time:
            st.info(f"登录时间：{st.session_state.login_time}")
        
        if st.button("🚪 退出登录", use_container_width=True):
            for key in ['logged_in', 'store_name', 'user_id', 'login_time']:
                st.session_state[key] = False if key == 'logged_in' else ""
            st.rerun()

# 主界面逻辑
if not st.session_state.logged_in:
    # 登录界面
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.subheader("🔐 用户登录")
        
        if permissions_file:
            try:
                # 读取权限表
                if permissions_file.name.endswith('.csv'):
                    permissions_df = pd.read_csv(permissions_file)
                else:
                    permissions_df = pd.read_excel(permissions_file)
                
                # 数据预处理：去除空值和重复值
                permissions_df = permissions_df.dropna()
                permissions_df = permissions_df.drop_duplicates()
                
                if len(permissions_df.columns) >= 2:
                    # 获取列名
                    store_column = permissions_df.columns[0]
                    id_column = permissions_df.columns[1]
                    
                    # 转换数据类型
                    permissions_df[store_column] = permissions_df[store_column].astype(str)
                    permissions_df[id_column] = permissions_df[id_column].astype(str)
                    
                    # 获取门店列表
                    stores = sorted(permissions_df[store_column].unique().tolist())
                    
                    # 登录表单
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
                    
                    # 显示权限表预览（管理员模式）
                    if admin_mode and admin_password == "admin123":
                        with st.expander("查看权限表"):
                            st.dataframe(permissions_df)
                            
                else:
                    st.error("❌ 权限表格式错误：至少需要两列（门店名称和人员编号）")
                    
            except Exception as e:
                st.error(f"❌ 读取权限表时出错：{str(e)}")
        else:
            st.info("ℹ️ 请先在侧边栏上传门店权限表")

else:
    # 已登录状态 - 显示报表
    st.markdown(f"""
        <div class="store-info">
            <h3>当前门店：{st.session_state.store_name}</h3>
            <p>操作员：{st.session_state.user_id} | 登录时间：{st.session_state.login_time}</p>
        </div>
    """, unsafe_allow_html=True)
    
    if reports_file:
        try:
            # 读取Excel文件的所有sheet名称
            excel_file = pd.ExcelFile(reports_file)
            sheet_names = excel_file.sheet_names
            
            # 查找匹配的sheet
            matching_sheets = []
            for sheet in sheet_names:
                # 更灵活的匹配逻辑
                if (st.session_state.store_name in sheet or 
                    sheet in st.session_state.store_name or
                    sheet.replace(" ", "") == st.session_state.store_name.replace(" ", "")):
                    matching_sheets.append(sheet)
            
            if matching_sheets:
                # 如果有多个匹配的sheet，让用户选择
                if len(matching_sheets) > 1:
                    selected_sheet = st.selectbox(
                        "找到多个相关报表，请选择：", 
                        matching_sheets
                    )
                else:
                    selected_sheet = matching_sheets[0]
                
                # 读取选定的sheet
                df = pd.read_excel(reports_file, sheet_name=selected_sheet)
                
                # 报表显示和操作
                st.subheader(f"📊 {st.session_state.store_name} - 财务报表")
                
                # 添加筛选功能
                col1, col2 = st.columns([3, 1])
                with col1:
                    # 搜索框
                    search_term = st.text_input("🔍 搜索报表内容", placeholder="输入关键词搜索...")
                
                with col2:
                    # 显示行数选择
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
                
                # 数据分析功能（可选）
                if st.checkbox("📊 显示数据分析和趋势图", value=True):
                    st.subheader("数据分析")
                    
                    # 创建标签页
                    tab1, tab2, tab3 = st.tabs(["📈 月度收入趋势", "📊 数据统计", "🔍 自定义分析"])
                    
                    with tab1:
                        # 月度财务趋势分析
                        st.write("### 月度财务趋势分析")
                        
                        # 尝试查找特定的财务指标
                        target_metrics = {
                            "三. 毛利-线上": None,
                            "五. 净利润": None,
                            "应收-未收额": None
                        }
                        
                        # 查找指标所在的行 - 检查第一列
                        first_col = df.columns[0]
                        for i, row in df.iterrows():
                            row_name = str(row[first_col])
                            for metric in target_metrics.keys():
                                if metric in row_name:
                                    target_metrics[metric] = i
                        
                        # 处理多级列标题的情况
                        # 检查是否有多级列标题（如：1月下有类团和锌了么）
                        has_multiindex = isinstance(df.columns, pd.MultiIndex)
                        
                        if any(target_metrics.values() is not None):
                            # 准备月份数据
                            months_data = {}
                            month_order = ['1月', '2月', '3月', '4月', '5月', '6月', '7月', '8月', '9月', '10月', '11月', '12月']
                            
                            # 遍历所有列，识别月份数据
                            for col in df.columns[1:]:  # 跳过第一列（项目名称）
                                col_str = str(col)
                                
                                # 跳过合计列
                                if any(keyword in col_str.lower() for keyword in ['合计', '总计', 'total']):
                                    continue
                                
                                # 识别月份
                                for month in month_order:
                                    if month in col_str:
                                        if month not in months_data:
                                            months_data[month] = []
                                        months_data[month].append(col)
                                        break
                            
                            # 按月份顺序排序
                            sorted_months = [m for m in month_order if m in months_data]
                            
                            if sorted_months:
                                # 提取各指标数据
                                gross_profit_online = []
                                net_profit = []
                                
                                for month in sorted_months:
                                    # 对于每个月份，合并所有业务板块的数据
                                    gp_value = 0
                                    np_value = 0
                                    
                                    for col in months_data[month]:
                                        # 毛利-线上
                                        if target_metrics["三. 毛利-线上"] is not None:
                                            try:
                                                val = df.loc[target_metrics["三. 毛利-线上"], col]
                                                if pd.notna(val) and str(val).strip() not in ['', '-']:
                                                    # 处理可能的字符串格式数字
                                                    val_str = str(val).replace(',', '').replace(' ', '')
                                                    gp_value += float(val_str)
                                            except:
                                                pass
                                        
                                        # 净利润
                                        if target_metrics["五. 净利润"] is not None:
                                            try:
                                                val = df.loc[target_metrics["五. 净利润"], col]
                                                if pd.notna(val) and str(val).strip() not in ['', '-']:
                                                    val_str = str(val).replace(',', '').replace(' ', '')
                                                    np_value += float(val_str)
                                            except:
                                                pass
                                    
                                    gross_profit_online.append(gp_value)
                                    net_profit.append(np_value)
                                
                                # 创建主图表
                                fig = go.Figure()
                                
                                # 添加毛利-线上柱状图
                                fig.add_trace(go.Bar(
                                    x=sorted_months,
                                    y=gross_profit_online,
                                    name='毛利-线上',
                                    marker_color='lightgreen',
                                    text=[f'¥{v:,.0f}' for v in gross_profit_online],
                                    textposition='outside',
                                ))
                                
                                # 添加净利润线图
                                fig.add_trace(go.Scatter(
                                    x=sorted_months,
                                    y=net_profit,
                                    name='净利润',
                                    line=dict(color='darkgreen', width=3),
                                    mode='lines+markers',
                                    marker=dict(size=10, color='darkgreen'),
                                    text=[f'¥{v:,.0f}' for v in net_profit],
                                    textposition='top center',
                                ))
                                
                                # 设置布局
                                fig.update_layout(
                                    title=f'{st.session_state.store_name} - 月度财务指标趋势',
                                    xaxis_title='月份',
                                    yaxis_title='金额（元）',
                                    hovermode='x unified',
                                    height=500,
                                    showlegend=True,
                                    legend=dict(
                                        orientation="h",
                                        yanchor="bottom",
                                        y=1.02,
                                        xanchor="right",
                                        x=1
                                    ),
                                    plot_bgcolor='white',
                                    xaxis=dict(showgrid=True, gridcolor='lightgray'),
                                    yaxis=dict(showgrid=True, gridcolor='lightgray')
                                )
                                
                                st.plotly_chart(fig, use_container_width=True)
                                
                                # 查找应收-未收额
                                receivable_unpaid = None
                                
                                # 查找合计列
                                total_cols = [col for col in df.columns if any(keyword in str(col).lower() 
                                            for keyword in ['合计', '总计', 'total'])]
                                
                                if total_cols and target_metrics["应收-未收额"] is not None:
                                    for col in total_cols:
                                        try:
                                            val = df.loc[target_metrics["应收-未收额"], col]
                                            if pd.notna(val) and str(val).strip() not in ['', '-', '0']:
                                                val_str = str(val).replace(',', '').replace(' ', '')
                                                receivable_unpaid = float(val_str)
                                                break
                                        except:
                                            pass
                                
                                # 显示关键指标
                                col1, col2, col3, col4 = st.columns(4)
                                
                                with col1:
                                    total_gp = sum(gross_profit_online)
                                    st.metric("毛利-线上总额", f"¥{total_gp:,.2f}")
                                
                                with col2:
                                    total_np = sum(net_profit)
                                    st.metric("净利润总额", f"¥{total_np:,.2f}")
                                
                                with col3:
                                    if total_gp > 0:
                                        profit_margin = (total_np / total_gp) * 100
                                        st.metric("净利率", f"{profit_margin:.1f}%")
                                    else:
                                        st.metric("净利率", "-")
                                
                                with col4:
                                    if receivable_unpaid and receivable_unpaid > 0:
                                        st.metric("应收-未收额", f"¥{receivable_unpaid:,.2f}", 
                                                delta=f"待收款", delta_color="inverse")
                                    else:
                                        st.metric("应收-未收额", "¥0.00")
                                
                                # 显示月度对比表
                                with st.expander("查看月度明细数据"):
                                    comparison_df = pd.DataFrame({
                                        '月份': sorted_months,
                                        '毛利-线上': [f"¥{v:,.2f}" for v in gross_profit_online],
                                        '净利润': [f"¥{v:,.2f}" for v in net_profit],
                                        '净利率': [f"{(np/gp*100):.1f}%" if gp > 0 else "-" 
                                                  for gp, np in zip(gross_profit_online, net_profit)]
                                    })
                                    st.dataframe(comparison_df, use_container_width=True)
                                    
                                    # 下载月度数据
                                    csv = comparison_df.to_csv(index=False, encoding='utf-8-sig')
                                    st.download_button(
                                        label="📥 下载月度数据",
                                        data=csv,
                                        file_name=f"{st.session_state.store_name}_月度财务数据_{datetime.now().strftime('%Y%m%d')}.csv",
                                        mime="text/csv"
                                    )
                                
                                # 如果找到应收-未收额，显示特别提醒
                                if receivable_unpaid and receivable_unpaid > 0:
                                    st.markdown(f"""
                                    <div class="metric-highlight">
                                        <strong>⚠️ 应收款项提醒</strong><br>
                                        当前有 <strong>¥{receivable_unpaid:,.2f}</strong> 的应收款项尚未收回
                                    </div>
                                    """, unsafe_allow_html=True)
                                    
                                    # 计算应收款占比
                                    if total_gp > 0:
                                        receivable_ratio = (receivable_unpaid / total_gp) * 100
                                        st.info(f"应收未收额占毛利-线上总额的 **{receivable_ratio:.1f}%**")
                                
                                # 业务板块分析（如果有多个业务）
                                if len(months_data.get(sorted_months[0], [])) > 1:
                                    with st.expander("查看各业务板块贡献"):
                                        st.info("检测到多个业务板块数据，正在开发分业务板块分析功能...")
                                        
                            else:
                                st.warning("未找到月份数据，请检查报表格式")
                        
                        else:
                            st.info("未找到指定的财务指标（毛利-线上、净利润、应收-未收额）")
                            
                            # 显示可用的行名称供参考
                            with st.expander("查看报表中的所有项目"):
                                available_items = df[first_col].dropna().unique()
                                for item in available_items[:20]:  # 只显示前20个
                                    st.write(f"- {item}")
                                if len(available_items) > 20:
                                    st.write(f"... 还有 {len(available_items)-20} 个项目")
                            
                            # 提供手动选择功能
                            st.subheader("手动选择分析指标")
                            available_items = df[first_col].dropna().unique().tolist()
                            selected_items = st.multiselect(
                                "选择要分析的财务指标",
                                available_items,
                                default=[item for item in available_items if any(
                                    keyword in str(item) for keyword in ['毛利', '净利', '收入', '成本']
                                )][:3]
                            )
                            
                            if selected_items and st.button("生成趋势图"):
                                generic_custom_analysis(df, selected_items)
                    
                    with tab2:
                        # 数值列统计
                        numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
                        if len(numeric_cols) > 0:
                            st.write("### 数值列统计信息")
                            st.dataframe(df[numeric_cols].describe(), use_container_width=True)
                            
                            # 数据分布图
                            if st.checkbox("显示数据分布图"):
                                selected_col = st.selectbox("选择要分析的列", numeric_cols)
                                fig = px.histogram(df, x=selected_col, title=f"{selected_col} 分布图")
                                st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.info("报表中没有数值列可供统计分析")
                    
                    with tab3:
                        # 自定义分析
                        st.write("### 自定义数据分析")
                        
                        # 数据透视表
                        if st.checkbox("创建数据透视表"):
                            all_cols = df.columns.tolist()
                            col1, col2, col3 = st.columns(3)
                            
                            with col1:
                                index_cols = st.multiselect("选择行索引", all_cols)
                            with col2:
                                values_cols = st.multiselect("选择数值列", df.select_dtypes(include=['float64', 'int64']).columns.tolist())
                            with col3:
                                agg_func = st.selectbox("聚合方式", ['sum', 'mean', 'count', 'max', 'min'])
                            
                            if index_cols and values_cols:
                                if st.button("生成透视表"):
                                    try:
                                        pivot_table = pd.pivot_table(df, values=values_cols, index=index_cols, aggfunc=agg_func)
                                        st.dataframe(pivot_table, use_container_width=True)
                                    except Exception as e:
                                        st.error(f"创建透视表时出错：{str(e)}")
                        
                        # 相关性分析
                        numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
                        if len(numeric_cols) >= 2:
                            if st.checkbox("显示相关性热力图"):
                                corr_matrix = df[numeric_cols].corr()
                                fig = px.imshow(corr_matrix, 
                                              labels=dict(color="相关系数"),
                                              title="数值列相关性热力图",
                                              color_continuous_scale='RdBu')
                                st.plotly_chart(fig, use_container_width=True)
                
            else:
                st.error(f"❌ 未找到与门店 '{st.session_state.store_name}' 匹配的报表")
                st.info("💡 提示：请确认报表文件中的Sheet名称包含门店名称")
                
                # 显示所有可用的sheet供参考
                with st.expander("查看所有可用的报表"):
                    for i, sheet in enumerate(sheet_names, 1):
                        st.write(f"{i}. {sheet}")
                        
        except Exception as e:
            st.error(f"❌ 读取报表时出错：{str(e)}")
            st.info("💡 请检查报表文件格式是否正确")
    else:
        st.info("ℹ️ 请在侧边栏上传财务报表文件")
        
        # 显示使用说明
        with st.expander("📖 使用说明"):
            st.markdown("""
            ### 如何使用本系统：
            
            1. **上传权限表**：在侧边栏上传包含门店名称和人员编号的Excel文件
               - 第一列：门店名称
               - 第二列：人员编号
            
            2. **上传财务报表**：上传包含多个门店Sheet的Excel文件
               - 每个Sheet名称应包含门店名称
               - 系统会自动匹配对应的报表
            
            3. **查看和下载**：登录成功后可以查看、搜索和下载报表
            
            ### 注意事项：
            - 请确保权限表和报表文件格式正确
            - Sheet名称需要包含门店名称以便系统匹配
            - 支持Excel (.xlsx, .xls) 和 CSV 格式
            """)

# 页脚
st.divider()
st.markdown("""
    <div style="text-align: center; color: #888; font-size: 0.8rem;">
        门店报表查询系统 v2.1 | 技术支持：IT部门
    </div>
""", unsafe_allow_html=True)
