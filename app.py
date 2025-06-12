import streamlit as st
import pandas as pd
import io
import os
import pickle
import hashlib
from datetime import datetime, timedelta
import plotly.graph_objects as go
import plotly.express as px
from pathlib import Path
import time

# 页面配置
st.set_page_config(
    page_title="门店报表查询系统", 
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 系统配置
ADMIN_PASSWORD = "admin123"  # 建议修改为复杂密码
DATA_DIR = "data"  # 数据存储目录
PERMISSIONS_FILE = os.path.join(DATA_DIR, "permissions.pkl")
REPORTS_FILE = os.path.join(DATA_DIR, "reports.pkl")
SYSTEM_INFO_FILE = os.path.join(DATA_DIR, "system_info.pkl")

# 创建数据目录
os.makedirs(DATA_DIR, exist_ok=True)

# 自定义CSS样式
st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        padding: 1rem 0;
        margin-bottom: 2rem;
    }
    .store-info {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1.5rem;
        border-radius: 10px;
        margin: 1rem 0;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .admin-panel {
        background: linear-gradient(135deg, #ffeaa7 0%, #fab1a0 100%);
        padding: 1.5rem;
        border-radius: 10px;
        border: 2px solid #fdcb6e;
        margin: 1rem 0;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .metric-card {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #007bff;
        margin: 0.5rem 0;
    }
    .success-message {
        background-color: #d4edda;
        color: #155724;
        padding: 1rem;
        border-radius: 5px;
        border: 1px solid #c3e6cb;
        margin: 1rem 0;
    }
    .warning-message {
        background-color: #fff3cd;
        color: #856404;
        padding: 1rem;
        border-radius: 5px;
        border: 1px solid #ffeaa7;
        margin: 1rem 0;
    }
    .search-container {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 8px;
        margin: 1rem 0;
    }
    </style>
""", unsafe_allow_html=True)

# 数据持久化函数
@st.cache_data
def load_data_from_file(filepath):
    """从文件加载数据"""
    try:
        if os.path.exists(filepath):
            with open(filepath, 'rb') as f:
                return pickle.load(f)
    except Exception as e:
        st.error(f"加载数据失败: {str(e)}")
    return None

def save_data_to_file(data, filepath):
    """保存数据到文件"""
    try:
        with open(filepath, 'wb') as f:
            pickle.dump(data, f)
        return True
    except Exception as e:
        st.error(f"保存数据失败: {str(e)}")
        return False

def get_file_hash(file_data):
    """获取文件的MD5哈希值"""
    return hashlib.md5(file_data).hexdigest()

# 系统信息管理
def get_system_info():
    """获取系统信息"""
    default_info = {
        'last_update': None,
        'total_stores': 0,
        'total_users': 0,
        'permissions_hash': None,
        'reports_hash': None
    }
    info = load_data_from_file(SYSTEM_INFO_FILE)
    return info if info else default_info

def update_system_info(**kwargs):
    """更新系统信息"""
    info = get_system_info()
    info.update(kwargs)
    info['last_update'] = datetime.now()
    save_data_to_file(info, SYSTEM_INFO_FILE)

# 权限验证函数
def verify_user_permission(store_name, user_id, permissions_data):
    """验证用户权限"""
    if permissions_data is None or len(permissions_data.columns) < 2:
        return False
    
    store_column = permissions_data.columns[0]
    id_column = permissions_data.columns[1]
    
    # 数据类型转换
    permissions_data[store_column] = permissions_data[store_column].astype(str)
    permissions_data[id_column] = permissions_data[id_column].astype(str)
    
    # 模糊匹配门店名称
    for _, row in permissions_data.iterrows():
        stored_store = str(row[store_column]).strip()
        stored_id = str(row[id_column]).strip()
        
        # 门店名称匹配（支持包含关系）
        if (store_name in stored_store or stored_store in store_name) and stored_id == str(user_id):
            return True
    
    return False

def find_matching_reports(store_name, reports_data):
    """查找匹配的报表"""
    matching_sheets = []
    store_name_clean = store_name.strip()
    
    for sheet_name in reports_data.keys():
        sheet_name_clean = sheet_name.strip()
        # 支持多种匹配方式
        if (store_name_clean in sheet_name_clean or 
            sheet_name_clean in store_name_clean or
            store_name_clean.replace(" ", "") in sheet_name_clean.replace(" ", "") or
            sheet_name_clean.replace(" ", "") in store_name_clean.replace(" ", "")):
            matching_sheets.append(sheet_name)
    
    return matching_sheets

# 数据分析函数
def analyze_financial_data(df):
    """分析财务数据"""
    analysis_results = {}
    
    if len(df.columns) == 0:
        return analysis_results
    
    first_col = df.columns[0]
    
    # 查找关键财务指标
    key_indicators = {
        '营业收入': ['营业收入', '收入', '销售收入'],
        '毛利润': ['毛利', '毛利润', '毛利-线上'],
        '净利润': ['净利润', '净利'],
        '成本': ['成本', '营业成本'],
        '费用': ['费用', '管理费用', '销售费用'],
        '应收款': ['应收', '应收款', '应收-未收']
    }
    
    for indicator, keywords in key_indicators.items():
        for idx, row in df.iterrows():
            row_name = str(row[first_col]) if pd.notna(row[first_col]) else ""
            
            for keyword in keywords:
                if keyword in row_name:
                    # 计算该行的数值
                    total = 0
                    monthly_data = {}
                    
                    for col in df.columns[1:]:
                        try:
                            val = row[col]
                            if pd.notna(val):
                                # 清理数值
                                val_str = str(val).replace(',', '').replace('¥', '').replace('￥', '').strip()
                                if val_str.replace('.', '').replace('-', '').isdigit():
                                    num_val = float(val_str)
                                    
                                    # 识别月份
                                    col_str = str(col)
                                    for month_num in range(1, 13):
                                        month_pattern = f"{month_num}月"
                                        if month_pattern in col_str:
                                            monthly_data[month_pattern] = num_val
                                            break
                                    
                                    if '合计' not in col_str and '总计' not in col_str:
                                        total += num_val
                        except:
                            continue
                    
                    if total != 0 or monthly_data:
                        analysis_results[indicator] = {
                            'total': total,
                            'monthly': monthly_data,
                            'row_index': idx
                        }
                    break
                if indicator in analysis_results:
                    break
    
    return analysis_results

# 初始化会话状态
def init_session_state():
    """初始化会话状态"""
    defaults = {
        'logged_in': False,
        'store_name': "",
        'user_id': "",
        'login_time': None,
        'is_admin': False,
        'permissions_data': None,
        'reports_data': {},
        'system_info': get_system_info(),
        'data_loaded': False
    }
    
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

# 加载持久化数据
def load_persistent_data():
    """加载持久化数据"""
    if not st.session_state.data_loaded:
        # 加载权限数据
        permissions = load_data_from_file(PERMISSIONS_FILE)
        if permissions is not None:
            st.session_state.permissions_data = permissions
        
        # 加载报表数据
        reports = load_data_from_file(REPORTS_FILE)
        if reports is not None:
            st.session_state.reports_data = reports
        
        # 更新系统信息
        st.session_state.system_info = get_system_info()
        st.session_state.data_loaded = True

init_session_state()
load_persistent_data()

# 主标题
st.markdown('<h1 class="main-header">📊 门店报表查询系统</h1>', unsafe_allow_html=True)

# 显示系统状态
if st.session_state.system_info['last_update']:
    last_update = st.session_state.system_info['last_update']
    if isinstance(last_update, str):
        last_update = datetime.fromisoformat(last_update)
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("总门店数", st.session_state.system_info['total_stores'])
    with col2:
        st.metric("授权用户数", st.session_state.system_info['total_users'])
    with col3:
        st.metric("最后更新", last_update.strftime("%m-%d %H:%M"))

# 侧边栏
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
                help="包含门店名称和人员编号的Excel文件"
            )
            
            if permissions_file:
                try:
                    # 检查文件是否有变化
                    file_data = permissions_file.getvalue()
                    current_hash = get_file_hash(file_data)
                    
                    if current_hash != st.session_state.system_info.get('permissions_hash'):
                        df = pd.read_excel(permissions_file)
                        
                        # 验证文件格式
                        if len(df.columns) >= 2:
                            st.session_state.permissions_data = df
                            
                            # 保存到文件
                            if save_data_to_file(df, PERMISSIONS_FILE):
                                # 统计信息
                                total_users = len(df)
                                unique_stores = df.iloc[:, 0].nunique()
                                
                                update_system_info(
                                    total_users=total_users,
                                    permissions_hash=current_hash
                                )
                                
                                st.success(f"✅ 权限表已上传：{total_users} 个用户，{unique_stores} 个门店")
                            else:
                                st.error("保存权限表失败")
                        else:
                            st.error("权限表格式错误：至少需要两列（门店名称、人员编号）")
                    else:
                        st.info("文件未发生变化")
                        
                except Exception as e:
                    st.error(f"读取权限表失败：{str(e)}")
            
            # 显示当前权限表状态
            if st.session_state.permissions_data is not None:
                df = st.session_state.permissions_data
                st.info(f"📋 当前权限表：{len(df)} 个用户，{df.iloc[:, 0].nunique()} 个门店")
                
                if st.checkbox("查看权限表预览"):
                    st.dataframe(df.head(10), use_container_width=True)
                    
                    # 门店分布统计
                    if st.checkbox("查看门店分布"):
                        store_counts = df.iloc[:, 0].value_counts()
                        st.bar_chart(store_counts)
            
            st.divider()
            
            # 上传财务报表
            reports_file = st.file_uploader(
                "上传财务报表", 
                type=['xlsx', 'xls'],
                help="包含多个门店Sheet的Excel文件"
            )
            
            if reports_file:
                try:
                    # 检查文件是否有变化
                    file_data = reports_file.getvalue()
                    current_hash = get_file_hash(file_data)
                    
                    if current_hash != st.session_state.system_info.get('reports_hash'):
                        with st.spinner("正在处理报表文件..."):
                            excel_file = pd.ExcelFile(reports_file)
                            sheets = excel_file.sheet_names
                            
                            # 清空之前的数据
                            st.session_state.reports_data = {}
                            
                            # 批量处理sheet
                            progress_bar = st.progress(0)
                            for i, sheet in enumerate(sheets):
                                try:
                                    df = pd.read_excel(reports_file, sheet_name=sheet)
                                    if not df.empty:
                                        st.session_state.reports_data[sheet] = df
                                    progress_bar.progress((i + 1) / len(sheets))
                                except Exception as e:
                                    st.warning(f"跳过Sheet '{sheet}'：{str(e)}")
                                    continue
                            
                            progress_bar.empty()
                            
                            # 保存到文件
                            if save_data_to_file(st.session_state.reports_data, REPORTS_FILE):
                                update_system_info(
                                    total_stores=len(st.session_state.reports_data),
                                    reports_hash=current_hash
                                )
                                
                                st.success(f"✅ 报表已上传：{len(st.session_state.reports_data)} 个门店")
                                st.info("包含的门店：" + ", ".join(list(st.session_state.reports_data.keys())[:10]) + 
                                       ("..." if len(st.session_state.reports_data) > 10 else ""))
                            else:
                                st.error("保存报表失败")
                    else:
                        st.info("报表文件未发生变化")
                        
                except Exception as e:
                    st.error(f"读取报表失败：{str(e)}")
            
            # 显示当前报表状态
            if st.session_state.reports_data:
                st.info(f"📊 当前报表：{len(st.session_state.reports_data)} 个门店")
                
                if st.checkbox("查看已上传的门店列表"):
                    stores = list(st.session_state.reports_data.keys())
                    for i in range(0, len(stores), 3):
                        cols = st.columns(3)
                        for j, store in enumerate(stores[i:i+3]):
                            with cols[j]:
                                st.write(f"• {store}")
            
            st.divider()
            
            # 管理功能
            st.subheader("🛠️ 管理功能")
            
            col1, col2 = st.columns(2)
            
            with col1:
                if st.button("🔄 重新加载数据", use_container_width=True):
                    st.session_state.data_loaded = False
                    load_persistent_data()
                    st.success("数据已重新加载")
                    st.rerun()
            
            with col2:
                if st.button("🗑️ 清空所有数据", type="secondary", use_container_width=True):
                    # 删除文件
                    for filepath in [PERMISSIONS_FILE, REPORTS_FILE, SYSTEM_INFO_FILE]:
                        if os.path.exists(filepath):
                            os.remove(filepath)
                    
                    # 重置状态
                    st.session_state.permissions_data = None
                    st.session_state.reports_data = {}
                    st.session_state.system_info = get_system_info()
                    
                    st.success("所有数据已清空")
                    st.rerun()
            
            if st.button("🚪 退出管理员", use_container_width=True):
                st.session_state.is_admin = False
                st.rerun()
    
    # 普通用户登录状态
    else:
        if st.session_state.logged_in:
            st.divider()
            st.subheader("👤 当前登录")
            st.info(f"**门店：** {st.session_state.store_name}")
            st.info(f"**编号：** {st.session_state.user_id}")
            if st.session_state.login_time:
                st.info(f"**时间：** {st.session_state.login_time}")
            
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
            <p>您可以在左侧边栏上传和管理权限表和财务报表文件</p>
        </div>
    """, unsafe_allow_html=True)
    
    # 系统概览
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        permissions_status = "已上传" if st.session_state.permissions_data is not None else "未上传"
        permissions_count = len(st.session_state.permissions_data) if st.session_state.permissions_data is not None else 0
        st.metric("权限表状态", permissions_status, f"{permissions_count} 用户")
    
    with col2:
        reports_count = len(st.session_state.reports_data)
        st.metric("报表门店数", f"{reports_count} 个", "已就绪" if reports_count > 0 else "未上传")
    
    with col3:
        if st.session_state.permissions_data is not None:
            unique_stores = st.session_state.permissions_data.iloc[:, 0].nunique()
            st.metric("授权门店数", f"{unique_stores} 个")
        else:
            st.metric("授权门店数", "0 个")
    
    with col4:
        last_update = st.session_state.system_info.get('last_update')
        if last_update:
            if isinstance(last_update, str):
                last_update = datetime.fromisoformat(last_update)
            update_time = last_update.strftime("%H:%M")
            st.metric("最后更新", update_time)
        else:
            st.metric("最后更新", "无")
    
    # 数据一致性检查
    if st.session_state.permissions_data is not None and st.session_state.reports_data:
        st.subheader("📋 数据一致性检查")
        
        # 获取权限表中的门店
        permission_stores = set(st.session_state.permissions_data.iloc[:, 0].unique())
        # 获取报表中的门店
        report_stores = set(st.session_state.reports_data.keys())
        
        # 找出差异
        missing_reports = permission_stores - report_stores
        extra_reports = report_stores - permission_stores
        
        col1, col2 = st.columns(2)
        
        with col1:
            if missing_reports:
                st.warning(f"⚠️ 有权限但缺少报表的门店 ({len(missing_reports)}个):")
                for store in list(missing_reports)[:5]:
                    st.write(f"• {store}")
                if len(missing_reports) > 5:
                    st.write(f"... 还有 {len(missing_reports) - 5} 个")
            else:
                st.success("✅ 所有授权门店都有对应报表")
        
        with col2:
            if extra_reports:
                st.info(f"ℹ️ 有报表但无权限的门店 ({len(extra_reports)}个):")
                for store in list(extra_reports)[:5]:
                    st.write(f"• {store}")
                if len(extra_reports) > 5:
                    st.write(f"... 还有 {len(extra_reports) - 5} 个")
            else:
                st.success("✅ 所有报表门店都有对应权限")
    
    # 使用说明
    with st.expander("📖 管理员操作指南"):
        st.markdown("""
        ### 🚀 快速开始：
        
        **第一步：上传权限表**
        - Excel文件，包含两列：门店名称、人员编号
        - 支持一个门店多个用户
        - 建议使用标准化的门店名称
        
        **第二步：上传财务报表**
        - Excel文件，每个Sheet代表一个门店
        - Sheet名称应与权限表中的门店名称对应（支持模糊匹配）
        - 系统会自动处理70+门店的大型文件
        
        **第三步：数据验证**
        - 查看数据一致性检查结果
        - 确认门店数量和用户数量
        - 测试用户登录功能
        
        ### 💡 最佳实践：
        
        - **门店命名规范**：保持权限表和报表中门店名称的一致性
        - **定期更新**：建议每月更新一次报表数据
        - **备份数据**：重要数据请做好本地备份
        - **性能优化**：单个报表文件建议不超过50MB
        
        ### 🔧 故障排除：
        
        - **文件上传失败**：检查文件格式和大小
        - **门店匹配失败**：检查门店名称是否一致
        - **用户登录失败**：确认权限表中有对应记录
        - **数据丢失**：重新上传文件即可恢复
        """)

elif user_type == "管理员" and not st.session_state.is_admin:
    # 提示输入管理员密码
    st.info("👈 请在左侧边栏输入管理员密码以访问管理功能")
    
    # 显示系统状态（非敏感信息）
    if st.session_state.system_info['last_update']:
        st.markdown("""
            <div class="warning-message">
                <h4>🏪 系统状态</h4>
                <p>系统已配置并正在运行，用户可以正常查询报表</p>
            </div>
        """, unsafe_allow_html=True)

else:
    # 普通用户界面
    if not st.session_state.logged_in:
        # 登录界面
        st.subheader("🔐 用户登录")
        
        # 检查是否有权限数据
        if st.session_state.permissions_data is None:
            st.markdown("""
                <div class="warning-message">
                    <h4>⚠️ 系统维护中</h4>
                    <p>系统暂无数据，请联系管理员上传权限表和报表文件</p>
                </div>
            """, unsafe_allow_html=True)
        else:
            permissions_df = st.session_state.permissions_data
            
            if len(permissions_df.columns) >= 2:
                store_column = permissions_df.columns[0]
                id_column = permissions_df.columns[1]
                
                # 数据清理和转换
                permissions_df[store_column] = permissions_df[store_column].astype(str).str.strip()
                permissions_df[id_column] = permissions_df[id_column].astype(str).str.strip()
                
                # 获取门店列表
                stores = sorted(permissions_df[store_column].unique().tolist())
                
                # 登录表单
                col1, col2, col3 = st.columns([1, 2, 1])
                
                with col2:
                    st.markdown('<div class="search-container">', unsafe_allow_html=True)
                    
                    with st.form("login_form"):
                        st.markdown("#### 请输入登录信息")
                        
                        # 门店选择（支持搜索）
                        selected_store = st.selectbox(
                            "选择门店", 
                            stores,
                            help="请选择您所属的门店"
                        )
                        
                        # 人员编号输入
                        user_id = st.text_input(
                            "人员编号", 
                            placeholder="请输入您的人员编号",
                            help="请输入系统分配给您的人员编号"
                        )
                        
                        # 登录按钮
                        col_a, col_b, col_c = st.columns([1, 1, 1])
                        with col_b:
                            submit = st.form_submit_button("🚀 登录", use_container_width=True)
                        
                        if submit:
                            if selected_store and user_id.strip():
                                # 验证权限
                                if verify_user_permission(selected_store, user_id.strip(), permissions_df):
                                    st.session_state.logged_in = True
                                    st.session_state.store_name = selected_store
                                    st.session_state.user_id = user_id.strip()
                                    st.session_state.login_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                    st.success("✅ 登录成功！正在跳转...")
                                    st.balloons()
                                    time.sleep(1)
                                    st.rerun()
                                else:
                                    st.error("❌ 门店或人员编号错误！请检查后重试。")
                            else:
                                st.warning("⚠️ 请填写完整的登录信息")
                    
                    st.markdown('</div>', unsafe_allow_html=True)
                
                # 登录提示
                st.markdown("""
                    <div style="text-align: center; margin-top: 2rem; color: #666;">
                        <p>💡 <strong>登录提示：</strong></p>
                        <p>请选择您的门店并输入管理员分配给您的人员编号</p>
                        <p>如遇问题，请联系系统管理员</p>
                    </div>
                """, unsafe_allow_html=True)
            else:
                st.error("权限表格式错误，请联系管理员重新上传")
    
    else:
        # 已登录 - 显示报表
        st.markdown(f"""
            <div class="store-info">
                <h3>🏪 {st.session_state.store_name}</h3>
                <p><strong>操作员：</strong>{st.session_state.user_id} &nbsp;|&nbsp; <strong>登录时间：</strong>{st.session_state.login_time}</p>
            </div>
        """, unsafe_allow_html=True)
        
        # 查找对应的报表
        matching_sheets = find_matching_reports(st.session_state.store_name, st.session_state.reports_data)
        
        if matching_sheets:
            # 如果有多个匹配的sheet，让用户选择
            if len(matching_sheets) > 1:
                selected_sheet = st.selectbox(
                    "🔍 找到多个相关报表，请选择：", 
                    matching_sheets,
                    help="系统找到了多个可能匹配的报表"
                )
            else:
                selected_sheet = matching_sheets[0]
                st.info(f"📊 已找到报表：{selected_sheet}")
            
            # 获取报表数据
            df = st.session_state.reports_data[selected_sheet]
            
            # 报表操作界面
            st.subheader(f"📈 财务报表 - {st.session_state.store_name}")
            
            # 搜索和过滤工具
            st.markdown('<div class="search-container">', unsafe_allow_html=True)
            
            col1, col2, col3 = st.columns([2, 1, 1])
            
            with col1:
                search_term = st.text_input(
                    "🔍 搜索报表内容", 
                    placeholder="输入关键词搜索...",
                    help="支持搜索所有列的内容"
                )
            
            with col2:
                n_rows = st.selectbox("显示行数", [10, 25, 50, 100, "全部"])
            
            with col3:
                show_analysis = st.checkbox("📊 显示数据分析", value=False)
            
            st.markdown('</div>', unsafe_allow_html=True)
            
            # 应用搜索过滤
            if search_term:
                mask = df.astype(str).apply(
                    lambda x: x.str.contains(search_term, case=False, na=False)
                ).any(axis=1)
                filtered_df = df[mask]
                st.info(f"🔍 找到 {len(filtered_df)} 条包含 '{search_term}' 的记录")
            else:
                filtered_df = df
            
            # 显示数据统计
            total_rows = len(filtered_df)
            st.markdown(f"""
                <div class="metric-card">
                    📊 <strong>数据统计：</strong>共 {total_rows} 条记录 | 
                    📅 <strong>报表列数：</strong>{len(df.columns)} 列
                </div>
            """, unsafe_allow_html=True)
            
            # 显示数据表
            if total_rows > 0:
                display_df = filtered_df.head(n_rows) if n_rows != "全部" else filtered_df
                st.dataframe(display_df, use_container_width=True, height=400)
            else:
                st.warning("没有找到符合条件的数据")
            
            # 下载功能
            st.subheader("📥 数据下载")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                # 下载完整报表
                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False, sheet_name=st.session_state.store_name)
                
                st.download_button(
                    label="📥 下载完整报表 (Excel)",
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
                        label="📥 下载筛选结果 (Excel)",
                        data=buffer_filtered.getvalue(),
                        file_name=f"{st.session_state.store_name}_筛选报表_{datetime.now().strftime('%Y%m%d')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
                else:
                    st.button("📥 下载筛选结果 (Excel)", disabled=True, use_container_width=True,
                            help="没有筛选结果可下载")
            
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
            
            # 数据分析模块
            if show_analysis:
                st.subheader("📊 财务数据分析")
                
                try:
                    analysis_results = analyze_financial_data(df)
                    
                    if analysis_results:
                        # 关键指标展示
                        st.markdown("#### 🎯 关键财务指标")
                        
                        # 创建指标卡片
                        metric_cols = st.columns(min(len(analysis_results), 4))
                        
                        for i, (indicator, data) in enumerate(analysis_results.items()):
                            with metric_cols[i % 4]:
                                total_value = data['total']
                                if total_value != 0:
                                    formatted_value = f"¥{total_value:,.0f}"
                                    if '应收' in indicator or '成本' in indicator or '费用' in indicator:
                                        st.metric(indicator, formatted_value, delta="需关注", delta_color="inverse")
                                    else:
                                        st.metric(indicator, formatted_value)
                        
                        # 月度趋势分析
                        st.markdown("#### 📈 月度趋势")
                        
                        # 选择要分析的指标
                        indicators_with_monthly = [k for k, v in analysis_results.items() if v['monthly']]
                        
                        if indicators_with_monthly:
                            selected_indicator = st.selectbox(
                                "选择指标进行月度分析", 
                                indicators_with_monthly
                            )
                            
                            monthly_data = analysis_results[selected_indicator]['monthly']
                            
                            if monthly_data:
                                # 创建月度趋势图
                                months = list(monthly_data.keys())
                                values = list(monthly_data.values())
                                
                                fig = px.line(
                                    x=months, 
                                    y=values,
                                    title=f"{selected_indicator} - 月度趋势",
                                    labels={'x': '月份', 'y': '金额'},
                                    markers=True
                                )
                                fig.update_layout(height=400)
                                st.plotly_chart(fig, use_container_width=True)
                                
                                # 月度数据表
                                monthly_df = pd.DataFrame({
                                    '月份': months,
                                    '金额': [f"¥{v:,.0f}" for v in values]
                                })
                                st.dataframe(monthly_df, use_container_width=True)
                        
                        # 财务比率分析
                        if '营业收入' in analysis_results and '净利润' in analysis_results:
                            revenue = analysis_results['营业收入']['total']
                            profit = analysis_results['净利润']['total']
                            
                            if revenue > 0:
                                profit_margin = (profit / revenue) * 100
                                st.markdown("#### 💹 财务比率")
                                
                                col1, col2, col3 = st.columns(3)
                                with col1:
                                    st.metric("净利率", f"{profit_margin:.1f}%")
                                
                                if '毛利润' in analysis_results:
                                    gross_profit = analysis_results['毛利润']['total']
                                    gross_margin = (gross_profit / revenue) * 100
                                    with col2:
                                        st.metric("毛利率", f"{gross_margin:.1f}%")
                                
                                if '成本' in analysis_results:
                                    cost = analysis_results['成本']['total']
                                    cost_ratio = (cost / revenue) * 100
                                    with col3:
                                        st.metric("成本率", f"{cost_ratio:.1f}%")
                    
                    else:
                        st.info("🔍 无法识别标准财务指标，显示通用数据统计")
                        
                        # 通用统计分析
                        numeric_cols = df.select_dtypes(include=['number']).columns
                        
                        if len(numeric_cols) > 0:
                            st.markdown("#### 📊 数值列统计")
                            stats_df = df[numeric_cols].describe().round(2)
                            st.dataframe(stats_df, use_container_width=True)
                        else:
                            st.info("报表中没有可分析的数值数据")
                
                except Exception as e:
                    st.error(f"数据分析时出错：{str(e)}")
                    st.info("💡 建议：确保报表格式符合标准财务报表格式")
        
        else:
            st.error(f"❌ 未找到门店 '{st.session_state.store_name}' 的报表")
            st.markdown("""
                <div class="warning-message">
                    <h4>🔍 找不到报表？</h4>
                    <p><strong>可能的原因：</strong></p>
                    <ul>
                        <li>管理员尚未上传包含该门店的报表文件</li>
                        <li>报表中的Sheet名称与门店名称不匹配</li>
                        <li>报表文件正在更新中</li>
                    </ul>
                    <p><strong>解决方案：</strong></p>
                    <ul>
                        <li>联系管理员确认报表是否已上传</li>
                        <li>确认门店名称是否正确</li>
                        <li>稍后重试或重新登录</li>
                    </ul>
                </div>
            """, unsafe_allow_html=True)

# 页脚
st.divider()
st.markdown("""
    <div style="text-align: center; color: #888; font-size: 0.8rem; padding: 1rem;">
        <p>🏪 门店报表查询系统 v4.0 - 企业级版本</p>
        <p>💡 支持70+门店 | 🔒 权限分离 | 💾 数据持久化 | 📊 智能分析</p>
        <p>技术支持：IT部门 | 建议使用Chrome浏览器访问</p>
    </div>
""", unsafe_allow_html=True)
