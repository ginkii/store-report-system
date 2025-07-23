import streamlit as st
import pandas as pd
import io
import logging
from datetime import datetime
from typing import Optional, Dict, List
import traceback

# 页面配置必须在最开始
st.set_page_config(
    page_title="门店报表查询系统", 
    page_icon="📊",
    layout="wide"
)

# 尝试导入pymongo，如果失败则显示友好错误
try:
    import pymongo
    PYMONGO_AVAILABLE = True
    PYMONGO_ERROR = None
except ImportError as e:
    PYMONGO_AVAILABLE = False
    PYMONGO_ERROR = str(e)

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 系统配置
def get_config():
    """安全获取配置信息"""
    try:
        # 获取MongoDB配置
        mongodb_config = st.secrets.get("mongodb", {})
        if not mongodb_config:
            return None, "MongoDB配置未找到，请检查secrets.toml文件"
        
        # 获取系统配置
        system_config = st.secrets.get("system", {})
        admin_password = system_config.get("admin_password", "admin123")
        
        return {
            "mongodb_uri": mongodb_config.get("uri"),
            "admin_password": admin_password,
            "max_file_size_mb": system_config.get("max_file_size_mb", 10)
        }, None
        
    except Exception as e:
        return None, f"配置加载失败: {str(e)}"

# CSS样式
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
    }
    .receivable-positive {
        background: linear-gradient(135deg, #ff9a9e 0%, #fecfef 100%);
        color: #721c24;
        padding: 2rem;
        border-radius: 15px;
        border: 3px solid #f093fb;
        margin: 1rem 0;
        text-align: center;
    }
    .receivable-negative {
        background: linear-gradient(135deg, #a8edea 0%, #d299c2 100%);
        color: #0c4128;
        padding: 2rem;
        border-radius: 15px;
        border: 3px solid #48cab2;
        margin: 1rem 0;
        text-align: center;
    }
    .status-box {
        padding: 0.75rem;
        border-radius: 5px;
        margin: 0.5rem 0;
    }
    .success { background: #d4edda; color: #155724; border: 1px solid #c3e6cb; }
    .error { background: #f8d7da; color: #721c24; border: 1px solid #f5c6cb; }
    .warning { background: #fff3cd; color: #856404; border: 1px solid #ffeaa7; }
    .info { background: #d1ecf1; color: #0c5460; border: 1px solid #bee5eb; }
    .connection-status {
        background: linear-gradient(135deg, #74b9ff 0%, #0984e3 100%);
        color: white;
        padding: 1rem;
        border-radius: 10px;
        margin: 1rem 0;
        text-align: center;
    }
    </style>
""", unsafe_allow_html=True)

def show_message(message: str, msg_type: str = "info"):
    """显示状态消息"""
    st.markdown(f'<div class="status-box {msg_type}">{message}</div>', unsafe_allow_html=True)

def show_error_details(error_msg: str, show_details: bool = False):
    """显示错误详情"""
    show_message(f"❌ {error_msg}", "error")
    if show_details:
        with st.expander("🔍 详细错误信息"):
            st.code(traceback.format_exc())

# ===== 数据库连接管理 =====
def test_database_connection(config):
    """测试数据库连接"""
    if not PYMONGO_AVAILABLE:
        return {"status": "error", "message": f"pymongo模块未安装: {PYMONGO_ERROR}"}
    
    if not config:
        return {"status": "error", "message": "配置信息获取失败"}
    
    try:
        # 使用短超时进行快速测试
        client = pymongo.MongoClient(
            config["mongodb_uri"], 
            serverSelectionTimeoutMS=3000,
            connectTimeoutMS=3000,
            socketTimeoutMS=3000
        )
        
        # 快速ping测试
        client.admin.command('ping')
        client.close()
        
        return {"status": "success", "message": "数据库连接正常"}
        
    except Exception as e:
        return {"status": "error", "message": f"连接失败: {str(e)}"}

@st.cache_resource
def get_mongodb_client():
    """获取MongoDB客户端"""
    config, error = get_config()
    if error:
        raise Exception(error)
    
    if not PYMONGO_AVAILABLE:
        raise Exception(f"pymongo模块未安装: {PYMONGO_ERROR}")
    
    try:
        client = pymongo.MongoClient(
            config["mongodb_uri"], 
            serverSelectionTimeoutMS=10000,
            connectTimeoutMS=5000,
            socketTimeoutMS=20000
        )
        
        # 测试连接
        client.admin.command('ping')
        return client
        
    except Exception as e:
        raise Exception(f"数据库连接失败: {str(e)}")

def get_database():
    """获取数据库实例"""
    try:
        client = get_mongodb_client()
        return client['store_reports']
    except Exception as e:
        st.error(f"数据库连接失败: {str(e)}")
        return None

# ===== 文件处理模块 =====
def validate_file(uploaded_file, max_size_mb=10) -> bool:
    """验证上传文件"""
    if uploaded_file is None:
        return False
    
    if uploaded_file.size > max_size_mb * 1024 * 1024:
        show_message(f"❌ 文件过大，最大支持 {max_size_mb}MB", "error")
        return False
    
    allowed_types = ['xlsx', 'xls', 'csv']
    file_ext = uploaded_file.name.split('.')[-1].lower()
    if file_ext not in allowed_types:
        show_message(f"❌ 不支持的文件格式，请上传 {', '.join(allowed_types)} 文件", "error")
        return False
    
    return True

def parse_excel_file(uploaded_file) -> Dict[str, pd.DataFrame]:
    """解析Excel文件"""
    try:
        file_ext = uploaded_file.name.split('.')[-1].lower()
        
        if file_ext == 'csv':
            # 尝试不同编码
            for encoding in ['utf-8-sig', 'utf-8', 'gbk', 'gb2312']:
                try:
                    uploaded_file.seek(0)
                    df = pd.read_csv(uploaded_file, encoding=encoding)
                    return {'Sheet1': df}
                except UnicodeDecodeError:
                    continue
            raise Exception("无法解析CSV文件编码")
        else:
            excel_file = pd.ExcelFile(uploaded_file)
            sheets_dict = {}
            
            for sheet_name in excel_file.sheet_names:
                try:
                    df = pd.read_excel(uploaded_file, sheet_name=sheet_name)
                    if not df.empty:
                        df = df.fillna('')
                        df = df.astype(str)
                        sheets_dict[sheet_name] = df
                        logger.info(f"解析工作表 '{sheet_name}': {len(df)} 行")
                except Exception as e:
                    logger.warning(f"跳过工作表 '{sheet_name}': {str(e)}")
                    continue
            
            if not sheets_dict:
                raise Exception("未找到有效的工作表数据")
            
            return sheets_dict
            
    except Exception as e:
        logger.error(f"文件解析失败: {str(e)}")
        show_error_details(f"文件解析失败: {str(e)}")
        return {}

# ===== 数据存储模块 =====
def load_permissions() -> Optional[pd.DataFrame]:
    """加载权限数据"""
    try:
        db = get_database()
        if db is None:
            return None
        
        permissions = list(db.permissions.find({}, {'_id': 0}))
        
        if not permissions:
            return None
        
        df = pd.DataFrame(permissions)
        logger.info(f"权限数据加载成功: {len(df)} 条记录")
        return df[['store_name', 'user_id']].copy()
        
    except Exception as e:
        logger.error(f"加载权限数据失败: {str(e)}")
        show_error_details(f"加载权限数据失败: {str(e)}")
        return None

def get_store_list() -> List[str]:
    """获取门店列表"""
    try:
        db = get_database()
        if db is None:
            return []
        
        reports = db.reports.find({}, {'store_name': 1, '_id': 0})
        store_names = [doc['store_name'] for doc in reports if not doc['store_name'].endswith('_错误')]
        return sorted(list(set(store_names)))
        
    except Exception as e:
        logger.error(f"获取门店列表失败: {str(e)}")
        return []

def load_reports() -> Dict[str, pd.DataFrame]:
    """加载报表数据"""
    try:
        db = get_database()
        if db is None:
            return {}
        
        reports = list(db.reports.find({}, {'_id': 0}))
        
        if not reports:
            return {}
        
        reports_dict = {}
        for report in reports:
            try:
                store_name = report['store_name']
                if store_name.endswith('_错误'):
                    continue
                
                data_records = report.get('data', [])
                if data_records:
                    df = pd.DataFrame(data_records)
                    reports_dict[store_name] = df
            except Exception as e:
                logger.warning(f"跳过损坏的报表数据: {str(e)}")
                continue
        
        logger.info(f"加载报表数据成功: {len(reports_dict)} 个门店")
        return reports_dict
        
    except Exception as e:
        logger.error(f"加载报表数据失败: {str(e)}")
        show_error_details(f"加载报表数据失败: {str(e)}")
        return {}

def save_permissions(df: pd.DataFrame) -> bool:
    """保存权限数据"""
    try:
        db = get_database()
        if db is None:
            return False
        
        collection = db.permissions
        
        # 清空现有数据
        collection.delete_many({})
        
        permissions_data = []
        for _, row in df.iterrows():
            permissions_data.append({
                'store_name': str(row.iloc[0]).strip(),
                'user_id': str(row.iloc[1]).strip(),
                'update_time': datetime.now().isoformat()
            })
        
        if permissions_data:
            collection.insert_many(permissions_data)
        
        logger.info(f"权限数据保存成功: {len(permissions_data)} 条记录")
        return True
        
    except Exception as e:
        logger.error(f"保存权限数据失败: {str(e)}")
        show_error_details(f"保存权限数据失败: {str(e)}")
        return False

def save_reports(reports_dict: Dict[str, pd.DataFrame]) -> bool:
    """保存报表数据"""
    try:
        db = get_database()
        if db is None:
            return False
        
        collection = db.reports
        
        # 清空现有数据
        collection.delete_many({})
        
        reports_data = []
        current_time = datetime.now().isoformat()
        
        for store_name, df in reports_dict.items():
            try:
                data_records = df.to_dict('records')
                
                reports_data.append({
                    'store_name': store_name,
                    'data': data_records,
                    'update_time': current_time,
                    'file_info': {
                        'rows': len(df),
                        'columns': len(df.columns),
                        'filename': f"{store_name}.xlsx"
                    }
                })
                
            except Exception as e:
                logger.error(f"处理 {store_name} 数据失败: {str(e)}")
                reports_data.append({
                    'store_name': f"{store_name}_错误",
                    'data': [],
                    'error': str(e),
                    'update_time': current_time
                })
        
        if reports_data:
            collection.insert_many(reports_data)
        
        logger.info(f"报表数据保存成功: {len(reports_data)} 个门店")
        return True
        
    except Exception as e:
        logger.error(f"保存报表数据失败: {str(e)}")
        show_error_details(f"保存报表数据失败: {str(e)}")
        return False

# ===== 应收未收额分析模块 =====
def analyze_receivable_data(df: pd.DataFrame) -> Dict:
    """分析应收未收额数据"""
    result = {}
    
    if df.empty:
        return result
    
    keywords = ['应收-未收额', '应收未收额', '应收-未收', '应收未收']
    
    # 字段查找
    for keyword in keywords:
        matching_cols = [col for col in df.columns if keyword in str(col)]
        if matching_cols:
            for col in matching_cols:
                for idx, value in df[col].items():
                    try:
                        if pd.notna(value) and str(value).strip() not in ['', '0', '0.0']:
                            cleaned = str(value).replace(',', '').replace('¥', '').replace('￥', '').strip()
                            if cleaned.startswith('(') and cleaned.endswith(')'):
                                cleaned = '-' + cleaned[1:-1]
                            
                            amount = float(cleaned)
                            if amount != 0:
                                result['应收-未收额'] = {
                                    'amount': amount,
                                    'method': '字段查找',
                                    'column_name': col,
                                    'row_index': idx,
                                    'source': f'在列"{col}"第{idx+1}行找到'
                                }
                                return result
                    except (ValueError, TypeError):
                        continue
    
    # 行查找
    for idx, row in df.iterrows():
        try:
            row_name = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
            
            for keyword in keywords:
                if keyword in row_name:
                    for col_idx in range(len(row)-1, 0, -1):
                        val = row.iloc[col_idx]
                        if pd.notna(val) and str(val).strip() not in ['', '0', '0.0']:
                            try:
                                cleaned = str(val).replace(',', '').replace('¥', '').replace('￥', '').strip()
                                if cleaned.startswith('(') and cleaned.endswith(')'):
                                    cleaned = '-' + cleaned[1:-1]
                                
                                amount = float(cleaned)
                                if amount != 0:
                                    result['应收-未收额'] = {
                                        'amount': amount,
                                        'method': '行查找',
                                        'column_name': str(df.columns[col_idx]),
                                        'row_name': row_name,
                                        'row_index': idx,
                                        'source': f'在第{idx+1}行找到'
                                    }
                                    return result
                            except (ValueError, TypeError):
                                continue
                    break
        except Exception:
            continue
    
    result['debug_info'] = {
        'total_rows': len(df),
        'total_columns': len(df.columns),
        'columns_with_keywords': [col for col in df.columns if any(kw in str(col) for kw in keywords)],
        'searched_methods': ['字段查找', '行查找']
    }
    
    return result

def verify_user_permission(store_name: str, user_id: str, permissions_data: Optional[pd.DataFrame]) -> bool:
    """验证用户权限"""
    if permissions_data is None or len(permissions_data) == 0:
        return False
    
    for _, row in permissions_data.iterrows():
        stored_store = str(row['store_name']).strip()
        stored_id = str(row['user_id']).strip()
        
        if (store_name in stored_store or stored_store in store_name) and stored_id == str(user_id):
            return True
    
    return False

def find_matching_reports(store_name: str, reports_data: Dict[str, pd.DataFrame]) -> List[str]:
    """查找匹配的报表"""
    matching = []
    for sheet_name in reports_data.keys():
        if store_name in sheet_name or sheet_name in store_name:
            matching.append(sheet_name)
    return matching

def show_connection_status():
    """显示连接状态组件"""
    st.markdown('<div class="connection-status"><h4>🔗 数据库连接状态</h4></div>', unsafe_allow_html=True)
    
    if st.button("🧪 测试数据库连接"):
        config, error = get_config()
        
        if error:
            show_message(f"❌ 配置错误: {error}", "error")
            return
        
        with st.spinner("测试连接中..."):
            result = test_database_connection(config)
            
            if result["status"] == "success":
                show_message(f"✅ {result['message']}", "success")
            else:
                show_message(f"❌ {result['message']}", "error")
                
                # 显示故障排除信息
                with st.expander("🔧 故障排除"):
                    st.markdown("""
                    **常见解决方案：**
                    
                    1. **检查secrets.toml配置文件**
                       ```toml
                       [mongodb]
                       uri = "mongodb+srv://username:password@cluster.mongodb.net/database?retryWrites=true&w=majority"
                       
                       [system]
                       admin_password = "your_admin_password"
                       max_file_size_mb = 10
                       ```
                    
                    2. **检查MongoDB Atlas网络访问**
                       - 登录 MongoDB Atlas
                       - 点击 "Network Access"
                       - 确保添加了 IP: `0.0.0.0/0`
                    
                    3. **检查连接字符串**
                       - 确保用户名密码正确
                       - 确保没有特殊字符编码问题
                    
                    4. **检查数据库用户权限**
                       - 确保用户有读写权限
                       - 建议设置为 "Atlas Admin"
                    """)

def initialize_session_state():
    """初始化会话状态"""
    if 'logged_in' not in st.session_state:
        st.session_state.logged_in = False
    if 'is_admin' not in st.session_state:
        st.session_state.is_admin = False
    if 'store_name' not in st.session_state:
        st.session_state.store_name = ""
    if 'user_id' not in st.session_state:
        st.session_state.user_id = ""

def main():
    """主应用函数"""
    # 初始化会话状态
    initialize_session_state()
    
    # 获取配置
    config, config_error = get_config()
    
    # 主标题
    st.markdown('<h1 class="main-header">📊 门店报表查询系统</h1>', unsafe_allow_html=True)
    
    # 显示系统状态
    if config_error:
        show_message(f"❌ 系统配置错误: {config_error}", "error")
        st.stop()
    
    if not PYMONGO_AVAILABLE:
        show_message(f"❌ pymongo模块未安装: {PYMONGO_ERROR}", "error")
        st.markdown("""
        **解决方案：**
        ```bash
        pip install pymongo
        ```
        """)
        st.stop()
    else:
        show_message("✅ 系统模块加载成功", "success")
    
    # 侧边栏
    with st.sidebar:
        st.title("⚙️ 系统功能")
        
        # 系统信息
        st.subheader("📊 系统信息")
        st.write("**Python版本:** 3.8+")
        st.write("**Streamlit版本:**", st.__version__)
        st.write("**PyMongo状态:** ✅ 已安装")
        
        user_type = st.radio("选择用户类型", ["普通用户", "管理员"])
        
        if user_type == "管理员":
            st.subheader("🔐 管理员登录")
            admin_password = st.text_input("管理员密码", type="password")
            
            if st.button("验证身份"):
                if admin_password == config["admin_password"]:
                    st.session_state.is_admin = True
                    show_message("✅ 管理员验证成功", "success")
                    st.rerun()
                else:
                    show_message("❌ 密码错误", "error")
            
            if st.session_state.is_admin:
                st.subheader("📁 文件管理")
                
                # 权限表上传
                permissions_file = st.file_uploader("上传权限表", type=['xlsx', 'xls', 'csv'])
                if permissions_file and validate_file(permissions_file, config["max_file_size_mb"]):
                    sheets_dict = parse_excel_file(permissions_file)
                    if sheets_dict:
                        first_sheet = list(sheets_dict.values())[0]
                        if len(first_sheet.columns) >= 2:
                            try:
                                if save_permissions(first_sheet):
                                    show_message(f"✅ 权限表上传成功: {len(first_sheet)} 个用户", "success")
                                    st.balloons()
                            except Exception as e:
                                show_error_details(f"上传失败: {str(e)}")
                        else:
                            show_message("❌ 权限表需要至少两列（门店名称、人员编号）", "error")
                
                # 报表上传
                reports_file = st.file_uploader("上传财务报表", type=['xlsx', 'xls', 'csv'])
                if reports_file and validate_file(reports_file, config["max_file_size_mb"]):
                    with st.spinner("解析报表文件..."):
                        sheets_dict = parse_excel_file(reports_file)
                        if sheets_dict:
                            try:
                                if save_reports(sheets_dict):
                                    show_message(f"✅ 报表上传成功: {len(sheets_dict)} 个工作表", "success")
                                    st.balloons()
                            except Exception as e:
                                show_error_details(f"上传失败: {str(e)}")
        else:
            if st.session_state.logged_in:
                st.subheader("👤 当前登录")
                st.info(f"门店：{st.session_state.store_name}")
                st.info(f"编号：{st.session_state.user_id}")
                
                if st.button("🚪 退出登录"):
                    st.session_state.logged_in = False
                    st.session_state.store_name = ""
                    st.session_state.user_id = ""
                    st.rerun()
    
    # 主界面内容
    if user_type == "管理员" and st.session_state.is_admin:
        st.markdown('<div class="admin-panel"><h3>👨‍💼 管理员控制面板</h3></div>', unsafe_allow_html=True)
        
        # 连接状态检查
        show_connection_status()
        
        # 尝试获取数据统计
        try:
            with st.spinner("加载数据统计..."):
                permissions_data = load_permissions()
                store_list = get_store_list()
                
            col1, col2, col3 = st.columns(3)
            with col1:
                perms_count = len(permissions_data) if permissions_data is not None else 0
                st.metric("权限用户数", perms_count)
            with col2:
                st.metric("报表门店数", len(store_list))
            with col3:
                st.metric("系统版本", "v6.0")
            
            # 数据预览
            if permissions_data is not None and len(permissions_data) > 0:
                st.subheader("👥 权限数据预览")
                st.dataframe(permissions_data.head(10), use_container_width=True)
            
            if store_list:
                st.subheader("📊 门店列表预览")
                st.write("**当前系统中的门店:**")
                for i, store in enumerate(store_list[:10], 1):
                    st.write(f"{i}. {store}")
                if len(store_list) > 10:
                    st.write(f"... 还有 {len(store_list) - 10} 个门店")
                    
        except Exception as e:
            show_error_details(f"数据加载异常: {str(e)}")
    
    elif user_type == "管理员":
        st.info("👈 请在左侧输入管理员密码")
    
    else:
        if not st.session_state.logged_in:
            st.subheader("🔐 用户登录")
            
            # 显示连接状态
            show_connection_status()
            
            try:
                with st.spinner("加载权限数据..."):
                    permissions_data = load_permissions()
                
                if permissions_data is None:
                    st.warning("⚠️ 系统维护中，请联系管理员")
                else:
                    stores = sorted(permissions_data['store_name'].unique().tolist())
                    
                    with st.form("login_form"):
                        selected_store = st.selectbox("选择门店", stores)
                        user_id = st.text_input("人员编号")
                        submit = st.form_submit_button("🚀 登录")
                        
                        if submit and selected_store and user_id:
                            if verify_user_permission(selected_store, user_id, permissions_data):
                                st.session_state.logged_in = True
                                st.session_state.store_name = selected_store
                                st.session_state.user_id = user_id
                                show_message("✅ 登录成功", "success")
                                st.balloons()
                                st.rerun()
                            else:
                                show_message("❌ 门店或编号错误", "error")
                                
            except Exception as e:
                show_error_details(f"权限数据加载失败: {str(e)}")
        
        else:
            # 用户报表查询界面
            st.markdown(f'<div class="store-info"><h3>🏪 {st.session_state.store_name}</h3><p>操作员：{st.session_state.user_id}</p></div>', unsafe_allow_html=True)
            
            try:
                with st.spinner("加载报表数据..."):
                    reports_data = load_reports()
                    matching_sheets = find_matching_reports(st.session_state.store_name, reports_data)
                
                if matching_sheets:
                    if len(matching_sheets) > 1:
                        selected_sheet = st.selectbox("选择报表", matching_sheets)
                    else:
                        selected_sheet = matching_sheets[0]
                    
                    df = reports_data[selected_sheet]
                    
                    # 应收-未收额分析
                    st.subheader("💰 应收-未收额")
                    analysis_results = analyze_receivable_data(df)
                    
                    if '应收-未收额' in analysis_results:
                        data = analysis_results['应收-未收额']
                        amount = data['amount']
                        
                        col1, col2, col3 = st.columns([1, 2, 1])
                        with col2:
                            if amount > 0:
                                st.markdown(f'''
                                    <div class="receivable-positive">
                                        <h1 style="margin: 0; font-size: 3rem;">💳 ¥{amount:,.2f}</h1>
                                        <h3 style="margin: 0.5rem 0;">门店应付款</h3>
                                        <p style="margin: 0;">{data['source']} ({data['method']})</p>
                                    </div>
                                ''', unsafe_allow_html=True)
                            elif amount < 0:
                                st.markdown(f'''
                                    <div class="receivable-negative">
                                        <h1 style="margin: 0; font-size: 3rem;">💚 ¥{abs(amount):,.2f}</h1>
                                        <h3 style="margin: 0.5rem 0;">总部应退款</h3>
                                        <p style="margin: 0;">{data['source']} ({data['method']})</p>
                                    </div>
                                ''', unsafe_allow_html=True)
                            else:
                                st.markdown('''
                                    <div style="background: #e8f5e8; color: #2e7d32; padding: 2rem; border-radius: 15px; text-align: center;">
                                        <h1 style="margin: 0; font-size: 3rem;">⚖️ ¥0.00</h1>
                                        <h3 style="margin: 0.5rem 0;">收支平衡</h3>
                                    </div>
                                ''', unsafe_allow_html=True)
                    else:
                        st.warning("⚠️ 未找到应收-未收额数据")
                        with st.expander("🔍 调试信息"):
                            debug_info = analysis_results.get('debug_info', {})
                            st.json(debug_info)
                    
                    st.divider()
                    
                    # 报表数据展示
                    st.subheader("📋 完整报表数据")
                    st.info(f"📊 共 {len(df)} 行，{len(df.columns)} 列")
                    st.dataframe(df, use_container_width=True, height=400)
                    
                    # 下载功能
                    st.subheader("📥 数据下载")
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        buffer = io.BytesIO()
                        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                            df.to_excel(writer, index=False)
                        
                        st.download_button(
                            "📥 下载Excel格式",
                            buffer.getvalue(),
                            f"{st.session_state.store_name}_报表_{datetime.now().strftime('%Y%m%d')}.xlsx",
                            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                    
                    with col2:
                        csv = df.to_csv(index=False, encoding='utf-8-sig')
                        st.download_button(
                            "📥 下载CSV格式",
                            csv,
                            f"{st.session_state.store_name}_报表_{datetime.now().strftime('%Y%m%d')}.csv",
                            "text/csv"
                        )
                
                else:
                    st.error(f"❌ 未找到门店 '{st.session_state.store_name}' 的报表")
                    
            except Exception as e:
                show_error_details(f"报表加载失败: {str(e)}")
    
    # 页面底部
    st.divider()
    col1, col2, col3 = st.columns(3)
    with col1:
        st.caption(f"🕒 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    with col2:
        st.caption("💾 MongoDB Atlas")
    with col3:
        st.caption("🔧 v6.0 (优化版)")

# Streamlit应用入口
if __name__ == "__main__":
    main()
