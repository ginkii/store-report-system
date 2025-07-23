import streamlit as st
import pandas as pd
import io
import pymongo
import logging
from datetime import datetime
from typing import Optional, Dict, List

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 页面配置
st.set_page_config(
    page_title="门店报表查询系统", 
    page_icon="📊",
    layout="wide"
)

# 系统配置
ADMIN_PASSWORD = st.secrets.get("system", {}).get("admin_password", "admin123")
MAX_FILE_SIZE_MB = 10

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
    </style>
""", unsafe_allow_html=True)

def show_message(message: str, msg_type: str = "info"):
    """显示状态消息"""
    st.markdown(f'<div class="status-box {msg_type}">{message}</div>', unsafe_allow_html=True)

# ===== 缓存管理 =====
def show_cache_management():
    """缓存管理界面"""
    st.subheader("💾 缓存管理")
    
    # 缓存控制按钮
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🗑️ 清除数据缓存"):
            st.cache_data.clear()
            show_message("✅ 数据缓存已清除", "success")
            st.rerun()
    
    with col2:
        if st.button("🔄 清除资源缓存"):
            st.cache_resource.clear()
            show_message("✅ 资源缓存已清除", "success")
            st.rerun()
    
    with col3:
        if st.button("🧹 清除所有缓存"):
            st.cache_data.clear()
            st.cache_resource.clear()
            show_message("✅ 所有缓存已清除", "success")
            st.rerun()
    
    # 缓存策略说明
    with st.expander("📖 缓存策略说明"):
        st.markdown("""
        **当前缓存策略:**
        - ✅ **权限数据**: 缓存2分钟（数据小，提升性能）
        - ✅ **数据库连接**: 长期缓存（必需的系统资源）  
        - ✅ **门店列表**: 缓存1分钟（元数据，占用极小）
        - ❌ **报表数据**: 不缓存（数据大，避免问题）
        
        **缓存管理:**
        - 🎯 **建议**: 定期清理缓存保持最新数据
        - 🔄 **自动过期**: 缓存会自动过期更新
        - 🧹 **手动清理**: 上传新数据后建议清理缓存
        """)

# ===== MongoDB连接管理 =====
@st.cache_resource
def get_mongodb_client():
    """获取MongoDB客户端 - 长期缓存连接对象"""
    try:
        if "mongodb" not in st.secrets:
            raise Exception("MongoDB配置未找到，请检查secrets设置")
        
        client = pymongo.MongoClient(st.secrets["mongodb"]["uri"], serverSelectionTimeoutMS=5000)
        client.admin.command('ping')  # 测试连接
        logger.info("MongoDB连接成功")
        return client
    except Exception as e:
        logger.error(f"MongoDB连接失败: {str(e)}")
        st.error(f"❌ 数据库连接失败: {str(e)}")
        st.stop()

@st.cache_resource
def get_database():
    """获取数据库实例 - 长期缓存"""
    client = get_mongodb_client()
    return client['store_reports']

def get_database_stats():
    """获取数据库统计信息 - 不缓存，实时查询"""
    try:
        db = get_database()
        stats = db.command("dbStats")
        collections = db.list_collection_names()
        
        collection_stats = {}
        for coll_name in collections:
            try:
                coll_stats = db.command("collStats", coll_name)
                collection_stats[coll_name] = {
                    'count': coll_stats.get('count', 0),
                    'size_mb': coll_stats.get('size', 0) / 1024 / 1024
                }
            except:
                collection_stats[coll_name] = {'count': 0, 'size_mb': 0}
        
        return {
            'data_size_mb': stats.get('dataSize', 0) / 1024 / 1024,
            'storage_size_mb': stats.get('storageSize', 0) / 1024 / 1024,
            'collections': collection_stats
        }
    except Exception as e:
        logger.error(f"获取数据库统计失败: {str(e)}")
        return None

# ===== 文件处理统一模块 =====
def validate_file(uploaded_file) -> bool:
    """验证上传文件"""
    if uploaded_file is None:
        return False
    
    # 检查文件大小
    if uploaded_file.size > MAX_FILE_SIZE_MB * 1024 * 1024:
        show_message(f"❌ 文件过大，最大支持 {MAX_FILE_SIZE_MB}MB", "error")
        return False
    
    # 检查文件格式
    allowed_types = ['xlsx', 'xls', 'csv']
    file_ext = uploaded_file.name.split('.')[-1].lower()
    if file_ext not in allowed_types:
        show_message(f"❌ 不支持的文件格式，请上传 {', '.join(allowed_types)} 文件", "error")
        return False
    
    return True

def parse_excel_file(uploaded_file) -> Dict[str, pd.DataFrame]:
    """统一的Excel文件解析器 - 不缓存，直接处理"""
    try:
        file_ext = uploaded_file.name.split('.')[-1].lower()
        
        if file_ext == 'csv':
            # CSV文件处理
            df = pd.read_csv(uploaded_file, encoding='utf-8-sig')
            return {'Sheet1': df}
        else:
            # Excel文件处理
            excel_file = pd.ExcelFile(uploaded_file)
            sheets_dict = {}
            
            for sheet_name in excel_file.sheet_names:
                try:
                    df = pd.read_excel(uploaded_file, sheet_name=sheet_name)
                    if not df.empty:
                        # 基础数据清理
                        df = df.fillna('')
                        df = df.astype(str)
                        sheets_dict[sheet_name] = df
                        logger.info(f"解析工作表 '{sheet_name}': {len(df)} 行")
                except Exception as e:
                    logger.warning(f"跳过工作表 '{sheet_name}': {str(e)}")
                    continue
            
            return sheets_dict
            
    except Exception as e:
        logger.error(f"文件解析失败: {str(e)}")
        show_message(f"❌ 文件解析失败: {str(e)}", "error")
        return {}

# ===== 数据存储模块 =====
@st.cache_data(ttl=120)  # 权限数据小，缓存2分钟
def load_permissions() -> Optional[pd.DataFrame]:
    """加载权限数据 - 小数据缓存"""
    try:
        db = get_database()
        permissions = list(db.permissions.find({}, {'_id': 0}))
        
        if not permissions:
            return None
        
        df = pd.DataFrame(permissions)
        logger.info(f"权限数据加载成功: {len(df)} 条记录")
        return df[['store_name', 'user_id']].copy()
    except Exception as e:
        logger.error(f"加载权限数据失败: {str(e)}")
        return None

@st.cache_data(ttl=60)   # 门店列表缓存1分钟
def get_store_list() -> List[str]:
    """获取门店列表 - 元数据缓存"""
    try:
        db = get_database()
        reports = db.reports.find({}, {'store_name': 1, '_id': 0})
        store_names = [doc['store_name'] for doc in reports if not doc['store_name'].endswith('_错误')]
        return sorted(list(set(store_names)))
    except Exception as e:
        logger.error(f"获取门店列表失败: {str(e)}")
        return []

def load_reports() -> Dict[str, pd.DataFrame]:
    """加载报表数据 - 不缓存，直接从数据库读取"""
    try:
        db = get_database()
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
        return {}

def save_permissions(df: pd.DataFrame) -> bool:
    """保存权限数据"""
    try:
        db = get_database()
        collection = db.permissions
        
        # 清空现有数据
        collection.delete_many({})
        
        # 准备新数据
        permissions_data = []
        for _, row in df.iterrows():
            permissions_data.append({
                'store_name': str(row.iloc[0]).strip(),
                'user_id': str(row.iloc[1]).strip(),
                'update_time': datetime.now().isoformat()
            })
        
        # 批量插入
        if permissions_data:
            collection.insert_many(permissions_data)
        
        # 清除相关缓存
        load_permissions.clear()
        get_store_list.clear()
        
        logger.info(f"权限数据保存成功: {len(permissions_data)} 条记录")
        return True
        
    except Exception as e:
        logger.error(f"保存权限数据失败: {str(e)}")
        show_message(f"❌ 保存权限数据失败: {str(e)}", "error")
        return False

def save_reports(reports_dict: Dict[str, pd.DataFrame]) -> bool:
    """保存报表数据"""
    try:
        db = get_database()
        collection = db.reports
        
        # 清空现有数据
        collection.delete_many({})
        
        # 准备新数据
        reports_data = []
        current_time = datetime.now().isoformat()
        
        for store_name, df in reports_dict.items():
            try:
                # 转换DataFrame为记录格式
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
                # 保存错误信息
                reports_data.append({
                    'store_name': f"{store_name}_错误",
                    'data': [],
                    'error': str(e),
                    'update_time': current_time
                })
        
        # 批量保存
        if reports_data:
            collection.insert_many(reports_data)
        
        # 清除相关缓存
        get_store_list.clear()
        
        logger.info(f"报表数据保存成功: {len(reports_data)} 个门店")
        return True
        
    except Exception as e:
        logger.error(f"保存报表数据失败: {str(e)}")
        show_message(f"❌ 保存报表数据失败: {str(e)}", "error")
        return False

# ===== 应收未收额分析模块 =====
def analyze_receivable_data(df: pd.DataFrame) -> Dict:
    """分析应收未收额数据 - 字段查找优先，行查找备用"""
    result = {}
    
    if df.empty:
        return result
    
    # 方法1: 字段名查找（优先）
    keywords = ['应收-未收额', '应收未收额', '应收-未收', '应收未收']
    
    for keyword in keywords:
        # 在列名中查找
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
    
    # 方法2: 在数据行中查找关键词（备用）
    for idx, row in df.iterrows():
        try:
            row_name = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
            
            for keyword in keywords:
                if keyword in row_name:
                    # 从右到左查找数值
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
    
    # 方法3: 特定第69行查找（兼容旧逻辑）
    if len(df) > 68:  # 第69行存在
        try:
            row = df.iloc[68]  # 第69行
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
                                        'method': '第69行查找',
                                        'column_name': str(df.columns[col_idx]),
                                        'row_name': row_name,
                                        'row_index': 68,
                                        'source': '在第69行找到（传统位置）'
                                    }
                                    return result
                            except (ValueError, TypeError):
                                continue
                    break
        except Exception:
            pass
    
    # 调试信息
    result['debug_info'] = {
        'total_rows': len(df),
        'total_columns': len(df.columns),
        'columns_with_keywords': [col for col in df.columns if any(kw in str(col) for kw in keywords)],
        'searched_methods': ['字段查找', '行查找', '第69行查找']
    }
    
    return result

# ===== 用户权限验证 =====
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

# ===== 存储管理界面 =====
def show_storage_management():
    """显示存储管理界面"""
    st.subheader("💾 MongoDB存储管理")
    
    stats = get_database_stats()
    if not stats:
        st.error("❌ 无法获取存储统计信息")
        return
    
    # 存储概览
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("数据大小", f"{stats['data_size_mb']:.2f} MB")
    with col2:
        st.metric("存储使用", f"{stats['storage_size_mb']:.2f} MB")
    with col3:
        usage_percent = (stats['storage_size_mb'] / 512) * 100
        st.metric("使用率", f"{usage_percent:.1f}%")
    
    # 存储警告
    if usage_percent > 80:
        show_message(f"🚨 存储使用量过高：{stats['storage_size_mb']:.1f} MB / 512 MB", "error")
    elif usage_percent > 60:
        show_message(f"⚠️ 存储使用量较高：{stats['storage_size_mb']:.1f} MB / 512 MB", "warning")
    else:
        show_message("✅ 存储空间充足", "success")
    
    # 集合详情
    if stats['collections']:
        st.subheader("📋 数据集合详情")
        for coll_name, coll_stats in stats['collections'].items():
            with st.expander(f"📄 {coll_name} ({coll_stats['count']} 条记录)"):
                col1, col2 = st.columns(2)
                with col1:
                    st.write(f"**记录数：** {coll_stats['count']}")
                    st.write(f"**大小：** {coll_stats['size_mb']:.2f} MB")
                
                with col2:
                    if st.button(f"🗑️ 清空 {coll_name}", key=f"clear_{coll_name}"):
                        try:
                            db = get_database()
                            db[coll_name].delete_many({})
                            # 清除相关缓存
                            if coll_name == 'permissions':
                                load_permissions.clear()
                            elif coll_name == 'reports':
                                get_store_list.clear()
                            show_message(f"✅ 已清空 {coll_name}", "success")
                            st.rerun()
                        except Exception as e:
                            show_message(f"❌ 清空失败: {str(e)}", "error")
    
    # 全局操作
    st.subheader("🔧 全局操作")
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🔄 刷新统计"):
            st.rerun()
    
    with col2:
        if st.button("🗑️ 清空所有数据", type="primary"):
            try:
                db = get_database()
                for coll_name in stats['collections'].keys():
                    db[coll_name].delete_many({})
                # 清除所有缓存
                st.cache_data.clear()
                show_message("✅ 所有数据已清空", "success")
                st.rerun()
            except Exception as e:
                show_message(f"❌ 清空失败: {str(e)}", "error")

# ===== 主应用界面 =====
def main():
    # 初始化会话状态
    if 'logged_in' not in st.session_state:
        st.session_state.logged_in = False
    if 'is_admin' not in st.session_state:
        st.session_state.is_admin = False
    
    # 主标题
    st.markdown('<h1 class="main-header">📊 门店报表查询系统</h1>', unsafe_allow_html=True)
    
    # 测试数据库连接
    try:
        get_database()
        show_message("✅ MongoDB数据库连接成功", "success")
    except Exception as e:
        show_message(f"❌ 数据库连接失败: {str(e)}", "error")
        return
    
    # 侧边栏
    with st.sidebar:
        st.title("⚙️ 系统功能")
        
        # 系统状态
        stats = get_database_stats()
        
        if stats:
            st.success("🟢 数据库已连接")
            st.caption(f"存储: {stats['storage_size_mb']:.1f}MB")
        else:
            st.error("🔴 数据库异常")
        
        user_type = st.radio("选择用户类型", ["普通用户", "管理员"])
        
        if user_type == "管理员":
            st.subheader("🔐 管理员登录")
            admin_password = st.text_input("管理员密码", type="password")
            
            if st.button("验证身份"):
                if admin_password == ADMIN_PASSWORD:
                    st.session_state.is_admin = True
                    show_message("✅ 管理员验证成功", "success")
                    st.rerun()
                else:
                    show_message("❌ 密码错误", "error")
            
            if st.session_state.is_admin:
                st.subheader("📁 文件管理")
                
                # 权限表上传
                permissions_file = st.file_uploader("上传权限表", type=['xlsx', 'xls', 'csv'])
                if permissions_file and validate_file(permissions_file):
                    sheets_dict = parse_excel_file(permissions_file)
                    if sheets_dict:
                        # 取第一个工作表作为权限数据
                        first_sheet = list(sheets_dict.values())[0]
                        if len(first_sheet.columns) >= 2:
                            if save_permissions(first_sheet):
                                show_message(f"✅ 权限表上传成功: {len(first_sheet)} 个用户", "success")
                                st.balloons()
                        else:
                            show_message("❌ 权限表需要至少两列（门店名称、人员编号）", "error")
                
                # 报表上传
                reports_file = st.file_uploader("上传财务报表", type=['xlsx', 'xls', 'csv'])
                if reports_file and validate_file(reports_file):
                    with st.spinner("解析报表文件..."):
                        sheets_dict = parse_excel_file(reports_file)
                        if sheets_dict:
                            if save_reports(sheets_dict):
                                show_message(f"✅ 报表上传成功: {len(sheets_dict)} 个工作表", "success")
                                st.balloons()
        else:
            if st.session_state.logged_in:
                st.subheader("👤 当前登录")
                st.info(f"门店：{st.session_state.store_name}")
                st.info(f"编号：{st.session_state.user_id}")
                
                if st.button("🚪 退出登录"):
                    st.session_state.logged_in = False
                    st.rerun()
    
    # 主界面内容
    if user_type == "管理员" and st.session_state.is_admin:
        st.markdown('<div class="admin-panel"><h3>👨‍💼 管理员控制面板</h3></div>', unsafe_allow_html=True)
        
        # 数据统计
        permissions_data = load_permissions()
        store_list = get_store_list()  # 使用缓存的门店列表
        
        col1, col2, col3 = st.columns(3)
        with col1:
            perms_count = len(permissions_data) if permissions_data is not None else 0
            st.metric("权限用户数", perms_count)
        with col2:
            st.metric("报表门店数", len(store_list))
        with col3:
            stats = get_database_stats()
            usage = stats['storage_size_mb'] if stats else 0
            st.metric("存储使用", f"{usage:.1f}MB")
        
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
        
        # 存储管理
        st.divider()
        show_storage_management()
        
        # 缓存管理
        st.divider()
        show_cache_management()
    
    elif user_type == "管理员":
        st.info("👈 请在左侧输入管理员密码")
    
    else:
        if not st.session_state.logged_in:
            st.subheader("🔐 用户登录")
            
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
        
        else:
            # 用户报表查询界面
            st.markdown(f'<div class="store-info"><h3>🏪 {st.session_state.store_name}</h3><p>操作员：{st.session_state.user_id}</p></div>', unsafe_allow_html=True)
            
            # 实时加载报表数据（不缓存）
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
                    # Excel下载
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
                    # CSV下载
                    csv = df.to_csv(index=False, encoding='utf-8-sig')
                    st.download_button(
                        "📥 下载CSV格式",
                        csv,
                        f"{st.session_state.store_name}_报表_{datetime.now().strftime('%Y%m%d')}.csv",
                        "text/csv"
                    )
            
            else:
                st.error(f"❌ 未找到门店 '{st.session_state.store_name}' 的报表")
    
    # 页面底部
    st.divider()
    col1, col2, col3 = st.columns(3)
    with col1:
        st.caption(f"🕒 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    with col2:
        st.caption("💾 MongoDB Atlas")
    with col3:
        st.caption("🔧 v4.2 (部署优化版)")

if __name__ == "__main__":
    main()
