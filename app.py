import streamlit as st
import pandas as pd
import io
import json
from datetime import datetime
import time
import logging
from typing import Optional, Dict, Any, List
import hashlib
import pickle
import traceback
from contextlib import contextmanager
from qcloud_cos import CosConfig, CosS3Client
from qcloud_cos.cos_exception import CosServiceError
import openpyxl

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
ADMIN_PASSWORD = "admin123"
MAX_RETRIES = 3
RETRY_DELAY = 1
CACHE_DURATION = 300  # 缓存5分钟

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
    .status-success {
        background: #d4edda;
        color: #155724;
        padding: 0.75rem;
        border-radius: 5px;
        border: 1px solid #c3e6cb;
        margin: 0.5rem 0;
    }
    .status-error {
        background: #f8d7da;
        color: #721c24;
        padding: 0.75rem;
        border-radius: 5px;
        border: 1px solid #f5c6cb;
        margin: 0.5rem 0;
    }
    .status-warning {
        background: #fff3cd;
        color: #856404;
        padding: 0.75rem;
        border-radius: 5px;
        border: 1px solid #ffeaa7;
        margin: 0.5rem 0;
    }
    </style>
""", unsafe_allow_html=True)

class CosOperationError(Exception):
    """腾讯云COS操作异常"""
    pass

class DataProcessingError(Exception):
    """数据处理异常"""
    pass

@contextmanager
def error_handler(operation_name: str):
    """通用错误处理上下文管理器"""
    try:
        yield
    except Exception as e:
        logger.error(f"{operation_name} 失败: {str(e)}")
        logger.error(traceback.format_exc())
        st.error(f"❌ {operation_name} 失败: {str(e)}")
        raise

def retry_operation(func, *args, max_retries=MAX_RETRIES, delay=RETRY_DELAY, **kwargs):
    """重试操作装饰器"""
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if attempt == max_retries - 1:
                logger.error(f"操作失败，已重试 {max_retries} 次: {str(e)}")
                raise
            logger.warning(f"操作失败，第 {attempt + 1} 次重试: {str(e)}")
            time.sleep(delay * (attempt + 1))  # 递增延迟

def get_cache_key(operation: str, params: str) -> str:
    """生成缓存键"""
    return hashlib.md5(f"{operation}_{params}".encode()).hexdigest()

def set_cache(key: str, data: Any, duration: int = CACHE_DURATION):
    """设置缓存"""
    try:
        cache_data = {
            'data': data,
            'timestamp': time.time(),
            'duration': duration
        }
        st.session_state[f"cache_{key}"] = cache_data
    except Exception as e:
        logger.warning(f"设置缓存失败: {str(e)}")

def get_cache(key: str) -> Optional[Any]:
    """获取缓存"""
    try:
        cache_key = f"cache_{key}"
        if cache_key in st.session_state:
            cache_data = st.session_state[cache_key]
            if time.time() - cache_data['timestamp'] < cache_data['duration']:
                return cache_data['data']
            else:
                del st.session_state[cache_key]
    except Exception as e:
        logger.warning(f"获取缓存失败: {str(e)}")
    return None

@st.cache_resource(show_spinner="连接腾讯云存储...")
def get_cos_client():
    """获取腾讯云COS客户端 - 使用缓存"""
    try:
        cos_config = st.secrets["tencent_cloud"]
        
        config = CosConfig(
            Region=cos_config["region"],
            SecretId=cos_config["secret_id"],
            SecretKey=cos_config["secret_key"],
        )
        
        client = CosS3Client(config)
        logger.info("腾讯云COS客户端创建成功")
        return client, cos_config["bucket_name"], cos_config["permissions_file"]
    except Exception as e:
        logger.error(f"腾讯云COS客户端创建失败: {str(e)}")
        raise CosOperationError(f"连接失败: {str(e)}")

def safe_cos_operation(operation_func, *args, **kwargs):
    """安全的COS操作"""
    return retry_operation(operation_func, *args, **kwargs)

def save_permissions_to_cos(df: pd.DataFrame, cos_client, bucket_name: str, permissions_file: str) -> bool:
    """保存权限数据到COS"""
    with error_handler("保存权限数据"):
        def _save_operation():
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # 准备CSV数据
            csv_data = []
            csv_data.append(['门店名称', '人员编号', '更新时间'])
            
            for _, row in df.iterrows():
                csv_data.append([
                    str(row.iloc[0]).strip(),
                    str(row.iloc[1]).strip(),
                    current_time
                ])
            
            # 转换为CSV格式
            csv_buffer = io.StringIO()
            pd.DataFrame(csv_data[1:], columns=csv_data[0]).to_csv(csv_buffer, index=False, encoding='utf-8-sig')
            csv_content = csv_buffer.getvalue()
            
            # 上传到COS
            cos_client.put_object(
                Bucket=bucket_name,
                Body=csv_content.encode('utf-8-sig'),
                Key=permissions_file,
                ContentType='text/csv'
            )
            
            logger.info(f"权限数据保存成功: {len(df)} 条记录")
            
            # 清除相关缓存
            cache_key = get_cache_key("permissions", "load")
            if f"cache_{cache_key}" in st.session_state:
                del st.session_state[f"cache_{cache_key}"]
            
            return True
        
        return safe_cos_operation(_save_operation)

def load_permissions_from_cos(cos_client, bucket_name: str, permissions_file: str) -> Optional[pd.DataFrame]:
    """从COS加载权限数据 - 使用缓存"""
    cache_key = get_cache_key("permissions", "load")
    cached_data = get_cache(cache_key)
    if cached_data is not None:
        logger.info("从缓存加载权限数据")
        return cached_data
    
    with error_handler("加载权限数据"):
        def _load_operation():
            try:
                # 从COS下载文件
                response = cos_client.get_object(
                    Bucket=bucket_name,
                    Key=permissions_file
                )
                
                # 读取CSV内容
                csv_content = response['Body'].read().decode('utf-8-sig')
                df = pd.read_csv(io.StringIO(csv_content))
                
                if len(df) == 0:
                    logger.info("权限表为空")
                    return None
                
                result_df = df[['门店名称', '人员编号']].copy()
                
                # 数据清理
                result_df['门店名称'] = result_df['门店名称'].str.strip()
                result_df['人员编号'] = result_df['人员编号'].str.strip()
                
                # 移除空行
                result_df = result_df[
                    (result_df['门店名称'] != '') & 
                    (result_df['人员编号'] != '')
                ]
                
                logger.info(f"权限数据加载成功: {len(result_df)} 条记录")
                
                # 设置缓存
                set_cache(cache_key, result_df)
                return result_df
                
            except CosServiceError as e:
                if e.get_error_code() == 'NoSuchKey':
                    logger.info("权限文件不存在")
                    return None
                else:
                    raise e
        
        return safe_cos_operation(_load_operation)

def save_report_to_cos(df: pd.DataFrame, cos_client, bucket_name: str, store_name: str) -> bool:
    """保存单个门店报表到COS"""
    try:
        # 生成文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"reports/{store_name}_{timestamp}.xlsx"
        
        # 创建Excel文件
        excel_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name=store_name[:31])  # Excel工作表名最长31字符
        
        excel_content = excel_buffer.getvalue()
        
        # 上传到COS
        cos_client.put_object(
            Bucket=bucket_name,
            Body=excel_content,
            Key=filename,
            ContentType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        
        logger.info(f"报表保存成功: {store_name} -> {filename}")
        return True
        
    except Exception as e:
        logger.error(f"保存 {store_name} 报表失败: {str(e)}")
        return False

def save_reports_to_cos(reports_dict: Dict[str, pd.DataFrame], cos_client, bucket_name: str) -> bool:
    """保存报表数据到COS"""
    with error_handler("保存报表数据"):
        def _save_operation():
            success_count = 0
            total_count = len(reports_dict)
            
            # 显示进度
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            for idx, (store_name, df) in enumerate(reports_dict.items()):
                status_text.text(f"保存 {store_name}...")
                
                if save_report_to_cos(df, cos_client, bucket_name, store_name):
                    success_count += 1
                
                # 更新进度
                progress = (idx + 1) / total_count
                progress_bar.progress(progress)
                
                # API限制延迟
                time.sleep(0.5)
            
            progress_bar.empty()
            status_text.empty()
            
            # 清除相关缓存
            cache_key = get_cache_key("reports", "load")
            if f"cache_{cache_key}" in st.session_state:
                del st.session_state[f"cache_{cache_key}"]
            
            logger.info(f"报表数据保存完成: {success_count}/{total_count}")
            return success_count == total_count
        
        return safe_cos_operation(_save_operation)

def load_reports_from_cos(cos_client, bucket_name: str) -> Dict[str, pd.DataFrame]:
    """从COS加载报表数据 - 使用缓存"""
    cache_key = get_cache_key("reports", "load")
    cached_data = get_cache(cache_key)
    if cached_data is not None:
        logger.info("从缓存加载报表数据")
        return cached_data
    
    with error_handler("加载报表数据"):
        def _load_operation():
            try:
                reports_dict = {}
                
                # 列出reports目录下的所有文件
                response = cos_client.list_objects(
                    Bucket=bucket_name,
                    Prefix='reports/',
                    MaxKeys=1000
                )
                
                if 'Contents' not in response:
                    logger.info("报表目录为空")
                    return {}
                
                # 处理每个Excel文件
                for obj in response['Contents']:
                    key = obj['Key']
                    if key.endswith('.xlsx') and not key.endswith('/'):
                        try:
                            # 从文件名提取门店名称
                            filename = key.split('/')[-1]  # 获取文件名
                            store_name = filename.split('_')[0]  # 提取门店名称
                            
                            # 下载文件
                            file_response = cos_client.get_object(
                                Bucket=bucket_name,
                                Key=key
                            )
                            
                            # 读取Excel文件
                            excel_content = file_response['Body'].read()
                            df = pd.read_excel(io.BytesIO(excel_content))
                            
                            # 数据清理 - 保持与原代码相同的逻辑
                            if len(df) > 0:
                                # 检查第一行是否是门店名称
                                first_row = df.iloc[0]
                                non_empty_count = sum(1 for val in first_row if pd.notna(val) and str(val).strip() != '')
                                
                                if non_empty_count <= 2 and len(df) > 1:
                                    df = df.iloc[1:].reset_index(drop=True)
                            
                            # 处理表头
                            if len(df) > 1:
                                header_row = df.iloc[0].fillna('').astype(str).tolist()
                                data_rows = df.iloc[1:].copy()
                                
                                # 清理列名并处理重复
                                cols = []
                                for i, col in enumerate(header_row):
                                    col = str(col).strip()
                                    if col == '' or col == 'nan' or col == '0':
                                        col = f'列{i+1}' if i > 0 else '项目名称'
                                    
                                    # 处理重复列名
                                    original_col = col
                                    counter = 1
                                    while col in cols:
                                        col = f"{original_col}_{counter}"
                                        counter += 1
                                    cols.append(col)
                                
                                # 确保列数匹配
                                min_cols = min(len(data_rows.columns), len(cols))
                                cols = cols[:min_cols]
                                data_rows = data_rows.iloc[:, :min_cols]
                                
                                data_rows.columns = cols
                                df = data_rows.reset_index(drop=True).fillna('')
                            else:
                                # 处理少于3行的数据
                                df = df.fillna('')
                                default_cols = []
                                for i in range(len(df.columns)):
                                    col_name = f'列{i+1}' if i > 0 else '项目名称'
                                    default_cols.append(col_name)
                                df.columns = default_cols
                            
                            reports_dict[store_name] = df
                            logger.info(f"加载报表: {store_name} ({len(df)} 行)")
                            
                        except Exception as e:
                            logger.warning(f"跳过文件 {key}: {str(e)}")
                            continue
                
                logger.info(f"报表数据加载成功: {len(reports_dict)} 个门店")
                
                # 设置缓存
                set_cache(cache_key, reports_dict)
                return reports_dict
                
            except CosServiceError as e:
                logger.error(f"COS操作失败: {str(e)}")
                return {}
        
        return safe_cos_operation(_load_operation)

def analyze_receivable_data(df: pd.DataFrame) -> Dict[str, Any]:
    """分析应收未收额数据 - 专门查找第69行"""
    result = {}
    
    if len(df.columns) == 0 or len(df) == 0:
        return result
    
    # 检查第一行是否是门店名称
    original_df = df.copy()
    first_row = df.iloc[0] if len(df) > 0 else None
    if first_row is not None:
        non_empty_count = sum(1 for val in first_row if pd.notna(val) and str(val).strip() != '')
        if non_empty_count <= 2:
            df = df.iloc[1:].reset_index(drop=True)
            result['skipped_store_name_row'] = True
    
    # 查找第69行
    target_row_index = 68  # 第69行
    
    if len(df) > target_row_index:
        row = df.iloc[target_row_index]
        first_col_value = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
        
        # 检查关键词
        keywords = ['应收-未收额', '应收未收额', '应收-未收', '应收未收']
        
        for keyword in keywords:
            if keyword in first_col_value:
                # 查找数值
                for col_idx in range(len(row)-1, 0, -1):
                    val = row.iloc[col_idx]
                    if pd.notna(val) and str(val).strip() not in ['', 'None', 'nan']:
                        cleaned = str(val).replace(',', '').replace('¥', '').replace('￥', '').strip()
                        
                        if cleaned.startswith('(') and cleaned.endswith(')'):
                            cleaned = '-' + cleaned[1:-1]
                        
                        try:
                            amount = float(cleaned)
                            if amount != 0:
                                result['应收-未收额'] = {
                                    'amount': amount,
                                    'column_name': str(df.columns[col_idx]),
                                    'row_name': first_col_value,
                                    'row_index': target_row_index,
                                    'actual_row_number': target_row_index + 1
                                }
                                return result
                        except ValueError:
                            continue
                break
    
    # 备用查找
    if '应收-未收额' not in result:
        keywords = ['应收-未收额', '应收未收额', '应收-未收', '应收未收']
        
        for idx, row in df.iterrows():
            try:
                row_name = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
                
                if not row_name.strip():
                    continue
                
                for keyword in keywords:
                    if keyword in row_name:
                        for col_idx in range(len(row)-1, 0, -1):
                            val = row.iloc[col_idx]
                            if pd.notna(val) and str(val).strip() not in ['', 'None', 'nan']:
                                cleaned = str(val).replace(',', '').replace('¥', '').replace('￥', '').strip()
                                
                                if cleaned.startswith('(') and cleaned.endswith(')'):
                                    cleaned = '-' + cleaned[1:-1]
                                
                                try:
                                    amount = float(cleaned)
                                    if amount != 0:
                                        result['应收-未收额'] = {
                                            'amount': amount,
                                            'column_name': str(df.columns[col_idx]),
                                            'row_name': row_name,
                                            'row_index': idx,
                                            'actual_row_number': idx + 1,
                                            'note': f'在第{idx+1}行找到（非第69行）'
                                        }
                                        return result
                                except ValueError:
                                    continue
                        break
            except Exception:
                continue
    
    # 调试信息
    result['debug_info'] = {
        'total_rows': len(df),
        'checked_row_69': len(df) > target_row_index,
        'row_69_content': str(df.iloc[target_row_index].iloc[0]) if len(df) > target_row_index else 'N/A'
    }
    
    return result

def verify_user_permission(store_name: str, user_id: str, permissions_data: Optional[pd.DataFrame]) -> bool:
    """验证用户权限"""
    if permissions_data is None or len(permissions_data.columns) < 2:
        return False
    
    store_col = permissions_data.columns[0]
    id_col = permissions_data.columns[1]
    
    for _, row in permissions_data.iterrows():
        stored_store = str(row[store_col]).strip()
        stored_id = str(row[id_col]).strip()
        
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

def show_status_message(message: str, status_type: str = "info"):
    """显示状态消息"""
    css_class = f"status-{status_type}"
    st.markdown(f'<div class="{css_class}">{message}</div>', unsafe_allow_html=True)

# 初始化会话状态
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
if 'store_name' not in st.session_state:
    st.session_state.store_name = ""
if 'user_id' not in st.session_state:
    st.session_state.user_id = ""
if 'is_admin' not in st.session_state:
    st.session_state.is_admin = False
if 'cos_client' not in st.session_state:
    st.session_state.cos_client = None
if 'operation_status' not in st.session_state:
    st.session_state.operation_status = []

# 主标题
st.markdown('<h1 class="main-header">📊 门店报表查询系统</h1>', unsafe_allow_html=True)

# 初始化腾讯云COS客户端
if not st.session_state.cos_client:
    try:
        with st.spinner("连接腾讯云存储..."):
            cos_client, bucket_name, permissions_file = get_cos_client()
            st.session_state.cos_client = (cos_client, bucket_name, permissions_file)
            show_status_message("✅ 腾讯云存储连接成功！", "success")
    except Exception as e:
        show_status_message(f"❌ 连接失败: {str(e)}", "error")
        st.stop()

cos_client, bucket_name, permissions_file = st.session_state.cos_client

# 显示操作状态
for status in st.session_state.operation_status:
    show_status_message(status['message'], status['type'])

# 侧边栏
with st.sidebar:
    st.title("⚙️ 系统功能")
    
    # 系统状态
    st.subheader("📡 系统状态")
    if cos_client:
        st.success("🟢 腾讯云存储已连接")
    else:
        st.error("🔴 腾讯云存储断开")
    
    user_type = st.radio("选择用户类型", ["普通用户", "管理员"])
    
    if user_type == "管理员":
        st.subheader("🔐 管理员登录")
        admin_password = st.text_input("管理员密码", type="password")
        
        if st.button("验证管理员身份"):
            if admin_password == ADMIN_PASSWORD:
                st.session_state.is_admin = True
                show_status_message("✅ 管理员验证成功！", "success")
                st.rerun()
            else:
                show_status_message("❌ 密码错误！", "error")
        
        if st.session_state.is_admin:
            st.subheader("📁 文件管理")
            
            # 上传权限表
            permissions_file_upload = st.file_uploader("上传门店权限表", type=['xlsx', 'xls'])
            if permissions_file_upload:
                try:
                    with st.spinner("处理权限表文件..."):
                        df = pd.read_excel(permissions_file_upload)
                        if len(df.columns) >= 2:
                            with st.spinner("保存到腾讯云..."):
                                if save_permissions_to_cos(df, cos_client, bucket_name, permissions_file):
                                    show_status_message(f"✅ 权限表已上传：{len(df)} 个用户", "success")
                                    st.balloons()
                                else:
                                    show_status_message("❌ 保存失败", "error")
                        else:
                            show_status_message("❌ 格式错误：需要至少两列（门店名称、人员编号）", "error")
                except Exception as e:
                    show_status_message(f"❌ 处理失败：{str(e)}", "error")
            
            # 上传财务报表
            reports_file_upload = st.file_uploader("上传财务报表", type=['xlsx', 'xls'])
            if reports_file_upload:
                try:
                    with st.spinner("处理报表文件..."):
                        excel_file = pd.ExcelFile(reports_file_upload)
                        reports_dict = {}
                        
                        for sheet in excel_file.sheet_names:
                            try:
                                df = pd.read_excel(reports_file_upload, sheet_name=sheet)
                                if not df.empty:
                                    reports_dict[sheet] = df
                                    logger.info(f"读取工作表 '{sheet}': {len(df)} 行")
                            except Exception as e:
                                logger.warning(f"跳过工作表 '{sheet}': {str(e)}")
                                continue
                        
                        if reports_dict:
                            with st.spinner("保存到腾讯云..."):
                                if save_reports_to_cos(reports_dict, cos_client, bucket_name):
                                    show_status_message(f"✅ 报表已上传：{len(reports_dict)} 个门店", "success")
                                    st.balloons()
                                else:
                                    show_status_message("❌ 保存失败", "error")
                        else:
                            show_status_message("❌ 文件中没有有效的工作表", "error")
                            
                except Exception as e:
                    show_status_message(f"❌ 处理失败：{str(e)}", "error")
            
            # 缓存管理
            st.subheader("🗂️ 缓存管理")
            if st.button("清除所有缓存"):
                cache_keys = [key for key in st.session_state.keys() if key.startswith('cache_')]
                for key in cache_keys:
                    del st.session_state[key]
                show_status_message("✅ 缓存已清除", "success")
                st.rerun()
    
    else:
        if st.session_state.logged_in:
            st.subheader("👤 当前登录")
            st.info(f"门店：{st.session_state.store_name}")
            st.info(f"编号：{st.session_state.user_id}")
            
            if st.button("🚪 退出登录"):
                st.session_state.logged_in = False
                st.session_state.store_name = ""
                st.session_state.user_id = ""
                show_status_message("👋 已退出登录", "success")
                st.rerun()

# 清除状态消息
st.session_state.operation_status = []

# 主界面
if user_type == "管理员" and st.session_state.is_admin:
    st.markdown('<div class="admin-panel"><h3>👨‍💼 管理员控制面板</h3><p>数据永久保存在腾讯云，支持高效存储和缓存机制</p></div>', unsafe_allow_html=True)
    
    try:
        with st.spinner("加载数据统计..."):
            permissions_data = load_permissions_from_cos(cos_client, bucket_name, permissions_file)
            reports_data = load_reports_from_cos(cos_client, bucket_name)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            perms_count = len(permissions_data) if permissions_data is not None else 0
            st.metric("权限表用户数", perms_count)
        with col2:
            reports_count = len(reports_data)
            st.metric("报表门店数", reports_count)
        with col3:
            cache_count = len([key for key in st.session_state.keys() if key.startswith('cache_')])
            st.metric("缓存项目数", cache_count)
            
        # 数据预览
        if permissions_data is not None and len(permissions_data) > 0:
            st.subheader("👥 权限数据预览")
            st.dataframe(permissions_data.head(10), use_container_width=True)
        
        if reports_data:
            st.subheader("📊 报表数据预览")
            report_names = list(reports_data.keys())[:5]  # 显示前5个
            for name in report_names:
                with st.expander(f"📋 {name}"):
                    df = reports_data[name]
                    st.write(f"数据规模: {len(df)} 行 × {len(df.columns)} 列")
                    st.dataframe(df.head(3), use_container_width=True)
                    
    except Exception as e:
        show_status_message(f"❌ 数据加载失败：{str(e)}", "error")

elif user_type == "管理员" and not st.session_state.is_admin:
    st.info("👈 请在左侧边栏输入管理员密码")

else:
    if not st.session_state.logged_in:
        st.subheader("🔐 用户登录")
        
        try:
            with st.spinner("加载权限数据..."):
                permissions_data = load_permissions_from_cos(cos_client, bucket_name, permissions_file)
            
            if permissions_data is None:
                st.warning("⚠️ 系统维护中，请联系管理员")
            else:
                stores = sorted(permissions_data[permissions_data.columns[0]].unique().tolist())
                
                with st.form("login_form"):
                    selected_store = st.selectbox("选择门店", stores)
                    user_id = st.text_input("人员编号")
                    submit = st.form_submit_button("🚀 登录")
                    
                    if submit and selected_store and user_id:
                        if verify_user_permission(selected_store, user_id, permissions_data):
                            st.session_state.logged_in = True
                            st.session_state.store_name = selected_store
                            st.session_state.user_id = user_id
                            show_status_message("✅ 登录成功！", "success")
                            st.balloons()
                            st.rerun()
                        else:
                            show_status_message("❌ 门店或编号错误！", "error")
                            
        except Exception as e:
            show_status_message(f"❌ 权限验证失败：{str(e)}", "error")
    
    else:
        # 已登录 - 显示报表
        st.markdown(f'<div class="store-info"><h3>🏪 {st.session_state.store_name}</h3><p>操作员：{st.session_state.user_id}</p></div>', unsafe_allow_html=True)
        
        try:
            with st.spinner("加载报表数据..."):
                reports_data = load_reports_from_cos(cos_client, bucket_name)
                matching_sheets = find_matching_reports(st.session_state.store_name, reports_data)
            
            if matching_sheets:
                if len(matching_sheets) > 1:
                    selected_sheet = st.selectbox("选择报表", matching_sheets)
                else:
                    selected_sheet = matching_sheets[0]
                
                df = reports_data[selected_sheet]
                
                # 应收-未收额看板
                st.subheader("💰 应收-未收额")
                
                try:
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
                                        <p style="margin: 0; font-size: 0.9rem;">数据来源: {data['row_name']} (第{data['actual_row_number']}行)</p>
                                    </div>
                                ''', unsafe_allow_html=True)
                            
                            elif amount < 0:
                                st.markdown(f'''
                                    <div class="receivable-negative">
                                        <h1 style="margin: 0; font-size: 3rem;">💚 ¥{abs(amount):,.2f}</h1>
                                        <h3 style="margin: 0.5rem 0;">总部应退款</h3>
                                        <p style="margin: 0; font-size: 0.9rem;">数据来源: {data['row_name']} (第{data['actual_row_number']}行)</p>
                                    </div>
                                ''', unsafe_allow_html=True)
                            
                            else:
                                st.markdown('''
                                    <div style="background: #e8f5e8; color: #2e7d32; padding: 2rem; border-radius: 15px; text-align: center;">
                                        <h1 style="margin: 0; font-size: 3rem;">⚖️ ¥0.00</h1>
                                        <h3 style="margin: 0.5rem 0;">收支平衡</h3>
                                        <p style="margin: 0;">应收未收额为零，账目平衡</p>
                                    </div>
                                ''', unsafe_allow_html=True)
                    
                    else:
                        st.warning("⚠️ 未找到应收-未收额数据")
                        
                        with st.expander("🔍 查看详情", expanded=False):
                            debug_info = analysis_results.get('debug_info', {})
                            
                            st.markdown("### 📋 数据查找说明")
                            st.write(f"- **报表总行数：** {debug_info.get('total_rows', 0)} 行")
                            
                            if debug_info.get('checked_row_69'):
                                st.write(f"- **第69行内容：** {debug_info.get('row_69_content', 'N/A')}")
                            else:
                                st.write("- **第69行：** 报表行数不足69行")
                            
                            st.markdown("""
                            ### 💡 可能的原因
                            1. 第69行不包含"应收-未收额"相关关键词
                            2. 第69行的数值为空或格式不正确
                            3. 报表格式与预期不符
                            
                            ### 🛠️ 建议
                            - 请检查Excel报表第69行是否包含"应收-未收额"
                            - 确认该行有对应的金额数据
                            - 如需调整查找位置，请联系技术支持
                            """)
                
                except Exception as e:
                    show_status_message(f"❌ 分析数据时出错：{str(e)}", "error")
                
                st.divider()
                
                # 完整报表数据
                st.subheader("📋 完整报表数据")
                
                search_term = st.text_input("🔍 搜索报表内容")
                
                try:
                    if search_term:
                        search_df = df.copy()
                        for col in search_df.columns:
                            search_df[col] = search_df[col].astype(str).fillna('')
                        
                        mask = search_df.apply(
                            lambda x: x.str.contains(search_term, case=False, na=False, regex=False)
                        ).any(axis=1)
                        filtered_df = df[mask]
                        st.info(f"找到 {len(filtered_df)} 条包含 '{search_term}' 的记录")
                    else:
                        filtered_df = df
                    
                    st.info(f"📊 数据统计：共 {len(filtered_df)} 条记录，{len(df.columns)} 列")
                    
                    if len(filtered_df) > 0:
                        display_df = filtered_df.copy()
                        
                        # 确保列名唯一
                        unique_columns = []
                        for i, col in enumerate(display_df.columns):
                            col_name = str(col)
                            if col_name in unique_columns:
                                col_name = f"{col_name}_{i}"
                            unique_columns.append(col_name)
                        display_df.columns = unique_columns
                        
                        # 清理数据内容
                        for col in display_df.columns:
                            display_df[col] = display_df[col].astype(str).fillna('')
                        
                        st.dataframe(display_df, use_container_width=True, height=400)
                    
                    else:
                        st.warning("没有找到符合条件的数据")
                        
                except Exception as e:
                    show_status_message(f"❌ 数据处理时出错：{str(e)}", "error")
                
                # 下载功能
                st.subheader("📥 数据下载")
                
                col1, col2 = st.columns(2)
                with col1:
                    try:
                        buffer = io.BytesIO()
                        download_df = df.copy()
                        
                        # 确保列名唯一
                        unique_cols = []
                        for i, col in enumerate(download_df.columns):
                            col_name = str(col)
                            if col_name in unique_cols:
                                col_name = f"{col_name}_{i}"
                            unique_cols.append(col_name)
                        download_df.columns = unique_cols
                        
                        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                            download_df.to_excel(writer, index=False)
                        
                        st.download_button(
                            "📥 下载完整报表 (Excel)",
                            buffer.getvalue(),
                            f"{st.session_state.store_name}_报表_{datetime.now().strftime('%Y%m%d')}.xlsx",
                            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                    except Exception as e:
                        show_status_message(f"Excel下载准备失败：{str(e)}", "error")
                
                with col2:
                    try:
                        csv_df = df.copy()
                        unique_cols = []
                        for i, col in enumerate(csv_df.columns):
                            col_name = str(col)
                            if col_name in unique_cols:
                                col_name = f"{col_name}_{i}"
                            unique_cols.append(col_name)
                        csv_df.columns = unique_cols
                        
                        csv = csv_df.to_csv(index=False, encoding='utf-8-sig')
                        st.download_button(
                            "📥 下载CSV格式",
                            csv,
                            f"{st.session_state.store_name}_报表_{datetime.now().strftime('%Y%m%d')}.csv",
                            "text/csv"
                        )
                    except Exception as e:
                        show_status_message(f"CSV下载准备失败：{str(e)}", "error")
            
            else:
                st.error(f"❌ 未找到门店 '{st.session_state.store_name}' 的报表")
                
        except Exception as e:
            show_status_message(f"❌ 报表加载失败：{str(e)}", "error")

# 页面底部状态信息
st.divider()
col1, col2, col3 = st.columns(3)
with col1:
    st.caption(f"🕒 当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
with col2:
    cache_count = len([key for key in st.session_state.keys() if key.startswith('cache_')])
    st.caption(f"💾 缓存项目: {cache_count}")
with col3:
    st.caption("🔧 版本: v3.0 (腾讯云版)")
