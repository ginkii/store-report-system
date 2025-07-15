import streamlit as st
import pandas as pd
import io
import json
from datetime import datetime
import time
import gspread
from google.oauth2.service_account import Credentials
import logging
from typing import Optional, Dict, Any, List
import hashlib
import pickle
import traceback
from contextlib import contextmanager
import os
import tempfile

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
PERMISSIONS_SHEET_NAME = "store_permissions"
REPORTS_SHEET_NAME = "store_reports"
SYSTEM_INFO_SHEET_NAME = "system_info"
MAX_RETRIES = 3
RETRY_DELAY = 1
MAX_CHUNK_SIZE = 30000  # 减小分片大小
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
    .diagnostic-panel {
        background: #f8f9fa;
        border: 1px solid #dee2e6;
        border-radius: 8px;
        padding: 1rem;
        margin: 1rem 0;
    }
    </style>
""", unsafe_allow_html=True)

class SheetOperationError(Exception):
    """Google Sheets操作异常"""
    pass

class DataProcessingError(Exception):
    """数据处理异常"""
    pass

class PermissionError(Exception):
    """权限验证异常"""
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

def diagnose_google_sheets_config() -> Dict[str, Any]:
    """诊断Google Sheets配置"""
    diagnosis = {
        'has_secrets': False,
        'credentials_valid': False,
        'required_fields': [],
        'missing_fields': [],
        'error_message': None
    }
    
    try:
        # 检查secrets配置
        if "google_sheets" in st.secrets:
            diagnosis['has_secrets'] = True
            credentials_info = st.secrets["google_sheets"]
            
            # 检查必需字段
            required_fields = [
                'type', 'project_id', 'private_key_id', 'private_key',
                'client_email', 'client_id', 'auth_uri', 'token_uri'
            ]
            
            for field in required_fields:
                if field in credentials_info:
                    diagnosis['required_fields'].append(field)
                else:
                    diagnosis['missing_fields'].append(field)
            
            # 检查凭据格式
            if len(diagnosis['missing_fields']) == 0:
                diagnosis['credentials_valid'] = True
            else:
                diagnosis['error_message'] = f"缺少必需字段: {', '.join(diagnosis['missing_fields'])}"
        else:
            diagnosis['error_message'] = "未找到 google_sheets 密钥配置"
            
    except Exception as e:
        diagnosis['error_message'] = f"配置检查失败: {str(e)}"
    
    return diagnosis

def create_google_sheets_client_with_diagnosis():
    """创建Google Sheets客户端并提供诊断信息"""
    diagnosis = diagnose_google_sheets_config()
    
    if not diagnosis['credentials_valid']:
        raise SheetOperationError(f"Google Sheets配置错误: {diagnosis['error_message']}")
    
    try:
        credentials_info = st.secrets["google_sheets"]
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.file",
            "https://www.googleapis.com/auth/drive"
        ]
        
        credentials = Credentials.from_service_account_info(credentials_info, scopes=scopes)
        client = gspread.authorize(credentials)
        
        # 测试连接
        try:
            # 尝试访问一个测试表格或创建一个临时表格来验证权限
            test_sheet = client.create("权限测试表格_" + str(int(time.time())))
            test_sheet.share('', perm_type='anyone', role='reader')
            client.del_spreadsheet(test_sheet.id)  # 删除测试表格
            logger.info("Google Sheets客户端创建成功，权限验证通过")
        except Exception as perm_error:
            logger.warning(f"权限测试失败: {str(perm_error)}")
            # 即使权限测试失败，也尝试继续使用客户端
        
        return client
        
    except Exception as e:
        logger.error(f"Google Sheets客户端创建失败: {str(e)}")
        raise SheetOperationError(f"连接失败: {str(e)}")

@st.cache_resource(show_spinner="连接云数据库...")
def get_google_sheets_client():
    """获取Google Sheets客户端 - 使用缓存"""
    return create_google_sheets_client_with_diagnosis()

def safe_sheet_operation(operation_func, *args, **kwargs):
    """安全的表格操作"""
    return retry_operation(operation_func, *args, **kwargs)

def get_or_create_spreadsheet(gc, name="门店报表系统数据"):
    """获取或创建表格 - 增强错误处理"""
    def _operation():
        try:
            # 首先尝试打开现有表格
            spreadsheet = gc.open(name)
            logger.info(f"表格 '{name}' 已存在")
            return spreadsheet
        except gspread.SpreadsheetNotFound:
            logger.info(f"创建新表格 '{name}'")
            try:
                spreadsheet = gc.create(name)
                # 设置权限为可编辑
                spreadsheet.share('', perm_type='anyone', role='writer')
                return spreadsheet
            except Exception as create_error:
                logger.error(f"创建表格失败: {str(create_error)}")
                # 如果创建失败，尝试使用备用名称
                backup_name = f"{name}_{int(time.time())}"
                logger.info(f"尝试创建备用表格: {backup_name}")
                spreadsheet = gc.create(backup_name)
                spreadsheet.share('', perm_type='anyone', role='writer')
                return spreadsheet
        except Exception as e:
            logger.error(f"表格操作失败: {str(e)}")
            raise SheetOperationError(f"无法访问或创建表格: {str(e)}")
    
    return safe_sheet_operation(_operation)

def get_or_create_worksheet(spreadsheet, name, rows=1000, cols=20):
    """获取或创建工作表 - 增强错误处理"""
    def _operation():
        try:
            worksheet = spreadsheet.worksheet(name)
            logger.info(f"工作表 '{name}' 已存在")
            return worksheet
        except gspread.WorksheetNotFound:
            logger.info(f"创建新工作表 '{name}'")
            try:
                worksheet = spreadsheet.add_worksheet(title=name, rows=rows, cols=cols)
                return worksheet
            except Exception as create_error:
                # 如果创建失败，尝试使用第一个工作表
                logger.warning(f"创建工作表失败: {str(create_error)}")
                worksheets = spreadsheet.worksheets()
                if worksheets:
                    worksheet = worksheets[0]
                    logger.info(f"使用现有工作表: {worksheet.title}")
                    return worksheet
                else:
                    raise SheetOperationError("无法创建或找到工作表")
        except Exception as e:
            logger.error(f"工作表操作失败: {str(e)}")
            raise SheetOperationError(f"无法访问或创建工作表: {str(e)}")
    
    return safe_sheet_operation(_operation)

def create_local_backup(data: Any, backup_type: str) -> str:
    """创建本地备份"""
    try:
        backup_dir = tempfile.gettempdir()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = os.path.join(backup_dir, f"{backup_type}_backup_{timestamp}.json")
        
        if isinstance(data, pd.DataFrame):
            data_dict = data.to_dict('records')
        else:
            data_dict = data
        
        with open(backup_file, 'w', encoding='utf-8') as f:
            json.dump(data_dict, f, ensure_ascii=False, indent=2)
        
        logger.info(f"本地备份创建成功: {backup_file}")
        return backup_file
    except Exception as e:
        logger.error(f"创建本地备份失败: {str(e)}")
        return ""

def load_local_backup(backup_type: str) -> Optional[Any]:
    """加载最新的本地备份"""
    try:
        backup_dir = tempfile.gettempdir()
        backup_files = [f for f in os.listdir(backup_dir) if f.startswith(f"{backup_type}_backup_")]
        
        if not backup_files:
            return None
        
        # 按时间排序，获取最新的备份
        backup_files.sort(reverse=True)
        latest_backup = os.path.join(backup_dir, backup_files[0])
        
        with open(latest_backup, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        logger.info(f"本地备份加载成功: {latest_backup}")
        return data
    except Exception as e:
        logger.error(f"加载本地备份失败: {str(e)}")
        return None

def clean_dataframe_for_json(df: pd.DataFrame) -> pd.DataFrame:
    """清理DataFrame以便JSON序列化"""
    try:
        df_cleaned = df.copy()
        
        # 处理各种数据类型
        for col in df_cleaned.columns:
            # 转换为字符串并处理特殊值
            df_cleaned[col] = df_cleaned[col].astype(str)
            df_cleaned[col] = df_cleaned[col].replace({
                'nan': '',
                'None': '',
                'NaT': '',
                'null': '',
                '<NA>': ''
            })
            
            # 处理过长的字符串
            df_cleaned[col] = df_cleaned[col].apply(
                lambda x: x[:1000] + '...' if len(str(x)) > 1000 else x
            )
        
        logger.info(f"DataFrame清理完成: {len(df_cleaned)} 行 x {len(df_cleaned.columns)} 列")
        return df_cleaned
        
    except Exception as e:
        logger.error(f"清理DataFrame失败: {str(e)}")
        raise DataProcessingError(f"数据清理失败: {str(e)}")

def save_permissions_to_sheets(df: pd.DataFrame, gc) -> bool:
    """保存权限数据 - 增强版"""
    with error_handler("保存权限数据"):
        # 先创建本地备份
        backup_file = create_local_backup(df, "permissions")
        
        def _save_operation():
            try:
                spreadsheet = get_or_create_spreadsheet(gc)
                worksheet = get_or_create_worksheet(spreadsheet, PERMISSIONS_SHEET_NAME)
                
                # 清空现有数据
                worksheet.clear()
                time.sleep(1)  # API限制延迟
                
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                all_data = [['门店名称', '人员编号', '更新时间']]
                
                # 准备数据
                for _, row in df.iterrows():
                    all_data.append([
                        str(row.iloc[0]).strip(),
                        str(row.iloc[1]).strip(),
                        current_time
                    ])
                
                # 批量更新
                worksheet.update('A1', all_data)
                logger.info(f"权限数据保存成功: {len(df)} 条记录")
                
                # 清除相关缓存
                cache_key = get_cache_key("permissions", "load")
                if f"cache_{cache_key}" in st.session_state:
                    del st.session_state[f"cache_{cache_key}"]
                
                return True
                
            except Exception as e:
                logger.error(f"保存到云端失败: {str(e)}")
                # 如果云端保存失败，至少有本地备份
                if backup_file:
                    st.warning(f"云端保存失败，但已创建本地备份: {backup_file}")
                raise
        
        try:
            return safe_sheet_operation(_save_operation)
        except Exception:
            # 如果完全失败，尝试使用session state保存
            st.session_state['permissions_fallback'] = df.to_dict('records')
            st.warning("数据已临时保存到浏览器缓存中")
            return False

def load_permissions_from_sheets(gc) -> Optional[pd.DataFrame]:
    """加载权限数据 - 使用缓存和备用方案"""
    cache_key = get_cache_key("permissions", "load")
    cached_data = get_cache(cache_key)
    if cached_data is not None:
        logger.info("从缓存加载权限数据")
        return cached_data
    
    with error_handler("加载权限数据"):
        def _load_operation():
            try:
                spreadsheet = get_or_create_spreadsheet(gc)
                worksheet = spreadsheet.worksheet(PERMISSIONS_SHEET_NAME)
                data = worksheet.get_all_values()
                
                if len(data) <= 1:
                    logger.info("权限表为空")
                    return None
                
                df = pd.DataFrame(data[1:], columns=['门店名称', '人员编号', '更新时间'])
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
                
            except gspread.WorksheetNotFound:
                logger.info("权限表不存在")
                return None
            except Exception as e:
                logger.error(f"从云端加载失败: {str(e)}")
                
                # 尝试从session state加载
                if 'permissions_fallback' in st.session_state:
                    logger.info("从浏览器缓存加载权限数据")
                    fallback_data = st.session_state['permissions_fallback']
                    df = pd.DataFrame(fallback_data)
                    return df[['门店名称', '人员编号']] if len(df.columns) >= 2 else None
                
                # 尝试从本地备份加载
                backup_data = load_local_backup("permissions")
                if backup_data:
                    logger.info("从本地备份加载权限数据")
                    df = pd.DataFrame(backup_data)
                    return df[['门店名称', '人员编号']] if len(df.columns) >= 2 else None
                
                raise
        
        try:
            return safe_sheet_operation(_load_operation)
        except Exception:
            return None

def save_large_data_to_sheets(data_dict: Dict[str, Any], worksheet, batch_size: int = 15) -> bool:
    """分批保存大数据到表格"""
    try:
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        all_data = [['门店名称', '报表数据JSON', '行数', '列数', '更新时间', '分片序号', '总分片数', '数据哈希']]
        
        for store_name, df in data_dict.items():
            try:
                # 清理数据
                df_cleaned = clean_dataframe_for_json(df)
                
                # 转换为JSON
                json_data = df_cleaned.to_json(orient='records', force_ascii=False, ensure_ascii=False)
                
                # 计算数据哈希用于验证
                data_hash = hashlib.md5(json_data.encode('utf-8')).hexdigest()[:16]
                
                # 检查是否需要分片
                if len(json_data) <= MAX_CHUNK_SIZE:
                    # 不需要分片
                    all_data.append([
                        store_name, 
                        json_data, 
                        len(df), 
                        len(df.columns), 
                        current_time, 
                        "1", 
                        "1",
                        data_hash
                    ])
                else:
                    # 分片存储
                    chunks = []
                    for i in range(0, len(json_data), MAX_CHUNK_SIZE):
                        chunks.append(json_data[i:i + MAX_CHUNK_SIZE])
                    
                    total_chunks = len(chunks)
                    
                    for idx, chunk in enumerate(chunks):
                        chunk_name = f"{store_name}_分片{idx+1}"
                        all_data.append([
                            chunk_name, 
                            chunk, 
                            len(df), 
                            len(df.columns), 
                            current_time, 
                            str(idx+1), 
                            str(total_chunks),
                            data_hash
                        ])
                
                logger.info(f"准备保存 {store_name}: {len(df)} 行数据")
                
            except Exception as e:
                logger.error(f"处理 {store_name} 时出错: {str(e)}")
                # 保存错误信息
                error_data = {
                    "error": str(e),
                    "rows": len(df) if 'df' in locals() else 0,
                    "columns": len(df.columns) if 'df' in locals() else 0,
                    "timestamp": current_time
                }
                all_data.append([
                    f"{store_name}_错误", 
                    json.dumps(error_data, ensure_ascii=False), 
                    0, 
                    0, 
                    current_time, 
                    "1", 
                    "1",
                    "ERROR"
                ])
                continue
        
        # 分批上传数据
        if len(all_data) > 1:
            for i in range(1, len(all_data), batch_size):
                batch_data = all_data[i:i+batch_size]
                
                if i == 1:
                    # 第一批包含标题行
                    worksheet.update('A1', [all_data[0]] + batch_data)
                else:
                    # 后续批次
                    row_num = i + 1
                    worksheet.update(f'A{row_num}', batch_data)
                
                # API限制延迟
                time.sleep(0.8)
                
                # 显示进度
                progress = min(i + batch_size, len(all_data) - 1)
                st.progress(progress / (len(all_data) - 1))
        
        logger.info(f"数据保存完成: {len(all_data) - 1} 条记录")
        return True
        
    except Exception as e:
        logger.error(f"保存大数据失败: {str(e)}")
        raise

def save_reports_to_sheets(reports_dict: Dict[str, pd.DataFrame], gc) -> bool:
    """保存报表数据 - 增强版"""
    with error_handler("保存报表数据"):
        # 先创建本地备份
        backup_file = create_local_backup(reports_dict, "reports")
        
        def _save_operation():
            try:
                spreadsheet = get_or_create_spreadsheet(gc)
                worksheet = get_or_create_worksheet(spreadsheet, REPORTS_SHEET_NAME, rows=2000, cols=10)
                
                # 清空现有数据
                with st.spinner("清理旧数据..."):
                    worksheet.clear()
                    time.sleep(1)
                
                # 保存数据
                with st.spinner("保存新数据..."):
                    success = save_large_data_to_sheets(reports_dict, worksheet)
                
                if success:
                    # 清除相关缓存
                    cache_key = get_cache_key("reports", "load")
                    if f"cache_{cache_key}" in st.session_state:
                        del st.session_state[f"cache_{cache_key}"]
                    
                    logger.info("报表数据保存成功")
                    return True
                return False
                
            except Exception as e:
                logger.error(f"保存到云端失败: {str(e)}")
                # 如果云端保存失败，至少有本地备份
                if backup_file:
                    st.warning(f"云端保存失败，但已创建本地备份: {backup_file}")
                raise
        
        try:
            return safe_sheet_operation(_save_operation)
        except Exception:
            # 如果完全失败，尝试使用session state保存
            st.session_state['reports_fallback'] = {
                name: df.to_dict('records') for name, df in reports_dict.items()
            }
            st.warning("数据已临时保存到浏览器缓存中")
            return False

def reconstruct_fragmented_data(fragments: List[Dict[str, Any]], store_name: str) -> Optional[pd.DataFrame]:
    """重构分片数据"""
    try:
        if len(fragments) == 1:
            # 单片数据
            json_data = fragments[0]['json_data']
        else:
            # 多片数据需要重构
            fragments.sort(key=lambda x: int(x['chunk_num']))
            json_data = ''.join([frag['json_data'] for frag in fragments])
        
        # 验证数据完整性
        expected_hash = fragments[0].get('data_hash', '')
        if expected_hash and expected_hash != 'ERROR':
            actual_hash = hashlib.md5(json_data.encode('utf-8')).hexdigest()[:16]
            if actual_hash != expected_hash:
                logger.warning(f"{store_name} 数据哈希不匹配，可能存在数据损坏")
        
        # 解析JSON
        df = pd.read_json(json_data, orient='records')
        
        # 数据后处理
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
        
        logger.info(f"{store_name} 数据重构成功: {len(df)} 行")
        return df
        
    except Exception as e:
        logger.error(f"重构 {store_name} 数据失败: {str(e)}")
        return None

def load_reports_from_sheets(gc) -> Dict[str, pd.DataFrame]:
    """加载报表数据 - 使用缓存、分片重构和备用方案"""
    cache_key = get_cache_key("reports", "load")
    cached_data = get_cache(cache_key)
    if cached_data is not None:
        logger.info("从缓存加载报表数据")
        return cached_data
    
    with error_handler("加载报表数据"):
        def _load_operation():
            try:
                spreadsheet = get_or_create_spreadsheet(gc)
                worksheet = spreadsheet.worksheet(REPORTS_SHEET_NAME)
                data = worksheet.get_all_values()
                
                if len(data) <= 1:
                    logger.info("报表数据为空")
                    return {}
                
                # 解析数据
                reports_dict = {}
                fragments_dict = {}  # 存储分片数据
                
                for row in data[1:]:
                    if len(row) >= 7:  # 确保有足够的列
                        store_name = row[0]
                        json_data = row[1]
                        rows_count = row[2]
                        cols_count = row[3]
                        update_time = row[4]
                        chunk_num = row[5]
                        total_chunks = row[6]
                        data_hash = row[7] if len(row) > 7 else ''
                        
                        # 跳过错误数据
                        if store_name.endswith('_错误'):
                            logger.warning(f"跳过错误数据: {store_name}")
                            continue
                        
                        # 处理分片数据
                        if '_分片' in store_name:
                            base_name = store_name.split('_分片')[0]
                            if base_name not in fragments_dict:
                                fragments_dict[base_name] = []
                            
                            fragments_dict[base_name].append({
                                'json_data': json_data,
                                'chunk_num': chunk_num,
                                'total_chunks': total_chunks,
                                'data_hash': data_hash
                            })
                        else:
                            # 单片数据
                            fragments_dict[store_name] = [{
                                'json_data': json_data,
                                'chunk_num': '1',
                                'total_chunks': '1',
                                'data_hash': data_hash
                            }]
                
                # 重构所有分片数据
                for store_name, fragments in fragments_dict.items():
                    df = reconstruct_fragmented_data(fragments, store_name)
                    if df is not None:
                        reports_dict[store_name] = df
                
                logger.info(f"报表数据加载成功: {len(reports_dict)} 个门店")
                
                # 设置缓存
                set_cache(cache_key, reports_dict)
                return reports_dict
                
            except gspread.WorksheetNotFound:
                logger.info("报表数据表不存在")
                return {}
            except Exception as e:
                logger.error(f"从云端加载失败: {str(e)}")
                
                # 尝试从session state加载
                if 'reports_fallback' in st.session_state:
                    logger.info("从浏览器缓存加载报表数据")
                    fallback_data = st.session_state['reports_fallback']
                    reports_dict = {}
                    for name, records in fallback_data.items():
                        reports_dict[name] = pd.DataFrame(records)
                    return reports_dict
                
                # 尝试从本地备份加载
                backup_data = load_local_backup("reports")
                if backup_data:
                    logger.info("从本地备份加载报表数据")
                    reports_dict = {}
                    for name, records in backup_data.items():
                        reports_dict[name] = pd.DataFrame(records)
                    return reports_dict
                
                raise
        
        try:
            return safe_sheet_operation(_load_operation)
        except Exception:
            return {}

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

def show_system_diagnostics():
    """显示系统诊断信息"""
    st.subheader("🔍 系统诊断")
    
    with st.expander("查看系统状态", expanded=False):
        # Google Sheets配置诊断
        diagnosis = diagnose_google_sheets_config()
        
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("### 📊 Google Sheets 连接")
            if diagnosis['has_secrets']:
                st.success("✅ 密钥配置已找到")
            else:
                st.error("❌ 密钥配置缺失")
            
            if diagnosis['credentials_valid']:
                st.success("✅ 凭据格式正确")
            else:
                st.error(f"❌ 凭据问题: {diagnosis['error_message']}")
        
        with col2:
            st.markdown("### 🗂️ 缓存状态")
            cache_count = len([key for key in st.session_state.keys() if key.startswith('cache_')])
            st.info(f"缓存项目数: {cache_count}")
            
            fallback_count = len([key for key in st.session_state.keys() if key.endswith('_fallback')])
            if fallback_count > 0:
                st.warning(f"备用数据项: {fallback_count}")
            else:
                st.success("无备用数据")
        
        # 详细配置信息
        if diagnosis['required_fields']:
            st.markdown("### ✅ 已配置字段")
            st.code(', '.join(diagnosis['required_fields']))
        
        if diagnosis['missing_fields']:
            st.markdown("### ❌ 缺失字段")
            st.code(', '.join(diagnosis['missing_fields']))

# 初始化会话状态
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
if 'store_name' not in st.session_state:
    st.session_state.store_name = ""
if 'user_id' not in st.session_state:
    st.session_state.user_id = ""
if 'is_admin' not in st.session_state:
    st.session_state.is_admin = False
if 'google_sheets_client' not in st.session_state:
    st.session_state.google_sheets_client = None
if 'operation_status' not in st.session_state:
    st.session_state.operation_status = []

# 主标题
st.markdown('<h1 class="main-header">📊 门店报表查询系统</h1>', unsafe_allow_html=True)

# 初始化Google Sheets客户端
if not st.session_state.google_sheets_client:
    try:
        with st.spinner("连接云数据库..."):
            gc = get_google_sheets_client()
            st.session_state.google_sheets_client = gc
            show_status_message("✅ 云数据库连接成功！", "success")
    except Exception as e:
        show_status_message(f"❌ 连接失败: {str(e)}", "error")
        # 显示诊断信息
        show_system_diagnostics()
        st.stop()

gc = st.session_state.google_sheets_client

# 显示操作状态
for status in st.session_state.operation_status:
    show_status_message(status['message'], status['type'])

# 侧边栏
with st.sidebar:
    st.title("⚙️ 系统功能")
    
    # 系统状态
    st.subheader("📡 系统状态")
    if gc:
        st.success("🟢 云数据库已连接")
    else:
        st.error("🔴 云数据库断开")
    
    # 添加诊断按钮
    if st.button("🔍 系统诊断"):
        show_system_diagnostics()
    
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
            permissions_file = st.file_uploader("上传门店权限表", type=['xlsx', 'xls'])
            if permissions_file:
                try:
                    with st.spinner("处理权限表文件..."):
                        df = pd.read_excel(permissions_file)
                        if len(df.columns) >= 2:
                            with st.spinner("保存到云端..."):
                                if save_permissions_to_sheets(df, gc):
                                    show_status_message(f"✅ 权限表已上传：{len(df)} 个用户", "success")
                                    st.balloons()
                                else:
                                    show_status_message("⚠️ 云端保存失败，已使用备用存储", "warning")
                        else:
                            show_status_message("❌ 格式错误：需要至少两列（门店名称、人员编号）", "error")
                except Exception as e:
                    show_status_message(f"❌ 处理失败：{str(e)}", "error")
            
            # 上传财务报表
            reports_file = st.file_uploader("上传财务报表", type=['xlsx', 'xls'])
            if reports_file:
                try:
                    with st.spinner("处理报表文件..."):
                        excel_file = pd.ExcelFile(reports_file)
                        reports_dict = {}
                        
                        for sheet in excel_file.sheet_names:
                            try:
                                df = pd.read_excel(reports_file, sheet_name=sheet)
                                if not df.empty:
                                    reports_dict[sheet] = df
                                    logger.info(f"读取工作表 '{sheet}': {len(df)} 行")
                            except Exception as e:
                                logger.warning(f"跳过工作表 '{sheet}': {str(e)}")
                                continue
                        
                        if reports_dict:
                            with st.spinner("保存到云端..."):
                                if save_reports_to_sheets(reports_dict, gc):
                                    show_status_message(f"✅ 报表已上传：{len(reports_dict)} 个门店", "success")
                                    st.balloons()
                                else:
                                    show_status_message("⚠️ 云端保存失败，已使用备用存储", "warning")
                        else:
                            show_status_message("❌ 文件中没有有效的工作表", "error")
                            
                except Exception as e:
                    show_status_message(f"❌ 处理失败：{str(e)}", "error")
            
            # 缓存管理
            st.subheader("🗂️ 缓存管理")
            if st.button("清除所有缓存"):
                cache_keys = [key for key in st.session_state.keys() if key.startswith('cache_')]
                fallback_keys = [key for key in st.session_state.keys() if key.endswith('_fallback')]
                
                for key in cache_keys + fallback_keys:
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
    st.markdown('<div class="admin-panel"><h3>👨‍💼 管理员控制面板</h3><p>数据永久保存在云端，支持分片存储和缓存机制，包含本地备份和故障恢复</p></div>', unsafe_allow_html=True)
    
    try:
        with st.spinner("加载数据统计..."):
            permissions_data = load_permissions_from_sheets(gc)
            reports_data = load_reports_from_sheets(gc)
        
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
        show_system_diagnostics()

elif user_type == "管理员" and not st.session_state.is_admin:
    st.info("👈 请在左侧边栏输入管理员密码")

else:
    if not st.session_state.logged_in:
        st.subheader("🔐 用户登录")
        
        try:
            with st.spinner("加载权限数据..."):
                permissions_data = load_permissions_from_sheets(gc)
            
            if permissions_data is None:
                st.warning("⚠️ 系统维护中，请联系管理员")
                if st.button("显示系统诊断"):
                    show_system_diagnostics()
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
            if st.button("显示诊断信息"):
                show_system_diagnostics()
    
    else:
        # 已登录 - 显示报表
        st.markdown(f'<div class="store-info"><h3>🏪 {st.session_state.store_name}</h3><p>操作员：{st.session_state.user_id}</p></div>', unsafe_allow_html=True)
        
        try:
            with st.spinner("加载报表数据..."):
                reports_data = load_reports_from_sheets(gc)
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
            if st.button("显示系统诊断"):
                show_system_diagnostics()

# 页面底部状态信息
st.divider()
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.caption(f"🕒 当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
with col2:
    cache_count = len([key for key in st.session_state.keys() if key.startswith('cache_')])
    st.caption(f"💾 缓存项目: {cache_count}")
with col3:
    fallback_count = len([key for key in st.session_state.keys() if key.endswith('_fallback')])
    if fallback_count > 0:
        st.caption(f"⚠️ 备用数据: {fallback_count}")
    else:
        st.caption("✅ 云端数据正常")
with col4:
    st.caption("🔧 版本: v2.1 (增强版)")
