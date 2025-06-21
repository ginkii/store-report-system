import streamlit as st
import pandas as pd
import io
import json
from datetime import datetime
import time
import gspread
from google.oauth2.service_account import Credentials
import logging
import traceback
from typing import Dict, Optional, List, Tuple
from collections import deque
import threading

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
MAX_RETRIES = 2  # 减少重试次数
RETRY_DELAY = 1  # 减少重试延迟

# API速率限制配置（优化后的配置）
WRITE_REQUESTS_PER_MINUTE = 60
READ_REQUESTS_PER_MINUTE = 100
MIN_REQUEST_INTERVAL = 0.3  # 进一步减少最小请求间隔
BATCH_SIZE = 200  # 进一步增大批量操作大小

# CSS样式（保持原有样式）
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
    .dashboard-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1.5rem;
        border-radius: 12px;
        margin: 0.5rem 0;
        text-align: center;
    }
    .status-good {
        background-color: #d4edda;
        color: #155724;
        padding: 0.5rem;
        border-radius: 5px;
        margin: 0.5rem 0;
    }
    .status-warning {
        background-color: #fff3cd;
        color: #856404;
        padding: 0.5rem;
        border-radius: 5px;
        margin: 0.5rem 0;
    }
    </style>
""", unsafe_allow_html=True)

def retry_on_failure(func, *args, max_retries=2, delay=1, **kwargs):
    """重试装饰器 - 优化版（减少重试次数）"""
    for attempt in range(max_retries):
        try:
            result = func(*args, **kwargs)
            # 成功后重置错误计数
            if 'api_error_count' in st.session_state:
                st.session_state.api_error_count = 0
            return result
        except Exception as e:
            error_str = str(e)
            
            # 记录错误
            if 'api_error_count' in st.session_state:
                st.session_state.api_error_count += 1
            
            # 特殊处理速率限制错误
            if "429" in error_str or "quota" in error_str.lower():
                wait_time = 30  # 减少等待时间
                logger.warning(f"Rate limit error on attempt {attempt + 1}. Waiting {wait_time} seconds...")
                
                # 只在第一次显示警告
                if attempt == 0:
                    st.warning(f"⚠️ API速率限制，等待{wait_time}秒后自动重试...")
                
                time.sleep(wait_time)
                continue
            
            if attempt == max_retries - 1:
                raise e
            
            logger.warning(f"Attempt {attempt + 1} failed: {error_str}. Retrying...")
            time.sleep(delay * (attempt + 1))  # 递增延迟

class RateLimiter:
    """API速率限制器 - 高性能版"""
    def __init__(self, max_requests_per_minute=WRITE_REQUESTS_PER_MINUTE):
        self.max_requests = max_requests_per_minute
        self.min_interval = 60.0 / max_requests_per_minute
        self.requests = deque()
        self.lock = threading.Lock()
        self.last_request_time = 0
    
    def wait_if_needed(self):
        """智能等待 - 只在必要时等待"""
        with self.lock:
            now = time.time()
            
            # 清理一分钟前的请求记录
            while self.requests and self.requests[0] < now - 60:
                self.requests.popleft()
            
            # 只有在接近限制时才等待（达到90%时开始控制）
            if len(self.requests) >= self.max_requests * 0.9:
                # 如果达到速率限制，等待
                if len(self.requests) >= self.max_requests:
                    wait_time = 60 - (now - self.requests[0]) + 0.1
                    if wait_time > 0:
                        logger.info(f"Rate limit reached, waiting {wait_time:.1f} seconds...")
                        time.sleep(wait_time)
                        # 再次清理
                        while self.requests and self.requests[0] < time.time() - 60:
                            self.requests.popleft()
                
                # 确保最小间隔（只在高负载时）
                if self.last_request_time > 0:
                    time_since_last = now - self.last_request_time
                    if time_since_last < self.min_interval * 0.8:  # 80%的最小间隔
                        time.sleep(self.min_interval * 0.8 - time_since_last)
            
            # 记录这次请求
            self.requests.append(time.time())
            self.last_request_time = time.time()

# 创建全局速率限制器
write_limiter = RateLimiter(WRITE_REQUESTS_PER_MINUTE)
read_limiter = RateLimiter(READ_REQUESTS_PER_MINUTE)

def safe_batch_update(worksheet, data_list, start_row=1, batch_size=BATCH_SIZE, show_progress=True):
    """安全的批量更新 - 使用values_update优化版"""
    success = True
    total_rows = len(data_list)
    
    if total_rows == 0:
        return True
    
    # 确定最大列数
    max_cols = max(len(row) for row in data_list) if data_list else 0
    
    # 进度显示
    if show_progress:
        progress_text = st.empty()
        progress_bar = st.progress(0)
    
    try:
        # 构建完整范围
        if max_cols <= 26:
            end_col = chr(64 + max_cols)
        else:
            col_num = max_cols - 1
            col_letters = ''
            while col_num >= 0:
                col_letters = chr(65 + (col_num % 26)) + col_letters
                col_num = col_num // 26 - 1
            end_col = col_letters
        
        # 计算结束行
        end_row = start_row + total_rows - 1
        full_range = f'A{start_row}:{end_col}{end_row}'
        
        if show_progress:
            progress_text.text("正在保存数据...")
            progress_bar.progress(0.5)
        
        # 尝试一次性更新所有数据
        write_limiter.wait_if_needed()
        worksheet.update(full_range, data_list, value_input_option='RAW')
        
        if show_progress:
            progress_bar.progress(1.0)
            progress_text.empty()
            progress_bar.empty()
        
        return True
        
    except Exception as e:
        error_str = str(e)
        logger.error(f"Batch update failed: {error_str}")
        
        # 如果一次性更新失败，回退到分批更新
        if show_progress:
            progress_text.text("切换到分批模式...")
        
        # 动态调整批量大小
        if "413" in error_str or "too large" in error_str.lower():
            batch_size = max(50, batch_size // 2)
        
        success = True
        for i in range(0, total_rows, batch_size):
            batch = data_list[i:i+batch_size]
            batch_num = i // batch_size + 1
            total_batches = (total_rows + batch_size - 1) // batch_size
            
            if show_progress:
                progress_text.text(f"正在保存数据... 批次 {batch_num}/{total_batches}")
                progress_bar.progress(batch_num / total_batches)
            
            try:
                end_row = start_row + i + len(batch) - 1
                range_name = f'A{start_row + i}:{end_col}{end_row}'
                
                write_limiter.wait_if_needed()
                worksheet.update(range_name, batch, value_input_option='RAW')
                
            except Exception as batch_error:
                logger.error(f"Failed to update batch {batch_num}: {batch_error}")
                
                # 速率限制处理
                if "429" in str(batch_error):
                    if show_progress:
                        progress_text.text("API限制，等待中...")
                    time.sleep(30)
                    try:
                        write_limiter.wait_if_needed()
                        worksheet.update(range_name, batch, value_input_option='RAW')
                    except:
                        success = False
                else:
                    success = False
        
        if show_progress:
            progress_text.empty()
            progress_bar.empty()
        
        return success

def get_google_sheets_client(force_new=False):
    """获取Google Sheets客户端，支持强制刷新"""
    try:
        if force_new or 'google_sheets_client' not in st.session_state or st.session_state.google_sheets_client is None:
            credentials_info = st.secrets["google_sheets"]
            scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
            credentials = Credentials.from_service_account_info(credentials_info, scopes=scopes)
            client = gspread.authorize(credentials)
            st.session_state.google_sheets_client = client
            logger.info("Google Sheets client created successfully")
        return st.session_state.google_sheets_client
    except Exception as e:
        logger.error(f"Failed to create Google Sheets client: {str(e)}")
        st.error(f"连接失败: {str(e)}")
        return None

def verify_connection(gc):
    """验证连接是否有效 - 优化版"""
    try:
        # 简单的连接测试，不计入速率限制
        gc.list_spreadsheet_files(q="name='test'")[:1]  # 只获取一个结果
        return True
    except Exception as e:
        logger.error(f"Connection verification failed: {str(e)}")
        return False

def get_or_create_spreadsheet(gc, name="门店报表系统数据"):
    """获取或创建表格 - 优化版"""
    try:
        # 首先尝试打开
        spreadsheet = gc.open(name)
        logger.info(f"Opened existing spreadsheet: {name}")
        return spreadsheet
    except gspread.SpreadsheetNotFound:
        # 如果不存在，创建新的
        try:
            spreadsheet = gc.create(name)
            logger.info(f"Created new spreadsheet: {name}")
            # 分享给服务账号邮箱，确保访问权限
            spreadsheet.share('', perm_type='anyone', role='reader')
            return spreadsheet
        except Exception as e:
            logger.error(f"Failed to create spreadsheet: {str(e)}")
            raise
    except Exception as e:
        logger.error(f"Error accessing spreadsheet: {str(e)}")
        raise

def get_or_create_worksheet(spreadsheet, name):
    """获取或创建工作表"""
    try:
        return spreadsheet.worksheet(name)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=name, rows=1000, cols=20)
        logger.info(f"Created new worksheet: {name}")
        return worksheet
    except Exception as e:
        logger.error(f"Error accessing worksheet {name}: {str(e)}")
        raise

# 备份元数据功能（可选，暂时禁用以提高性能）
# def save_backup_metadata(gc, data_type, data_hash, row_count):
#     """保存备份元数据 - 已禁用"""
#     return True

def save_permissions_to_sheets(df, gc):
    """保存权限数据 - 高速版"""
    try:
        spreadsheet = get_or_create_spreadsheet(gc)
        worksheet = get_or_create_worksheet(spreadsheet, PERMISSIONS_SHEET_NAME)
        
        # 准备数据
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        all_data = [['门店名称', '人员编号', '更新时间']]
        
        for _, row in df.iterrows():
            all_data.append([str(row.iloc[0]), str(row.iloc[1]), current_time])
        
        # 清空并写入新数据（使用batch_update优化）
        try:
            worksheet.clear()
            if safe_batch_update(worksheet, all_data, 1, BATCH_SIZE, show_progress=True):
                logger.info(f"Successfully saved {len(df)} permission records")
                return True
            else:
                return False
        except Exception as e:
            logger.error(f"Failed to update permissions: {str(e)}")
            return False
        
    except Exception as e:
        logger.error(f"Failed to save permissions: {str(e)}")
        st.error(f"保存失败: {str(e)}")
        return False

def load_permissions_from_sheets(gc):
    """加载权限数据 - 优化版"""
    try:
        # 验证连接
        if not verify_connection(gc):
            gc = get_google_sheets_client(force_new=True)
            if not gc:
                return None
        
        spreadsheet = get_or_create_spreadsheet(gc)
        worksheet = get_or_create_worksheet(spreadsheet, PERMISSIONS_SHEET_NAME)
        
        # 直接获取数据，不需要速率限制
        data = worksheet.get_all_values()
        
        if not data or len(data) <= 1:
            return None
        
        # 直接读取所有数据
        df = pd.DataFrame(data[1:], columns=data[0])
        
        # 只保留前两列
        if len(df.columns) >= 2:
            df = df.iloc[:, :2]
            df.columns = ['门店名称', '人员编号']
            
            # 过滤空行
            df = df[(df['门店名称'].str.strip() != '') & (df['人员编号'].str.strip() != '')]
            
            logger.info(f"Loaded {len(df)} permission records")
            return df
        
        return None
        
    except Exception as e:
        logger.error(f"Failed to load permissions: {str(e)}")
        return None

def save_reports_to_sheets(reports_dict, gc):
    """保存报表数据 - 高速版"""
    try:
        spreadsheet = get_or_create_spreadsheet(gc)
        
        success_count = 0
        total_stores = len(reports_dict)
        failed_stores = []
        
        progress_text = st.empty()
        progress_bar = st.progress(0)
        
        # 批量处理所有门店
        for idx, (store_name, df) in enumerate(reports_dict.items()):
            try:
                progress_text.text(f"正在保存门店 {idx+1}/{total_stores}: {store_name}")
                progress_bar.progress((idx + 1) / total_stores)
                
                # 创建安全的工作表名称
                safe_sheet_name = store_name.replace('/', '_').replace('\\', '_')[:31]
                
                # 获取或创建工作表
                worksheet = get_or_create_worksheet(spreadsheet, safe_sheet_name)
                
                # 清理数据
                df_cleaned = df.copy()
                for col in df_cleaned.columns:
                    df_cleaned[col] = df_cleaned[col].astype(str).replace('nan', '').replace('None', '')
                
                # 转换为列表格式
                data_list = [df_cleaned.columns.tolist()] + df_cleaned.values.tolist()
                
                # 清空并批量写入
                worksheet.clear()
                
                # 使用batch_update优化
                if safe_batch_update(worksheet, data_list, 1, BATCH_SIZE, show_progress=False):
                    success_count += 1
                else:
                    failed_stores.append(store_name)
                
            except Exception as e:
                logger.error(f"Failed to save report for {store_name}: {str(e)}")
                failed_stores.append(store_name)
                
                # 速率限制时短暂等待
                if "429" in str(e):
                    time.sleep(5)
        
        progress_text.empty()
        progress_bar.empty()
        
        # 更新系统信息
        try:
            info_worksheet = get_or_create_worksheet(spreadsheet, SYSTEM_INFO_SHEET_NAME)
            info_data = [
                ['Last Update', datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
                ['Total Stores', str(total_stores)],
                ['Success Count', str(success_count)],
                ['Failed Count', str(len(failed_stores))],
                ['Status', 'Active' if success_count > 0 else 'Error']
            ]
            
            info_worksheet.clear()
            info_worksheet.update('A1', info_data, value_input_option='RAW')
        except:
            pass  # 系统信息更新失败不影响主功能
        
        # 显示结果
        if failed_stores:
            st.warning(f"以下门店保存失败: {', '.join(failed_stores)}")
        
        return success_count > 0
        
    except Exception as e:
        logger.error(f"Failed to save reports: {str(e)}")
        st.error(f"保存报表失败: {str(e)}")
        return False

def load_reports_from_sheets(gc):
    """加载报表数据 - 优化版"""
    try:
        # 验证连接
        if not verify_connection(gc):
            gc = get_google_sheets_client(force_new=True)
            if not gc:
                return {}
        
        spreadsheet = get_or_create_spreadsheet(gc)
        
        # 获取所有工作表
        worksheets = spreadsheet.worksheets()
        
        reports_dict = {}
        
        # 批量加载，显示进度
        show_progress = len(worksheets) > 10  # 超过10个工作表才显示进度
        if show_progress:
            progress_text = st.empty()
            progress_bar = st.progress(0)
        
        for idx, worksheet in enumerate(worksheets):
            # 跳过系统工作表
            if worksheet.title in [PERMISSIONS_SHEET_NAME, SYSTEM_INFO_SHEET_NAME]:
                continue
            
            try:
                if show_progress:
                    progress_text.text(f"加载工作表 {idx+1}/{len(worksheets)}: {worksheet.title}")
                    progress_bar.progress((idx + 1) / len(worksheets))
                
                # 直接获取数据，不需要速率限制
                data = worksheet.get_all_values()
                
                if len(data) > 1:
                    # 第一行作为列名
                    df = pd.DataFrame(data[1:], columns=data[0])
                    reports_dict[worksheet.title] = df
                    logger.info(f"Loaded report for {worksheet.title}")
                    
            except Exception as e:
                logger.warning(f"Failed to load worksheet {worksheet.title}: {str(e)}")
                continue
        
        if show_progress:
            progress_text.empty()
            progress_bar.empty()
        
        return reports_dict
        
    except Exception as e:
        logger.error(f"Failed to load reports: {str(e)}")
        return {}

def check_system_status(gc):
    """检查系统状态 - 优化版"""
    try:
        # 使用缓存避免频繁检查
        if 'last_status_check' in st.session_state:
            if time.time() - st.session_state.last_status_check < 60:  # 60秒内使用缓存
                return st.session_state.cached_status
        
        spreadsheet = get_or_create_spreadsheet(gc)
        try:
            info_worksheet = spreadsheet.worksheet(SYSTEM_INFO_SHEET_NAME)
            info_data = info_worksheet.get_all_values()
            
            if info_data:
                status_dict = {row[0]: row[1] for row in info_data if len(row) >= 2}
                st.session_state.cached_status = status_dict
                st.session_state.last_status_check = time.time()
                return status_dict
        except:
            return {'Status': 'Unknown'}
    except:
        return {'Status': 'Error'}

def analyze_receivable_data(df):
    """分析应收未收额数据 - 专门查找第69行（保持原有逻辑）"""
    result = {}
    
    if len(df.columns) == 0 or len(df) == 0:
        return result
    
    # 检查第一行是否是门店名称（通常第一行只有第一个单元格有值）
    first_row = df.iloc[0] if len(df) > 0 else None
    if first_row is not None:
        non_empty_count = sum(1 for val in first_row if pd.notna(val) and str(val).strip() != '')
        # 如果第一行只有1-2个非空值，可能是门店名称行
        if non_empty_count <= 2:
            # 跳过第一行，使用第二行作为新的第一行
            df = df.iloc[1:].reset_index(drop=True)
            result['skipped_store_name_row'] = True
    
    # 查找第69行（如果跳过了第一行，实际是原始数据的第70行）
    target_row_index = 68  # 第69行
    
    if len(df) > target_row_index:
        row = df.iloc[target_row_index]
        first_col_value = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
        
        # 检查第一列是否包含"应收-未收额"相关关键词
        keywords = ['应收-未收额', '应收未收额', '应收-未收', '应收未收']
        
        for keyword in keywords:
            if keyword in first_col_value:
                # 查找该行中的数值（从后向前查找，通常合计在后面的列）
                for col_idx in range(len(row)-1, 0, -1):
                    val = row.iloc[col_idx]
                    if pd.notna(val) and str(val).strip() not in ['', 'None', 'nan']:
                        # 清理数值
                        cleaned = str(val).replace(',', '').replace('¥', '').replace('￥', '').strip()
                        
                        # 处理括号表示的负数
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
                                    'actual_row_number': target_row_index + 1  # 实际行号
                                }
                                
                                # 查找备注信息（通常在最后几列或特定的备注列）
                                remarks = []
                                for col in df.columns:
                                    col_lower = str(col).lower()
                                    if any(keyword in col_lower for keyword in ['备注', '说明', '注释', 'remark', 'note', '参考资', '单位']):
                                        # 找到备注列，提取第69行的备注
                                        remark_val = str(row[col]) if col in row.index else ""
                                        if remark_val and remark_val.strip() not in ['', 'nan', 'None', '0']:
                                            remarks.append(f"{col}: {remark_val}")
                                
                                # 也检查最后几列是否有备注信息
                                for col_idx in range(max(0, len(row)-3), len(row)):
                                    if col_idx < len(row) and col_idx != len(row)-1:  # 排除已经作为金额的列
                                        val = str(row.iloc[col_idx]) if pd.notna(row.iloc[col_idx]) else ""
                                        if val and val.strip() not in ['', 'nan', 'None', '0'] and not val.replace('.', '').replace('-', '').isdigit():
                                            col_name = df.columns[col_idx]
                                            if col_name not in [r.split(':')[0] for r in remarks]:  # 避免重复
                                                remarks.append(f"{col_name}: {val}")
                                
                                if remarks:
                                    result['应收-未收额']['remarks'] = remarks
                                
                                return result
                        except ValueError:
                            continue
                break
    
    # 如果第69行没找到，提供备用查找方案
    if '应收-未收额' not in result:
        # 在所有行中查找
        keywords = ['应收-未收额', '应收未收额', '应收-未收', '应收未收']
        
        for idx, row in df.iterrows():
            try:
                row_name = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
                
                if not row_name.strip():
                    continue
                
                # 检查是否匹配关键词
                for keyword in keywords:
                    if keyword in row_name:
                        # 查找该行中的数值
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
                                        
                                        # 查找备注信息
                                        remarks = []
                                        for col in df.columns:
                                            col_lower = str(col).lower()
                                            if any(keyword in col_lower for keyword in ['备注', '说明', '注释', 'remark', 'note', '参考资', '单位']):
                                                remark_val = str(row[col]) if col in row.index else ""
                                                if remark_val and remark_val.strip() not in ['', 'nan', 'None', '0']:
                                                    remarks.append(f"{col}: {remark_val}")
                                        
                                        if remarks:
                                            result['应收-未收额']['remarks'] = remarks
                                        
                                        return result
                                except ValueError:
                                    continue
                        break
            except Exception:
                continue
    
    # 返回调试信息
    result['debug_info'] = {
        'total_rows': len(df),
        'checked_row_69': len(df) > target_row_index,
        'row_69_content': str(df.iloc[target_row_index].iloc[0]) if len(df) > target_row_index else 'N/A'
    }
    
    return result

def verify_user_permission(store_name, user_id, permissions_data):
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

def find_matching_reports(store_name, reports_data):
    """查找匹配的报表"""
    matching = []
    for sheet_name in reports_data.keys():
        if store_name in sheet_name or sheet_name in store_name:
            matching.append(sheet_name)
    return matching

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
if 'last_connection_check' not in st.session_state:
    st.session_state.last_connection_check = None
if 'api_error_count' not in st.session_state:
    st.session_state.api_error_count = 0
if 'last_status_check' not in st.session_state:
    st.session_state.last_status_check = 0
if 'cached_status' not in st.session_state:
    st.session_state.cached_status = {'Status': 'Unknown'}

# 主标题
st.markdown('<h1 class="main-header">📊 门店报表查询系统</h1>', unsafe_allow_html=True)

# 初始化或检查Google Sheets客户端
current_time = time.time()
if (not st.session_state.google_sheets_client or 
    st.session_state.last_connection_check is None or
    current_time - st.session_state.last_connection_check > 300):  # 每5分钟检查一次
    
    with st.spinner("连接云数据库..."):
        gc = get_google_sheets_client(force_new=True)
        if gc:
            st.session_state.google_sheets_client = gc
            st.session_state.last_connection_check = current_time
        else:
            st.error("❌ 连接失败，请检查配置")
            st.stop()

gc = st.session_state.google_sheets_client

# 显示系统状态（使用缓存）
system_status = check_system_status(gc)
status_color = "status-good" if system_status.get("Status") == "Active" else "status-warning"

status_html = f'''
<div class="{status_color}">
    <strong>系统状态:</strong> {system_status.get("Status", "Unknown")} | 
    <strong>最后更新:</strong> {system_status.get("Last Update", "N/A")}
</div>
'''
st.markdown(status_html, unsafe_allow_html=True)

# 侧边栏
with st.sidebar:
    st.title("⚙️ 系统功能")
    
    user_type = st.radio("选择用户类型", ["普通用户", "管理员"])
    
    if user_type == "管理员":
        st.subheader("🔐 管理员登录")
        admin_password = st.text_input("管理员密码", type="password")
        
        if st.button("验证管理员身份"):
            if admin_password == ADMIN_PASSWORD:
                st.session_state.is_admin = True
                st.success("✅ 验证成功！")
                st.rerun()
            else:
                st.error("❌ 密码错误！")
        
        if st.session_state.is_admin:
            st.subheader("📁 文件管理")
            
            # 上传权限表
            permissions_file = st.file_uploader("上传门店权限表", type=['xlsx', 'xls'])
            if permissions_file:
                try:
                    df = pd.read_excel(permissions_file)
                    if len(df.columns) >= 2:
                        with st.spinner("正在保存权限数据..."):
                            if save_permissions_to_sheets(df, gc):
                                st.success(f"✅ 权限表已上传：{len(df)} 个用户")
                                st.balloons()
                            else:
                                st.error("❌ 保存失败，请重试")
                    else:
                        st.error("❌ 格式错误：需要至少两列（门店名称、人员编号）")
                except Exception as e:
                    st.error(f"❌ 读取失败：{str(e)}")
                    logger.error(f"Failed to read permissions file: {str(e)}")
            
            # 上传财务报表
            reports_file = st.file_uploader("上传财务报表", type=['xlsx', 'xls'])
            if reports_file:
                try:
                    excel_file = pd.ExcelFile(reports_file)
                    reports_dict = {}
                    
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    for i, sheet in enumerate(excel_file.sheet_names):
                        try:
                            status_text.text(f"正在读取: {sheet}")
                            df = pd.read_excel(reports_file, sheet_name=sheet)
                            if not df.empty:
                                reports_dict[sheet] = df
                            progress_bar.progress((i + 1) / len(excel_file.sheet_names))
                        except Exception as e:
                            st.warning(f"跳过工作表 {sheet}: {str(e)}")
                            continue
                    
                    progress_bar.empty()
                    status_text.empty()
                    
                    if reports_dict:
                        with st.spinner(f"正在保存 {len(reports_dict)} 个门店的报表..."):
                            if save_reports_to_sheets(reports_dict, gc):
                                st.success(f"✅ 报表已上传：{len(reports_dict)} 个门店")
                                st.balloons()
                            else:
                                st.error("❌ 部分或全部保存失败，请检查日志")
                    else:
                        st.error("❌ 没有有效的报表数据")
                except Exception as e:
                    st.error(f"❌ 读取失败：{str(e)}")
                    logger.error(f"Failed to read reports file: {str(e)}")
            
            # 系统维护功能
            st.subheader("🔧 系统维护")
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("刷新连接"):
                    gc = get_google_sheets_client(force_new=True)
                    if gc and verify_connection(gc):
                        st.session_state.google_sheets_client = gc
                        st.success("✅ 连接已刷新")
                    else:
                        st.error("❌ 刷新失败")
            
            with col2:
                if st.button("重置速率限制"):
                    write_limiter.requests.clear()
                    read_limiter.requests.clear()
                    st.success("✅ 速率限制已重置")
            
            # 数据备份功能
            st.subheader("💾 数据备份")
            col1, col2 = st.columns(2)
            
            with col1:
                if st.button("导出权限数据"):
                    if permissions_data is not None and len(permissions_data) > 0:
                        try:
                            buffer = io.BytesIO()
                            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                                permissions_data.to_excel(writer, index=False, sheet_name='权限数据')
                            
                            st.download_button(
                                "📥 下载权限备份",
                                buffer.getvalue(),
                                f"权限备份_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                            )
                        except Exception as e:
                            st.error(f"导出失败: {str(e)}")
                    else:
                        st.warning("没有权限数据可导出")
            
            with col2:
                if st.button("导出所有报表"):
                    if reports_data:
                        try:
                            buffer = io.BytesIO()
                            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                                for sheet_name, df in reports_data.items():
                                    # 确保工作表名称有效
                                    safe_name = sheet_name[:31]
                                    df.to_excel(writer, index=False, sheet_name=safe_name)
                            
                            st.download_button(
                                "📥 下载报表备份",
                                buffer.getvalue(),
                                f"报表备份_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                            )
                        except Exception as e:
                            st.error(f"导出失败: {str(e)}")
                    else:
                        st.warning("没有报表数据可导出")
            
            # 显示当前API使用情况
            st.caption("📊 API使用情况")
            write_usage = len(write_limiter.requests)
            read_usage = len(read_limiter.requests)
            
            st.progress(write_usage / WRITE_REQUESTS_PER_MINUTE)
            st.caption(f"写入请求: {write_usage}/{WRITE_REQUESTS_PER_MINUTE}")
            
            st.progress(read_usage / READ_REQUESTS_PER_MINUTE)
            st.caption(f"读取请求: {read_usage}/{READ_REQUESTS_PER_MINUTE}")
            
            # API配额说明
            with st.expander("ℹ️ API限制说明"):
                st.markdown("""
                **Google Sheets API限制：**
                - 写入请求：60次/分钟
                - 读取请求：100次/分钟
                - 单次请求最大单元格数：50,000
                
                **优化建议：**
                1. 批量上传数据，避免频繁操作
                2. 大文件请分批上传
                3. 遇到限制错误请等待1分钟
                4. 使用"重置速率限制"可清空计数器
                """)
            
            # 紧急清理功能
            with st.expander("🚨 紧急维护"):
                if st.button("清空所有数据"):
                    confirm = st.checkbox("我确认要清空所有数据（此操作不可恢复）")
                    if confirm:
                        try:
                            spreadsheet = get_or_create_spreadsheet(gc)
                            # 清理所有工作表
                            worksheets = spreadsheet.worksheets()
                            cleared_count = 0
                            
                            for worksheet in worksheets:
                                try:
                                    worksheet.clear()
                                    cleared_count += 1
                                except Exception as e:
                                    st.warning(f"清理 {worksheet.title} 失败: {str(e)}")
                            
                            st.success(f"✅ 已清空 {cleared_count} 个工作表")
                        except Exception as e:
                            st.error(f"❌ 清理失败: {str(e)}")
    
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

# 主界面
if user_type == "管理员" and st.session_state.is_admin:
    st.markdown('<div class="admin-panel"><h3>👨‍💼 管理员控制面板</h3></div>', unsafe_allow_html=True)
    
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
        st.metric("系统状态", "正常" if system_status.get("Status") == "Active" else "异常")

elif user_type == "管理员" and not st.session_state.is_admin:
    st.info("👈 请在左侧边栏输入管理员密码")

else:
    if not st.session_state.logged_in:
        st.subheader("🔐 用户登录")
        
        with st.spinner("加载权限数据..."):
            permissions_data = load_permissions_from_sheets(gc)
        
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
                        st.success("✅ 登录成功！")
                        st.balloons()
                        st.rerun()
                    else:
                        st.error("❌ 门店或编号错误！")
    
    else:
        # 已登录 - 显示报表
        st.markdown(f'<div class="store-info"><h3>🏪 {st.session_state.store_name}</h3><p>操作员：{st.session_state.user_id}</p></div>', unsafe_allow_html=True)
        
        with st.spinner("加载报表数据..."):
            reports_data = load_reports_from_sheets(gc)
        
        matching_sheets = find_matching_reports(st.session_state.store_name, reports_data)
        
        if matching_sheets:
            if len(matching_sheets) > 1:
                selected_sheet = st.selectbox("选择报表", matching_sheets)
            else:
                selected_sheet = matching_sheets[0]
            
            df = reports_data[selected_sheet]
            
            # 检查并处理第一行是否为门店名称
            original_df = df.copy()
            if len(df) > 0:
                first_row = df.iloc[0]
                non_empty_count = sum(1 for val in first_row if pd.notna(val) and str(val).strip() != '')
                
                # 如果第一行只有少数非空值，可能是门店名称，跳过它
                if non_empty_count <= 2 and len(df) > 1:
                    df = df.iloc[1:].reset_index(drop=True)
            
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
                                </div>
                            ''', unsafe_allow_html=True)
                        
                        elif amount < 0:
                            st.markdown(f'''
                                <div class="receivable-negative">
                                    <h1 style="margin: 0; font-size: 3rem;">💚 ¥{abs(amount):,.2f}</h1>
                                    <h3 style="margin: 0.5rem 0;">总部应退款</h3>
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
                    
                    # 显示调试信息
                    with st.expander("🔍 查看详情", expanded=True):
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
                st.error(f"❌ 分析数据时出错：{str(e)}")
                logger.error(f"Analysis error: {str(e)}\n{traceback.format_exc()}")
            
            st.divider()
            
            # 完整报表数据
            st.subheader("📋 完整报表数据")
            
            search_term = st.text_input("🔍 搜索报表内容")
            
            # 数据过滤
            try:
                if search_term:
                    # 安全的搜索实现
                    search_df = df.copy()
                    # 确保所有数据都是字符串
                    for col in search_df.columns:
                        search_df[col] = search_df[col].astype(str).fillna('')
                    
                    mask = search_df.apply(
                        lambda x: x.str.contains(search_term, case=False, na=False, regex=False)
                    ).any(axis=1)
                    filtered_df = df[mask]
                    st.info(f"找到 {len(filtered_df)} 条包含 '{search_term}' 的记录")
                else:
                    filtered_df = df
                
                # 数据统计
                st.info(f"📊 数据统计：共 {len(filtered_df)} 条记录，{len(df.columns)} 列")
                
                # 显示数据表格
                if len(filtered_df) > 0:
                    # 清理数据以确保显示正常
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
                    
                    # 显示数据
                    st.dataframe(display_df, use_container_width=True, height=400)
                
                else:
                    st.warning("没有找到符合条件的数据")
                    
            except Exception as e:
                st.error(f"❌ 数据处理时出错：{str(e)}")
                logger.error(f"Data processing error: {str(e)}")
            
            # 下载功能
            st.subheader("📥 数据下载")
            
            col1, col2 = st.columns(2)
            with col1:
                try:
                    buffer = io.BytesIO()
                    # 准备下载数据
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
                    st.error(f"Excel下载准备失败：{str(e)}")
                    logger.error(f"Excel download error: {str(e)}")
            
            with col2:
                try:
                    # CSV下载
                    csv_df = df.copy()
                    # 处理列名
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
                    st.error(f"CSV下载准备失败：{str(e)}")
                    logger.error(f"CSV download error: {str(e)}")
        
        else:
            st.error(f"❌ 未找到门店 '{st.session_state.store_name}' 的报表")
            st.info("💡 提示：请联系管理员确认报表是否已上传")
