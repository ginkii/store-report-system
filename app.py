import streamlit as st
import pandas as pd
import io
import json
from datetime import datetime
import time
import gspread
from google.oauth2.service_account import Credentials
import logging
from typing import Optional, Dict, Any, List, Tuple
import hashlib
import pickle
import traceback
from contextlib import contextmanager
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue
import random
import math

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
CACHE_DURATION = 300  # 缓存5分钟

# 优化的分批上传配置
class BatchConfig:
    def __init__(self):
        self.initial_batch_size = 3  # 初始批次大小
        self.max_batch_size = 15     # 最大批次大小
        self.min_batch_size = 1      # 最小批次大小
        self.max_chunk_size = 25000  # 单个数据块最大大小
        self.base_delay = 0.8        # 基础延迟
        self.max_delay = 10          # 最大延迟
        self.max_retries = 5         # 最大重试次数
        self.quota_backoff_factor = 2  # 配额限制退避因子
        self.success_rate_threshold = 0.8  # 成功率阈值
        self.max_concurrent_uploads = 3    # 最大并发上传数

BATCH_CONFIG = BatchConfig()

# CSS样式保持不变
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
    .upload-status {
        background: linear-gradient(135deg, #74b9ff 0%, #0984e3 100%);
        color: white;
        padding: 1rem;
        border-radius: 8px;
        margin: 0.5rem 0;
    }
    .batch-info {
        background: #f8f9fa;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #007bff;
        margin: 0.5rem 0;
    }
    .quota-warning {
        background: #fff3cd;
        color: #856404;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #ffc107;
        margin: 0.5rem 0;
    }
    .quota-danger {
        background: #f8d7da;
        color: #721c24;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #dc3545;
        margin: 0.5rem 0;
    }
    .success-animation {
        animation: pulse 2s infinite;
    }
    @keyframes pulse {
        0% { transform: scale(1); }
        50% { transform: scale(1.05); }
        100% { transform: scale(1); }
    }
    </style>
""", unsafe_allow_html=True)

class SheetOperationError(Exception):
    """Google Sheets操作异常"""
    pass

class DataProcessingError(Exception):
    """数据处理异常"""
    pass

class QuotaLimitError(Exception):
    """配额限制异常"""
    pass

class AdaptiveBatchUploader:
    """自适应批次上传器"""
    
    def __init__(self, gc, config: BatchConfig = None):
        self.gc = gc
        self.config = config or BATCH_CONFIG
        self.current_batch_size = self.config.initial_batch_size
        self.api_call_count = 0
        self.success_count = 0
        self.error_count = 0
        self.quota_errors = 0
        self.last_api_call_time = 0
        self.upload_queue = queue.Queue()
        self.results = []
        self.progress_callback = None
        self.status_callback = None
        
    def set_callbacks(self, progress_callback, status_callback):
        """设置回调函数"""
        self.progress_callback = progress_callback
        self.status_callback = status_callback
        
    def log_api_call(self):
        """记录API调用"""
        self.api_call_count += 1
        self.last_api_call_time = time.time()
        
    def calculate_delay(self, attempt: int = 0, is_quota_error: bool = False) -> float:
        """计算智能延迟"""
        if is_quota_error:
            # 配额错误使用指数退避
            base_delay = self.config.base_delay * (self.config.quota_backoff_factor ** attempt)
            jitter = random.uniform(0, base_delay * 0.3)  # 添加随机抖动
            return min(base_delay + jitter, self.config.max_delay)
        else:
            # 正常延迟
            return self.config.base_delay + random.uniform(0, 0.2)
    
    def adjust_batch_size(self, success_rate: float, recent_errors: int):
        """动态调整批次大小"""
        old_size = self.current_batch_size
        
        if success_rate >= self.config.success_rate_threshold and recent_errors == 0:
            # 成功率高，增加批次大小
            self.current_batch_size = min(
                self.current_batch_size + 1, 
                self.config.max_batch_size
            )
        elif success_rate < 0.6 or recent_errors > 2:
            # 成功率低或错误较多，减少批次大小
            self.current_batch_size = max(
                self.current_batch_size - 1, 
                self.config.min_batch_size
            )
        
        if old_size != self.current_batch_size:
            if self.status_callback:
                self.status_callback(f"📊 批次大小调整: {old_size} → {self.current_batch_size}")
    
    def handle_api_error(self, error: Exception, attempt: int) -> Tuple[bool, float]:
        """处理API错误"""
        error_str = str(error).lower()
        
        if any(keyword in error_str for keyword in ['quota', 'rate limit', 'limit exceeded']):
            self.quota_errors += 1
            delay = self.calculate_delay(attempt, is_quota_error=True)
            if self.status_callback:
                self.status_callback(f"⚠️ 配额限制，等待 {delay:.1f}s 后重试...")
            return True, delay
        
        elif any(keyword in error_str for keyword in ['timeout', 'connection', 'network']):
            delay = self.calculate_delay(attempt) * 2
            if self.status_callback:
                self.status_callback(f"🔄 网络错误，等待 {delay:.1f}s 后重试...")
            return True, delay
        
        return False, 0
    
    def upload_single_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """上传单个项目"""
        max_retries = self.config.max_retries
        
        for attempt in range(max_retries):
            try:
                # API调用前的延迟控制
                current_time = time.time()
                if current_time - self.last_api_call_time < self.config.base_delay:
                    time.sleep(self.config.base_delay - (current_time - self.last_api_call_time))
                
                # 执行上传操作
                result = self._perform_upload(item)
                self.log_api_call()
                self.success_count += 1
                
                return {
                    'success': True,
                    'item': item,
                    'result': result,
                    'attempts': attempt + 1
                }
                
            except Exception as e:
                self.error_count += 1
                
                should_retry, delay = self.handle_api_error(e, attempt)
                
                if should_retry and attempt < max_retries - 1:
                    time.sleep(delay)
                    continue
                else:
                    return {
                        'success': False,
                        'item': item,
                        'error': str(e),
                        'attempts': attempt + 1
                    }
        
        return {
            'success': False,
            'item': item,
            'error': f'超过最大重试次数 ({max_retries})',
            'attempts': max_retries
        }
    
    def _perform_upload(self, item: Dict[str, Any]) -> Any:
        """执行实际的上传操作 - 子类需要实现"""
        raise NotImplementedError("子类必须实现 _perform_upload 方法")
    
    def process_batch(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """处理一个批次"""
        batch_results = []
        
        if self.status_callback:
            self.status_callback(f"🔄 处理批次: {len(items)} 个项目")
        
        for item in items:
            result = self.upload_single_item(item)
            batch_results.append(result)
            
            if self.progress_callback:
                self.progress_callback(1)
        
        return batch_results
    
    def upload_all(self, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        """上传所有项目"""
        total_items = len(items)
        processed = 0
        all_results = []
        
        # 初始化进度
        if self.progress_callback:
            self.progress_callback(0, total_items)
        
        # 分批处理
        for i in range(0, total_items, self.current_batch_size):
            batch = items[i:i + self.current_batch_size]
            batch_num = (i // self.current_batch_size) + 1
            
            if self.status_callback:
                self.status_callback(f"📦 第 {batch_num} 批次，共 {len(batch)} 个项目")
            
            batch_results = self.process_batch(batch)
            all_results.extend(batch_results)
            
            # 计算成功率
            batch_success_count = sum(1 for r in batch_results if r['success'])
            batch_success_rate = batch_success_count / len(batch_results)
            recent_errors = sum(1 for r in batch_results if not r['success'])
            
            # 调整批次大小
            self.adjust_batch_size(batch_success_rate, recent_errors)
            
            processed += len(batch)
            
            # 批次间延迟
            if processed < total_items:
                delay = self.calculate_delay()
                time.sleep(delay)
        
        # 统计结果
        successful = sum(1 for r in all_results if r['success'])
        failed = total_items - successful
        
        return {
            'total': total_items,
            'successful': successful,
            'failed': failed,
            'success_rate': successful / total_items,
            'results': all_results,
            'api_calls': self.api_call_count,
            'quota_errors': self.quota_errors
        }

class SheetsDataUploader(AdaptiveBatchUploader):
    """Google Sheets数据上传器"""
    
    def __init__(self, gc, spreadsheet_name: str, worksheet_name: str, config: BatchConfig = None):
        super().__init__(gc, config)
        self.spreadsheet_name = spreadsheet_name
        self.worksheet_name = worksheet_name
        self.worksheet = None
        self._setup_worksheet()
    
    def _setup_worksheet(self):
        """设置工作表"""
        spreadsheet = get_or_create_spreadsheet(self.gc, self.spreadsheet_name)
        self.worksheet = get_or_create_worksheet(
            spreadsheet, 
            self.worksheet_name, 
            rows=5000, 
            cols=20
        )
    
    def _perform_upload(self, item: Dict[str, Any]) -> Any:
        """执行数据上传"""
        data = item['data']
        start_row = item['start_row']
        
        # 更新数据到工作表
        if data:
            range_name = f'A{start_row}'
            self.worksheet.update(range_name, data)
            return f"已更新 {len(data)} 行数据到 {range_name}"
        
        return "空数据，跳过上传"

class PermissionsUploader(SheetsDataUploader):
    """权限数据上传器"""
    
    def __init__(self, gc, config: BatchConfig = None):
        super().__init__(gc, "门店报表系统数据", PERMISSIONS_SHEET_NAME, config)
    
    def upload_permissions(self, df: pd.DataFrame) -> Dict[str, Any]:
        """上传权限数据"""
        # 清空现有数据
        self.worksheet.clear()
        time.sleep(1)
        
        # 准备数据
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # 分批准备数据
        batch_size = 50  # 每批50行
        items = []
        
        # 添加表头
        headers = [['门店名称', '人员编号', '更新时间']]
        items.append({
            'data': headers,
            'start_row': 1
        })
        
        # 分批准备数据行
        for i in range(0, len(df), batch_size):
            batch_df = df.iloc[i:i + batch_size]
            batch_data = []
            
            for _, row in batch_df.iterrows():
                batch_data.append([
                    str(row.iloc[0]).strip(),
                    str(row.iloc[1]).strip(),
                    current_time
                ])
            
            items.append({
                'data': batch_data,
                'start_row': i + 2  # +2 因为有表头
            })
        
        return self.upload_all(items)

class ReportsUploader(SheetsDataUploader):
    """报表数据上传器"""
    
    def __init__(self, gc, config: BatchConfig = None):
        super().__init__(gc, "门店报表系统数据", REPORTS_SHEET_NAME, config)
    
    def upload_reports(self, reports_dict: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """上传报表数据"""
        # 清空现有数据
        self.worksheet.clear()
        time.sleep(1)
        
        # 准备数据
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        items = []
        
        # 添加表头
        headers = [['门店名称', '报表数据JSON', '行数', '列数', '更新时间', '分片序号', '总分片数', '数据哈希']]
        items.append({
            'data': headers,
            'start_row': 1
        })
        
        row_counter = 2
        
        for store_name, df in reports_dict.items():
            try:
                # 清理数据
                df_cleaned = clean_dataframe_for_json(df)
                
                # 转换为JSON
                json_data = df_cleaned.to_json(orient='records', force_ascii=False, ensure_ascii=False)
                
                # 计算数据哈希
                data_hash = hashlib.md5(json_data.encode('utf-8')).hexdigest()[:16]
                
                # 检查是否需要分片
                if len(json_data) <= self.config.max_chunk_size:
                    # 不需要分片
                    items.append({
                        'data': [[
                            store_name,
                            json_data,
                            len(df),
                            len(df.columns),
                            current_time,
                            "1",
                            "1",
                            data_hash
                        ]],
                        'start_row': row_counter
                    })
                    row_counter += 1
                else:
                    # 分片存储
                    chunks = []
                    for i in range(0, len(json_data), self.config.max_chunk_size):
                        chunks.append(json_data[i:i + self.config.max_chunk_size])
                    
                    total_chunks = len(chunks)
                    
                    for idx, chunk in enumerate(chunks):
                        chunk_name = f"{store_name}_分片{idx+1}"
                        items.append({
                            'data': [[
                                chunk_name,
                                chunk,
                                len(df),
                                len(df.columns),
                                current_time,
                                str(idx+1),
                                str(total_chunks),
                                data_hash
                            ]],
                            'start_row': row_counter
                        })
                        row_counter += 1
                
                logger.info(f"准备上传 {store_name}: {len(df)} 行数据")
                
            except Exception as e:
                logger.error(f"处理 {store_name} 时出错: {str(e)}")
                # 添加错误记录
                error_data = {
                    "error": str(e),
                    "rows": len(df) if 'df' in locals() else 0,
                    "columns": len(df.columns) if 'df' in locals() else 0,
                    "timestamp": current_time
                }
                
                items.append({
                    'data': [[
                        f"{store_name}_错误",
                        json.dumps(error_data, ensure_ascii=False),
                        0,
                        0,
                        current_time,
                        "1",
                        "1",
                        "ERROR"
                    ]],
                    'start_row': row_counter
                })
                row_counter += 1
        
        return self.upload_all(items)

class UploadProgressManager:
    """上传进度管理器"""
    
    def __init__(self):
        self.progress_bar = None
        self.status_text = None
        self.info_container = None
        self.total_items = 0
        self.processed_items = 0
        
    def setup_ui(self):
        """设置UI组件"""
        self.progress_bar = st.progress(0)
        self.status_text = st.empty()
        self.info_container = st.container()
        
        # 创建实时统计显示区域
        self.stats_container = st.container()
        with self.stats_container:
            self.col1, self.col2, self.col3 = st.columns(3)
            with self.col1:
                self.processed_metric = st.empty()
            with self.col2:
                self.success_metric = st.empty()
            with self.col3:
                self.error_metric = st.empty()
    
    def update_progress(self, increment: int, total: int = None):
        """更新进度"""
        if total is not None:
            self.total_items = total
            self.processed_items = 0
        
        self.processed_items += increment
        
        if self.total_items > 0:
            progress = self.processed_items / self.total_items
            self.progress_bar.progress(progress)
            
            # 更新统计信息
            self.processed_metric.metric("已处理", self.processed_items)
    
    def update_status(self, message: str):
        """更新状态消息"""
        self.status_text.text(message)
        
        # 在info容器中显示详细信息
        with self.info_container:
            timestamp = datetime.now().strftime("%H:%M:%S")
            st.markdown(f'<div class="batch-info">🕒 {timestamp} - {message}</div>', unsafe_allow_html=True)
    
    def show_quota_warning(self, api_calls: int, quota_errors: int):
        """显示配额警告"""
        if quota_errors > 0:
            warning_msg = f"⚠️ 配额限制警告: {quota_errors} 次限制，已完成 {api_calls} 次API调用"
            if quota_errors > 5:
                st.markdown(f'<div class="quota-danger">{warning_msg}</div>', unsafe_allow_html=True)
            else:
                st.markdown(f'<div class="quota-warning">{warning_msg}</div>', unsafe_allow_html=True)
    
    def show_final_results(self, results: Dict[str, Any]):
        """显示最终结果"""
        total = results['total']
        successful = results['successful']
        failed = results['failed']
        success_rate = results['success_rate']
        
        # 更新最终统计
        self.processed_metric.metric("总计", total)
        self.success_metric.metric("成功", successful, delta=f"{success_rate:.1%}")
        self.error_metric.metric("失败", failed)
        
        # 显示详细结果
        if success_rate >= 0.95:
            st.markdown(f'<div class="upload-status success-animation">✅ 上传完成！成功率: {success_rate:.1%}</div>', unsafe_allow_html=True)
            st.balloons()
        elif success_rate >= 0.8:
            st.markdown(f'<div class="upload-status">✅ 上传基本完成，成功率: {success_rate:.1%}</div>', unsafe_allow_html=True)
        else:
            st.markdown(f'<div class="quota-warning">⚠️ 上传完成但有较多失败，成功率: {success_rate:.1%}</div>', unsafe_allow_html=True)
        
        # 显示配额使用情况
        self.show_quota_warning(results['api_calls'], results['quota_errors'])
        
        # 显示失败的项目
        if failed > 0:
            with st.expander(f"查看失败项目 ({failed} 个)"):
                failed_items = [r for r in results['results'] if not r['success']]
                for item in failed_items[:10]:  # 只显示前10个
                    st.error(f"❌ {item.get('error', '未知错误')}")
                if len(failed_items) > 10:
                    st.info(f"... 还有 {len(failed_items) - 10} 个失败项目")

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

@st.cache_resource(show_spinner="连接云数据库...")
def get_google_sheets_client():
    """获取Google Sheets客户端 - 使用缓存"""
    try:
        credentials_info = st.secrets["google_sheets"]
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        credentials = Credentials.from_service_account_info(credentials_info, scopes=scopes)
        client = gspread.authorize(credentials)
        logger.info("Google Sheets客户端创建成功")
        return client
    except Exception as e:
        logger.error(f"Google Sheets客户端创建失败: {str(e)}")
        raise SheetOperationError(f"连接失败: {str(e)}")

def safe_sheet_operation(operation_func, *args, **kwargs):
    """安全的表格操作"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            return operation_func(*args, **kwargs)
        except Exception as e:
            if attempt == max_retries - 1:
                logger.error(f"操作失败，已重试 {max_retries} 次: {str(e)}")
                raise
            logger.warning(f"操作失败，第 {attempt + 1} 次重试: {str(e)}")
            time.sleep(1 * (attempt + 1))

def get_or_create_spreadsheet(gc, name="门店报表系统数据"):
    """获取或创建表格"""
    def _operation():
        try:
            spreadsheet = gc.open(name)
            logger.info(f"表格 '{name}' 已存在")
            return spreadsheet
        except gspread.SpreadsheetNotFound:
            logger.info(f"创建新表格 '{name}'")
            spreadsheet = gc.create(name)
            spreadsheet.share('', perm_type='anyone', role='writer')
            return spreadsheet
    
    return safe_sheet_operation(_operation)

def get_or_create_worksheet(spreadsheet, name, rows=1000, cols=20):
    """获取或创建工作表"""
    def _operation():
        try:
            worksheet = spreadsheet.worksheet(name)
            logger.info(f"工作表 '{name}' 已存在")
            return worksheet
        except gspread.WorksheetNotFound:
            logger.info(f"创建新工作表 '{name}'")
            worksheet = spreadsheet.add_worksheet(title=name, rows=rows, cols=cols)
            return worksheet
    
    return safe_sheet_operation(_operation)

def clean_dataframe_for_json(df: pd.DataFrame) -> pd.DataFrame:
    """清理DataFrame以便JSON序列化"""
    try:
        df_cleaned = df.copy()
        
        for col in df_cleaned.columns:
            df_cleaned[col] = df_cleaned[col].astype(str)
            df_cleaned[col] = df_cleaned[col].replace({
                'nan': '',
                'None': '',
                'NaT': '',
                'null': '',
                '<NA>': ''
            })
            
            df_cleaned[col] = df_cleaned[col].apply(
                lambda x: x[:1000] + '...' if len(str(x)) > 1000 else x
            )
        
        logger.info(f"DataFrame清理完成: {len(df_cleaned)} 行 x {len(df_cleaned.columns)} 列")
        return df_cleaned
        
    except Exception as e:
        logger.error(f"清理DataFrame失败: {str(e)}")
        raise DataProcessingError(f"数据清理失败: {str(e)}")

def save_permissions_to_sheets(df: pd.DataFrame, gc) -> bool:
    """保存权限数据 - 使用优化的上传器"""
    with error_handler("保存权限数据"):
        progress_manager = UploadProgressManager()
        progress_manager.setup_ui()
        
        uploader = PermissionsUploader(gc)
        uploader.set_callbacks(
            progress_manager.update_progress,
            progress_manager.update_status
        )
        
        progress_manager.update_status("🚀 开始上传权限数据...")
        
        results = uploader.upload_permissions(df)
        
        progress_manager.show_final_results(results)
        
        if results['success_rate'] >= 0.8:
            # 清除相关缓存
            cache_key = get_cache_key("permissions", "load")
            if f"cache_{cache_key}" in st.session_state:
                del st.session_state[f"cache_{cache_key}"]
            
            logger.info(f"权限数据保存成功: {results['successful']}/{results['total']}")
            return True
        
        return False

def save_reports_to_sheets(reports_dict: Dict[str, pd.DataFrame], gc) -> bool:
    """保存报表数据 - 使用优化的上传器"""
    with error_handler("保存报表数据"):
        progress_manager = UploadProgressManager()
        progress_manager.setup_ui()
        
        uploader = ReportsUploader(gc)
        uploader.set_callbacks(
            progress_manager.update_progress,
            progress_manager.update_status
        )
        
        progress_manager.update_status("🚀 开始上传报表数据...")
        
        results = uploader.upload_reports(reports_dict)
        
        progress_manager.show_final_results(results)
        
        if results['success_rate'] >= 0.8:
            # 清除相关缓存
            cache_key = get_cache_key("reports", "load")
            if f"cache_{cache_key}" in st.session_state:
                del st.session_state[f"cache_{cache_key}"]
            
            logger.info(f"报表数据保存成功: {results['successful']}/{results['total']}")
            return True
        
        return False

def load_permissions_from_sheets(gc) -> Optional[pd.DataFrame]:
    """加载权限数据 - 使用缓存"""
    cache_key = get_cache_key("permissions", "load")
    cached_data = get_cache(cache_key)
    if cached_data is not None:
        logger.info("从缓存加载权限数据")
        return cached_data
    
    with error_handler("加载权限数据"):
        def _load_operation():
            spreadsheet = get_or_create_spreadsheet(gc)
            
            try:
                worksheet = spreadsheet.worksheet(PERMISSIONS_SHEET_NAME)
                data = worksheet.get_all_values()
                
                if len(data) <= 1:
                    logger.info("权限表为空")
                    return None
                
                df = pd.DataFrame(data[1:], columns=['门店名称', '人员编号', '更新时间'])
                result_df = df[['门店名称', '人员编号']].copy()
                
                result_df['门店名称'] = result_df['门店名称'].str.strip()
                result_df['人员编号'] = result_df['人员编号'].str.strip()
                
                result_df = result_df[
                    (result_df['门店名称'] != '') & 
                    (result_df['人员编号'] != '')
                ]
                
                logger.info(f"权限数据加载成功: {len(result_df)} 条记录")
                
                set_cache(cache_key, result_df)
                return result_df
                
            except gspread.WorksheetNotFound:
                logger.info("权限表不存在")
                return None
        
        return safe_sheet_operation(_load_operation)

def reconstruct_fragmented_data(fragments: List[Dict[str, Any]], store_name: str) -> Optional[pd.DataFrame]:
    """重构分片数据"""
    try:
        if len(fragments) == 1:
            json_data = fragments[0]['json_data']
        else:
            fragments.sort(key=lambda x: int(x['chunk_num']))
            json_data = ''.join([frag['json_data'] for frag in fragments])
        
        # 验证数据完整性
        expected_hash = fragments[0].get('data_hash', '')
        if expected_hash and expected_hash != 'ERROR':
            actual_hash = hashlib.md5(json_data.encode('utf-8')).hexdigest()[:16]
            if actual_hash != expected_hash:
                logger.warning(f"{store_name} 数据哈希不匹配，可能存在数据损坏")
        
        df = pd.read_json(json_data, orient='records')
        
        if len(df) > 0:
            first_row = df.iloc[0]
            non_empty_count = sum(1 for val in first_row if pd.notna(val) and str(val).strip() != '')
            
            if non_empty_count <= 2 and len(df) > 1:
                df = df.iloc[1:].reset_index(drop=True)
        
        if len(df) > 1:
            header_row = df.iloc[0].fillna('').astype(str).tolist()
            data_rows = df.iloc[1:].copy()
            
            cols = []
            for i, col in enumerate(header_row):
                col = str(col).strip()
                if col == '' or col == 'nan' or col == '0':
                    col = f'列{i+1}' if i > 0 else '项目名称'
                
                original_col = col
                counter = 1
                while col in cols:
                    col = f"{original_col}_{counter}"
                    counter += 1
                cols.append(col)
            
            min_cols = min(len(data_rows.columns), len(cols))
            cols = cols[:min_cols]
            data_rows = data_rows.iloc[:, :min_cols]
            
            data_rows.columns = cols
            df = data_rows.reset_index(drop=True).fillna('')
        else:
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
    """加载报表数据 - 使用缓存和分片重构"""
    cache_key = get_cache_key("reports", "load")
    cached_data = get_cache(cache_key)
    if cached_data is not None:
        logger.info("从缓存加载报表数据")
        return cached_data
    
    with error_handler("加载报表数据"):
        def _load_operation():
            spreadsheet = get_or_create_spreadsheet(gc)
            
            try:
                worksheet = spreadsheet.worksheet(REPORTS_SHEET_NAME)
                data = worksheet.get_all_values()
                
                if len(data) <= 1:
                    logger.info("报表数据为空")
                    return {}
                
                reports_dict = {}
                fragments_dict = {}
                
                for row in data[1:]:
                    if len(row) >= 7:
                        store_name = row[0]
                        json_data = row[1]
                        rows_count = row[2]
                        cols_count = row[3]
                        update_time = row[4]
                        chunk_num = row[5]
                        total_chunks = row[6]
                        data_hash = row[7] if len(row) > 7 else ''
                        
                        if store_name.endswith('_错误'):
                            logger.warning(f"跳过错误数据: {store_name}")
                            continue
                        
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
                            fragments_dict[store_name] = [{
                                'json_data': json_data,
                                'chunk_num': '1',
                                'total_chunks': '1',
                                'data_hash': data_hash
                            }]
                
                for store_name, fragments in fragments_dict.items():
                    df = reconstruct_fragmented_data(fragments, store_name)
                    if df is not None:
                        reports_dict[store_name] = df
                
                logger.info(f"报表数据加载成功: {len(reports_dict)} 个门店")
                
                set_cache(cache_key, reports_dict)
                return reports_dict
                
            except gspread.WorksheetNotFound:
                logger.info("报表数据表不存在")
                return {}
        
        return safe_sheet_operation(_load_operation)

def analyze_receivable_data(df: pd.DataFrame) -> Dict[str, Any]:
    """分析应收未收额数据 - 专门查找第69行"""
    result = {}
    
    if len(df.columns) == 0 or len(df) == 0:
        return result
    
    original_df = df.copy()
    first_row = df.iloc[0] if len(df) > 0 else None
    if first_row is not None:
        non_empty_count = sum(1 for val in first_row if pd.notna(val) and str(val).strip() != '')
        if non_empty_count <= 2:
            df = df.iloc[1:].reset_index(drop=True)
            result['skipped_store_name_row'] = True
    
    target_row_index = 68  # 第69行
    
    if len(df) > target_row_index:
        row = df.iloc[target_row_index]
        first_col_value = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
        
        keywords = ['应收-未收额', '应收未收额', '应收-未收', '应收未收']
        
        for keyword in keywords:
            if keyword in first_col_value:
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
        
        # 添加上传配置
        st.subheader("🔧 上传配置")
        batch_size = st.slider("批次大小", 1, 20, BATCH_CONFIG.initial_batch_size)
        max_chunk_size = st.slider("数据块大小", 10000, 50000, BATCH_CONFIG.max_chunk_size, step=5000)
        
        # 更新配置
        BATCH_CONFIG.initial_batch_size = batch_size
        BATCH_CONFIG.max_chunk_size = max_chunk_size
        
        # 显示当前配置
        st.info(f"当前配置:\n- 批次大小: {batch_size}\n- 数据块: {max_chunk_size:,}")
        
    else:
        st.error("🔴 云数据库断开")
    
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
            st.subheader("📁 智能文件管理")
            
            # 上传权限表
            permissions_file = st.file_uploader("上传门店权限表", type=['xlsx', 'xls'])
            if permissions_file:
                try:
                    with st.spinner("处理权限表文件..."):
                        df = pd.read_excel(permissions_file)
                        if len(df.columns) >= 2:
                            st.success(f"✅ 文件已读取：{len(df)} 个用户")
                            
                            # 预览数据
                            st.subheader("📋 数据预览")
                            st.dataframe(df.head(), use_container_width=True)
                            
                            if st.button("🚀 开始智能上传权限表", key="upload_permissions"):
                                with st.container():
                                    st.markdown("### 📊 上传进度")
                                    if save_permissions_to_sheets(df, gc):
                                        st.success("🎉 权限表上传成功！")
                                    else:
                                        st.error("❌ 上传失败，请检查日志")
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
                            st.success(f"✅ 文件已读取：{len(reports_dict)} 个门店")
                            
                            # 预览数据
                            st.subheader("📋 数据预览")
                            for name, df in list(reports_dict.items())[:3]:
                                with st.expander(f"📊 {name}"):
                                    st.write(f"数据规模: {len(df)} 行 × {len(df.columns)} 列")
                                    st.dataframe(df.head(3), use_container_width=True)
                            
                            if st.button("🚀 开始智能上传报表", key="upload_reports"):
                                with st.container():
                                    st.markdown("### 📊 上传进度")
                                    if save_reports_to_sheets(reports_dict, gc):
                                        st.success("🎉 报表上传成功！")
                                    else:
                                        st.error("❌ 上传失败，请检查日志")
                        else:
                            show_status_message("❌ 文件中没有有效的工作表", "error")
                            
                except Exception as e:
                    show_status_message(f"❌ 处理失败：{str(e)}", "error")
            
            # 缓存管理
            st.subheader("🗂️ 缓存管理")
            cache_count = len([key for key in st.session_state.keys() if key.startswith('cache_')])
            st.metric("缓存项目数", cache_count)
            
            if st.button("🧹 清除所有缓存"):
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
    st.markdown('<div class="admin-panel"><h3>👨‍💼 管理员控制面板</h3><p>智能分批上传，自动适应API配额，支持断点续传和实时监控</p></div>', unsafe_allow_html=True)
    
    try:
        with st.spinner("加载数据统计..."):
            permissions_data = load_permissions_from_sheets(gc)
            reports_data = load_reports_from_sheets(gc)
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            perms_count = len(permissions_data) if permissions_data is not None else 0
            st.metric("权限表用户数", perms_count)
        with col2:
            reports_count = len(reports_data)
            st.metric("报表门店数", reports_count)
        with col3:
            cache_count = len([key for key in st.session_state.keys() if key.startswith('cache_')])
            st.metric("缓存项目数", cache_count)
        with col4:
            st.metric("批次大小", BATCH_CONFIG.initial_batch_size)
            
        # 数据预览
        if permissions_data is not None and len(permissions_data) > 0:
            st.subheader("👥 权限数据预览")
            st.dataframe(permissions_data.head(10), use_container_width=True)
        
        if reports_data:
            st.subheader("📊 报表数据预览")
            report_names = list(reports_data.keys())[:5]
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

# 页面底部状态信息
st.divider()
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.caption(f"🕒 时间: {datetime.now().strftime('%H:%M:%S')}")
with col2:
    cache_count = len([key for key in st.session_state.keys() if key.startswith('cache_')])
    st.caption(f"💾 缓存: {cache_count}")
with col3:
    st.caption(f"🔧 批次: {BATCH_CONFIG.initial_batch_size}")
with col4:
    st.caption("📊 版本: v3.0 (智能分批)")
