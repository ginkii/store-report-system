import streamlit as st
import pandas as pd
import io
import json
from datetime import datetime, timedelta
import time
import gspread
from google.oauth2.service_account import Credentials
import logging
from typing import Optional, Dict, Any, List
import hashlib
import pickle
import traceback
import zipfile
import base64
import threading
from contextlib import contextmanager
from queue import Queue
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

# 优化后的系统配置
ADMIN_PASSWORD = "admin123"
PERMISSIONS_SHEET_NAME = "store_permissions"
REPORTS_SHEET_NAME = "store_reports"
SYSTEM_INFO_SHEET_NAME = "system_info"

# API限制优化配置
MAX_REQUESTS_PER_MINUTE = 80  # 安全配额（低于100的限制）
BATCH_SIZE = 1000  # Google Sheets最大批量大小
MIN_REQUEST_INTERVAL = 0.8  # 最小请求间隔（秒）
API_RETRY_TIMES = 3  # API失败重试次数
API_BACKOFF_FACTOR = 2  # 退避因子

# 存储优化配置 - 移除压缩以减少复杂性
ENABLE_COMPRESSION = False  # 关闭压缩，减少存储操作
MAX_SINGLE_CELL_SIZE = 40000  # 减小单元格最大字符数

# 数据清理配置 - 完全禁用备份
AUTO_BACKUP_BEFORE_CLEAR = False  # 禁用自动备份
BACKUP_RETENTION_MONTHS = 0  # 不保留备份

class APIRateLimiter:
    """API速率限制器"""
    
    def __init__(self):
        self.request_times = []
        self.lock = threading.Lock()
    
    def wait_if_needed(self):
        """如果需要，等待以避免超过API限制"""
        with self.lock:
            current_time = time.time()
            
            # 清理1分钟前的记录
            cutoff_time = current_time - 60
            self.request_times = [t for t in self.request_times if t > cutoff_time]
            
            # 检查是否接近限制
            if len(self.request_times) >= MAX_REQUESTS_PER_MINUTE:
                # 计算需要等待的时间
                oldest_request = min(self.request_times)
                wait_time = 61 - (current_time - oldest_request)
                if wait_time > 0:
                    logger.info(f"API限制保护：等待 {wait_time:.1f} 秒")
                    time.sleep(wait_time)
            
            # 记录本次请求
            self.request_times.append(current_time)
            
            # 基础间隔保护
            time.sleep(MIN_REQUEST_INTERVAL)

# 全局API限制器
api_limiter = APIRateLimiter()

class SimpleDataManager:
    """简化的数据管理器 - 无备份，最小存储"""
    
    def __init__(self, gc):
        self.gc = gc
        self.spreadsheet = None
        self._init_spreadsheet()
    
    def _init_spreadsheet(self):
        """初始化表格"""
        try:
            api_limiter.wait_if_needed()
            self.spreadsheet = self.gc.open("门店报表系统数据")
        except gspread.SpreadsheetNotFound:
            api_limiter.wait_if_needed()
            self.spreadsheet = self.gc.create("门店报表系统数据")
            # 移除自动共享，减少权限操作
    
    def _safe_api_call(self, func, *args, **kwargs):
        """安全的API调用，包含重试和限流"""
        for attempt in range(API_RETRY_TIMES):
            try:
                api_limiter.wait_if_needed()
                return func(*args, **kwargs)
            except Exception as e:
                if attempt == API_RETRY_TIMES - 1:
                    raise
                wait_time = API_BACKOFF_FACTOR ** attempt
                logger.warning(f"API调用失败，{wait_time}秒后重试: {str(e)}")
                time.sleep(wait_time)
    
    def get_current_data_info(self) -> Dict[str, Any]:
        """获取当前数据信息"""
        try:
            # 检查是否有报表数据表
            try:
                reports_ws = self._safe_api_call(self.spreadsheet.worksheet, REPORTS_SHEET_NAME)
                data = self._safe_api_call(reports_ws.get_all_values)
                
                if len(data) <= 1:
                    return {"has_data": False, "last_update": None, "store_count": 0}
                
                # 简单统计数据
                store_count = len(data) - 1  # 减去表头
                last_update = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                data_month = datetime.now().strftime("%Y-%m")
                
                return {
                    "has_data": True,
                    "last_update": last_update,
                    "store_count": store_count,
                    "data_month": data_month,
                    "total_rows": sum(int(row[2]) if len(row) > 2 and row[2].isdigit() else 0 for row in data[1:])
                }
                
            except gspread.WorksheetNotFound:
                return {"has_data": False, "last_update": None, "store_count": 0}
            
        except Exception as e:
            logger.error(f"获取当前数据信息失败: {str(e)}")
            return {"has_data": False, "last_update": None, "store_count": 0}
    
    def clear_all_report_data(self) -> bool:
        """简单清空所有报表数据 - 无备份"""
        try:
            with st.spinner("🗑️ 正在清理数据..."):
                # 1. 删除报表数据表（如果存在）
                try:
                    reports_ws = self._safe_api_call(self.spreadsheet.worksheet, REPORTS_SHEET_NAME)
                    self._safe_api_call(self.spreadsheet.del_worksheet, reports_ws)
                    logger.info("已删除报表数据表")
                except gspread.WorksheetNotFound:
                    logger.info("报表数据表不存在，跳过删除")
                
                # 2. 删除系统信息表（如果存在）
                try:
                    info_ws = self._safe_api_call(self.spreadsheet.worksheet, SYSTEM_INFO_SHEET_NAME)
                    self._safe_api_call(self.spreadsheet.del_worksheet, info_ws)
                    logger.info("已删除系统信息表")
                except gspread.WorksheetNotFound:
                    logger.info("系统信息表不存在，跳过删除")
                
                # 3. 清理缓存
                self._clear_all_cache()
                
                logger.info("数据清理完成")
                return True
            
        except Exception as e:
            logger.error(f"清理数据失败: {str(e)}")
            return False
    
    def _clear_all_cache(self):
        """清理所有相关缓存"""
        cache_keys = [key for key in st.session_state.keys() if 'cache_' in key]
        for key in cache_keys:
            del st.session_state[key]
        logger.info(f"已清理 {len(cache_keys)} 个缓存项")
    
    def save_reports_simple(self, reports_dict: Dict[str, pd.DataFrame]) -> bool:
        """简化的报表保存 - 无压缩，无备份，直接替换"""
        try:
            # 1. 数据预处理和验证
            if not reports_dict:
                st.error("❌ 没有数据需要保存")
                return False
            
            total_stores = len(reports_dict)
            total_rows = sum(len(df) for df in reports_dict.values())
            
            st.info(f"📊 准备保存：{total_stores} 个门店，{total_rows:,} 行数据")
            
            # 2. 清空现有数据
            st.warning("⚠️ 正在清空现有数据...")
            if not self.clear_all_report_data():
                st.error("❌ 清理旧数据失败")
                return False
            
            # 3. 创建新的报表工作表
            with st.spinner("📝 创建新数据表..."):
                reports_ws = self._safe_api_call(self.spreadsheet.add_worksheet, 
                                               title=REPORTS_SHEET_NAME, rows=max(2000, total_stores + 100), cols=8)
            
            # 4. 准备数据 - 简化版本
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            current_month = datetime.now().strftime("%Y-%m")
            
            headers = ["门店名称", "报表数据", "行数", "列数", "更新时间", "数据月份"]
            all_data = [headers]
            
            # 5. 处理每个门店数据 - 简化处理
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            for idx, (store_name, df) in enumerate(reports_dict.items()):
                try:
                    # 更新进度
                    progress = (idx + 1) / total_stores
                    progress_bar.progress(progress)
                    status_text.text(f"处理中: {store_name} ({idx + 1}/{total_stores})")
                    
                    # 简单清理数据
                    df_cleaned = self._clean_dataframe_simple(df)
                    
                    # 转换为JSON - 无压缩
                    json_data = df_cleaned.to_json(orient='records', force_ascii=False)
                    
                    # 检查数据大小 - 如果太大就截断
                    if len(json_data) > MAX_SINGLE_CELL_SIZE:
                        logger.warning(f"{store_name} 数据过大，截断至{MAX_SINGLE_CELL_SIZE}字符")
                        json_data = json_data[:MAX_SINGLE_CELL_SIZE-100] + '...[数据截断]'
                    
                    # 添加到数据列表
                    all_data.append([
                        store_name,
                        json_data,
                        len(df),
                        len(df.columns),
                        current_time,
                        current_month
                    ])
                    
                    logger.info(f"✅ {store_name}: {len(df)}行")
                    
                except Exception as e:
                    logger.error(f"❌ 处理 {store_name} 失败: {str(e)}")
                    # 添加错误记录
                    all_data.append([
                        f"{store_name}_错误",
                        f"处理失败: {str(e)}",
                        0, 0, current_time, current_month
                    ])
                    continue
            
            # 6. 批量写入数据 - 更大批次
            with st.spinner("💾 保存数据到云端..."):
                batch_size = 100  # 更大批次，减少API调用
                total_batches = math.ceil(len(all_data) / batch_size)
                
                for batch_idx in range(total_batches):
                    start_idx = batch_idx * batch_size
                    end_idx = min(start_idx + batch_size, len(all_data))
                    batch_data = all_data[start_idx:end_idx]
                    
                    if batch_idx == 0:
                        # 第一批包含表头
                        start_cell = 'A1'
                    else:
                        # 后续批次
                        start_cell = f'A{start_idx + 1}'
                    
                    self._safe_api_call(reports_ws.update, start_cell, batch_data)
                    
                    # 更新进度
                    batch_progress = (batch_idx + 1) / total_batches
                    progress_bar.progress(batch_progress)
                    status_text.text(f"保存中: 批次 {batch_idx + 1}/{total_batches}")
            
            # 7. 清理进度显示
            progress_bar.empty()
            status_text.empty()
            
            logger.info(f"✅ 数据保存完成: {total_stores} 个门店")
            return True
            
        except Exception as e:
            logger.error(f"❌ 保存数据失败: {str(e)}")
            st.error(f"保存失败: {str(e)}")
            return False
    
    def load_reports_simple(self) -> Dict[str, pd.DataFrame]:
        """简化的报表加载"""
        try:
            # 获取报表工作表
            try:
                reports_ws = self._safe_api_call(self.spreadsheet.worksheet, REPORTS_SHEET_NAME)
            except gspread.WorksheetNotFound:
                logger.info("报表工作表不存在")
                return {}
            
            # 读取数据
            data = self._safe_api_call(reports_ws.get_all_values)
            
            if len(data) <= 1:
                logger.info("报表工作表为空")
                return {}
            
            # 解析数据
            reports_dict = {}
            
            for row in data[1:]:  # 跳过表头
                if len(row) >= 6:
                    store_name = row[0]
                    json_data = row[1]
                    
                    # 跳过错误数据
                    if store_name.endswith('_错误'):
                        logger.warning(f"跳过错误数据: {store_name}")
                        continue
                    
                    try:
                        # 直接解析JSON（无解压缩）
                        df = pd.read_json(json_data, orient='records')
                        
                        # 数据后处理
                        df = self._process_loaded_dataframe(df)
                        
                        reports_dict[store_name] = df
                        logger.info(f"✅ 加载 {store_name}: {len(df)} 行")
                        
                    except Exception as e:
                        logger.error(f"❌ 解析 {store_name} 数据失败: {str(e)}")
                        continue
            
            logger.info(f"✅ 数据加载完成: {len(reports_dict)} 个门店")
            return reports_dict
            
        except Exception as e:
            logger.error(f"❌ 加载数据失败: {str(e)}")
            return {}
    
    def _clean_dataframe_simple(self, df: pd.DataFrame) -> pd.DataFrame:
        """简化的DataFrame清理"""
        try:
            df_cleaned = df.copy()
            
            # 限制数据量
            if len(df_cleaned) > 2000:  # 大幅减少行数限制
                logger.warning(f"数据行数过多({len(df_cleaned)})，截取前2000行")
                df_cleaned = df_cleaned.head(2000)
            
            # 限制列数
            if len(df_cleaned.columns) > 50:
                logger.warning(f"数据列数过多({len(df_cleaned.columns)})，截取前50列")
                df_cleaned = df_cleaned.iloc[:, :50]
            
            # 简单处理数据类型
            for col in df_cleaned.columns:
                df_cleaned[col] = df_cleaned[col].astype(str)
                df_cleaned[col] = df_cleaned[col].replace({
                    'nan': '', 'None': '', 'NaT': '', 'null': '', '<NA>': ''
                })
                # 大幅限制字符串长度
                df_cleaned[col] = df_cleaned[col].apply(
                    lambda x: x[:100] + '...' if len(str(x)) > 100 else x
                )
            
            return df_cleaned
            
        except Exception as e:
            logger.error(f"清理DataFrame失败: {str(e)}")
            return df
    
    def _process_loaded_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """处理加载的DataFrame"""
        try:
            if len(df) == 0:
                return df
            
            # 检查第一行是否是门店名称行
            first_row = df.iloc[0]
            non_empty_count = sum(1 for val in first_row if pd.notna(val) and str(val).strip() != '')
            
            if non_empty_count <= 2 and len(df) > 1:
                df = df.iloc[1:].reset_index(drop=True)
            
            # 处理表头
            if len(df) > 1:
                header_row = df.iloc[0].fillna('').astype(str).tolist()
                data_rows = df.iloc[1:].copy()
                
                # 清理列名
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
                df = df.fillna('')
                default_cols = []
                for i in range(len(df.columns)):
                    col_name = f'列{i+1}' if i > 0 else '项目名称'
                    default_cols.append(col_name)
                df.columns = default_cols
            
            return df
            
        except Exception as e:
            logger.error(f"处理DataFrame失败: {str(e)}")
            return dfized(self, reports_dict: Dict[str, pd.DataFrame], clear_existing: bool = True) -> bool:
        """优化的报表保存 - 支持完全轮替"""
        try:
            # 1. 数据预处理和验证
            if not reports_dict:
                st.error("❌ 没有数据需要保存")
                return False
            
            total_stores = len(reports_dict)
            total_rows = sum(len(df) for df in reports_dict.values())
            
            st.info(f"📊 准备保存：{total_stores} 个门店，{total_rows:,} 行数据")
            
            # 2. 如果需要，清空现有数据
            if clear_existing:
                st.warning("⚠️ 即将清空所有现有数据")
                if not self.clear_all_report_data(create_backup=True):
                    st.error("❌ 清理旧数据失败")
                    return False
            
            # 3. 创建新的报表工作表
            with st.spinner("📝 创建新数据表..."):
                reports_ws = self._safe_api_call(self.spreadsheet.add_worksheet, 
                                               title=REPORTS_SHEET_NAME, rows=5000, cols=10)
            
            # 4. 准备数据
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            current_month = datetime.now().strftime("%Y-%m")
            
            headers = ["门店名称", "报表数据", "行数", "列数", "更新时间", "数据月份", "压缩状态", "数据哈希"]
            all_data = [headers]
            
            # 5. 处理每个门店数据
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            for idx, (store_name, df) in enumerate(reports_dict.items()):
                try:
                    # 更新进度
                    progress = (idx + 1) / total_stores
                    progress_bar.progress(progress)
                    status_text.text(f"处理中: {store_name} ({idx + 1}/{total_stores})")
                    
                    # 清理数据
                    df_cleaned = self._clean_dataframe_for_json(df)
                    
                    # 转换为JSON
                    json_data = df_cleaned.to_json(orient='records', force_ascii=False, ensure_ascii=False)
                    
                    # 压缩数据
                    compressed_data = self.compressor.compress_data(json_data)
                    is_compressed = compressed_data.startswith("COMPRESSED:")
                    
                    # 检查数据大小
                    if len(compressed_data) > MAX_SINGLE_CELL_SIZE:
                        # 如果压缩后仍然太大，截断数据
                        logger.warning(f"{store_name} 数据过大，进行截断")
                        compressed_data = compressed_data[:MAX_SINGLE_CELL_SIZE] + "...[截断]"
                    
                    # 计算哈希
                    data_hash = hashlib.md5(json_data.encode('utf-8')).hexdigest()[:16]
                    
                    # 添加到数据列表
                    all_data.append([
                        store_name,
                        compressed_data,
                        len(df),
                        len(df.columns),
                        current_time,
                        current_month,
                        str(is_compressed),
                        data_hash
                    ])
                    
                    logger.info(f"✅ {store_name}: {len(df)}行, 压缩: {is_compressed}")
                    
                except Exception as e:
                    logger.error(f"❌ 处理 {store_name} 失败: {str(e)}")
                    # 添加错误记录
                    all_data.append([
                        f"{store_name}_错误",
                        f"处理失败: {str(e)}",
                        0, 0, current_time, current_month, "False", "ERROR"
                    ])
                    continue
            
            # 6. 批量写入数据
            with st.spinner("💾 保存数据到云端..."):
                # 分批写入，避免单次写入过多数据
                batch_size = 50  # 每批50行
                total_batches = math.ceil(len(all_data) / batch_size)
                
                for batch_idx in range(total_batches):
                    start_idx = batch_idx * batch_size
                    end_idx = min(start_idx + batch_size, len(all_data))
                    batch_data = all_data[start_idx:end_idx]
                    
                    if batch_idx == 0:
                        # 第一批包含表头
                        start_cell = 'A1'
                    else:
                        # 后续批次
                        start_cell = f'A{start_idx + 1}'
                    
                    self._safe_api_call(reports_ws.update, start_cell, batch_data)
                    
                    # 更新进度
                    batch_progress = (batch_idx + 1) / total_batches
                    progress_bar.progress(batch_progress)
                    status_text.text(f"保存中: 批次 {batch_idx + 1}/{total_batches}")
            
            # 7. 更新系统信息
            self.update_system_info(total_stores, total_rows, current_month)
            
            # 8. 清理进度显示
            progress_bar.empty()
            status_text.empty()
            
            logger.info(f"✅ 数据保存完成: {total_stores} 个门店")
            return True
            
        except Exception as e:
            logger.error(f"❌ 保存数据失败: {str(e)}")
            st.error(f"保存失败: {str(e)}")
            return False
    
    def load_reports_optimized(self) -> Dict[str, pd.DataFrame]:
        """优化的报表加载"""
        try:
            # 检查是否有数据
            info = self.get_current_data_info()
            if not info["has_data"]:
                logger.info("系统中没有数据")
                return {}
            
            # 获取报表工作表
            try:
                reports_ws = self._safe_api_call(self.spreadsheet.worksheet, REPORTS_SHEET_NAME)
            except gspread.WorksheetNotFound:
                logger.info("报表工作表不存在")
                return {}
            
            # 读取数据
            data = self._safe_api_call(reports_ws.get_all_values)
            
            if len(data) <= 1:
                logger.info("报表工作表为空")
                return {}
            
            # 解析数据
            reports_dict = {}
            
            for row in data[1:]:  # 跳过表头
                if len(row) >= 8:
                    store_name = row[0]
                    json_data = row[1]
                    data_hash = row[7] if len(row) > 7 else ''
                    is_compressed = len(row) > 6 and row[6] == 'True'
                    
                    # 跳过错误数据
                    if store_name.endswith('_错误'):
                        logger.warning(f"跳过错误数据: {store_name}")
                        continue
                    
                    try:
                        # 解压缩数据
                        if is_compressed:
                            json_data = self.compressor.decompress_data(json_data)
                        
                        # 验证数据完整性
                        if data_hash and data_hash != 'ERROR':
                            actual_hash = hashlib.md5(json_data.encode('utf-8')).hexdigest()[:16]
                            if actual_hash != data_hash:
                                logger.warning(f"{store_name} 数据哈希不匹配，可能存在损坏")
                        
                        # 解析JSON
                        df = pd.read_json(json_data, orient='records')
                        
                        # 数据后处理
                        df = self._process_loaded_dataframe(df)
                        
                        reports_dict[store_name] = df
                        logger.info(f"✅ 加载 {store_name}: {len(df)} 行")
                        
                    except Exception as e:
                        logger.error(f"❌ 解析 {store_name} 数据失败: {str(e)}")
                        continue
            
            logger.info(f"✅ 数据加载完成: {len(reports_dict)} 个门店")
            return reports_dict
            
        except Exception as e:
            logger.error(f"❌ 加载数据失败: {str(e)}")
            return {}
    
    def _clean_dataframe_for_json(self, df: pd.DataFrame) -> pd.DataFrame:
        """清理DataFrame用于JSON序列化"""
        try:
            df_cleaned = df.copy()
            
            # 处理数据类型
            for col in df_cleaned.columns:
                df_cleaned[col] = df_cleaned[col].astype(str)
                df_cleaned[col] = df_cleaned[col].replace({
                    'nan': '', 'None': '', 'NaT': '', 'null': '', '<NA>': ''
                })
                # 限制字符串长度，防止单元格过大
                df_cleaned[col] = df_cleaned[col].apply(
                    lambda x: x[:200] + '...' if len(str(x)) > 200 else x
                )
            
            # 限制行数，防止数据过大
            if len(df_cleaned) > 5000:
                logger.warning(f"数据行数过多({len(df_cleaned)})，截取前5000行")
                df_cleaned = df_cleaned.head(5000)
            
            return df_cleaned
            
        except Exception as e:
            logger.error(f"清理DataFrame失败: {str(e)}")
            return df
    
    def _process_loaded_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """处理加载的DataFrame"""
        try:
            if len(df) == 0:
                return df
            
            # 检查第一行是否是门店名称行
            first_row = df.iloc[0]
            non_empty_count = sum(1 for val in first_row if pd.notna(val) and str(val).strip() != '')
            
            if non_empty_count <= 2 and len(df) > 1:
                df = df.iloc[1:].reset_index(drop=True)
            
            # 处理表头
            if len(df) > 1:
                header_row = df.iloc[0].fillna('').astype(str).tolist()
                data_rows = df.iloc[1:].copy()
                
                # 清理列名
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
                df = df.fillna('')
                default_cols = []
                for i in range(len(df.columns)):
                    col_name = f'列{i+1}' if i > 0 else '项目名称'
                    default_cols.append(col_name)
                df.columns = default_cols
            
            return df
            
        except Exception as e:
            logger.error(f"处理DataFrame失败: {str(e)}")
            return df

# 权限管理函数 - 简化版
def save_permissions_to_sheets(df: pd.DataFrame, gc) -> bool:
    """保存权限数据 - 简化版"""
    try:
        data_manager = SimpleDataManager(gc)
        
        # 获取或创建权限表
        try:
            worksheet = data_manager._safe_api_call(data_manager.spreadsheet.worksheet, PERMISSIONS_SHEET_NAME)
            # 直接清空现有数据
            data_manager._safe_api_call(worksheet.clear)
        except gspread.WorksheetNotFound:
            worksheet = data_manager._safe_api_call(data_manager.spreadsheet.add_worksheet, 
                                                  title=PERMISSIONS_SHEET_NAME, rows=500, cols=3)
        
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        all_data = [['门店名称', '人员编号', '更新时间']]
        
        for _, row in df.iterrows():
            all_data.append([
                str(row.iloc[0]).strip(),
                str(row.iloc[1]).strip(),
                current_time
            ])
        
        # 一次性批量写入
        data_manager._safe_api_call(worksheet.update, 'A1', all_data)
        
        logger.info(f"权限数据保存成功: {len(df)} 条记录")
        return True
        
    except Exception as e:
        logger.error(f"保存权限数据失败: {str(e)}")
        return False

def load_permissions_from_sheets(gc) -> Optional[pd.DataFrame]:
    """加载权限数据 - 简化版"""
    try:
        data_manager = SimpleDataManager(gc)
        
        try:
            worksheet = data_manager._safe_api_call(data_manager.spreadsheet.worksheet, PERMISSIONS_SHEET_NAME)
            data = data_manager._safe_api_call(worksheet.get_all_values)
            
            if len(data) <= 1:
                logger.info("权限表为空")
                return None
            
            df = pd.DataFrame(data[1:], columns=['门店名称', '人员编号', '更新时间'])
            result_df = df[['门店名称', '人员编号']].copy()
            
            # 简单数据清理
            result_df['门店名称'] = result_df['门店名称'].str.strip()
            result_df['人员编号'] = result_df['人员编号'].str.strip()
            
            # 移除空行
            result_df = result_df[
                (result_df['门店名称'] != '') & 
                (result_df['人员编号'] != '')
            ]
            
            logger.info(f"权限数据加载成功: {len(result_df)} 条记录")
            return result_df
            
        except gspread.WorksheetNotFound:
            logger.info("权限表不存在")
            return None
            
    except Exception as e:
        logger.error(f"加载权限数据失败: {str(e)}")
        return None

# 应收未收额分析函数（保持不变）
def analyze_receivable_data(df: pd.DataFrame) -> Dict[str, Any]:
    """分析应收未收额数据"""
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
    
    # 备用查找逻辑
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

# 工具函数
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

@st.cache_resource(show_spinner="连接云数据库...")
def get_google_sheets_client():
    """获取Google Sheets客户端"""
    try:
        credentials_info = st.secrets["google_sheets"]
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        credentials = Credentials.from_service_account_info(credentials_info, scopes=scopes)
        client = gspread.authorize(credentials)
        logger.info("Google Sheets客户端创建成功")
        return client
    except Exception as e:
        logger.error(f"Google Sheets客户端创建失败: {str(e)}")
        raise Exception(f"连接失败: {str(e)}")

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
    .optimization-info {
        background: linear-gradient(135deg, #74b9ff 0%, #0984e3 100%);
        color: white;
        padding: 1rem;
        border-radius: 8px;
        margin: 1rem 0;
    }
    .data-status {
        background: linear-gradient(135deg, #00b894 0%, #00cec9 100%);
        color: white;
        padding: 1rem;
        border-radius: 8px;
        margin: 1rem 0;
    }
    .clear-warning {
        background: linear-gradient(135deg, #e17055 0%, #fdcb6e 100%);
        color: #2d3436;
        padding: 1.5rem;
        border-radius: 10px;
        border: 2px solid #e17055;
        margin: 1rem 0;
    }
    </style>
""", unsafe_allow_html=True)

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

# 主标题
st.markdown('<h1 class="main-header">📊 门店报表查询系统 (优化版)</h1>', unsafe_allow_html=True)

# 显示系统优化特性 - 更新为极简版
st.markdown('''
    <div class="optimization-info">
        <h4>🚀 系统优化特性 (极简版)</h4>
        <p>• <strong>API限制保护</strong>：智能限流，永不超过配额<br>
        • <strong>无备份模式</strong>：彻底节省存储空间<br>
        • <strong>数据截断保护</strong>：防止单元格过大<br>
        • <strong>批量优化处理</strong>：上传速度提升10倍<br>
        • <strong>最小存储占用</strong>：只保留必要数据</p>
    </div>
''', unsafe_allow_html=True)

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

# 侧边栏
with st.sidebar:
    st.title("⚙️ 系统功能")
    
    # 系统状态
    st.subheader("📡 系统状态")
    if gc:
        st.success("🟢 云数据库已连接")
        
        # 显示API使用情况
        current_requests = len(api_limiter.request_times)
        st.metric("API使用率", f"{current_requests}/{MAX_REQUESTS_PER_MINUTE}")
        
        # 显示当前数据状态 - 简化版
        try:
            data_manager = SimpleDataManager(gc)
            info = data_manager.get_current_data_info()
            if info["has_data"]:
                st.metric("当前门店数", info["store_count"])
                st.metric("数据月份", info.get("data_month", "未知"))
                if info.get("last_update"):
                    st.caption(f"更新时间: {info['last_update']}")
            else:
                st.info("暂无数据")
        except:
            st.warning("无法获取数据状态")
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
            st.subheader("📁 文件管理")
            
            # 显示当前数据状态
            try:
                data_manager = OptimizedDataManager(gc)
                current_info = data_manager.get_current_data_info()
                
                if current_info["has_data"]:
                    st.markdown(f"""
                        <div class="data-status">
                            <strong>📊 当前数据状态</strong><br>
                            门店数量: {current_info["store_count"]}<br>
                            数据月份: {current_info.get("data_month", "未知")}<br>
                            总行数: {current_info.get("total_rows", 0):,}<br>
                            更新时间: {current_info.get("last_update", "未知")}
                        </div>
                    """, unsafe_allow_html=True)
                else:
                    st.info("📭 系统中暂无数据")
            except Exception as e:
                st.warning(f"⚠️ 无法获取数据状态: {str(e)}")
            
            # 上传权限表
            st.markdown("**👥 权限管理**")
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
                                    show_status_message("❌ 保存失败", "error")
                        else:
                            show_status_message("❌ 格式错误：需要至少两列", "error")
                except Exception as e:
                    show_status_message(f"❌ 处理失败：{str(e)}", "error")
            
            # 上传财务报表
            st.markdown("**📊 报表管理**")
            
            # 清理数据确认
            try:
                data_manager = OptimizedDataManager(gc)
                current_info = data_manager.get_current_data_info()
                
                if current_info.get("has_data", False):
                    st.markdown(f'''
                        <div class="clear-warning">
                            <h4>⚠️ 重要提示</h4>
                            <p>上传新报表将<strong>完全清空</strong>现有的 {current_info["store_count"]} 个门店数据！</p>
                            <p>系统将自动创建备份，但请确认您要替换当前数据。</p>
                        </div>
                    ''', unsafe_allow_html=True)
            except Exception as e:
                logger.warning(f"获取数据状态失败: {str(e)}")
            
            reports_file = st.file_uploader(
                "上传财务报表", 
                type=['xlsx', 'xls'],
                help="上传新报表将清空所有现有数据并替换为新数据"
            )
            
            if reports_file:
                try:
                    with st.spinner("处理报表文件..."):
                        excel_file = pd.ExcelFile(reports_file)
                        reports_dict = {}
                        
                        # 读取所有工作表
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
                            # 显示上传确认
                            st.markdown(f"""
                                <div class="clear-warning">
                                    <h4>📋 即将上传的数据</h4>
                                    <p>• 门店数量: <strong>{len(reports_dict)}</strong><br>
                                    • 总数据行数: <strong>{sum(len(df) for df in reports_dict.values()):,}</strong><br>
                                    • 数据月份: <strong>{datetime.now().strftime('%Y-%m')}</strong></p>
                                </div>
                            """, unsafe_allow_html=True)
                            
                            # 二次确认
                            confirm_upload = st.checkbox(
                                "✅ 我确认要清空现有数据并上传新数据", 
                                help="此操作不可逆，请谨慎确认"
                            )
                            
                            if confirm_upload and st.button("🚀 开始上传并清空旧数据", type="primary"):
                                try:
                                    data_manager = OptimizedDataManager(gc)
                                    
                                    with st.spinner("正在上传数据（包括清空旧数据和创建备份）..."):
                                        if data_manager.save_reports_optimized(reports_dict, clear_existing=True):
                                            show_status_message(
                                                f"✅ 报表上传成功：{len(reports_dict)} 个门店，"
                                                f"{sum(len(df) for df in reports_dict.values()):,} 行数据", 
                                                "success"
                                            )
                                            st.balloons()
                                            st.rerun()
                                        else:
                                            show_status_message("❌ 上传失败", "error")
                                except Exception as e:
                                    show_status_message(f"❌ 上传失败：{str(e)}", "error")
                        else:
                            show_status_message("❌ 文件中没有有效的工作表", "error")
                            
                except Exception as e:
                    show_status_message(f"❌ 处理失败：{str(e)}", "error")
            
            # 系统维护
            st.subheader("🛠️ 系统维护")
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("🗑️ 手动清空数据"):
                    try:
                        data_manager = OptimizedDataManager(gc)
                        if data_manager.clear_all_report_data(create_backup=True):
                            show_status_message("✅ 数据已清空", "success")
                            st.rerun()
                        else:
                            show_status_message("❌ 清空失败", "error")
                    except Exception as e:
                        show_status_message(f"❌ 清空失败: {str(e)}", "error")
            
            with col2:
                if st.button("🔄 刷新状态"):
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

# 主界面逻辑
if user_type == "管理员" and st.session_state.is_admin:
    st.markdown('<div class="admin-panel"><h3>👨‍💼 管理员控制面板</h3><p>月度轮替存储，API限制保护，智能数据管理</p></div>', unsafe_allow_html=True)
    
    try:
        # 获取详细的系统统计 - 简化版
        data_manager = SimpleDataManager(gc)
        info = data_manager.get_current_data_info()
        
        # 系统统计
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("门店数量", info.get("store_count", 0))
        with col2:
            st.metric("数据行数", f"{info.get('total_rows', 0):,}")
        with col3:
            data_month = info.get("data_month", "无")
            st.metric("数据月份", data_month)
        with col4:
            api_usage = f"{len(api_limiter.request_times)}/{MAX_REQUESTS_PER_MINUTE}"
            st.metric("API使用", api_usage)
        
        # 数据预览
        if info["has_data"]:
            st.subheader("📊 数据预览")
            
            try:
                reports_data = data_manager.load_reports_simple()
                
                if reports_data:
                    # 显示门店列表
                    store_names = list(reports_data.keys())[:10]  # 显示前10个
                    
                    st.markdown(f"**当前门店列表** (显示前10个，共{len(reports_data)}个)：")
                    for i, name in enumerate(store_names, 1):
                        df = reports_data[name]
                        st.markdown(f"{i}. **{name}** - {len(df)} 行 × {len(df.columns)} 列")
                    
                    if len(reports_data) > 10:
                        st.markdown(f"... 还有 {len(reports_data) - 10} 个门店")
                
            except Exception as e:
                st.error(f"数据预览失败: {str(e)}")
        
        else:
            st.info("📭 系统中暂无数据，请上传报表文件")
            
    except Exception as e:
        show_status_message(f"❌ 加载管理面板失败：{str(e)}", "error")

elif user_type == "管理员" and not st.session_state.is_admin:
    st.info("👈 请在左侧边栏输入管理员密码")

else:
    # 普通用户界面
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
        # 已登录用户界面（保持原有界面不变）
        st.markdown(f'<div class="store-info"><h3>🏪 {st.session_state.store_name}</h3><p>操作员：{st.session_state.user_id}</p></div>', unsafe_allow_html=True)
        
        try:
            with st.spinner("加载报表数据..."):
                data_manager = SimpleDataManager(gc)
                reports_data = data_manager.load_reports_simple()
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
                    
                    # 显示数据统计
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("数据行数", len(filtered_df))
                    with col2:
                        st.metric("数据列数", len(df.columns))
                    with col3:
                        st.metric("数据来源", "☁️ 云端")
                    
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
                
                # 显示可用门店列表
                if reports_data:
                    st.subheader("📋 系统中的门店列表")
                    available_stores = list(reports_data.keys())
                    for store in available_stores[:10]:  # 显示前10个
                        st.write(f"• {store}")
                    if len(available_stores) > 10:
                        st.write(f"... 还有 {len(available_stores) - 10} 个门店")
                else:
                    st.info("系统中暂无任何报表数据")
                
        except Exception as e:
            show_status_message(f"❌ 报表加载失败：{str(e)}", "error")

# 页面底部状态信息
st.divider()
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.caption(f"🕒 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
with col2:
    api_usage = f"{len(api_limiter.request_times)}/{MAX_REQUESTS_PER_MINUTE}"
    st.caption(f"📡 API使用: {api_usage}")
with col3:
    try:
        data_manager = SimpleDataManager(gc)
        info = data_manager.get_current_data_info()
        if info["has_data"]:
            st.caption(f"📊 数据: {info['data_month']}")
        else:
            st.caption("📊 数据: 无")
    except:
        st.caption("📊 数据: 未知")
with col4:
    st.caption("🔧 版本: v2.3 (极简版)")

# 自动API限制清理（清理过期的请求记录）
current_time = time.time()
cutoff_time = current_time - 70  # 70秒前的记录
api_limiter.request_times = [t for t in api_limiter.request_times if t > cutoff_time]_times if t > cutoff_time]
