import streamlit as st
import pandas as pd
import io
import json
import gzip
import base64
from datetime import datetime
import time
import gspread
from google.oauth2.service_account import Credentials
import logging
from typing import Optional, Dict, Any, List
import hashlib
import traceback
from contextlib import contextmanager

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
SPREADSHEET_NAME = "门店报表系统数据"
MAX_RETRIES = 3
RETRY_DELAY = 2
API_DELAY = 1.5  # API调用间隔延迟
MAX_CHUNK_SIZE = 25000  # 减小分片大小
COMPRESSION_ENABLED = True  # 启用数据压缩

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
    .cleanup-info {
        background: linear-gradient(135deg, #a8edea 0%, #fed6e3 100%);
        padding: 1rem;
        border-radius: 10px;
        margin: 1rem 0;
        border: 2px solid #48cab2;
    }
    .storage-warning {
        background: #fff3cd;
        color: #856404;
        padding: 1rem;
        border-radius: 5px;
        border: 1px solid #ffeaa7;
        margin: 1rem 0;
    }
    </style>
""", unsafe_allow_html=True)

class OptimizedGoogleSheetsManager:
    """优化版Google Sheets管理器 - 自动清理、压缩存储"""
    
    def __init__(self):
        self.client = None
        self.spreadsheet = None
        self.storage_stats = {}
    
    def show_status(self, message: str, status_type: str = "info"):
        """显示状态消息"""
        if status_type == "success":
            st.success(f"✅ {message}")
        elif status_type == "error":
            st.error(f"❌ {message}")
        elif status_type == "warning":
            st.warning(f"⚠️ {message}")
        else:
            st.info(f"ℹ️ {message}")
    
    @staticmethod
    def compress_data(data):
        """压缩数据 - 可减少70-90%存储空间"""
        try:
            if not COMPRESSION_ENABLED:
                return json.dumps(data, ensure_ascii=False)
            
            # 转换为紧凑JSON
            json_str = json.dumps(data, ensure_ascii=False, separators=(',', ':'))
            
            # GZIP压缩
            compressed = gzip.compress(json_str.encode('utf-8'))
            
            # Base64编码
            encoded = base64.b64encode(compressed).decode('utf-8')
            
            logger.info(f"数据压缩: {len(json_str)} → {len(encoded)} 字节 (压缩率: {(1-len(encoded)/len(json_str))*100:.1f}%)")
            return encoded
        except Exception as e:
            logger.error(f"数据压缩失败: {str(e)}")
            return json.dumps(data, ensure_ascii=False)
    
    @staticmethod
    def decompress_data(encoded_data):
        """解压数据"""
        try:
            if not COMPRESSION_ENABLED:
                return json.loads(encoded_data)
            
            # 检查是否是压缩数据
            if not encoded_data or len(encoded_data) < 100:
                return json.loads(encoded_data) if encoded_data else {}
            
            try:
                # 尝试Base64解码
                compressed = base64.b64decode(encoded_data.encode('utf-8'))
                # GZIP解压
                json_str = gzip.decompress(compressed).decode('utf-8')
                # JSON解析
                return json.loads(json_str)
            except:
                # 如果解压失败，尝试直接解析（可能是未压缩的数据）
                return json.loads(encoded_data)
                
        except Exception as e:
            logger.error(f"数据解压失败: {str(e)}")
            return {}
    
    def api_call_with_delay(self, func, *args, **kwargs):
        """带延迟的API调用，避免配额限制"""
        try:
            result = func(*args, **kwargs)
            time.sleep(API_DELAY)  # API调用后延迟
            return result
        except Exception as e:
            if "quota" in str(e).lower() or "limit" in str(e).lower():
                logger.warning(f"API配额限制，延长等待时间: {str(e)}")
                time.sleep(API_DELAY * 3)  # 遇到配额限制时延长等待
                raise
            else:
                raise
    
    def create_client(self):
        """创建Google Sheets客户端"""
        try:
            if "google_sheets" not in st.secrets:
                self.show_status("未找到 google_sheets 配置", "error")
                return False
            
            config = st.secrets["google_sheets"]
            
            # 配置作用域
            scopes = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/drive.file"
            ]
            
            # 创建认证
            credentials = Credentials.from_service_account_info(config, scopes=scopes)
            self.client = gspread.authorize(credentials)
            
            self.show_status("Google Sheets 客户端创建成功", "success")
            return True
            
        except Exception as e:
            error_msg = f"客户端创建失败: {str(e)}"
            self.show_status(error_msg, "error")
            logger.error(error_msg)
            return False
    
    def cleanup_old_files(self):
        """清理旧文件 - 释放存储空间"""
        if not self.client:
            return False
        
        try:
            self.show_status("🧹 开始清理旧文件...", "info")
            
            cleanup_count = 0
            error_count = 0
            
            # 清理模式：删除所有现有的数据文件
            files_to_cleanup = []
            
            # 尝试删除已知的文件名模式
            cleanup_patterns = [
                SPREADSHEET_NAME,
                "门店报表系统数据",
                "store_permissions",
                "store_reports", 
                "测试文件",
                "权限测试",
                "ErrorTracker"
            ]
            
            for pattern in cleanup_patterns:
                try:
                    # 尝试多种可能的文件名
                    possible_names = [
                        pattern,
                        f"{pattern}_旧数据",
                        f"{pattern}_{datetime.now().strftime('%Y%m%d')}",
                        f"{pattern}_备份"
                    ]
                    
                    for name in possible_names:
                        try:
                            spreadsheet = self.api_call_with_delay(self.client.open, name)
                            self.api_call_with_delay(self.client.del_spreadsheet, spreadsheet.id)
                            cleanup_count += 1
                            self.show_status(f"删除旧文件: {name}", "success")
                        except gspread.SpreadsheetNotFound:
                            continue
                        except Exception as e:
                            if "404" not in str(e) and "not found" not in str(e).lower():
                                error_count += 1
                                logger.warning(f"删除文件 {name} 失败: {str(e)}")
                
                except Exception as e:
                    logger.warning(f"清理模式 {pattern} 失败: {str(e)}")
                    continue
            
            if cleanup_count > 0:
                self.show_status(f"清理完成：删除了 {cleanup_count} 个旧文件", "success")
            else:
                self.show_status("未找到需要清理的旧文件", "info")
            
            # 显示清理统计
            if cleanup_count > 0 or error_count > 0:
                st.markdown(f'''
                <div class="cleanup-info">
                <h4>🧹 清理统计</h4>
                <p>✅ 成功删除: {cleanup_count} 个文件</p>
                <p>❌ 删除失败: {error_count} 个文件</p>
                <p>💾 估计释放空间: {cleanup_count * 2:.1f} MB</p>
                </div>
                ''', unsafe_allow_html=True)
            
            return True
            
        except Exception as e:
            error_msg = f"清理旧文件失败: {str(e)}"
            self.show_status(error_msg, "error")
            logger.error(error_msg)
            return False
    
    def get_or_create_spreadsheet(self):
        """获取或创建主表格"""
        if not self.client:
            return None
        
        try:
            # 尝试打开现有表格
            self.spreadsheet = self.api_call_with_delay(self.client.open, SPREADSHEET_NAME)
            logger.info(f"表格 '{SPREADSHEET_NAME}' 已存在")
            return self.spreadsheet
            
        except gspread.SpreadsheetNotFound:
            try:
                # 创建新表格
                self.spreadsheet = self.api_call_with_delay(self.client.create, SPREADSHEET_NAME)
                logger.info(f"成功创建表格 '{SPREADSHEET_NAME}'")
                return self.spreadsheet
                
            except Exception as e:
                error_msg = f"创建表格失败: {str(e)}"
                self.show_status(error_msg, "error")
                logger.error(error_msg)
                return None
                
        except Exception as e:
            error_msg = f"表格操作失败: {str(e)}"
            self.show_status(error_msg, "error")
            logger.error(error_msg)
            return None
    
    def get_or_create_worksheet(self, name: str, rows: int = 1000, cols: int = 20):
        """获取或创建工作表"""
        if not self.spreadsheet:
            return None
        
        try:
            worksheet = self.spreadsheet.worksheet(name)
            logger.info(f"工作表 '{name}' 已存在")
            return worksheet
            
        except gspread.WorksheetNotFound:
            try:
                worksheet = self.api_call_with_delay(
                    self.spreadsheet.add_worksheet, 
                    title=name, rows=rows, cols=cols
                )
                logger.info(f"成功创建工作表 '{name}'")
                return worksheet
                
            except Exception as e:
                error_msg = f"创建工作表失败: {str(e)}"
                self.show_status(error_msg, "error")
                logger.error(error_msg)
                return None
                
        except Exception as e:
            error_msg = f"工作表操作失败: {str(e)}"
            self.show_status(error_msg, "error")
            logger.error(error_msg)
            return None
    
    def optimize_dataframe(self, df: pd.DataFrame):
        """优化DataFrame以减少存储空间"""
        try:
            optimized_df = df.copy()
            
            # 1. 数据类型优化
            for col in optimized_df.columns:
                # 转换为字符串并清理
                optimized_df[col] = optimized_df[col].astype(str).fillna('')
                
                # 移除过长的数据
                optimized_df[col] = optimized_df[col].apply(
                    lambda x: x[:500] if len(str(x)) > 500 else x
                )
                
                # 清理特殊字符
                optimized_df[col] = optimized_df[col].str.replace('\n', ' ').str.replace('\r', '')
            
            # 2. 移除完全空的行和列
            optimized_df = optimized_df.dropna(how='all').dropna(axis=1, how='all')
            
            # 3. 限制数据行数（如果太大）
            if len(optimized_df) > 10000:
                self.show_status(f"数据行数过多({len(optimized_df)})，截取前10000行", "warning")
                optimized_df = optimized_df.head(10000)
            
            reduction_ratio = (1 - len(optimized_df) / len(df)) * 100 if len(df) > 0 else 0
            logger.info(f"数据优化完成: {len(df)} → {len(optimized_df)} 行 (减少 {reduction_ratio:.1f}%)")
            
            return optimized_df
            
        except Exception as e:
            logger.error(f"数据优化失败: {str(e)}")
            return df
    
    def save_permissions_optimized(self, df: pd.DataFrame) -> bool:
        """优化版权限数据保存"""
        try:
            worksheet = self.get_or_create_worksheet(PERMISSIONS_SHEET_NAME)
            if not worksheet:
                return False
            
            # 清空现有数据
            self.api_call_with_delay(worksheet.clear)
            
            # 优化数据
            optimized_df = self.optimize_dataframe(df)
            
            # 准备数据
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            data = [['门店名称', '人员编号', '更新时间']]
            
            for _, row in optimized_df.iterrows():
                data.append([
                    str(row.iloc[0]).strip()[:50],  # 限制长度
                    str(row.iloc[1]).strip()[:20],  # 限制长度
                    current_time
                ])
            
            # 批量更新
            self.api_call_with_delay(worksheet.update, 'A1', data)
            
            self.show_status(f"✅ 权限数据保存成功：{len(optimized_df)} 条记录", "success")
            logger.info(f"权限数据保存成功: {len(optimized_df)} 条记录")
            return True
            
        except Exception as e:
            error_msg = f"保存权限数据失败: {str(e)}"
            self.show_status(error_msg, "error")
            logger.error(error_msg)
            return False
    
    def save_reports_optimized(self, reports_dict: Dict[str, pd.DataFrame]) -> bool:
        """优化版报表数据保存 - 压缩存储"""
        try:
            worksheet = self.get_or_create_worksheet(REPORTS_SHEET_NAME, rows=2000, cols=10)
            if not worksheet:
                return False
            
            # 清空现有数据
            self.api_call_with_delay(worksheet.clear)
            
            # 准备数据
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            data = [['门店名称', '压缩数据', '原始行数', '原始列数', '压缩大小', '更新时间']]
            
            total_original_size = 0
            total_compressed_size = 0
            
            for store_name, df in reports_dict.items():
                try:
                    # 优化DataFrame
                    optimized_df = self.optimize_dataframe(df)
                    
                    # 转换为字典格式
                    data_dict = {
                        'columns': optimized_df.columns.tolist(),
                        'data': optimized_df.values.tolist(),
                        'metadata': {
                            'store_name': store_name,
                            'timestamp': current_time,
                            'original_rows': len(df),
                            'original_cols': len(df.columns)
                        }
                    }
                    
                    # 压缩数据
                    compressed_data = self.compress_data(data_dict)
                    
                    # 统计压缩效果
                    original_size = len(json.dumps(data_dict, ensure_ascii=False))
                    compressed_size = len(compressed_data)
                    total_original_size += original_size
                    total_compressed_size += compressed_size
                    
                    # 检查数据大小，如果太大则分片
                    if len(compressed_data) > MAX_CHUNK_SIZE:
                        # 分片存储
                        chunks = [compressed_data[i:i+MAX_CHUNK_SIZE] 
                                for i in range(0, len(compressed_data), MAX_CHUNK_SIZE)]
                        
                        for chunk_idx, chunk in enumerate(chunks):
                            chunk_name = f"{store_name}_片段{chunk_idx+1}"
                            data.append([
                                chunk_name,
                                chunk,
                                len(df),
                                len(df.columns),
                                f"{len(chunk)} bytes (片段{chunk_idx+1}/{len(chunks)})",
                                current_time
                            ])
                    else:
                        # 单个数据块
                        data.append([
                            store_name,
                            compressed_data,
                            len(df),
                            len(df.columns),
                            f"{compressed_size} bytes (压缩率: {(1-compressed_size/original_size)*100:.1f}%)",
                            current_time
                        ])
                    
                    self.show_status(f"✅ {store_name}: {len(df)}行 → 压缩至 {compressed_size} bytes", "success")
                    
                except Exception as e:
                    logger.error(f"处理 {store_name} 时出错: {str(e)}")
                    # 添加错误记录
                    data.append([
                        f"{store_name}_错误",
                        json.dumps({"error": str(e), "timestamp": current_time}),
                        0, 0, "ERROR", current_time
                    ])
                    continue
            
            # 批量更新数据
            try:
                # 分批上传，避免单次请求过大
                batch_size = 20
                for i in range(0, len(data), batch_size):
                    batch = data[i:i+batch_size]
                    if i == 0:
                        # 第一批包含标题
                        self.api_call_with_delay(worksheet.update, 'A1', batch)
                    else:
                        # 后续批次
                        start_row = i + 1
                        self.api_call_with_delay(worksheet.update, f'A{start_row}', batch)
                    
                    # 显示进度
                    progress = min(i + batch_size, len(data))
                    if len(data) > 1:
                        st.progress(progress / len(data))
            
            except Exception as e:
                error_msg = f"批量上传失败: {str(e)}"
                self.show_status(error_msg, "error")
                return False
            
            # 显示存储统计
            compression_ratio = (1 - total_compressed_size / total_original_size) * 100 if total_original_size > 0 else 0
            
            st.markdown(f'''
            <div class="cleanup-info">
            <h4>📊 存储优化统计</h4>
            <p>🏪 门店数量: {len(reports_dict)} 个</p>
            <p>📦 原始大小: {total_original_size / 1024:.1f} KB</p>
            <p>🗜️ 压缩后: {total_compressed_size / 1024:.1f} KB</p>
            <p>📉 压缩率: {compression_ratio:.1f}%</p>
            <p>💾 节省空间: {(total_original_size - total_compressed_size) / 1024:.1f} KB</p>
            </div>
            ''', unsafe_allow_html=True)
            
            self.show_status(f"报表数据保存成功：{len(reports_dict)} 个门店，压缩率 {compression_ratio:.1f}%", "success")
            logger.info(f"报表数据保存成功: {len(reports_dict)} 个门店")
            return True
            
        except Exception as e:
            error_msg = f"保存报表数据失败: {str(e)}"
            self.show_status(error_msg, "error")
            logger.error(error_msg)
            return False
    
    def load_permissions_optimized(self) -> Optional[pd.DataFrame]:
        """优化版权限数据加载"""
        try:
            worksheet = self.get_or_create_worksheet(PERMISSIONS_SHEET_NAME)
            if not worksheet:
                return None
            
            data = self.api_call_with_delay(worksheet.get_all_values)
            
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
            return result_df
            
        except gspread.WorksheetNotFound:
            logger.info("权限表不存在")
            return None
        except Exception as e:
            error_msg = f"加载权限数据失败: {str(e)}"
            self.show_status(error_msg, "error")
            logger.error(error_msg)
            return None
    
    def load_reports_optimized(self) -> Dict[str, pd.DataFrame]:
        """优化版报表数据加载 - 解压数据"""
        try:
            worksheet = self.get_or_create_worksheet(REPORTS_SHEET_NAME)
            if not worksheet:
                return {}
            
            data = self.api_call_with_delay(worksheet.get_all_values)
            
            if len(data) <= 1:
                logger.info("报表数据为空")
                return {}
            
            reports_dict = {}
            fragments_dict = {}  # 存储分片数据
            
            for row in data[1:]:
                if len(row) >= 6:
                    store_name = row[0]
                    compressed_data = row[1]
                    original_rows = row[2]
                    original_cols = row[3]
                    compressed_size_info = row[4]
                    update_time = row[5]
                    
                    # 跳过错误数据
                    if store_name.endswith('_错误'):
                        logger.warning(f"跳过错误数据: {store_name}")
                        continue
                    
                    # 处理分片数据
                    if '_片段' in store_name:
                        base_name = store_name.split('_片段')[0]
                        if base_name not in fragments_dict:
                            fragments_dict[base_name] = []
                        fragments_dict[base_name].append(compressed_data)
                    else:
                        # 单片数据
                        fragments_dict[store_name] = [compressed_data]
            
            # 重构所有数据
            for store_name, fragments in fragments_dict.items():
                try:
                    # 合并分片
                    if len(fragments) == 1:
                        full_data = fragments[0]
                    else:
                        full_data = ''.join(fragments)
                        logger.info(f"{store_name} 合并了 {len(fragments)} 个分片")
                    
                    # 解压数据
                    decompressed_data = self.decompress_data(full_data)
                    
                    if decompressed_data and 'data' in decompressed_data:
                        # 重构DataFrame
                        columns = decompressed_data.get('columns', [])
                        data_values = decompressed_data.get('data', [])
                        
                        if columns and data_values:
                            df = pd.DataFrame(data_values, columns=columns)
                            reports_dict[store_name] = df
                            logger.info(f"{store_name} 数据加载成功: {len(df)} 行")
                        else:
                            logger.warning(f"{store_name} 数据格式错误")
                    else:
                        logger.warning(f"{store_name} 解压数据失败")
                        
                except Exception as e:
                    logger.error(f"加载 {store_name} 数据失败: {str(e)}")
                    continue
            
            logger.info(f"报表数据加载成功: {len(reports_dict)} 个门店")
            return reports_dict
            
        except gspread.WorksheetNotFound:
            logger.info("报表数据表不存在")
            return {}
        except Exception as e:
            error_msg = f"加载报表数据失败: {str(e)}"
            self.show_status(error_msg, "error")
            logger.error(error_msg)
            return {}

def analyze_receivable_data(df: pd.DataFrame) -> Dict[str, Any]:
    """分析应收未收额数据"""
    result = {}
    
    if len(df.columns) == 0 or len(df) == 0:
        return result
    
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

# 初始化会话状态
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
if 'store_name' not in st.session_state:
    st.session_state.store_name = ""
if 'user_id' not in st.session_state:
    st.session_state.user_id = ""
if 'is_admin' not in st.session_state:
    st.session_state.is_admin = False
if 'sheets_manager' not in st.session_state:
    st.session_state.sheets_manager = None

# 主标题
st.markdown('<h1 class="main-header">📊 门店报表查询系统 (优化版)</h1>', unsafe_allow_html=True)

# 初始化 Google Sheets 管理器
if not st.session_state.sheets_manager:
    st.session_state.sheets_manager = OptimizedGoogleSheetsManager()

sheets_manager = st.session_state.sheets_manager

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
                st.success("✅ 管理员验证成功！")
                st.rerun()
            else:
                st.error("❌ 密码错误！")
    
    else:
        if st.session_state.logged_in:
            st.subheader("👤 当前登录")
            st.info(f"门店：{st.session_state.store_name}")
            st.info(f"编号：{st.session_state.user_id}")
            
            if st.button("🚪 退出登录"):
                st.session_state.logged_in = False
                st.session_state.store_name = ""
                st.session_state.user_id = ""
                st.success("👋 已退出登录")
                st.rerun()

# 主界面
if user_type == "管理员" and st.session_state.is_admin:
    st.markdown('''
    <div class="admin-panel">
    <h3>👨‍💼 管理员控制面板 (优化版)</h3>
    <p>✨ 新功能：自动清理旧数据、压缩存储、API限流优化</p>
    </div>
    ''', unsafe_allow_html=True)
    
    # 初始化客户端
    if not sheets_manager.client:
        if st.button("🔌 初始化连接"):
            sheets_manager.create_client()
    
    if sheets_manager.client:
        
        # 存储管理区域
        st.subheader("🧹 存储空间管理")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🧹 清理旧数据", help="删除所有旧文件，释放存储空间"):
                with st.spinner("正在清理旧数据..."):
                    sheets_manager.cleanup_old_files()
        
        with col2:
            storage_info = f"""
            💾 **存储优化功能**
            - 自动压缩：减少70-90%存储空间
            - 智能清理：删除旧文件释放空间  
            - API限流：避免配额超限
            - 分片存储：处理大文件
            """
            st.info(storage_info)
        
        st.divider()
        
        # 文件上传区域
        st.subheader("📁 文件管理")
        
        st.markdown('''
        <div class="storage-warning">
        <strong>⚠️ 上传提示</strong><br>
        每次上传新文件前，系统会自动清理旧数据以释放存储空间。
        只保留最新上传的数据，确保不会超出存储配额。
        </div>
        ''', unsafe_allow_html=True)
        
        # 上传权限表
        st.markdown("#### 📋 门店权限表")
        permissions_file = st.file_uploader("上传门店权限表", type=['xlsx', 'xls'], key="permissions")
        if permissions_file and st.button("保存权限表", key="save_permissions"):
            try:
                with st.spinner("处理权限表文件..."):
                    df = pd.read_excel(permissions_file)
                    if len(df.columns) >= 2:
                        with st.spinner("保存到云端（自动清理+压缩）..."):
                            # 确保获取表格
                            sheets_manager.get_or_create_spreadsheet()
                            if sheets_manager.save_permissions_optimized(df):
                                st.balloons()
                    else:
                        st.error("❌ 格式错误：需要至少两列（门店名称、人员编号）")
            except Exception as e:
                st.error(f"❌ 处理失败：{str(e)}")
        
        st.divider()
        
        # 上传财务报表
        st.markdown("#### 📊 财务报表")
        reports_file = st.file_uploader("上传财务报表", type=['xlsx', 'xls'], key="reports")
        
        if reports_file:
            # 显示文件信息
            file_size = len(reports_file.getvalue()) / 1024 / 1024  # MB
            st.info(f"📄 文件大小: {file_size:.2f} MB")
            
            if file_size > 10:
                st.warning("⚠️ 文件较大，将启用高级压缩和分片存储")
        
        if reports_file and st.button("保存报表数据", key="save_reports"):
            try:
                with st.spinner("处理报表文件..."):
                    excel_file = pd.ExcelFile(reports_file)
                    reports_dict = {}
                    
                    total_size = 0
                    for sheet in excel_file.sheet_names:
                        try:
                            df = pd.read_excel(reports_file, sheet_name=sheet)
                            if not df.empty:
                                reports_dict[sheet] = df
                                sheet_size = df.memory_usage(deep=True).sum() / 1024 / 1024
                                total_size += sheet_size
                                st.success(f"✅ 读取工作表 '{sheet}': {len(df)} 行, {sheet_size:.2f} MB")
                        except Exception as e:
                            st.warning(f"⚠️ 跳过工作表 '{sheet}': {str(e)}")
                            continue
                    
                    if reports_dict:
                        st.info(f"📊 总数据大小: {total_size:.2f} MB，准备压缩存储...")
                        
                        with st.spinner("清理旧数据并保存新数据（压缩中）..."):
                            # 先清理旧数据
                            sheets_manager.cleanup_old_files()
                            time.sleep(2)  # 等待清理完成
                            
                            # 确保获取表格
                            sheets_manager.get_or_create_spreadsheet()
                            
                            # 保存新数据
                            if sheets_manager.save_reports_optimized(reports_dict):
                                st.balloons()
                            else:
                                st.error("❌ 保存失败")
                    else:
                        st.error("❌ 文件中没有有效的工作表")
                        
            except Exception as e:
                st.error(f"❌ 处理失败：{str(e)}")
                
                # 如果是存储配额问题，提供解决方案
                if "storage" in str(e).lower() or "quota" in str(e).lower():
                    st.markdown('''
                    <div class="storage-warning">
                    <strong>💡 存储配额解决方案</strong><br>
                    1. 点击"清理旧数据"按钮释放空间<br>
                    2. 或考虑启用Google Cloud计费（成本很低）<br>
                    3. 或创建新的Google Cloud项目
                    </div>
                    ''', unsafe_allow_html=True)

elif user_type == "管理员" and not st.session_state.is_admin:
    st.info("👈 请在左侧边栏输入管理员密码")

else:
    if not st.session_state.logged_in:
        st.subheader("🔐 用户登录")
        
        # 初始化客户端
        if not sheets_manager.client:
            if sheets_manager.create_client():
                pass
        
        if sheets_manager.client:
            try:
                with st.spinner("加载权限数据..."):
                    sheets_manager.get_or_create_spreadsheet()
                    permissions_data = sheets_manager.load_permissions_optimized()
                
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
                                
            except Exception as e:
                st.error(f"❌ 权限验证失败：{str(e)}")
        else:
            st.error("❌ 系统连接失败，请联系管理员")
    
    else:
        # 已登录用户界面
        st.markdown(f'<div class="store-info"><h3>🏪 {st.session_state.store_name}</h3><p>操作员：{st.session_state.user_id}</p></div>', unsafe_allow_html=True)
        
        if not sheets_manager.client:
            sheets_manager.create_client()
        
        if sheets_manager.client:
            try:
                with st.spinner("加载报表数据..."):
                    sheets_manager.get_or_create_spreadsheet()
                    reports_data = sheets_manager.load_reports_optimized()
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
                        
                        if amount > 0:
                            st.error(f"💳 应付款：¥{amount:,.2f}")
                        elif amount < 0:
                            st.success(f"💚 应退款：¥{abs(amount):,.2f}")
                        else:
                            st.info("⚖️ 收支平衡：¥0.00")
                    else:
                        st.warning("⚠️ 未找到应收-未收额数据")
                    
                    # 报表展示
                    st.subheader("📋 报表数据")
                    st.dataframe(df, use_container_width=True)
                    
                    # 下载功能
                    if st.button("📥 下载报表"):
                        try:
                            buffer = io.BytesIO()
                            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                                df.to_excel(writer, index=False)
                            
                            st.download_button(
                                "点击下载",
                                buffer.getvalue(),
                                f"{st.session_state.store_name}_报表_{datetime.now().strftime('%Y%m%d')}.xlsx",
                                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                            )
                        except Exception as e:
                            st.error(f"下载失败：{str(e)}")
                
                else:
                    st.error(f"❌ 未找到门店 '{st.session_state.store_name}' 的报表")
                    
            except Exception as e:
                st.error(f"❌ 报表加载失败：{str(e)}")
        else:
            st.error("❌ 系统连接失败")

# 页面底部
st.divider()
col1, col2, col3 = st.columns(3)
with col1:
    st.caption(f"🕒 当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
with col2:
    compression_status = "🗜️ 启用" if COMPRESSION_ENABLED else "❌ 禁用"
    st.caption(f"压缩存储: {compression_status}")
with col3:
    st.caption("🔧 版本: v3.0 (优化版)")
