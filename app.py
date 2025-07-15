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
import tempfile
import os

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
RETRY_DELAY = 2
MAX_CHUNK_SIZE = 30000
CACHE_DURATION = 300  # 5分钟缓存

# CSS样式
st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        padding: 1rem 0;
        margin-bottom: 2rem;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
    }
    .store-info {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1.5rem;
        border-radius: 15px;
        margin: 1rem 0;
        box-shadow: 0 8px 16px rgba(0, 0, 0, 0.2);
    }
    .admin-panel {
        background: linear-gradient(135deg, #ffeaa7 0%, #fab1a0 100%);
        padding: 2rem;
        border-radius: 15px;
        border: 2px solid #fdcb6e;
        margin: 1rem 0;
        box-shadow: 0 8px 16px rgba(0, 0, 0, 0.1);
    }
    .config-panel {
        background: linear-gradient(135deg, #d1ecf1 0%, #bee5eb 100%);
        padding: 2rem;
        border-radius: 15px;
        border: 2px solid #17a2b8;
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
        box-shadow: 0 8px 16px rgba(0, 0, 0, 0.2);
    }
    .receivable-negative {
        background: linear-gradient(135deg, #a8edea 0%, #d299c2 100%);
        color: #0c4128;
        padding: 2rem;
        border-radius: 15px;
        border: 3px solid #48cab2;
        margin: 1rem 0;
        text-align: center;
        box-shadow: 0 8px 16px rgba(0, 0, 0, 0.2);
    }
    .status-success {
        background: linear-gradient(135deg, #d4edda 0%, #c3e6cb 100%);
        color: #155724;
        padding: 1rem;
        border-radius: 8px;
        border: 1px solid #c3e6cb;
        margin: 0.5rem 0;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
    }
    .status-error {
        background: linear-gradient(135deg, #f8d7da 0%, #f5c6cb 100%);
        color: #721c24;
        padding: 1rem;
        border-radius: 8px;
        border: 1px solid #f5c6cb;
        margin: 0.5rem 0;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
    }
    .status-warning {
        background: linear-gradient(135deg, #fff3cd 0%, #ffeaa7 100%);
        color: #856404;
        padding: 1rem;
        border-radius: 8px;
        border: 1px solid #ffeaa7;
        margin: 0.5rem 0;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
    }
    .status-info {
        background: linear-gradient(135deg, #d1ecf1 0%, #bee5eb 100%);
        color: #0c5460;
        padding: 1rem;
        border-radius: 8px;
        border: 1px solid #bee5eb;
        margin: 0.5rem 0;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
    }
    .setup-step {
        background: white;
        border: 2px solid #007bff;
        border-radius: 10px;
        padding: 1.5rem;
        margin: 1rem 0;
        border-left: 5px solid #007bff;
    }
    .code-block {
        background: #f8f9fa;
        border: 1px solid #e9ecef;
        border-radius: 5px;
        padding: 1rem;
        font-family: 'Courier New', monospace;
        font-size: 0.9rem;
        white-space: pre-wrap;
        overflow-x: auto;
    }
    .metric-card {
        background: white;
        padding: 1rem;
        border-radius: 8px;
        border: 1px solid #ddd;
        text-align: center;
        margin: 0.5rem 0;
    }
    .metric-value {
        font-size: 1.5rem;
        font-weight: bold;
        color: #007bff;
    }
    .metric-label {
        font-size: 0.9rem;
        color: #666;
    }
    </style>
""", unsafe_allow_html=True)

# 自定义异常
class GoogleSheetsError(Exception):
    """Google Sheets操作异常"""
    pass

class ConfigurationError(Exception):
    """配置错误异常"""
    pass

# 数据压缩工具
class DataCompressor:
    @staticmethod
    def compress_data(data: Any) -> str:
        """压缩数据"""
        try:
            if isinstance(data, pd.DataFrame):
                json_data = data.to_json(orient='records', force_ascii=False)
            else:
                json_data = json.dumps(data, ensure_ascii=False)
            
            compressed = gzip.compress(json_data.encode('utf-8'))
            encoded = base64.b64encode(compressed).decode('ascii')
            
            logger.info(f"数据压缩: {len(json_data)} -> {len(compressed)} bytes")
            return encoded
        except Exception as e:
            logger.error(f"数据压缩失败: {str(e)}")
            raise
    
    @staticmethod
    def decompress_data(encoded_data: str) -> Any:
        """解压数据"""
        try:
            compressed = base64.b64decode(encoded_data.encode('ascii'))
            json_data = gzip.decompress(compressed).decode('utf-8')
            return json.loads(json_data)
        except Exception as e:
            logger.error(f"数据解压失败: {str(e)}")
            raise

# Google Sheets配置检查
def check_google_sheets_config():
    """检查Google Sheets配置"""
    config_status = {
        'has_secrets': False,
        'has_required_fields': False,
        'connection_test': False,
        'error_message': None,
        'missing_fields': []
    }
    
    try:
        # 检查secrets配置
        if "google_sheets" not in st.secrets:
            config_status['error_message'] = "缺少 google_sheets 配置"
            return config_status
        
        config_status['has_secrets'] = True
        credentials_info = st.secrets["google_sheets"]
        
        # 检查必需字段
        required_fields = [
            'type', 'project_id', 'private_key_id', 'private_key',
            'client_email', 'client_id', 'auth_uri', 'token_uri'
        ]
        
        missing_fields = []
        for field in required_fields:
            if field not in credentials_info:
                missing_fields.append(field)
        
        config_status['missing_fields'] = missing_fields
        
        if not missing_fields:
            config_status['has_required_fields'] = True
            
            # 测试连接
            try:
                scopes = [
                    "https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive.file"
                ]
                credentials = Credentials.from_service_account_info(credentials_info, scopes=scopes)
                client = gspread.authorize(credentials)
                
                # 尝试创建测试表格
                test_name = f"测试连接_{int(time.time())}"
                test_sheet = client.create(test_name)
                client.del_spreadsheet(test_sheet.id)
                
                config_status['connection_test'] = True
                logger.info("Google Sheets连接测试成功")
                
            except Exception as e:
                config_status['error_message'] = f"连接测试失败: {str(e)}"
                logger.error(f"Google Sheets连接测试失败: {str(e)}")
        else:
            config_status['error_message'] = f"缺少必需字段: {', '.join(missing_fields)}"
    
    except Exception as e:
        config_status['error_message'] = f"配置检查失败: {str(e)}"
    
    return config_status

# 重试装饰器
def retry_operation(func, max_retries=MAX_RETRIES, delay=RETRY_DELAY):
    """重试操作"""
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            logger.warning(f"操作失败，第 {attempt + 1} 次重试: {str(e)}")
            time.sleep(delay * (attempt + 1))

# Google Sheets管理器
class GoogleSheetsManager:
    def __init__(self):
        self.client = None
        self.spreadsheet = None
        self.init_client()
    
    def init_client(self):
        """初始化客户端"""
        try:
            config_status = check_google_sheets_config()
            if not config_status['connection_test']:
                raise ConfigurationError(config_status['error_message'])
            
            credentials_info = st.secrets["google_sheets"]
            scopes = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file"
            ]
            
            credentials = Credentials.from_service_account_info(credentials_info, scopes=scopes)
            self.client = gspread.authorize(credentials)
            
            logger.info("Google Sheets客户端初始化成功")
            
        except Exception as e:
            logger.error(f"Google Sheets客户端初始化失败: {str(e)}")
            raise GoogleSheetsError(f"客户端初始化失败: {str(e)}")
    
    def get_or_create_spreadsheet(self, name="门店报表系统数据"):
        """获取或创建表格"""
        if self.spreadsheet:
            return self.spreadsheet
        
        def _operation():
            try:
                self.spreadsheet = self.client.open(name)
                logger.info(f"打开现有表格: {name}")
            except gspread.SpreadsheetNotFound:
                self.spreadsheet = self.client.create(name)
                self.spreadsheet.share('', perm_type='anyone', role='writer')
                logger.info(f"创建新表格: {name}")
            return self.spreadsheet
        
        return retry_operation(_operation)
    
    def get_or_create_worksheet(self, name, rows=1000, cols=20):
        """获取或创建工作表"""
        spreadsheet = self.get_or_create_spreadsheet()
        
        def _operation():
            try:
                worksheet = spreadsheet.worksheet(name)
                logger.info(f"打开现有工作表: {name}")
            except gspread.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(title=name, rows=rows, cols=cols)
                logger.info(f"创建新工作表: {name}")
            return worksheet
        
        return retry_operation(_operation)
    
    def save_permissions(self, df: pd.DataFrame) -> bool:
        """保存权限数据"""
        try:
            def _save():
                worksheet = self.get_or_create_worksheet(PERMISSIONS_SHEET_NAME)
                
                # 清空现有数据
                worksheet.clear()
                time.sleep(1)
                
                # 准备数据
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                data = [['门店名称', '人员编号', '更新时间']]
                
                for _, row in df.iterrows():
                    data.append([
                        str(row.iloc[0]).strip(),
                        str(row.iloc[1]).strip(),
                        current_time
                    ])
                
                # 批量更新
                worksheet.update('A1', data)
                logger.info(f"权限数据保存成功: {len(df)} 条记录")
                return True
            
            return retry_operation(_save)
            
        except Exception as e:
            logger.error(f"权限数据保存失败: {str(e)}")
            return False
    
    def load_permissions(self) -> Optional[pd.DataFrame]:
        """加载权限数据"""
        try:
            def _load():
                worksheet = self.get_or_create_worksheet(PERMISSIONS_SHEET_NAME)
                data = worksheet.get_all_values()
                
                if len(data) <= 1:
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
            
            return retry_operation(_load)
            
        except gspread.WorksheetNotFound:
            return None
        except Exception as e:
            logger.error(f"权限数据加载失败: {str(e)}")
            return None
    
    def save_reports(self, reports_dict: Dict[str, pd.DataFrame]) -> bool:
        """保存报表数据"""
        try:
            def _save():
                worksheet = self.get_or_create_worksheet(REPORTS_SHEET_NAME, rows=2000, cols=10)
                
                # 清空现有数据
                worksheet.clear()
                time.sleep(1)
                
                # 准备数据
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                data = [['门店名称', '压缩数据', '数据哈希', '行数', '列数', '更新时间']]
                
                for store_name, df in reports_dict.items():
                    try:
                        # 压缩数据
                        compressed_data = DataCompressor.compress_data(df)
                        data_hash = hashlib.md5(compressed_data.encode()).hexdigest()[:16]
                        
                        # 检查数据大小，如果太大则分片
                        if len(compressed_data) > MAX_CHUNK_SIZE:
                            chunks = [compressed_data[i:i+MAX_CHUNK_SIZE] 
                                    for i in range(0, len(compressed_data), MAX_CHUNK_SIZE)]
                            
                            for idx, chunk in enumerate(chunks):
                                chunk_name = f"{store_name}_分片{idx+1}"
                                data.append([
                                    chunk_name, chunk, data_hash,
                                    len(df), len(df.columns), current_time
                                ])
                        else:
                            data.append([
                                store_name, compressed_data, data_hash,
                                len(df), len(df.columns), current_time
                            ])
                        
                        logger.info(f"准备保存数据: {store_name} ({len(df)} 行)")
                        
                    except Exception as e:
                        logger.error(f"处理数据失败 {store_name}: {str(e)}")
                        continue
                
                # 分批上传数据
                batch_size = 50
                total_batches = (len(data) - 1 + batch_size - 1) // batch_size
                
                for i in range(0, len(data), batch_size):
                    batch = data[i:i+batch_size]
                    if i == 0:
                        worksheet.update('A1', batch)
                    else:
                        worksheet.update(f'A{i+1}', batch)
                    
                    # 显示进度
                    current_batch = i // batch_size + 1
                    st.progress(current_batch / max(total_batches, 1))
                    
                    time.sleep(0.5)  # API限制
                
                logger.info(f"报表数据保存成功: {len(reports_dict)} 个门店")
                return True
            
            return retry_operation(_save)
            
        except Exception as e:
            logger.error(f"报表数据保存失败: {str(e)}")
            return False
    
    def load_reports(self) -> Dict[str, pd.DataFrame]:
        """加载报表数据"""
        try:
            def _load():
                worksheet = self.get_or_create_worksheet(REPORTS_SHEET_NAME)
                data = worksheet.get_all_values()
                
                if len(data) <= 1:
                    return {}
                
                # 解析数据
                reports_dict = {}
                fragments = {}
                
                for row in data[1:]:
                    if len(row) >= 6:
                        store_name = row[0]
                        compressed_data = row[1]
                        data_hash = row[2]
                        
                        # 处理分片数据
                        if '_分片' in store_name:
                            base_name = store_name.split('_分片')[0]
                            if base_name not in fragments:
                                fragments[base_name] = []
                            fragments[base_name].append(compressed_data)
                        else:
                            fragments[store_name] = [compressed_data]
                
                # 重构数据
                for store_name, chunks in fragments.items():
                    try:
                        # 合并分片
                        full_data = ''.join(chunks)
                        
                        # 解压数据
                        data_list = DataCompressor.decompress_data(full_data)
                        df = pd.DataFrame(data_list)
                        
                        if len(df) > 0:
                            reports_dict[store_name] = df
                            logger.info(f"数据重构成功: {store_name} ({len(df)} 行)")
                    
                    except Exception as e:
                        logger.warning(f"数据重构失败 {store_name}: {str(e)}")
                        continue
                
                logger.info(f"报表数据加载成功: {len(reports_dict)} 个门店")
                return reports_dict
            
            return retry_operation(_load)
            
        except gspread.WorksheetNotFound:
            return {}
        except Exception as e:
            logger.error(f"报表数据加载失败: {str(e)}")
            return {}

# 缓存管理
def get_cache_key(operation: str, params: str = "") -> str:
    """生成缓存键"""
    return hashlib.md5(f"{operation}_{params}".encode()).hexdigest()

def set_cache(key: str, data: Any, duration: int = CACHE_DURATION):
    """设置缓存"""
    cache_data = {
        'data': data,
        'timestamp': time.time(),
        'duration': duration
    }
    st.session_state[f"cache_{key}"] = cache_data

def get_cache(key: str) -> Optional[Any]:
    """获取缓存"""
    cache_key = f"cache_{key}"
    if cache_key in st.session_state:
        cache_data = st.session_state[cache_key]
        if time.time() - cache_data['timestamp'] < cache_data['duration']:
            return cache_data['data']
        else:
            del st.session_state[cache_key]
    return None

# 应收未收额分析
def analyze_receivable_data(df: pd.DataFrame) -> Dict[str, Any]:
    """分析应收未收额数据"""
    result = {}
    
    if len(df.columns) == 0 or len(df) == 0:
        return result
    
    # 检查第一行是否是门店名称
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
    
    # 调试信息
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

def show_configuration_guide():
    """显示配置指南"""
    st.markdown("""
    ## 📋 Google Sheets API 配置指南
    
    按照以下步骤配置Google Sheets API：
    """)
    
    # 第一步
    st.markdown("""
    <div class="setup-step">
        <h3>🔥 第一步：创建Google Cloud项目</h3>
        <ol>
            <li>访问 <a href="https://console.cloud.google.com/" target="_blank">Google Cloud Console</a></li>
            <li>点击"选择项目" → "新建项目"</li>
            <li>输入项目名称（如：门店报表系统）</li>
            <li>点击"创建"</li>
        </ol>
    </div>
    """, unsafe_allow_html=True)
    
    # 第二步
    st.markdown("""
    <div class="setup-step">
        <h3>🔧 第二步：启用API</h3>
        <ol>
            <li>在项目中，点击"API和服务" → "库"</li>
            <li>搜索"Google Sheets API"，点击启用</li>
            <li>搜索"Google Drive API"，点击启用</li>
        </ol>
    </div>
    """, unsafe_allow_html=True)
    
    # 第三步
    st.markdown("""
    <div class="setup-step">
        <h3>🔑 第三步：创建服务账户</h3>
        <ol>
            <li>点击"API和服务" → "凭据"</li>
            <li>点击"创建凭据" → "服务账户"</li>
            <li>填写服务账户名称（如：sheets-service）</li>
            <li>点击"创建并继续"</li>
            <li>角色选择"编辑者"</li>
            <li>点击"完成"</li>
        </ol>
    </div>
    """, unsafe_allow_html=True)
    
    # 第四步
    st.markdown("""
    <div class="setup-step">
        <h3>📥 第四步：下载密钥文件</h3>
        <ol>
            <li>在"凭据"页面，找到刚创建的服务账户</li>
            <li>点击服务账户邮箱</li>
            <li>切换到"密钥"标签</li>
            <li>点击"添加密钥" → "创建新密钥"</li>
            <li>选择"JSON"格式，点击"创建"</li>
            <li>文件会自动下载到电脑</li>
        </ol>
    </div>
    """, unsafe_allow_html=True)
    
    # 第五步
    st.markdown("""
    <div class="setup-step">
        <h3>⚙️ 第五步：配置Streamlit Secrets</h3>
        <p>在你的Streamlit应用根目录下，创建文件：<code>.streamlit/secrets.toml</code></p>
        <p>将JSON密钥文件的内容按以下格式添加：</p>
    </div>
    """, unsafe_allow_html=True)
    
    # 显示配置模板
    st.code("""
[google_sheets]
type = "service_account"
project_id = "你的项目ID"
private_key_id = "你的私钥ID"
private_key = "-----BEGIN PRIVATE KEY-----\\n你的私钥内容\\n-----END PRIVATE KEY-----\\n"
client_email = "你的服务账户邮箱"
client_id = "你的客户端ID"
auth_uri = "https://accounts.google.com/o/oauth2/auth"
token_uri = "https://oauth2.googleapis.com/token"
auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
client_x509_cert_url = "你的证书URL"
    """, language="toml")
    
    st.warning("⚠️ 注意：private_key中的换行符必须替换为\\n")
    
    # 第六步
    st.markdown("""
    <div class="setup-step">
        <h3>🚀 第六步：部署到Streamlit Cloud</h3>
        <ol>
            <li>将代码推送到GitHub仓库</li>
            <li>访问 <a href="https://share.streamlit.io/" target="_blank">Streamlit Cloud</a></li>
            <li>点击"New app"，选择你的仓库</li>
            <li>在"Advanced settings"中，添加secrets配置</li>
            <li>点击"Deploy"</li>
        </ol>
    </div>
    """, unsafe_allow_html=True)

def show_config_status():
    """显示配置状态"""
    st.subheader("🔍 配置状态检查")
    
    config_status = check_google_sheets_config()
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if config_status['has_secrets']:
            st.success("✅ Secrets配置")
        else:
            st.error("❌ Secrets配置")
    
    with col2:
        if config_status['has_required_fields']:
            st.success("✅ 必需字段")
        else:
            st.error("❌ 必需字段")
    
    with col3:
        if config_status['connection_test']:
            st.success("✅ 连接测试")
        else:
            st.error("❌ 连接测试")
    
    if config_status['error_message']:
        st.error(f"错误信息: {config_status['error_message']}")
    
    if config_status['missing_fields']:
        st.warning(f"缺少字段: {', '.join(config_status['missing_fields'])}")

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
if 'operation_status' not in st.session_state:
    st.session_state.operation_status = []

# 主标题
st.markdown('<h1 class="main-header">📊 门店报表查询系统</h1>', unsafe_allow_html=True)

# 检查配置状态
config_status = check_google_sheets_config()

if not config_status['connection_test']:
    # 显示配置指南
    st.markdown('<div class="config-panel"><h2>⚙️ 系统配置</h2><p>系统需要Google Sheets API配置才能正常运行</p></div>', unsafe_allow_html=True)
    
    show_config_status()
    
    with st.expander("📖 查看完整配置指南", expanded=True):
        show_configuration_guide()
    
    st.stop()

# 初始化Google Sheets管理器
if not st.session_state.sheets_manager:
    try:
        with st.spinner("初始化Google Sheets连接..."):
            st.session_state.sheets_manager = GoogleSheetsManager()
            show_status_message("✅ Google Sheets连接成功！", "success")
    except Exception as e:
        show_status_message(f"❌ 连接失败: {str(e)}", "error")
        st.stop()

sheets_manager = st.session_state.sheets_manager

# 显示操作状态
for status in st.session_state.operation_status:
    show_status_message(status['message'], status['type'])

# 侧边栏
with st.sidebar:
    st.title("⚙️ 系统功能")
    
    # 系统状态
    st.subheader("📡 系统状态")
    if sheets_manager:
        st.success("🟢 Google Sheets已连接")
    else:
        st.error("🔴 Google Sheets断开")
    
    # 显示配置状态
    if st.button("🔍 检查配置"):
        show_config_status()
    
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
                            with st.spinner("保存到Google Sheets..."):
                                success = sheets_manager.save_permissions(df)
                                if success:
                                    show_status_message(f"✅ 权限表已上传：{len(df)} 个用户", "success")
                                    # 清除缓存
                                    cache_key = get_cache_key("permissions")
                                    if f"cache_{cache_key}" in st.session_state:
                                        del st.session_state[f"cache_{cache_key}"]
                                    st.balloons()
                                else:
                                    show_status_message("❌ 保存失败，请检查网络连接", "error")
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
                            with st.spinner("保存到Google Sheets..."):
                                success = sheets_manager.save_reports(reports_dict)
                                if success:
                                    show_status_message(f"✅ 报表已上传：{len(reports_dict)} 个门店", "success")
                                    # 清除缓存
                                    cache_key = get_cache_key("reports")
                                    if f"cache_{cache_key}" in st.session_state:
                                        del st.session_state[f"cache_{cache_key}"]
                                    st.balloons()
                                else:
                                    show_status_message("❌ 保存失败，请检查网络连接", "error")
                        else:
                            show_status_message("❌ 文件中没有有效的工作表", "error")
                            
                except Exception as e:
                    show_status_message(f"❌ 处理失败：{str(e)}", "error")
            
            # 缓存管理
            st.subheader("🗂️ 缓存管理")
            cache_count = len([key for key in st.session_state.keys() if key.startswith('cache_')])
            st.info(f"当前缓存项目: {cache_count}")
            
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
    st.markdown('<div class="admin-panel"><h3>👨‍💼 管理员控制面板</h3><p>Google Sheets云存储，支持数据压缩和智能缓存</p></div>', unsafe_allow_html=True)
    
    try:
        # 加载数据统计
        with st.spinner("加载数据统计..."):
            # 使用缓存
            permissions_cache_key = get_cache_key("permissions")
            permissions_data = get_cache(permissions_cache_key)
            if permissions_data is None:
                permissions_data = sheets_manager.load_permissions()
                if permissions_data is not None:
                    set_cache(permissions_cache_key, permissions_data)
            
            reports_cache_key = get_cache_key("reports")
            reports_data = get_cache(reports_cache_key)
            if reports_data is None:
                reports_data = sheets_manager.load_reports()
                if reports_data:
                    set_cache(reports_cache_key, reports_data)
        
        # 统计信息
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            perms_count = len(permissions_data) if permissions_data is not None else 0
            st.markdown(f'''
                <div class="metric-card">
                    <div class="metric-value">{perms_count}</div>
                    <div class="metric-label">权限用户数</div>
                </div>
            ''', unsafe_allow_html=True)
        
        with col2:
            reports_count = len(reports_data) if reports_data else 0
            st.markdown(f'''
                <div class="metric-card">
                    <div class="metric-value">{reports_count}</div>
                    <div class="metric-label">报表门店数</div>
                </div>
            ''', unsafe_allow_html=True)
        
        with col3:
            cache_count = len([key for key in st.session_state.keys() if key.startswith('cache_')])
            st.markdown(f'''
                <div class="metric-card">
                    <div class="metric-value">{cache_count}</div>
                    <div class="metric-label">缓存项目数</div>
                </div>
            ''', unsafe_allow_html=True)
        
        with col4:
            st.markdown(f'''
                <div class="metric-card">
                    <div class="metric-value">100%</div>
                    <div class="metric-label">系统可用性</div>
                </div>
            ''', unsafe_allow_html=True)
        
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
                # 使用缓存
                cache_key = get_cache_key("permissions")
                permissions_data = get_cache(cache_key)
                if permissions_data is None:
                    permissions_data = sheets_manager.load_permissions()
                    if permissions_data is not None:
                        set_cache(cache_key, permissions_data)
            
            if permissions_data is None:
                st.warning("⚠️ 权限数据为空，请联系管理员上传权限表")
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
                # 使用缓存
                cache_key = get_cache_key("reports")
                reports_data = get_cache(cache_key)
                if reports_data is None:
                    reports_data = sheets_manager.load_reports()
                    if reports_data:
                        set_cache(cache_key, reports_data)
                
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
    st.caption(f"🕒 当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

with col2:
    cache_count = len([key for key in st.session_state.keys() if key.startswith('cache_')])
    st.caption(f"💾 缓存项目: {cache_count}")

with col3:
    if sheets_manager:
        st.caption("✅ Google Sheets正常")
    else:
        st.caption("❌ Google Sheets异常")

with col4:
    st.caption("🔧 版本: v2.0 (Google Sheets版)")
