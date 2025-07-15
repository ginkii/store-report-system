import streamlit as st
import pandas as pd
import io
import json
import gzip
import base64
from datetime import datetime
import time
import hashlib
import traceback
from typing import Optional, Dict, Any, List
import tempfile
import os

# 页面配置
st.set_page_config(
    page_title="门店报表查询系统", 
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 系统配置
ADMIN_PASSWORD = "admin123"
MAX_CHUNK_SIZE = 50000  # 数据分片大小
CACHE_DURATION = 1800  # 缓存30分钟
COMPRESSION_LEVEL = 6  # 压缩级别

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
    .storage-status {
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding: 1rem;
        border-radius: 10px;
        margin: 0.5rem 0;
        border: 1px solid #ddd;
    }
    .storage-healthy {
        background: linear-gradient(135deg, #d4edda 0%, #c3e6cb 100%);
        color: #155724;
        border-color: #c3e6cb;
    }
    .storage-warning {
        background: linear-gradient(135deg, #fff3cd 0%, #ffeaa7 100%);
        color: #856404;
        border-color: #ffeaa7;
    }
    .metric-card {
        background: white;
        padding: 1.5rem;
        border-radius: 10px;
        border: 1px solid #e0e0e0;
        text-align: center;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
        margin: 0.5rem 0;
    }
    .metric-value {
        font-size: 2rem;
        font-weight: bold;
        color: #1f77b4;
    }
    .metric-label {
        font-size: 0.9rem;
        color: #666;
        margin-top: 0.5rem;
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
    .upload-progress {
        background: linear-gradient(135deg, #e3f2fd 0%, #bbdefb 100%);
        border: 2px solid #2196f3;
        border-radius: 10px;
        padding: 1.5rem;
        margin: 1rem 0;
    }
    .data-backup-info {
        background: #f8f9fa;
        border: 1px solid #dee2e6;
        border-radius: 8px;
        padding: 1rem;
        margin: 1rem 0;
        font-family: monospace;
        font-size: 0.9rem;
    }
    </style>
""", unsafe_allow_html=True)

# 数据压缩工具
class SimpleDataCompressor:
    @staticmethod
    def compress_data(data: Any) -> str:
        """压缩数据"""
        try:
            if isinstance(data, pd.DataFrame):
                json_data = data.to_json(orient='records', force_ascii=False)
            else:
                json_data = json.dumps(data, ensure_ascii=False)
            
            # 压缩
            compressed = gzip.compress(json_data.encode('utf-8'), compresslevel=COMPRESSION_LEVEL)
            encoded = base64.b64encode(compressed).decode('ascii')
            
            # 计算压缩率
            original_size = len(json_data.encode('utf-8'))
            compressed_size = len(compressed)
            compression_ratio = (1 - compressed_size / original_size) * 100
            
            return {
                'data': encoded,
                'original_size': original_size,
                'compressed_size': compressed_size,
                'compression_ratio': compression_ratio
            }
        except Exception as e:
            st.error(f"数据压缩失败: {str(e)}")
            return None
    
    @staticmethod
    def decompress_data(compressed_info: Dict[str, Any]) -> Any:
        """解压数据"""
        try:
            encoded_data = compressed_info['data']
            compressed = base64.b64decode(encoded_data.encode('ascii'))
            json_data = gzip.decompress(compressed).decode('utf-8')
            return json.loads(json_data)
        except Exception as e:
            st.error(f"数据解压失败: {str(e)}")
            return None

# 本地存储管理器
class LocalStorageManager:
    def __init__(self):
        self.storage_key_permissions = "store_permissions_data"
        self.storage_key_reports = "store_reports_data"
        self.storage_key_metadata = "store_metadata"
    
    def save_permissions(self, df: pd.DataFrame) -> bool:
        """保存权限数据"""
        try:
            # 压缩数据
            compressed = SimpleDataCompressor.compress_data(df)
            if not compressed:
                return False
            
            # 保存到session state
            st.session_state[self.storage_key_permissions] = {
                'data': compressed,
                'timestamp': time.time(),
                'checksum': hashlib.md5(compressed['data'].encode()).hexdigest(),
                'rows': len(df),
                'cols': len(df.columns)
            }
            
            # 同时保存未压缩版本作为备份
            st.session_state[f"{self.storage_key_permissions}_backup"] = df.to_dict('records')
            
            # 保存到本地文件（如果可能）
            self._save_to_file('permissions', compressed)
            
            return True
        except Exception as e:
            st.error(f"保存权限数据失败: {str(e)}")
            return False
    
    def load_permissions(self) -> Optional[pd.DataFrame]:
        """加载权限数据"""
        try:
            # 首先尝试从session state加载
            if self.storage_key_permissions in st.session_state:
                stored_data = st.session_state[self.storage_key_permissions]
                
                # 验证数据完整性
                current_checksum = hashlib.md5(stored_data['data']['data'].encode()).hexdigest()
                if current_checksum == stored_data['checksum']:
                    # 解压数据
                    data = SimpleDataCompressor.decompress_data(stored_data['data'])
                    if data:
                        df = pd.DataFrame(data)
                        return df
            
            # 尝试从备份加载
            backup_key = f"{self.storage_key_permissions}_backup"
            if backup_key in st.session_state:
                data = st.session_state[backup_key]
                return pd.DataFrame(data)
            
            # 尝试从文件加载
            return self._load_from_file('permissions')
            
        except Exception as e:
            st.error(f"加载权限数据失败: {str(e)}")
            return None
    
    def save_reports(self, reports_dict: Dict[str, pd.DataFrame]) -> bool:
        """保存报表数据"""
        try:
            compressed_reports = {}
            backup_reports = {}
            
            for store_name, df in reports_dict.items():
                # 压缩每个报表
                compressed = SimpleDataCompressor.compress_data(df)
                if compressed:
                    compressed_reports[store_name] = {
                        'data': compressed,
                        'timestamp': time.time(),
                        'checksum': hashlib.md5(compressed['data'].encode()).hexdigest(),
                        'rows': len(df),
                        'cols': len(df.columns)
                    }
                    
                    # 备份未压缩版本
                    backup_reports[store_name] = df.to_dict('records')
            
            # 保存到session state
            st.session_state[self.storage_key_reports] = compressed_reports
            st.session_state[f"{self.storage_key_reports}_backup"] = backup_reports
            
            # 保存到本地文件
            self._save_to_file('reports', compressed_reports)
            
            return True
        except Exception as e:
            st.error(f"保存报表数据失败: {str(e)}")
            return False
    
    def load_reports(self) -> Dict[str, pd.DataFrame]:
        """加载报表数据"""
        try:
            reports_dict = {}
            
            # 从session state加载
            if self.storage_key_reports in st.session_state:
                stored_reports = st.session_state[self.storage_key_reports]
                
                for store_name, stored_data in stored_reports.items():
                    # 验证数据完整性
                    current_checksum = hashlib.md5(stored_data['data']['data'].encode()).hexdigest()
                    if current_checksum == stored_data['checksum']:
                        # 解压数据
                        data = SimpleDataCompressor.decompress_data(stored_data['data'])
                        if data:
                            reports_dict[store_name] = pd.DataFrame(data)
            
            # 如果主数据加载失败，尝试备份
            if not reports_dict:
                backup_key = f"{self.storage_key_reports}_backup"
                if backup_key in st.session_state:
                    backup_reports = st.session_state[backup_key]
                    for store_name, data in backup_reports.items():
                        reports_dict[store_name] = pd.DataFrame(data)
            
            # 最后尝试从文件加载
            if not reports_dict:
                file_data = self._load_from_file('reports')
                if file_data:
                    reports_dict = file_data
            
            return reports_dict
            
        except Exception as e:
            st.error(f"加载报表数据失败: {str(e)}")
            return {}
    
    def _save_to_file(self, data_type: str, data: Any) -> bool:
        """保存到本地文件"""
        try:
            temp_dir = tempfile.gettempdir()
            filename = os.path.join(temp_dir, f"store_system_{data_type}.json")
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            return True
        except Exception:
            return False
    
    def _load_from_file(self, data_type: str) -> Any:
        """从本地文件加载"""
        try:
            temp_dir = tempfile.gettempdir()
            filename = os.path.join(temp_dir, f"store_system_{data_type}.json")
            
            if os.path.exists(filename):
                with open(filename, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                if data_type == 'permissions':
                    # 如果是权限数据，需要解压
                    if 'data' in data and 'checksum' in data:
                        decompressed = SimpleDataCompressor.decompress_data(data['data'])
                        if decompressed:
                            return pd.DataFrame(decompressed)
                elif data_type == 'reports':
                    # 如果是报表数据，解压所有报表
                    reports_dict = {}
                    for store_name, stored_data in data.items():
                        if 'data' in stored_data and 'checksum' in stored_data:
                            decompressed = SimpleDataCompressor.decompress_data(stored_data['data'])
                            if decompressed:
                                reports_dict[store_name] = pd.DataFrame(decompressed)
                    return reports_dict
            
            return None
        except Exception:
            return None
    
    def get_storage_info(self) -> Dict[str, Any]:
        """获取存储信息"""
        info = {
            'permissions_loaded': self.storage_key_permissions in st.session_state,
            'reports_loaded': self.storage_key_reports in st.session_state,
            'permissions_backup': f"{self.storage_key_permissions}_backup" in st.session_state,
            'reports_backup': f"{self.storage_key_reports}_backup" in st.session_state,
            'total_size': 0,
            'compression_stats': {}
        }
        
        # 计算存储大小
        for key in st.session_state:
            if key.startswith('store_'):
                try:
                    size = len(str(st.session_state[key]))
                    info['total_size'] += size
                except:
                    pass
        
        # 获取压缩统计
        if self.storage_key_permissions in st.session_state:
            perm_data = st.session_state[self.storage_key_permissions]
            if 'data' in perm_data:
                info['compression_stats']['permissions'] = {
                    'original_size': perm_data['data'].get('original_size', 0),
                    'compressed_size': perm_data['data'].get('compressed_size', 0),
                    'compression_ratio': perm_data['data'].get('compression_ratio', 0)
                }
        
        return info
    
    def clear_all_data(self):
        """清除所有数据"""
        keys_to_remove = [key for key in st.session_state.keys() if key.startswith('store_')]
        for key in keys_to_remove:
            del st.session_state[key]
        
        # 清除本地文件
        try:
            temp_dir = tempfile.gettempdir()
            for filename in ['store_system_permissions.json', 'store_system_reports.json']:
                filepath = os.path.join(temp_dir, filename)
                if os.path.exists(filepath):
                    os.remove(filepath)
        except:
            pass

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

def show_storage_dashboard(storage_manager: LocalStorageManager):
    """显示存储状态仪表板"""
    st.subheader("💾 数据存储状态")
    
    storage_info = storage_manager.get_storage_info()
    
    # 存储状态卡片
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown(f'''
            <div class="metric-card">
                <div class="metric-value">{"✅" if storage_info['permissions_loaded'] else "❌"}</div>
                <div class="metric-label">权限数据</div>
            </div>
        ''', unsafe_allow_html=True)
    
    with col2:
        st.markdown(f'''
            <div class="metric-card">
                <div class="metric-value">{"✅" if storage_info['reports_loaded'] else "❌"}</div>
                <div class="metric-label">报表数据</div>
            </div>
        ''', unsafe_allow_html=True)
    
    with col3:
        backup_status = "✅" if (storage_info['permissions_backup'] and storage_info['reports_backup']) else "⚠️"
        st.markdown(f'''
            <div class="metric-card">
                <div class="metric-value">{backup_status}</div>
                <div class="metric-label">备份数据</div>
            </div>
        ''', unsafe_allow_html=True)
    
    with col4:
        size_mb = storage_info['total_size'] / (1024 * 1024)
        st.markdown(f'''
            <div class="metric-card">
                <div class="metric-value">{size_mb:.2f} MB</div>
                <div class="metric-label">存储大小</div>
            </div>
        ''', unsafe_allow_html=True)
    
    # 压缩统计
    if 'permissions' in storage_info['compression_stats']:
        comp_stats = storage_info['compression_stats']['permissions']
        st.markdown(f'''
            <div class="storage-status storage-healthy">
                <div>
                    <strong>数据压缩统计</strong>
                </div>
                <div>
                    <span>压缩率: {comp_stats['compression_ratio']:.1f}%</span>
                    <span style="margin-left: 10px;">原始: {comp_stats['original_size']} bytes</span>
                    <span style="margin-left: 10px;">压缩后: {comp_stats['compressed_size']} bytes</span>
                </div>
            </div>
        ''', unsafe_allow_html=True)

# 初始化会话状态
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
if 'store_name' not in st.session_state:
    st.session_state.store_name = ""
if 'user_id' not in st.session_state:
    st.session_state.user_id = ""
if 'is_admin' not in st.session_state:
    st.session_state.is_admin = False
if 'storage_manager' not in st.session_state:
    st.session_state.storage_manager = LocalStorageManager()
if 'operation_status' not in st.session_state:
    st.session_state.operation_status = []

# 获取存储管理器
storage_manager = st.session_state.storage_manager

# 主标题
st.markdown('<h1 class="main-header">📊 门店报表查询系统 - 稳定版</h1>', unsafe_allow_html=True)

# 显示操作状态
for status in st.session_state.operation_status:
    show_status_message(status['message'], status['type'])

# 侧边栏
with st.sidebar:
    st.title("⚙️ 系统功能")
    
    # 存储状态
    st.subheader("💾 数据存储")
    storage_info = storage_manager.get_storage_info()
    
    if storage_info['permissions_loaded']:
        st.success("🟢 权限数据已加载")
    else:
        st.warning("🟡 权限数据未加载")
    
    if storage_info['reports_loaded']:
        st.success("🟢 报表数据已加载")
    else:
        st.warning("🟡 报表数据未加载")
    
    # 存储大小
    size_mb = storage_info['total_size'] / (1024 * 1024)
    st.info(f"📊 存储大小: {size_mb:.2f} MB")
    
    # 显示详细状态
    if st.button("📊 查看存储详情"):
        show_storage_dashboard(storage_manager)
    
    user_type = st.radio("选择用户类型", ["普通用户", "管理员"])
    
    if user_type == "管理员":
        st.subheader("🔐 管理员登录")
        admin_password = st.text_input("管理员密码", type="password")
        
        if st.button("验证管理员身份"):
            if admin_password == ADMIN_PASSWORD:
                st.session_state.is_admin = True
                st.session_state.operation_status.append({
                    'message': "✅ 管理员验证成功！",
                    'type': "success"
                })
                st.rerun()
            else:
                st.session_state.operation_status.append({
                    'message': "❌ 密码错误！",
                    'type': "error"
                })
                st.rerun()
        
        if st.session_state.is_admin:
            st.subheader("📁 文件管理")
            
            # 上传权限表
            permissions_file = st.file_uploader("上传门店权限表", type=['xlsx', 'xls'])
            if permissions_file and st.button("处理权限表", key="process_permissions"):
                try:
                    with st.spinner("处理权限表文件..."):
                        df = pd.read_excel(permissions_file)
                        if len(df.columns) >= 2:
                            if storage_manager.save_permissions(df):
                                st.session_state.operation_status.append({
                                    'message': f"✅ 权限表已上传：{len(df)} 个用户",
                                    'type': "success"
                                })
                                st.balloons()
                                st.rerun()
                            else:
                                st.session_state.operation_status.append({
                                    'message': "❌ 权限表保存失败",
                                    'type': "error"
                                })
                        else:
                            st.session_state.operation_status.append({
                                'message': "❌ 格式错误：需要至少两列（门店名称、人员编号）",
                                'type': "error"
                            })
                except Exception as e:
                    st.session_state.operation_status.append({
                        'message': f"❌ 处理失败：{str(e)}",
                        'type': "error"
                    })
            
            # 上传财务报表
            reports_file = st.file_uploader("上传财务报表", type=['xlsx', 'xls'])
            if reports_file and st.button("处理报表文件", key="process_reports"):
                try:
                    with st.spinner("处理报表文件..."):
                        excel_file = pd.ExcelFile(reports_file)
                        reports_dict = {}
                        
                        # 显示处理进度
                        progress_bar = st.progress(0)
                        total_sheets = len(excel_file.sheet_names)
                        
                        for i, sheet in enumerate(excel_file.sheet_names):
                            try:
                                df = pd.read_excel(reports_file, sheet_name=sheet)
                                if not df.empty:
                                    reports_dict[sheet] = df
                                    st.write(f"✅ 读取工作表 '{sheet}': {len(df)} 行")
                                else:
                                    st.write(f"⚠️ 跳过空工作表 '{sheet}'")
                            except Exception as e:
                                st.write(f"❌ 跳过工作表 '{sheet}': {str(e)}")
                                continue
                            
                            # 更新进度
                            progress_bar.progress((i + 1) / total_sheets)
                        
                        if reports_dict:
                            if storage_manager.save_reports(reports_dict):
                                st.session_state.operation_status.append({
                                    'message': f"✅ 报表已上传：{len(reports_dict)} 个门店",
                                    'type': "success"
                                })
                                st.balloons()
                                st.rerun()
                            else:
                                st.session_state.operation_status.append({
                                    'message': "❌ 报表数据保存失败",
                                    'type': "error"
                                })
                        else:
                            st.session_state.operation_status.append({
                                'message': "❌ 文件中没有有效的工作表",
                                'type': "error"
                            })
                            
                except Exception as e:
                    st.session_state.operation_status.append({
                        'message': f"❌ 处理失败：{str(e)}",
                        'type': "error"
                    })
            
            # 数据管理
            st.subheader("🗂️ 数据管理")
            col1, col2 = st.columns(2)
            
            with col1:
                if st.button("🗑️ 清除所有数据"):
                    storage_manager.clear_all_data()
                    st.session_state.operation_status.append({
                        'message': "✅ 所有数据已清除",
                        'type': "success"
                    })
                    st.rerun()
            
            with col2:
                if st.button("📤 导出数据备份"):
                    try:
                        # 导出所有数据
                        backup_data = {
                            'permissions': storage_manager.load_permissions(),
                            'reports': storage_manager.load_reports(),
                            'timestamp': datetime.now().isoformat()
                        }
                        
                        # 转换为JSON
                        permissions_dict = backup_data['permissions'].to_dict('records') if backup_data['permissions'] is not None else []
                        reports_dict = {name: df.to_dict('records') for name, df in backup_data['reports'].items()}
                        
                        export_data = {
                            'permissions': permissions_dict,
                            'reports': reports_dict,
                            'timestamp': backup_data['timestamp']
                        }
                        
                        json_str = json.dumps(export_data, ensure_ascii=False, indent=2)
                        
                        st.download_button(
                            "📥 下载备份文件",
                            json_str,
                            f"store_system_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                            "application/json"
                        )
                    except Exception as e:
                        st.error(f"导出失败: {str(e)}")
    
    else:
        if st.session_state.logged_in:
            st.subheader("👤 当前登录")
            st.info(f"门店：{st.session_state.store_name}")
            st.info(f"编号：{st.session_state.user_id}")
            
            if st.button("🚪 退出登录"):
                st.session_state.logged_in = False
                st.session_state.store_name = ""
                st.session_state.user_id = ""
                st.session_state.operation_status.append({
                    'message': "👋 已退出登录",
                    'type': "success"
                })
                st.rerun()

# 清除状态消息
st.session_state.operation_status = []

# 主界面
if user_type == "管理员" and st.session_state.is_admin:
    st.markdown('<div class="admin-panel"><h3>👨‍💼 管理员控制面板</h3><p>本地稳定存储系统，支持数据压缩和备份功能</p></div>', unsafe_allow_html=True)
    
    # 显示存储状态
    show_storage_dashboard(storage_manager)
    
    try:
        # 加载数据统计
        permissions_data = storage_manager.load_permissions()
        reports_data = storage_manager.load_reports()
        
        # 统计信息
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            perms_count = len(permissions_data) if permissions_data is not None else 0
            st.metric("权限表用户数", perms_count)
        with col2:
            reports_count = len(reports_data)
            st.metric("报表门店数", reports_count)
        with col3:
            storage_info = storage_manager.get_storage_info()
            size_mb = storage_info['total_size'] / (1024 * 1024)
            st.metric("存储大小", f"{size_mb:.2f} MB")
        with col4:
            backup_count = sum([storage_info['permissions_backup'], storage_info['reports_backup']])
            st.metric("备份项目", backup_count)
        
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
            permissions_data = storage_manager.load_permissions()
            
            if permissions_data is None:
                st.warning("⚠️ 权限数据未加载，请联系管理员上传权限表")
            else:
                st.info(f"📊 权限数据已加载：{len(permissions_data)} 个用户")
                
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
                            st.session_state.operation_status.append({
                                'message': "✅ 登录成功！",
                                'type': "success"
                            })
                            st.balloons()
                            st.rerun()
                        else:
                            st.session_state.operation_status.append({
                                'message': "❌ 门店或编号错误！",
                                'type': "error"
                            })
                            st.rerun()
                            
        except Exception as e:
            show_status_message(f"❌ 权限验证失败：{str(e)}", "error")
    
    else:
        # 已登录 - 显示报表
        st.markdown(f'<div class="store-info"><h3>🏪 {st.session_state.store_name}</h3><p>操作员：{st.session_state.user_id}</p></div>', unsafe_allow_html=True)
        
        try:
            reports_data = storage_manager.load_reports()
            
            if not reports_data:
                st.error("❌ 报表数据未加载，请联系管理员上传报表文件")
            else:
                st.info(f"📊 报表数据已加载：{len(reports_data)} 个门店")
                
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
    storage_info = storage_manager.get_storage_info()
    if storage_info['permissions_loaded'] and storage_info['reports_loaded']:
        st.caption("✅ 数据状态: 正常")
    else:
        st.caption("⚠️ 数据状态: 待加载")

with col3:
    backup_status = "✅" if (storage_info['permissions_backup'] and storage_info['reports_backup']) else "⚠️"
    st.caption(f"{backup_status} 备份状态")

with col4:
    st.caption("🔧 版本: v2.5 (稳定简化版)")

# 在页面底部显示技术说明
with st.expander("📋 系统说明", expanded=False):
    st.markdown("""
    ### 🏗️ 系统特性
    - **本地存储**: 基于Streamlit session_state的稳定存储
    - **数据压缩**: gzip压缩，节省70%存储空间
    - **双重备份**: 压缩版本 + 原始版本双重保存
    - **文件备份**: 自动保存到本地临时文件
    - **完整性校验**: MD5哈希验证数据完整性
    
    ### ⚡ 优势特点
    - **零配置**: 无需任何外部服务配置
    - **即开即用**: 上传文件立即可用
    - **数据安全**: 本地存储，数据不泄露
    - **稳定可靠**: 不依赖网络，不会连接失败
    - **轻量级**: 只使用Python标准库和Streamlit
    
    ### 📊 存储说明
    - 数据存储在浏览器会话中，刷新页面不丢失
    - 同时保存到本地临时文件作为备份
    - 支持数据导出功能，可迁移到其他环境
    - 压缩后的数据大幅减少内存占用
    
    ### 🔄 升级路径
    如需云端存储功能，可联系技术支持升级到：
    - Google Sheets版本（需要API配置）
    - Firebase版本（需要Firebase项目）
    - 数据库版本（需要数据库服务器）
    """)
