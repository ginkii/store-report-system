import streamlit as st
import pandas as pd
import io
import json
import hashlib
import gzip
from datetime import datetime, timedelta
import time
from qcloud_cos import CosConfig
from qcloud_cos import CosS3Client
from qcloud_cos.cos_exception import CosServiceError, CosClientError
import logging
from typing import Optional, Dict, Any, List, Tuple
import traceback
import threading
import re

# ===================== 页面配置 =====================
st.set_page_config(
    page_title="门店报表查询系统", 
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ===================== 系统配置 =====================
ADMIN_PASSWORD = st.secrets.get("system", {}).get("admin_password", "admin123")
MAX_STORAGE_GB = 40  # 40GB存储限制
API_RATE_LIMIT = 500  # 每小时API调用限制 (从300提高到500)
COMPRESSION_LEVEL = 6  # GZIP压缩等级
RETRY_ATTEMPTS = 3  # 重试次数
RETRY_DELAY = 1  # 重试延迟(秒)

# ===================== CSS样式 =====================
st.markdown("""
    <style>
    .main-header {
        font-size: 2.8rem;
        color: #1f77b4;
        text-align: center;
        padding: 1.5rem 0;
        margin-bottom: 2rem;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: 700;
    }
    .store-info {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 2rem;
        border-radius: 20px;
        margin: 1.5rem 0;
        box-shadow: 0 10px 20px rgba(0, 0, 0, 0.15);
        text-align: center;
    }
    .admin-panel {
        background: linear-gradient(135deg, #ffeaa7 0%, #fab1a0 100%);
        padding: 2rem;
        border-radius: 20px;
        border: 3px solid #fdcb6e;
        margin: 1.5rem 0;
        box-shadow: 0 8px 16px rgba(0, 0, 0, 0.1);
    }
    .system-status {
        background: linear-gradient(135deg, #00cec9 0%, #55a3ff 100%);
        padding: 1.5rem;
        border-radius: 15px;
        margin: 1rem 0;
        color: white;
        text-align: center;
    }
    .success-alert {
        background: linear-gradient(135deg, #84fab0 0%, #8fd3f4 100%);
        padding: 1.5rem;
        border-radius: 15px;
        margin: 1rem 0;
        color: #2d3436;
        border-left: 5px solid #00b894;
    }
    .warning-alert {
        background: linear-gradient(135deg, #fdcb6e 0%, #e17055 100%);
        padding: 1.5rem;
        border-radius: 15px;
        margin: 1rem 0;
        color: white;
        border-left: 5px solid #e84393;
    }
    .error-alert {
        background: linear-gradient(135deg, #fd79a8 0%, #e84393 100%);
        padding: 1.5rem;
        border-radius: 15px;
        margin: 1rem 0;
        color: white;
        border-left: 5px solid #d63031;
    }
    .info-alert {
        background: linear-gradient(135deg, #74b9ff 0%, #0984e3 100%);
        padding: 1.5rem;
        border-radius: 15px;
        margin: 1rem 0;
        color: white;
        border-left: 5px solid #0984e3;
    }
    .debug-info {
        background: #f8f9fa;
        border: 1px solid #dee2e6;
        border-radius: 8px;
        padding: 1rem;
        margin: 1rem 0;
        font-family: monospace;
        font-size: 0.9rem;
        color: #495057;
    }
    .metric-card {
        background: white;
        padding: 1.5rem;
        border-radius: 12px;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
        border: 1px solid #e0e6ed;
        text-align: center;
        margin: 0.5rem 0;
    }
    .metric-value {
        font-size: 2rem;
        font-weight: 700;
        color: #2d3436;
        margin-bottom: 0.5rem;
    }
    .metric-label {
        color: #636e72;
        font-size: 0.9rem;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    </style>
""", unsafe_allow_html=True)

# ===================== 日志配置 =====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ===================== 调试日志管理器 =====================
class DebugLogger:
    """调试日志管理器，用于追踪数据流"""
    
    def __init__(self):
        self.logs = []
        self.max_logs = 100
    
    def log(self, level: str, message: str, data: Dict = None):
        """记录调试日志"""
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'level': level,
            'message': message,
            'data': data or {}
        }
        self.logs.append(log_entry)
        
        # 保持日志数量限制
        if len(self.logs) > self.max_logs:
            self.logs = self.logs[-self.max_logs:]
        
        # 同时记录到标准日志
        if level == 'ERROR':
            logger.error(f"[DEBUG] {message}")
        elif level == 'WARNING':
            logger.warning(f"[DEBUG] {message}")
        else:
            logger.info(f"[DEBUG] {message}")
    
    def get_recent_logs(self, count: int = 10) -> List[Dict]:
        """获取最近的调试日志"""
        return self.logs[-count:] if self.logs else []
    
    def clear_logs(self):
        """清空日志"""
        self.logs = []

# 全局调试日志实例
debug_logger = DebugLogger()

# ===================== 工具函数 =====================
def safe_execute(func, *args, max_retries=RETRY_ATTEMPTS, delay=RETRY_DELAY, **kwargs):
    """安全执行函数，带重试机制"""
    for attempt in range(max_retries):
        try:
            result = func(*args, **kwargs)
            debug_logger.log('INFO', f'函数 {func.__name__} 执行成功', {'attempt': attempt + 1})
            return result
        except Exception as e:
            error_msg = f"函数 {func.__name__} 第 {attempt + 1} 次尝试失败: {str(e)}"
            if attempt == max_retries - 1:
                debug_logger.log('ERROR', error_msg, {'final_attempt': True})
                logger.error(error_msg)
                raise e
            else:
                debug_logger.log('WARNING', error_msg, {'retry_delay': delay})
                logger.warning(error_msg)
                time.sleep(delay)

def sanitize_filename(filename: str) -> str:
    """清理文件名，移除特殊字符"""
    # 移除或替换特殊字符
    original_filename = filename
    filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
    # 移除多余的空格和点
    filename = re.sub(r'\s+', '_', filename.strip())
    filename = filename.strip('.')
    
    debug_logger.log('INFO', '文件名清理', {
        'original': original_filename,
        'sanitized': filename
    })
    
    return filename

def normalize_store_name(store_name: str) -> str:
    """标准化门店名称，用于更好的匹配"""
    if not store_name:
        return ""
    
    # 移除前后空格
    normalized = store_name.strip()
    # 移除常见的门店后缀
    suffixes = ['店', '分店', '门店', '营业部', '专卖店']
    for suffix in suffixes:
        if normalized.endswith(suffix):
            normalized = normalized[:-len(suffix)].strip()
            break
    
    debug_logger.log('INFO', '门店名称标准化', {
        'original': store_name,
        'normalized': normalized
    })
    
    return normalized

def validate_excel_file(file_data: bytes) -> Tuple[bool, str]:
    """验证Excel文件是否有效"""
    try:
        excel_file = pd.ExcelFile(io.BytesIO(file_data))
        if len(excel_file.sheet_names) == 0:
            debug_logger.log('ERROR', 'Excel文件验证失败：无工作表')
            return False, "Excel文件没有工作表"
        
        debug_logger.log('INFO', 'Excel文件验证成功', {
            'sheets': excel_file.sheet_names,
            'sheet_count': len(excel_file.sheet_names)
        })
        return True, "文件验证通过"
    except Exception as e:
        debug_logger.log('ERROR', f'Excel文件验证失败：{str(e)}')
        return False, f"Excel文件格式错误: {str(e)}"

def clean_dataframe_value(value) -> str:
    """清理DataFrame中的值"""
    if pd.isna(value):
        return ""
    value_str = str(value).strip()
    if value_str.lower() in ['nan', 'none', 'null', '']:
        return ""
    return value_str

# ===================== 压缩管理器 =====================
class CompressionManager:
    """简化的数据压缩管理器"""
    
    @staticmethod
    def compress_data(data: bytes) -> bytes:
        """压缩数据"""
        try:
            compressed = gzip.compress(data, compresslevel=COMPRESSION_LEVEL)
            debug_logger.log('INFO', '数据压缩成功', {
                'original_size': len(data),
                'compressed_size': len(compressed),
                'compression_ratio': (1 - len(compressed) / len(data)) * 100
            })
            return compressed
        except Exception as e:
            debug_logger.log('ERROR', f'数据压缩失败: {str(e)}')
            logger.error(f"数据压缩失败: {str(e)}")
            return data  # 压缩失败时返回原数据
    
    @staticmethod
    def decompress_data(data: bytes) -> bytes:
        """解压数据，支持容错"""
        try:
            # 尝试解压
            decompressed = gzip.decompress(data)
            debug_logger.log('INFO', '数据解压成功', {
                'compressed_size': len(data),
                'decompressed_size': len(decompressed)
            })
            return decompressed
        except Exception as e:
            debug_logger.log('WARNING', f'数据解压失败，返回原数据: {str(e)}')
            logger.warning(f"数据解压失败，返回原数据: {str(e)}")
            return data  # 解压失败时返回原数据
    
    @staticmethod
    def compress_json(data: dict) -> bytes:
        """压缩JSON数据"""
        try:
            json_str = json.dumps(data, ensure_ascii=False, separators=(',', ':'))
            json_bytes = json_str.encode('utf-8')
            compressed = gzip.compress(json_bytes, compresslevel=COMPRESSION_LEVEL)
            
            debug_logger.log('INFO', 'JSON压缩成功', {
                'original_size': len(json_bytes),
                'compressed_size': len(compressed),
                'compression_ratio': (1 - len(compressed) / len(json_bytes)) * 100
            })
            return compressed
        except Exception as e:
            debug_logger.log('ERROR', f'JSON压缩失败: {str(e)}')
            logger.error(f"JSON压缩失败: {str(e)}")
            # 返回未压缩的JSON
            json_str = json.dumps(data, ensure_ascii=False)
            return json_str.encode('utf-8')
    
    @staticmethod
    def decompress_json(data: bytes) -> dict:
        """解压JSON数据，支持容错"""
        try:
            # 尝试解压
            decompressed = gzip.decompress(data)
            result = json.loads(decompressed.decode('utf-8'))
            debug_logger.log('INFO', 'JSON解压成功')
            return result
        except Exception:
            try:
                # 可能是未压缩的JSON
                result = json.loads(data.decode('utf-8'))
                debug_logger.log('INFO', 'JSON直接解析成功（未压缩）')
                return result
            except Exception as e:
                debug_logger.log('ERROR', f'JSON解压失败: {str(e)}')
                logger.error(f"JSON解压失败: {str(e)}")
                return {}

# ===================== API频率限制器 =====================
class SimpleRateLimiter:
    """简化的API频率限制器"""
    
    def __init__(self, max_calls_per_hour: int = API_RATE_LIMIT):
        self.max_calls = max_calls_per_hour
        self.calls = []
        self.lock = threading.Lock()
    
    def can_make_call(self) -> bool:
        """检查是否可以进行API调用"""
        with self.lock:
            now = datetime.now()
            # 清理一小时前的记录
            self.calls = [call_time for call_time in self.calls 
                         if now - call_time < timedelta(hours=1)]
            
            can_call = len(self.calls) < self.max_calls
            debug_logger.log('INFO', f'API调用检查', {
                'current_calls': len(self.calls),
                'max_calls': self.max_calls,
                'can_call': can_call
            })
            return can_call
    
    def record_call(self):
        """记录API调用"""
        with self.lock:
            self.calls.append(datetime.now())
            debug_logger.log('INFO', f'API调用记录', {
                'total_calls_in_hour': len(self.calls)
            })
    
    def get_remaining_calls(self) -> int:
        """获取剩余可调用次数"""
        with self.lock:
            now = datetime.now()
            self.calls = [call_time for call_time in self.calls 
                         if now - call_time < timedelta(hours=1)]
            return max(0, self.max_calls - len(self.calls))

# ===================== 腾讯云COS管理器 =====================
class TencentCOSManager:
    """简化可靠的腾讯云COS管理器"""
    
    def __init__(self):
        self.client = None
        self.bucket_name = None
        self.region = None
        self.rate_limiter = SimpleRateLimiter(API_RATE_LIMIT)
        self.compression = CompressionManager()
        self.initialize_from_secrets()
    
    def initialize_from_secrets(self):
        """初始化COS客户端"""
        try:
            if "tencent_cos" not in st.secrets:
                raise Exception("未找到腾讯云COS配置")
            
            config = st.secrets["tencent_cos"]
            secret_id = config.get("secret_id")
            secret_key = config.get("secret_key")
            self.region = config.get("region", "ap-beijing")
            self.bucket_name = config.get("bucket_name")
            
            if not all([secret_id, secret_key, self.bucket_name]):
                raise Exception("腾讯云COS配置不完整")
            
            cos_config = CosConfig(
                Region=self.region,
                SecretId=secret_id,
                SecretKey=secret_key
            )
            
            self.client = CosS3Client(cos_config)
            
            debug_logger.log('INFO', '腾讯云COS初始化成功', {
                'bucket': self.bucket_name,
                'region': self.region
            })
            logger.info(f"腾讯云COS初始化成功: {self.bucket_name}")
            
        except Exception as e:
            debug_logger.log('ERROR', f'腾讯云COS初始化失败: {str(e)}')
            logger.error(f"腾讯云COS初始化失败: {str(e)}")
            raise
    
    def _execute_with_limit_check(self, operation):
        """执行带频率限制检查的操作"""
        if not self.rate_limiter.can_make_call():
            remaining = self.rate_limiter.get_remaining_calls()
            error_msg = f"API调用频率超限，剩余: {remaining}/小时"
            debug_logger.log('ERROR', error_msg)
            raise Exception(error_msg)
        
        result = operation()
        self.rate_limiter.record_call()
        return result
    
    def upload_file(self, file_data: bytes, filename: str, compress: bool = True) -> Optional[str]:
        """上传文件到COS，优化文件名处理"""
        try:
            # 清理文件名
            original_filename = filename
            filename = sanitize_filename(filename)
            
            # 压缩处理 - 修复文件名逻辑
            upload_data = file_data
            final_filename = filename
            
            if compress:
                compressed_data = self.compression.compress_data(file_data)
                if len(compressed_data) < len(file_data):
                    upload_data = compressed_data
                    # 确保只添加一次.gz后缀
                    if not filename.endswith('.gz'):
                        final_filename = filename + '.gz'
                    else:
                        final_filename = filename
                    
                    compression_ratio = (1 - len(compressed_data) / len(file_data)) * 100
                    debug_logger.log('INFO', '文件压缩完成', {
                        'original_filename': original_filename,
                        'final_filename': final_filename,
                        'original_size': len(file_data),
                        'compressed_size': len(compressed_data),
                        'compression_ratio': compression_ratio
                    })
                    st.info(f"📦 压缩效果: {len(file_data)/1024:.1f}KB → {len(compressed_data)/1024:.1f}KB (节省 {compression_ratio:.1f}%)")
            
            # 上传操作
            def upload_operation():
                return self.client.put_object(
                    Bucket=self.bucket_name,
                    Body=upload_data,
                    Key=final_filename,
                    ContentType='application/octet-stream'
                )
            
            safe_execute(lambda: self._execute_with_limit_check(upload_operation))
            
            file_url = f"https://{self.bucket_name}.cos.{self.region}.myqcloud.com/{final_filename}"
            
            debug_logger.log('INFO', '文件上传成功', {
                'filename': final_filename,
                'file_url': file_url,
                'file_size': len(upload_data)
            })
            logger.info(f"文件上传成功: {final_filename}")
            return file_url
            
        except Exception as e:
            debug_logger.log('ERROR', f'文件上传失败: {str(e)}', {
                'filename': filename,
                'file_size': len(file_data)
            })
            logger.error(f"文件上传失败: {str(e)}")
            raise Exception(f"文件上传失败: {str(e)}")
    
    def download_file(self, filename: str, decompress: bool = True) -> Optional[bytes]:
        """从COS下载文件，增强调试信息"""
        try:
            debug_logger.log('INFO', '开始下载文件', {'filename': filename})
            
            def download_operation():
                response = self.client.get_object(
                    Bucket=self.bucket_name,
                    Key=filename
                )
                return response['Body'].read()
            
            file_data = safe_execute(lambda: self._execute_with_limit_check(download_operation))
            
            debug_logger.log('INFO', '文件下载成功', {
                'filename': filename,
                'downloaded_size': len(file_data),
                'is_compressed': filename.endswith('.gz')
            })
            
            # 解压处理
            if decompress and filename.endswith('.gz'):
                original_size = len(file_data)
                file_data = self.compression.decompress_data(file_data)
                debug_logger.log('INFO', '文件解压完成', {
                    'filename': filename,
                    'compressed_size': original_size,
                    'decompressed_size': len(file_data)
                })
            
            logger.info(f"文件下载成功: {filename}")
            return file_data
            
        except CosServiceError as e:
            if e.get_error_code() == 'NoSuchKey':
                debug_logger.log('WARNING', f'文件不存在: {filename}')
                logger.info(f"文件不存在: {filename}")
                return None
            
            debug_logger.log('ERROR', f'COS服务错误: {e.get_error_msg()}', {'filename': filename})
            logger.error(f"COS服务错误: {e.get_error_msg()}")
            return None
        except Exception as e:
            debug_logger.log('ERROR', f'文件下载失败: {str(e)}', {'filename': filename})
            logger.error(f"文件下载失败: {str(e)}")
            return None
    
    def delete_file(self, filename: str) -> bool:
        """删除COS文件"""
        try:
            def delete_operation():
                return self.client.delete_object(
                    Bucket=self.bucket_name,
                    Key=filename
                )
            
            safe_execute(lambda: self._execute_with_limit_check(delete_operation))
            debug_logger.log('INFO', '文件删除成功', {'filename': filename})
            logger.info(f"文件删除成功: {filename}")
            return True
            
        except Exception as e:
            debug_logger.log('ERROR', f'文件删除失败: {str(e)}', {'filename': filename})
            logger.error(f"文件删除失败: {str(e)}")
            return False
    
    def list_files(self, prefix: str = "") -> List[Dict]:
        """列出文件"""
        try:
            def list_operation():
                return self.client.list_objects(
                    Bucket=self.bucket_name,
                    Prefix=prefix,
                    MaxKeys=1000
                )
            
            response = safe_execute(lambda: self._execute_with_limit_check(list_operation))
            
            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    files.append({
                        'filename': obj['Key'],
                        'size': obj['Size'],
                        'last_modified': obj['LastModified']
                    })
            
            debug_logger.log('INFO', f'文件列表获取成功', {
                'prefix': prefix,
                'file_count': len(files)
            })
            return files
            
        except Exception as e:
            debug_logger.log('ERROR', f'列出文件失败: {str(e)}', {'prefix': prefix})
            logger.error(f"列出文件失败: {str(e)}")
            return []
    
    def file_exists(self, filename: str) -> bool:
        """检查文件是否存在"""
        try:
            def head_operation():
                return self.client.head_object(
                    Bucket=self.bucket_name,
                    Key=filename
                )
            
            safe_execute(lambda: self._execute_with_limit_check(head_operation))
            debug_logger.log('INFO', f'文件存在检查: {filename} 存在')
            return True
            
        except:
            debug_logger.log('INFO', f'文件存在检查: {filename} 不存在')
            return False
    
    def upload_json(self, data: dict, filename: str) -> bool:
        """上传JSON数据，修复文件名处理"""
        try:
            json_bytes = self.compression.compress_json(data)
            
            # 确保JSON文件有正确的.gz后缀
            if not filename.endswith('.gz'):
                filename = filename + '.gz'
            
            result = self.upload_file(json_bytes, filename, compress=False)  # 已经压缩了
            success = result is not None
            
            debug_logger.log('INFO' if success else 'ERROR', 
                           f'JSON上传{"成功" if success else "失败"}', {
                'filename': filename,
                'data_size': len(json_bytes)
            })
            
            return success
        except Exception as e:
            debug_logger.log('ERROR', f'JSON上传失败: {str(e)}', {'filename': filename})
            logger.error(f"JSON上传失败: {str(e)}")
            return False
    
    def download_json(self, filename: str) -> Optional[dict]:
        """下载JSON数据，支持多种文件名格式"""
        try:
            # 尝试多种文件名格式
            possible_filenames = [filename]
            if not filename.endswith('.gz'):
                possible_filenames.append(filename + '.gz')
            if filename.endswith('.gz'):
                possible_filenames.append(filename[:-3])  # 移除.gz
            
            for fname in possible_filenames:
                debug_logger.log('INFO', f'尝试下载JSON文件: {fname}')
                file_data = self.download_file(fname, decompress=False)
                if file_data:
                    result = self.compression.decompress_json(file_data)
                    debug_logger.log('INFO', f'JSON下载成功: {fname}', {
                        'data_keys': list(result.keys()) if isinstance(result, dict) else 'not_dict'
                    })
                    return result
            
            debug_logger.log('WARNING', f'JSON文件未找到', {
                'attempted_filenames': possible_filenames
            })
            return None
        except Exception as e:
            debug_logger.log('ERROR', f'JSON下载失败: {str(e)}', {'filename': filename})
            logger.error(f"JSON下载失败: {str(e)}")
            return None
    
    def get_storage_stats(self) -> Dict:
        """获取存储统计"""
        try:
            files = self.list_files()
            total_size = sum(f['size'] for f in files)
            
            report_files = [f for f in files if f['filename'].startswith('reports/')]
            system_files = [f for f in files if f['filename'].startswith('system/')]
            
            stats = {
                'total_files': len(files),
                'total_size_gb': total_size / (1024**3),
                'report_files': len(report_files),
                'system_files': len(system_files),
                'usage_percent': (total_size / (1024**3)) / MAX_STORAGE_GB * 100,
                'remaining_calls': self.rate_limiter.get_remaining_calls()
            }
            
            debug_logger.log('INFO', '存储统计获取成功', stats)
            return stats
        except Exception as e:
            debug_logger.log('ERROR', f'获取存储统计失败: {str(e)}')
            logger.error(f"获取存储统计失败: {str(e)}")
            return {
                'total_files': 0,
                'total_size_gb': 0,
                'report_files': 0,
                'system_files': 0,
                'usage_percent': 0,
                'remaining_calls': 0
            }

# ===================== 主系统类 =====================
class StoreReportSystem:
    """门店报表系统主类"""
    
    def __init__(self):
        self.cos_manager = TencentCOSManager()
        self.permissions_file = "system/permissions.json"
        self.metadata_file = "system/metadata.json"
        self.compression = CompressionManager()
        debug_logger.log('INFO', '门店报表系统初始化完成')
    
    # ============ 权限管理 ============
    def load_permissions(self) -> List[Dict]:
        """加载权限数据，增强调试信息"""
        try:
            debug_logger.log('INFO', '开始加载权限数据')
            
            # 尝试多种文件名
            possible_files = [self.permissions_file + '.gz', self.permissions_file]
            
            for filename in possible_files:
                debug_logger.log('INFO', f'尝试加载权限文件: {filename}')
                data = self.cos_manager.download_json(filename)
                if data and 'permissions' in data:
                    permissions = data['permissions']
                    debug_logger.log('INFO', f'权限数据加载成功', {
                        'filename': filename,
                        'permissions_count': len(permissions),
                        'sample_stores': [p.get('store_name', '') for p in permissions[:3]]
                    })
                    logger.info(f"成功加载权限数据: {len(permissions)} 条记录")
                    return permissions
            
            debug_logger.log('WARNING', '未找到权限文件')
            logger.warning("未找到权限文件")
            return []
            
        except Exception as e:
            debug_logger.log('ERROR', f'加载权限数据失败: {str(e)}')
            logger.error(f"加载权限数据失败: {str(e)}")
            return []
    
    def save_permissions(self, permissions_data: List[Dict]) -> bool:
        """保存权限数据"""
        try:
            data = {
                'permissions': permissions_data,
                'last_updated': datetime.now().isoformat(),
                'count': len(permissions_data)
            }
            
            # 保存为压缩文件
            filename = self.permissions_file
            success = self.cos_manager.upload_json(data, filename)
            
            if success:
                debug_logger.log('INFO', f'权限数据保存成功', {
                    'permissions_count': len(permissions_data),
                    'filename': filename + '.gz'
                })
                logger.info(f"权限数据保存成功: {len(permissions_data)} 条记录")
            else:
                debug_logger.log('ERROR', '权限数据保存失败')
            
            return success
            
        except Exception as e:
            debug_logger.log('ERROR', f'保存权限数据失败: {str(e)}')
            logger.error(f"保存权限数据失败: {str(e)}")
            return False
    
    def load_metadata(self) -> Dict:
        """加载元数据，增强调试信息"""
        try:
            debug_logger.log('INFO', '开始加载元数据')
            
            # 尝试多种文件名
            possible_files = [self.metadata_file + '.gz', self.metadata_file]
            
            for filename in possible_files:
                debug_logger.log('INFO', f'尝试加载元数据文件: {filename}')
                data = self.cos_manager.download_json(filename)
                if data:
                    reports = data.get('reports', [])
                    debug_logger.log('INFO', f'元数据加载成功', {
                        'filename': filename,
                        'reports_count': len(reports),
                        'sample_stores': [r.get('store_name', '') for r in reports[:3]]
                    })
                    logger.info(f"成功加载元数据: {len(reports)} 个报表")
                    return data
            
            debug_logger.log('INFO', '未找到元数据文件，创建新的')
            logger.info("未找到元数据文件，创建新的")
            return {'reports': []}
            
        except Exception as e:
            debug_logger.log('ERROR', f'加载元数据失败: {str(e)}')
            logger.error(f"加载元数据失败: {str(e)}")
            return {'reports': []}
    
    def save_metadata(self, metadata: Dict) -> bool:
        """保存元数据"""
        try:
            metadata['last_updated'] = datetime.now().isoformat()
            
            # 保存为压缩文件
            filename = self.metadata_file
            success = self.cos_manager.upload_json(metadata, filename)
            
            if success:
                reports_count = len(metadata.get('reports', []))
                debug_logger.log('INFO', f'元数据保存成功', {
                    'reports_count': reports_count,
                    'filename': filename + '.gz'
                })
                logger.info(f"元数据保存成功: {reports_count} 个报表")
            else:
                debug_logger.log('ERROR', '元数据保存失败')
            
            return success
            
        except Exception as e:
            debug_logger.log('ERROR', f'保存元数据失败: {str(e)}')
            logger.error(f"保存元数据失败: {str(e)}")
            return False
    
    # ============ 文件处理 ============
    def process_permissions_file(self, uploaded_file) -> bool:
        """处理权限文件"""
        try:
            debug_logger.log('INFO', '开始处理权限文件')
            
            # 读取Excel文件
            df = pd.read_excel(uploaded_file)
            
            if len(df.columns) < 2:
                st.error("❌ 权限文件格式错误：需要至少两列（门店名称、人员编号）")
                debug_logger.log('ERROR', '权限文件格式错误：列数不足', {'columns': len(df.columns)})
                return False
            
            # 处理数据
            permissions_data = []
            processed_count = 0
            skipped_count = 0
            
            for index, row in df.iterrows():
                store_name = clean_dataframe_value(row.iloc[0])
                user_id = clean_dataframe_value(row.iloc[1])
                
                if store_name and user_id:
                    permissions_data.append({
                        "store_name": store_name,
                        "user_id": user_id,
                        "created_at": datetime.now().isoformat()
                    })
                    processed_count += 1
                else:
                    skipped_count += 1
            
            debug_logger.log('INFO', '权限文件处理完成', {
                'processed_count': processed_count,
                'skipped_count': skipped_count,
                'total_rows': len(df)
            })
            
            if processed_count == 0:
                st.error("❌ 没有找到有效的权限数据")
                debug_logger.log('ERROR', '没有找到有效的权限数据')
                return False
            
            # 保存数据
            success = self.save_permissions(permissions_data)
            
            if success:
                st.markdown(f'''
                <div class="success-alert">
                <h4>✅ 权限数据上传成功</h4>
                <p><strong>有效记录</strong>: {processed_count} 条</p>
                <p><strong>跳过记录</strong>: {skipped_count} 条</p>
                <p><strong>状态</strong>: 数据已保存并立即生效</p>
                </div>
                ''', unsafe_allow_html=True)
                
                # 显示权限预览
                if len(permissions_data) > 0:
                    st.subheader("📋 权限记录预览")
                    preview_df = pd.DataFrame(permissions_data[:10])
                    st.dataframe(preview_df[['store_name', 'user_id']], use_container_width=True)
                
                return True
            else:
                st.error("❌ 权限数据保存失败")
                return False
                
        except Exception as e:
            st.error(f"❌ 处理权限文件失败：{str(e)}")
            debug_logger.log('ERROR', f'处理权限文件失败: {str(e)}')
            logger.error(f"处理权限文件失败: {str(e)}")
            return False
    
    def process_reports_file(self, uploaded_file) -> bool:
        """处理报表文件，修复文件名处理逻辑"""
        try:
            file_size_mb = len(uploaded_file.getvalue()) / 1024 / 1024
            debug_logger.log('INFO', '开始处理报表文件', {
                'file_size_mb': file_size_mb
            })
            st.info(f"📄 文件大小: {file_size_mb:.2f} MB")
            
            # 验证Excel文件
            is_valid, validation_msg = validate_excel_file(uploaded_file.getvalue())
            if not is_valid:
                st.error(f"❌ {validation_msg}")
                return False
            
            # 生成文件名
            timestamp = int(time.time())
            file_hash = hashlib.md5(uploaded_file.getvalue()).hexdigest()[:8]
            base_filename = f"reports/report_{timestamp}_{file_hash}.xlsx"
            
            debug_logger.log('INFO', '生成报表文件名', {
                'base_filename': base_filename,
                'timestamp': timestamp,
                'file_hash': file_hash
            })
            
            # 上传文件
            with st.spinner("正在上传文件..."):
                file_url = self.cos_manager.upload_file(
                    uploaded_file.getvalue(), 
                    base_filename, 
                    compress=True
                )
                
                if not file_url:
                    st.error("❌ 文件上传失败")
                    debug_logger.log('ERROR', '文件上传失败')
                    return False
            
            st.success(f"✅ 文件上传成功")
            
            # 确定最终文件名（上传后可能添加了.gz）
            final_filename = base_filename + '.gz'  # 压缩后的文件名
            
            debug_logger.log('INFO', '文件上传完成', {
                'file_url': file_url,
                'final_filename': final_filename
            })
            
            # 解析文件内容
            with st.spinner("正在分析文件内容..."):
                excel_file = pd.ExcelFile(uploaded_file)
                metadata = self.load_metadata()
                reports_processed = 0
                
                for sheet_name in excel_file.sheet_names:
                    try:
                        debug_logger.log('INFO', f'处理工作表: {sheet_name}')
                        
                        df = pd.read_excel(uploaded_file, sheet_name=sheet_name)
                        
                        if df.empty:
                            debug_logger.log('WARNING', f'工作表 {sheet_name} 为空，跳过')
                            continue
                        
                        # 分析应收-未收额
                        analysis_result = self.analyze_receivable_amount(df)
                        
                        # 创建报表元数据
                        report_metadata = {
                            "store_name": sheet_name.strip(),
                            "filename": final_filename,  # 使用正确的文件名
                            "file_url": file_url,
                            "file_size_mb": file_size_mb,
                            "upload_time": datetime.now().isoformat(),
                            "row_count": len(df),
                            "col_count": len(df.columns),
                            "analysis": analysis_result,
                            "id": f"{sheet_name}_{timestamp}"
                        }
                        
                        debug_logger.log('INFO', f'创建报表元数据: {sheet_name}', {
                            'filename': final_filename,
                            'row_count': len(df),
                            'col_count': len(df.columns),
                            'analysis_found': '应收-未收额' in analysis_result
                        })
                        
                        # 移除同门店的旧记录
                        old_reports = metadata.get('reports', [])
                        metadata['reports'] = [r for r in old_reports 
                                             if normalize_store_name(r.get('store_name', '')) != normalize_store_name(sheet_name.strip())]
                        
                        # 添加新记录
                        metadata.setdefault('reports', []).append(report_metadata)
                        reports_processed += 1
                        
                        st.success(f"✅ {sheet_name}: {len(df)} 行数据已处理")
                        
                    except Exception as e:
                        st.warning(f"⚠️ 跳过工作表 '{sheet_name}': {str(e)}")
                        debug_logger.log('ERROR', f'处理工作表 {sheet_name} 失败: {str(e)}')
                        continue
                
                # 保存元数据
                if reports_processed > 0:
                    if self.save_metadata(metadata):
                        st.markdown(f'''
                        <div class="success-alert">
                        <h4>🎉 报表处理完成</h4>
                        <p><strong>处理工作表</strong>: {reports_processed} 个</p>
                        <p><strong>存储方式</strong>: 压缩存储，节省空间</p>
                        <p><strong>状态</strong>: 数据已保存并立即可用</p>
                        </div>
                        ''', unsafe_allow_html=True)
                        
                        debug_logger.log('INFO', '报表处理完成', {
                            'reports_processed': reports_processed,
                            'final_filename': final_filename
                        })
                        return True
                    else:
                        st.error("❌ 元数据保存失败")
                        debug_logger.log('ERROR', '元数据保存失败')
                        return False
                else:
                    st.error("❌ 没有成功处理任何工作表")
                    debug_logger.log('ERROR', '没有成功处理任何工作表')
                    return False
                
        except Exception as e:
            st.error(f"❌ 处理报表文件失败：{str(e)}")
            debug_logger.log('ERROR', f'处理报表文件失败: {str(e)}')
            logger.error(f"处理报表文件失败: {str(e)}")
            return False
    
    def analyze_receivable_amount(self, df: pd.DataFrame) -> Dict[str, Any]:
        """分析应收-未收额数据"""
        result = {}
        
        try:
            if len(df) <= 68:  # 检查是否有第69行
                debug_logger.log('WARNING', f'数据行数不足，无法分析应收-未收额', {
                    'total_rows': len(df),
                    'required_rows': 69
                })
                return result
            
            # 检查第69行
            row_69 = df.iloc[68]  # 第69行，索引为68
            first_col_value = clean_dataframe_value(row_69.iloc[0])
            
            debug_logger.log('INFO', '分析第69行应收-未收额', {
                'first_col_value': first_col_value,
                'row_data_sample': [clean_dataframe_value(row_69.iloc[i]) for i in range(min(5, len(row_69)))]
            })
            
            # 检查关键词
            keywords = ['应收-未收额', '应收未收额', '应收-未收', '应收未收']
            
            for keyword in keywords:
                if keyword in first_col_value:
                    debug_logger.log('INFO', f'找到关键词: {keyword}')
                    
                    # 从右往左查找数值
                    for col_idx in range(len(row_69)-1, -1, -1):
                        val = row_69.iloc[col_idx]
                        
                        if pd.notna(val):
                            val_str = str(val).strip()
                            if val_str and val_str not in ['', 'None', 'nan']:
                                # 清理数值
                                cleaned = val_str.replace(',', '').replace('¥', '').replace('￥', '').strip()
                                
                                # 处理括号表示负数
                                if cleaned.startswith('(') and cleaned.endswith(')'):
                                    cleaned = '-' + cleaned[1:-1]
                                
                                try:
                                    amount = float(cleaned)
                                    if amount != 0:
                                        result['应收-未收额'] = {
                                            'amount': amount,
                                            'column_name': str(df.columns[col_idx]),
                                            'row_name': first_col_value,
                                            'found': True
                                        }
                                        
                                        debug_logger.log('INFO', '成功分析应收-未收额', {
                                            'amount': amount,
                                            'column_name': str(df.columns[col_idx]),
                                            'column_index': col_idx
                                        })
                                        return result
                                except ValueError:
                                    continue
                    break
            
            debug_logger.log('WARNING', '未找到应收-未收额数据', {
                'first_col_value': first_col_value,
                'keywords_checked': keywords
            })
            
        except Exception as e:
            debug_logger.log('ERROR', f'分析应收-未收额数据时出错: {str(e)}')
            logger.warning(f"分析应收-未收额数据时出错: {str(e)}")
        
        return result
    
    # ============ 数据查询 ============
    def get_available_stores(self) -> List[str]:
        """获取可用门店列表，优化匹配逻辑"""
        try:
            debug_logger.log('INFO', '获取可用门店列表')
            
            permissions = self.load_permissions()
            stores = []
            
            for perm in permissions:
                store_name = perm.get('store_name', '').strip()
                if store_name and store_name not in stores:
                    stores.append(store_name)
            
            debug_logger.log('INFO', '可用门店列表获取成功', {
                'store_count': len(stores),
                'stores': stores[:5]  # 只记录前5个作为样例
            })
            
            return sorted(stores)
            
        except Exception as e:
            debug_logger.log('ERROR', f'获取门店列表失败: {str(e)}')
            logger.error(f"获取门店列表失败: {str(e)}")
            return []
    
    def verify_user_permission(self, store_name: str, user_id: str) -> bool:
        """验证用户权限，优化匹配算法"""
        try:
            debug_logger.log('INFO', '开始验证用户权限', {
                'store_name': store_name,
                'user_id': user_id
            })
            
            permissions = self.load_permissions()
            normalized_target_store = normalize_store_name(store_name)
            user_id_str = str(user_id).strip()
            
            for perm in permissions:
                stored_store = perm.get('store_name', '').strip()
                stored_id = perm.get('user_id', '').strip()
                normalized_stored_store = normalize_store_name(stored_store)
                
                # 多层匹配策略
                store_match = False
                
                # 1. 精确匹配
                if stored_store == store_name:
                    store_match = True
                # 2. 标准化后匹配
                elif normalized_stored_store == normalized_target_store:
                    store_match = True
                # 3. 包含关系匹配
                elif (store_name in stored_store or stored_store in store_name or
                      normalized_target_store in normalized_stored_store or 
                      normalized_stored_store in normalized_target_store):
                    store_match = True
                
                # 用户ID匹配
                if store_match and stored_id == user_id_str:
                    debug_logger.log('INFO', '权限验证成功', {
                        'matched_store': stored_store,
                        'matched_user_id': stored_id,
                        'match_type': 'exact' if stored_store == store_name else 'normalized'
                    })
                    return True
            
            debug_logger.log('WARNING', '权限验证失败', {
                'store_name': store_name,
                'user_id': user_id,
                'normalized_store': normalized_target_store,
                'available_stores': [normalize_store_name(p.get('store_name', '')) for p in permissions[:5]]
            })
            return False
            
        except Exception as e:
            debug_logger.log('ERROR', f'权限验证失败: {str(e)}', {
                'store_name': store_name,
                'user_id': user_id
            })
            logger.error(f"权限验证失败: {str(e)}")
            return False
    
    def load_store_data(self, store_name: str) -> Optional[pd.DataFrame]:
        """加载门店数据，优化匹配和调试"""
        try:
            debug_logger.log('INFO', f'开始加载门店数据: {store_name}')
            
            # 获取报表元数据
            metadata = self.load_metadata()
            reports = metadata.get('reports', [])
            
            debug_logger.log('INFO', '报表元数据加载完成', {
                'total_reports': len(reports),
                'available_stores': [r.get('store_name', '') for r in reports]
            })
            
            # 查找匹配的报表 - 优化匹配逻辑
            matching_report = None
            normalized_target = normalize_store_name(store_name)
            
            for report in reports:
                report_store_name = report.get('store_name', '').strip()
                normalized_report = normalize_store_name(report_store_name)
                
                # 多层匹配
                if (report_store_name == store_name or 
                    normalized_report == normalized_target or
                    store_name in report_store_name or 
                    report_store_name in store_name or
                    normalized_target in normalized_report or
                    normalized_report in normalized_target):
                    matching_report = report
                    debug_logger.log('INFO', f'找到匹配的报表', {
                        'target_store': store_name,
                        'matched_store': report_store_name,
                        'filename': report.get('filename')
                    })
                    break
            
            if not matching_report:
                debug_logger.log('ERROR', f'未找到门店 {store_name} 的报表', {
                    'available_stores': [r.get('store_name', '') for r in reports]
                })
                logger.warning(f"未找到门店 {store_name} 的报表")
                return None
            
            filename = matching_report.get('filename')
            if not filename:
                debug_logger.log('ERROR', '报表元数据中缺少文件名')
                logger.error("报表元数据中缺少文件名")
                return None
            
            # 下载文件
            with st.spinner(f"正在加载 {store_name} 的数据..."):
                file_data = self.cos_manager.download_file(filename, decompress=True)
                
                if not file_data:
                    debug_logger.log('ERROR', f'文件下载失败: {filename}')
                    logger.error(f"文件下载失败: {filename}")
                    return None
                
                # 解析Excel文件
                excel_file = pd.ExcelFile(io.BytesIO(file_data))
                
                debug_logger.log('INFO', 'Excel文件解析成功', {
                    'available_sheets': excel_file.sheet_names
                })
                
                # 查找匹配的工作表
                target_sheet = None
                normalized_target_sheet = normalize_store_name(store_name)
                
                # 多层工作表匹配
                if store_name in excel_file.sheet_names:
                    target_sheet = store_name
                else:
                    # 模糊匹配
                    for sheet in excel_file.sheet_names:
                        normalized_sheet = normalize_store_name(sheet)
                        if (normalized_sheet == normalized_target_sheet or
                            store_name in sheet or sheet in store_name or
                            normalized_target_sheet in normalized_sheet or
                            normalized_sheet in normalized_target_sheet):
                            target_sheet = sheet
                            break
                    
                    # 如果还是没找到，使用第一个工作表
                    if not target_sheet and excel_file.sheet_names:
                        target_sheet = excel_file.sheet_names[0]
                        debug_logger.log('WARNING', f'使用第一个工作表作为默认: {target_sheet}')
                
                if target_sheet:
                    df = pd.read_excel(io.BytesIO(file_data), sheet_name=target_sheet)
                    debug_logger.log('INFO', f'成功加载门店数据', {
                        'store_name': store_name,
                        'sheet_name': target_sheet,
                        'rows': len(df),
                        'columns': len(df.columns)
                    })
                    logger.info(f"成功加载 {store_name} 的数据: {len(df)} 行")
                    return df
                else:
                    debug_logger.log('ERROR', '未找到合适的工作表')
                    logger.error("未找到合适的工作表")
                    return None
            
        except Exception as e:
            debug_logger.log('ERROR', f'加载门店数据失败: {str(e)}', {
                'store_name': store_name
            })
            logger.error(f"加载门店数据失败: {str(e)}")
            return None
    
    # ============ 系统管理 ============
    def get_system_status(self) -> Dict:
        """获取系统状态"""
        try:
            debug_logger.log('INFO', '获取系统状态')
            
            permissions = self.load_permissions()
            metadata = self.load_metadata()
            storage_stats = self.cos_manager.get_storage_stats()
            
            # 检查系统文件
            permissions_exists = (self.cos_manager.file_exists(self.permissions_file) or 
                                self.cos_manager.file_exists(self.permissions_file + '.gz'))
            metadata_exists = (self.cos_manager.file_exists(self.metadata_file) or 
                             self.cos_manager.file_exists(self.metadata_file + '.gz'))
            
            status = {
                'permissions_count': len(permissions),
                'reports_count': len(metadata.get('reports', [])),
                'permissions_file_exists': permissions_exists,
                'metadata_file_exists': metadata_exists,
                'system_healthy': permissions_exists and metadata_exists and len(permissions) > 0,
                'storage_stats': storage_stats
            }
            
            debug_logger.log('INFO', '系统状态获取完成', status)
            return status
            
        except Exception as e:
            debug_logger.log('ERROR', f'获取系统状态失败: {str(e)}')
            logger.error(f"获取系统状态失败: {str(e)}")
            return {
                'permissions_count': 0,
                'reports_count': 0,
                'permissions_file_exists': False,
                'metadata_file_exists': False,
                'system_healthy': False,
                'storage_stats': {
                    'total_files': 0,
                    'total_size_gb': 0,
                    'usage_percent': 0,
                    'remaining_calls': 0
                }
            }
    
    def cleanup_old_files(self, days_old: int = 7) -> int:
        """清理旧文件"""
        try:
            debug_logger.log('INFO', f'开始清理 {days_old} 天前的旧文件')
            
            files = self.cos_manager.list_files("reports/")
            cutoff_date = datetime.now() - timedelta(days=days_old)
            
            deleted_count = 0
            for file_info in files:
                try:
                    # 解析文件时间
                    file_date_str = file_info['last_modified']
                    if 'Z' in file_date_str:
                        file_date_str = file_date_str.replace('Z', '+00:00')
                    
                    file_date = datetime.fromisoformat(file_date_str).replace(tzinfo=None)
                    
                    if file_date < cutoff_date:
                        if self.cos_manager.delete_file(file_info['filename']):
                            deleted_count += 1
                            
                except Exception as e:
                    debug_logger.log('WARNING', f'清理文件 {file_info["filename"]} 失败: {str(e)}')
                    logger.warning(f"清理文件 {file_info['filename']} 失败: {str(e)}")
                    continue
            
            # 同时清理元数据
            if deleted_count > 0:
                metadata = self.load_metadata()
                old_count = len(metadata.get('reports', []))
                
                metadata['reports'] = [
                    r for r in metadata.get('reports', [])
                    if datetime.fromisoformat(r.get('upload_time', '1970-01-01')) > cutoff_date
                ]
                
                new_count = len(metadata['reports'])
                if old_count != new_count:
                    self.save_metadata(metadata)
                    deleted_count += (old_count - new_count)
            
            debug_logger.log('INFO', f'清理完成，删除了 {deleted_count} 个文件')
            return deleted_count
            
        except Exception as e:
            debug_logger.log('ERROR', f'清理旧文件失败: {str(e)}')
            logger.error(f"清理旧文件失败: {str(e)}")
            return 0

# ===================== 会话状态初始化 =====================
def initialize_session_state():
    """初始化会话状态"""
    defaults = {
        'logged_in': False,
        'store_name': "",
        'user_id': "",
        'is_admin': False,
        'system': None,
        'debug_mode': False
    }
    
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

# ===================== UI组件 =====================
def show_system_header():
    """显示系统头部"""
    st.markdown('<h1 class="main-header">📊 门店报表查询系统</h1>', unsafe_allow_html=True)

def show_debug_info():
    """显示调试信息"""
    if st.session_state.debug_mode:
        with st.expander("🔍 调试信息", expanded=False):
            recent_logs = debug_logger.get_recent_logs(20)
            
            if recent_logs:
                st.write("**最近的调试日志:**")
                for log in reversed(recent_logs):  # 最新的在前
                    level_color = {
                        'ERROR': '#ff4757',
                        'WARNING': '#ffa502',
                        'INFO': '#3742fa'
                    }.get(log['level'], '#747d8c')
                    
                    st.markdown(f'''
                    <div class="debug-info">
                    <strong style="color: {level_color};">[{log['level']}]</strong> 
                    <small>{log['timestamp'][:19]}</small><br>
                    {log['message']}
                    {f"<br><small>数据: {json.dumps(log['data'], ensure_ascii=False, indent=2)}</small>" if log['data'] else ""}
                    </div>
                    ''', unsafe_allow_html=True)
            else:
                st.info("暂无调试日志")
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("🔄 刷新日志"):
                    st.rerun()
            with col2:
                if st.button("🗑️ 清空日志"):
                    debug_logger.clear_logs()
                    st.rerun()

def show_system_status(system_status: Dict):
    """显示系统状态"""
    storage_stats = system_status.get('storage_stats', {})
    
    st.markdown(f'''
    <div class="system-status">
    <h4>☁️ 系统状态监控</h4>
    <div style="display: flex; justify-content: space-between; flex-wrap: wrap;">
        <div style="text-align: center; flex: 1; min-width: 120px;">
            <div style="font-size: 1.5rem; font-weight: bold;">
                {system_status.get('permissions_count', 0)}
            </div>
            <div style="font-size: 0.9rem;">权限记录</div>
        </div>
        <div style="text-align: center; flex: 1; min-width: 120px;">
            <div style="font-size: 1.5rem; font-weight: bold;">
                {system_status.get('reports_count', 0)}
            </div>
            <div style="font-size: 0.9rem;">报表记录</div>
        </div>
        <div style="text-align: center; flex: 1; min-width: 120px;">
            <div style="font-size: 1.5rem; font-weight: bold;">
                {storage_stats.get('total_size_gb', 0):.1f}GB
            </div>
            <div style="font-size: 0.9rem;">存储使用</div>
        </div>
        <div style="text-align: center; flex: 1; min-width: 120px;">
            <div style="font-size: 1.5rem; font-weight: bold;">
                {storage_stats.get('remaining_calls', 0)}
            </div>
            <div style="font-size: 0.9rem;">API剩余</div>
        </div>
    </div>
    </div>
    ''', unsafe_allow_html=True)
    
    # 显示调试信息
    show_debug_info()

def show_sidebar_status(system_status: Dict):
    """显示侧边栏状态"""
    st.title("⚙️ 系统控制")
    
    # 系统健康状态
    if system_status.get('system_healthy'):
        st.success("🟢 系统运行正常")
    else:
        st.error("🔴 系统需要初始化")
    
    # 关键指标
    storage_stats = system_status.get('storage_stats', {})
    usage_percent = storage_stats.get('usage_percent', 0)
    
    if usage_percent > 80:
        st.error(f"🔴 存储: {usage_percent:.1f}%")
    elif usage_percent > 60:
        st.warning(f"🟡 存储: {usage_percent:.1f}%")
    else:
        st.success(f"🟢 存储: {usage_percent:.1f}%")
    
    st.caption(f"📋 权限: {system_status.get('permissions_count', 0)}")
    st.caption(f"📊 报表: {system_status.get('reports_count', 0)}")
    st.caption(f"⚡ API: {storage_stats.get('remaining_calls', 0)}/h (最大: {API_RATE_LIMIT})")

def show_storage_metrics(storage_stats: Dict):
    """显示存储指标"""
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown(f'''
        <div class="metric-card">
            <div class="metric-value">{storage_stats.get('total_files', 0)}</div>
            <div class="metric-label">总文件数</div>
        </div>
        ''', unsafe_allow_html=True)
    
    with col2:
        st.markdown(f'''
        <div class="metric-card">
            <div class="metric-value">{storage_stats.get('total_size_gb', 0):.1f}</div>
            <div class="metric-label">存储(GB)</div>
        </div>
        ''', unsafe_allow_html=True)
    
    with col3:
        usage_percent = storage_stats.get('usage_percent', 0)
        color = "#e74c3c" if usage_percent > 80 else "#f39c12" if usage_percent > 60 else "#27ae60"
        st.markdown(f'''
        <div class="metric-card">
            <div class="metric-value" style="color: {color};">{usage_percent:.1f}%</div>
            <div class="metric-label">使用率</div>
        </div>
        ''', unsafe_allow_html=True)
    
    with col4:
        st.markdown(f'''
        <div class="metric-card">
            <div class="metric-value">{storage_stats.get('remaining_calls', 0)}</div>
            <div class="metric-label">API剩余</div>
        </div>
        ''', unsafe_allow_html=True)

# ===================== 主程序 =====================
def main():
    """主程序"""
    # 初始化
    initialize_session_state()
    show_system_header()
    
    # 初始化系统
    if not st.session_state.system:
        try:
            with st.spinner("正在初始化系统..."):
                st.session_state.system = StoreReportSystem()
            st.success("✅ 系统初始化成功")
            debug_logger.log('INFO', '系统初始化成功')
        except Exception as e:
            st.error(f"❌ 系统初始化失败: {str(e)}")
            debug_logger.log('ERROR', f'系统初始化失败: {str(e)}')
            st.stop()
    
    system = st.session_state.system
    
    # 获取系统状态
    system_status = system.get_system_status()
    
    # 显示系统状态
    show_system_status(system_status)
    
    # 侧边栏
    with st.sidebar:
        show_sidebar_status(system_status)
        
        # 调试模式
        st.session_state.debug_mode = st.checkbox("🔍 调试模式", value=st.session_state.debug_mode)
        
        st.divider()
        
        # 用户类型选择
        user_type = st.radio("选择用户类型", ["普通用户", "管理员"])
        
        if user_type == "管理员":
            st.subheader("🔐 管理员登录")
            admin_password = st.text_input("密码", type="password")
            
            if st.button("验证身份"):
                if admin_password == ADMIN_PASSWORD:
                    st.session_state.is_admin = True
                    st.success("✅ 验证成功")
                    debug_logger.log('INFO', '管理员登录成功')
                    st.rerun()
                else:
                    st.error("❌ 密码错误")
                    debug_logger.log('WARNING', '管理员登录失败：密码错误')
        else:
            if st.session_state.logged_in:
                st.subheader("👤 当前用户")
                st.info(f"门店：{st.session_state.store_name}")
                st.info(f"编码：{st.session_state.user_id}")
                
                if st.button("🚪 退出登录"):
                    st.session_state.logged_in = False
                    st.session_state.store_name = ""
                    st.session_state.user_id = ""
                    debug_logger.log('INFO', '用户退出登录')
                    st.rerun()
    
    # 主内容区域
    if user_type == "管理员" and st.session_state.is_admin:
        show_admin_interface(system, system_status)
    elif user_type == "管理员":
        st.info("👈 请在左侧输入管理员密码")
    else:
        show_user_interface(system)

def show_admin_interface(system: StoreReportSystem, system_status: Dict):
    """显示管理员界面"""
    st.markdown('''
    <div class="admin-panel">
    <h3>👨‍💼 管理员控制面板</h3>
    <p>系统管理、数据上传、存储监控一体化控制台</p>
    </div>
    ''', unsafe_allow_html=True)
    
    # 存储指标
    st.subheader("📊 存储监控")
    show_storage_metrics(system_status.get('storage_stats', {}))
    
    st.divider()
    
    # 功能选项卡
    tab1, tab2, tab3, tab4 = st.tabs(["📋 权限管理", "📊 报表管理", "🧹 存储清理", "🔧 系统维护"])
    
    with tab1:
        st.markdown("#### 权限表管理")
        st.info("💡 上传Excel文件，第一列为门店名称，第二列为人员编号")
        
        permissions_file = st.file_uploader("选择权限Excel文件", type=['xlsx', 'xls'], key="permissions")
        
        if permissions_file and st.button("📤 上传权限表", type="primary"):
            with st.spinner("正在处理权限文件..."):
                if system.process_permissions_file(permissions_file):
                    st.balloons()
                    time.sleep(1)
                    st.rerun()
    
    with tab2:
        st.markdown("#### 财务报表管理")
        
        st.markdown('''
        <div class="info-alert">
        <h5>🚀 智能特性</h5>
        <p>• 自动压缩，节省60-80%存储空间</p>
        <p>• 支持多工作表，自动识别门店</p>
        <p>• 自动分析应收-未收额数据</p>
        <p>• 高速上传下载，中国区优化</p>
        <p>• API限制提升至 500次/小时</p>
        </div>
        ''', unsafe_allow_html=True)
        
        reports_file = st.file_uploader("选择报表Excel文件", type=['xlsx', 'xls'], key="reports")
        
        if reports_file:
            file_size = len(reports_file.getvalue()) / 1024 / 1024
            estimated_compressed = file_size * 0.3
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("原始大小", f"{file_size:.2f} MB")
            with col2:
                st.metric("压缩后", f"~{estimated_compressed:.2f} MB", f"节省 {file_size - estimated_compressed:.2f} MB")
        
        if reports_file and st.button("📤 上传报表数据", type="primary"):
            with st.spinner("正在处理报表文件..."):
                if system.process_reports_file(reports_file):
                    st.balloons()
                    time.sleep(1)
                    st.rerun()
    
    with tab3:
        st.markdown("#### 存储空间管理")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("##### 🗑️ 清理旧文件")
            st.info("清理7天前的报表文件")
            
            if st.button("🧹 清理旧文件", type="secondary"):
                with st.spinner("正在清理旧文件..."):
                    deleted_count = system.cleanup_old_files(7)
                    if deleted_count > 0:
                        st.success(f"✅ 已清理 {deleted_count} 个文件")
                        debug_logger.log('INFO', f'清理完成：删除了 {deleted_count} 个文件')
                    else:
                        st.info("ℹ️ 没有需要清理的文件")
                    time.sleep(1)
                    st.rerun()
        
        with col2:
            st.markdown("##### ⚠️ 完全重置")
            st.warning("将删除所有数据，请谨慎操作")
            
            if st.checkbox("我确认要删除所有数据", key="confirm_delete"):
                if st.button("🗑️ 完全重置", type="primary"):
                    with st.spinner("正在清理所有数据..."):
                        # 这里可以添加完全清理的逻辑
                        st.error("⚠️ 完全重置功能需要额外确认，请联系技术支持")
    
    with tab4:
        st.markdown("#### 系统诊断与维护")
        
        if st.button("🔍 运行完整诊断", type="primary"):
            with st.spinner("正在运行系统诊断..."):
                # 权限系统诊断
                permissions = system.load_permissions()
                if len(permissions) > 0:
                    st.success(f"✅ 权限系统正常: {len(permissions)} 条记录")
                    
                    # 显示权限统计
                    stores = system.get_available_stores()
                    st.info(f"📋 支持门店: {len(stores)} 个")
                    
                    if st.session_state.debug_mode and len(permissions) > 0:
                        st.write("**权限记录样例:**")
                        sample_df = pd.DataFrame(permissions[:5])
                        st.dataframe(sample_df[['store_name', 'user_id']], use_container_width=True)
                else:
                    st.error("❌ 权限系统异常: 无权限记录")
                
                # 报表系统诊断
                metadata = system.load_metadata()
                reports = metadata.get('reports', [])
                if len(reports) > 0:
                    st.success(f"✅ 报表系统正常: {len(reports)} 个报表")
                    
                    if st.session_state.debug_mode and len(reports) > 0:
                        st.write("**报表记录样例:**")
                        sample_reports = []
                        for report in reports[:5]:
                            sample_reports.append({
                                '门店': report.get('store_name'),
                                '文件名': report.get('filename'),
                                '大小': f"{report.get('file_size_mb', 0):.1f}MB",
                                '时间': report.get('upload_time', '')[:19]
                            })
                        st.dataframe(pd.DataFrame(sample_reports), use_container_width=True)
                else:
                    st.error("❌ 报表系统异常: 无报表记录")
                
                # 存储诊断
                storage_stats = system.cos_manager.get_storage_stats()
                st.info(f"💾 存储统计: {storage_stats.get('total_files', 0)} 个文件, {storage_stats.get('total_size_gb', 0):.2f}GB")
        
        st.divider()
        
        # 快速操作
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🔄 刷新系统状态"):
                debug_logger.log('INFO', '手动刷新系统状态')
                st.rerun()
        
        with col2:
            if st.button("📊 导出系统报告"):
                # 生成系统报告
                report_data = {
                    'timestamp': datetime.now().isoformat(),
                    'system_status': system_status,
                    'permissions_count': len(system.load_permissions()),
                    'reports_count': len(system.load_metadata().get('reports', [])),
                    'debug_logs': debug_logger.get_recent_logs(50)
                }
                
                report_json = json.dumps(report_data, ensure_ascii=False, indent=2)
                st.download_button(
                    "📥 下载系统报告",
                    report_json,
                    f"system_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                    "application/json"
                )

def show_user_interface(system: StoreReportSystem):
    """显示用户界面，增强调试信息"""
    if not st.session_state.logged_in:
        st.subheader("🔐 用户登录")
        
        try:
            stores = system.get_available_stores()
            
            if not stores:
                st.markdown('''
                <div class="warning-alert">
                <h4>⚠️ 系统维护中</h4>
                <p>暂无可用门店，请联系管理员配置权限数据</p>
                </div>
                ''', unsafe_allow_html=True)
            else:
                st.markdown(f'''
                <div class="success-alert">
                <h4>🏪 系统就绪</h4>
                <p>当前支持 <strong>{len(stores)}</strong> 个门店的查询服务</p>
                </div>
                ''', unsafe_allow_html=True)
                
                with st.form("login_form"):
                    selected_store = st.selectbox("选择门店", stores)
                    user_id = st.text_input("查询编码")
                    submit = st.form_submit_button("🚀 登录查询", type="primary")
                    
                    if submit and selected_store and user_id:
                        debug_logger.log('INFO', '用户尝试登录', {
                            'store': selected_store,
                            'user_id': user_id
                        })
                        
                        with st.spinner("正在验证权限..."):
                            if system.verify_user_permission(selected_store, user_id):
                                st.session_state.logged_in = True
                                st.session_state.store_name = selected_store
                                st.session_state.user_id = user_id
                                debug_logger.log('INFO', '用户登录成功', {
                                    'store': selected_store,
                                    'user_id': user_id
                                })
                                st.success("✅ 登录成功！")
                                st.balloons()
                                time.sleep(1)
                                st.rerun()
                            else:
                                st.error("❌ 门店或编号错误，请检查后重试")
                                debug_logger.log('WARNING', '用户登录失败', {
                                    'store': selected_store,
                                    'user_id': user_id
                                })
                        
        except Exception as e:
            st.error(f"❌ 系统连接失败：{str(e)}")
            debug_logger.log('ERROR', f'用户界面系统连接失败: {str(e)}')
    
    else:
        # 已登录用户界面
        st.markdown(f'''
        <div class="store-info">
        <h3>🏪 {st.session_state.store_name}</h3>
        <p>查询员工：{st.session_state.user_id}</p>
        <p>查询时间：{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
        </div>
        ''', unsafe_allow_html=True)
        
        # 数据刷新按钮
        col1, col2 = st.columns([4, 1])
        with col2:
            if st.button("🔄 刷新数据"):
                debug_logger.log('INFO', '用户手动刷新数据')
                st.rerun()
        
        try:
            # 加载门店数据
            debug_logger.log('INFO', '开始为用户加载门店数据', {
                'store_name': st.session_state.store_name,
                'user_id': st.session_state.user_id
            })
            
            df = system.load_store_data(st.session_state.store_name)
            
            if df is not None:
                # 应收-未收额分析
                st.subheader("💰 财务数据分析")
                
                analysis_results = system.analyze_receivable_amount(df)
                
                if '应收-未收额' in analysis_results:
                    data = analysis_results['应收-未收额']
                    amount = data['amount']
                    
                    debug_logger.log('INFO', '应收-未收额分析成功', {
                        'amount': amount,
                        'column': data['column_name'],
                        'store': st.session_state.store_name
                    })
                    
                    # 显示金额状态
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        if amount > 0:
                            st.markdown(f'''
                            <div class="error-alert">
                            <h4>💳 应付款项</h4>
                            <div style="font-size: 2rem; font-weight: bold; text-align: center;">
                                ¥{amount:,.2f}
                            </div>
                            </div>
                            ''', unsafe_allow_html=True)
                        elif amount < 0:
                            st.markdown(f'''
                            <div class="success-alert">
                            <h4>💚 应退款项</h4>
                            <div style="font-size: 2rem; font-weight: bold; text-align: center;">
                                ¥{abs(amount):,.2f}
                            </div>
                            </div>
                            ''', unsafe_allow_html=True)
                        else:
                            st.markdown(f'''
                            <div class="info-alert">
                            <h4>⚖️ 收支平衡</h4>
                            <div style="font-size: 2rem; font-weight: bold; text-align: center;">
                                ¥0.00
                            </div>
                            </div>
                            ''', unsafe_allow_html=True)
                    
                    with col2:
                        st.markdown(f'''
                        <div class="metric-card">
                            <div class="metric-value">{data['column_name']}</div>
                            <div class="metric-label">数据列</div>
                        </div>
                        ''', unsafe_allow_html=True)
                    
                    with col3:
                        st.markdown(f'''
                        <div class="metric-card">
                            <div class="metric-value">第69行</div>
                            <div class="metric-label">数据位置</div>
                        </div>
                        ''', unsafe_allow_html=True)
                    
                    # 详细信息
                    with st.expander("📊 数据详情"):
                        st.write(f"**行标题**: {data['row_name']}")
                        st.write(f"**所在列**: {data['column_name']}")
                        st.write(f"**金额**: ¥{amount:,.2f}")
                        st.write(f"**位置**: 第69行")
                
                else:
                    st.markdown('''
                    <div class="warning-alert">
                    <h4>⚠️ 未找到应收-未收额数据</h4>
                    <p>可能原因：</p>
                    <ul>
                    <li>报表格式与标准格式不符</li>
                    <li>第69行不包含应收-未收额信息</li>
                    <li>数据列位置发生变化</li>
                    </ul>
                    </div>
                    ''', unsafe_allow_html=True)
                    
                    debug_logger.log('WARNING', '未找到应收-未收额数据', {
                        'store': st.session_state.store_name,
                        'data_rows': len(df),
                        'first_col_row_69': clean_dataframe_value(df.iloc[68].iloc[0]) if len(df) > 68 else 'N/A'
                    })
                
                st.divider()
                
                # 报表数据展示
                st.subheader("📋 完整报表数据")
                
                # 数据统计
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("数据行数", len(df))
                with col2:
                    st.metric("数据列数", len(df.columns))
                with col3:
                    st.metric("非空数据", df.count().sum())
                with col4:
                    st.metric("数据完整度", f"{(df.count().sum() / (len(df) * len(df.columns)) * 100):.1f}%")
                
                # 数据表格
                st.dataframe(df, use_container_width=True, height=500)
                
                # 下载功能
                st.subheader("📥 数据导出")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    if st.button("📄 下载Excel格式", type="primary"):
                        try:
                            buffer = io.BytesIO()
                            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                                df.to_excel(writer, index=False, sheet_name=st.session_state.store_name)
                            
                            debug_logger.log('INFO', '用户下载Excel文件', {
                                'store': st.session_state.store_name,
                                'rows': len(df)
                            })
                            
                            st.download_button(
                                "📥 点击下载Excel文件",
                                buffer.getvalue(),
                                f"{st.session_state.store_name}_报表_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="download_excel"
                            )
                        except Exception as e:
                            st.error(f"生成Excel文件失败：{str(e)}")
                            debug_logger.log('ERROR', f'生成Excel文件失败: {str(e)}')
                
                with col2:
                    if st.button("📊 下载CSV格式", type="secondary"):
                        try:
                            csv_data = df.to_csv(index=False, encoding='utf-8-sig')
                            
                            debug_logger.log('INFO', '用户下载CSV文件', {
                                'store': st.session_state.store_name,
                                'rows': len(df)
                            })
                            
                            st.download_button(
                                "📥 点击下载CSV文件",
                                csv_data,
                                f"{st.session_state.store_name}_报表_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                "text/csv",
                                key="download_csv"
                            )
                        except Exception as e:
                            st.error(f"生成CSV文件失败：{str(e)}")
                            debug_logger.log('ERROR', f'生成CSV文件失败: {str(e)}')
            
            else:
                st.markdown(f'''
                <div class="error-alert">
                <h4>❌ 无法加载报表数据</h4>
                <p>门店 "<strong>{st.session_state.store_name}</strong>" 的报表数据暂不可用</p>
                <h5>可能的原因：</h5>
                <ul>
                <li>📋 管理员尚未上传该门店的报表文件</li>
                <li>⏳ 数据正在处理中，请稍后重试</li>
                <li>🔄 系统正在同步数据，请刷新页面</li>
                <li>🔗 网络连接不稳定，请检查网络</li>
                </ul>
                <h5>建议操作：</h5>
                <ul>
                <li>🔄 点击"刷新数据"按钮重新加载</li>
                <li>📞 联系管理员确认数据状态</li>
                <li>⏰ 等待5-10分钟后重试</li>
                </ul>
                </div>
                ''', unsafe_allow_html=True)
                
                debug_logger.log('ERROR', '门店数据加载失败', {
                    'store': st.session_state.store_name,
                    'user_id': st.session_state.user_id
                })
                
        except Exception as e:
            st.markdown(f'''
            <div class="error-alert">
            <h4>❌ 系统错误</h4>
            <p>数据加载过程中发生错误：{str(e)}</p>
            <p>请尝试刷新页面或联系技术支持</p>
            </div>
            ''', unsafe_allow_html=True)
            
            debug_logger.log('ERROR', f'用户界面发生系统错误: {str(e)}', {
                'store': st.session_state.store_name,
                'user_id': st.session_state.user_id
            })

# ===================== 页面底部 =====================
def show_footer():
    """显示页面底部信息"""
    st.divider()
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.caption(f"🕒 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    with col2:
        st.caption("☁️ 腾讯云COS")
    with col3:
        st.caption("📦 智能压缩")
    with col4:
        st.caption("🛡️ 安全可靠")
    with col5:
        st.caption(f"🚀 v9.0 优化版 (API: {API_RATE_LIMIT}/h)")

# ===================== 程序入口 =====================
if __name__ == "__main__":
    try:
        main()
        show_footer()
    except Exception as e:
        st.error(f"❌ 系统启动失败: {str(e)}")
        st.write("**错误详情:**", str(e))
        debug_logger.log('ERROR', f'系统启动失败: {str(e)}')
        if st.button("🔄 重新启动"):
            st.rerun()
