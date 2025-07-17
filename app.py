import streamlit as st
import pandas as pd
import io
import json
import gzip
import hashlib
from datetime import datetime, timedelta
import time
from qcloud_cos import CosConfig, CosS3Client
from qcloud_cos.cos_exception import CosServiceError, CosClientError
import logging
from typing import Optional, Dict, Any, List
import traceback
from contextlib import contextmanager
import threading
import re

# ===================== 页面配置 =====================
st.set_page_config(
    page_title="门店报表查询系统", 
    page_icon="📊",
    layout="wide"
)

# ===================== 系统配置 =====================
ADMIN_PASSWORD = st.secrets.get("system", {}).get("admin_password", "admin123")
MAX_STORAGE_GB = 40  # 40GB存储限制
API_RATE_LIMIT = 500  # 每小时API调用限制
COMPRESSION_LEVEL = 6  # GZIP压缩等级
RETRY_ATTEMPTS = 3  # 重试次数
RETRY_DELAY = 1  # 重试延迟(秒)
MAX_RETRIES = 3
CACHE_DURATION = 300  # 缓存5分钟

# ===================== CSS样式 =====================
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
        font-weight: 700;
    }
    .store-info {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1.5rem;
        border-radius: 10px;
        margin: 1rem 0;
        box-shadow: 0 10px 20px rgba(0, 0, 0, 0.15);
        text-align: center;
    }
    .admin-panel {
        background: linear-gradient(135deg, #ffeaa7 0%, #fab1a0 100%);
        padding: 1.5rem;
        border-radius: 10px;
        border: 2px solid #fdcb6e;
        margin: 1rem 0;
        box-shadow: 0 8px 16px rgba(0, 0, 0, 0.1);
    }
    .receivable-positive {
        background: linear-gradient(135deg, #ff9a9e 0%, #fecfef 100%);
        color: #721c24;
        padding: 2rem;
        border-radius: 15px;
        border: 3px solid #f093fb;
        margin: 1rem 0;
        text-align: center;
        box-shadow: 0 8px 20px rgba(0, 0, 0, 0.15);
    }
    .receivable-negative {
        background: linear-gradient(135deg, #a8edea 0%, #d299c2 100%);
        color: #0c4128;
        padding: 2rem;
        border-radius: 15px;
        border: 3px solid #48cab2;
        margin: 1rem 0;
        text-align: center;
        box-shadow: 0 8px 20px rgba(0, 0, 0, 0.15);
    }
    .status-success {
        background: linear-gradient(135deg, #84fab0 0%, #8fd3f4 100%);
        color: #2d3436;
        padding: 0.75rem;
        border-radius: 8px;
        border-left: 5px solid #00b894;
        margin: 0.5rem 0;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
    }
    .status-error {
        background: linear-gradient(135deg, #fd79a8 0%, #e84393 100%);
        color: white;
        padding: 0.75rem;
        border-radius: 8px;
        border-left: 5px solid #d63031;
        margin: 0.5rem 0;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
    }
    .status-warning {
        background: linear-gradient(135deg, #fdcb6e 0%, #e17055 100%);
        color: white;
        padding: 0.75rem;
        border-radius: 8px;
        border-left: 5px solid #e84393;
        margin: 0.5rem 0;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
    }
    .system-stats {
        background: linear-gradient(135deg, #00cec9 0%, #55a3ff 100%);
        color: white;
        padding: 1rem;
        border-radius: 10px;
        margin: 1rem 0;
        text-align: center;
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
    </style>
""", unsafe_allow_html=True)

# ===================== 日志配置 =====================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ===================== 异常类定义 =====================
class CosOperationError(Exception):
    """腾讯云COS操作异常"""
    pass

class DataProcessingError(Exception):
    """数据处理异常"""
    pass

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
@contextmanager
def error_handler(operation_name: str):
    """通用错误处理上下文管理器"""
    try:
        debug_logger.log('INFO', f'开始执行: {operation_name}')
        yield
        debug_logger.log('INFO', f'成功完成: {operation_name}')
    except Exception as e:
        error_msg = f"{operation_name} 失败: {str(e)}"
        debug_logger.log('ERROR', error_msg)
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        st.error(f"❌ {error_msg}")
        raise

def retry_operation(func, *args, max_retries=MAX_RETRIES, delay=RETRY_DELAY, **kwargs):
    """重试操作装饰器"""
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
                raise
            else:
                debug_logger.log('WARNING', error_msg, {'retry_delay': delay * (attempt + 1)})
                logger.warning(error_msg)
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
        debug_logger.log('INFO', f'缓存设置成功: {key}')
    except Exception as e:
        debug_logger.log('WARNING', f'设置缓存失败: {str(e)}')
        logger.warning(f"设置缓存失败: {str(e)}")

def get_cache(key: str) -> Optional[Any]:
    """获取缓存"""
    try:
        cache_key = f"cache_{key}"
        if cache_key in st.session_state:
            cache_data = st.session_state[cache_key]
            if time.time() - cache_data['timestamp'] < cache_data['duration']:
                debug_logger.log('INFO', f'缓存命中: {key}')
                return cache_data['data']
            else:
                del st.session_state[cache_key]
                debug_logger.log('INFO', f'缓存过期删除: {key}')
    except Exception as e:
        debug_logger.log('WARNING', f'获取缓存失败: {str(e)}')
        logger.warning(f"获取缓存失败: {str(e)}")
    return None

def sanitize_filename(filename: str) -> str:
    """清理文件名，移除特殊字符"""
    original_filename = filename
    filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
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

# ===================== 压缩管理器 =====================
class CompressionManager:
    """数据压缩管理器"""
    
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

# ===================== 腾讯云COS管理器 =====================
class TencentCOSManager:
    """腾讯云COS管理器"""
    
    def __init__(self):
        self.client = None
        self.bucket_name = None
        self.region = None
        self.rate_limiter = SimpleRateLimiter(API_RATE_LIMIT)
        self.compression = CompressionManager()
        self.permissions_file = "system/permissions.json"
        self.metadata_file = "system/metadata.json"
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
            raise CosOperationError(f"COS初始化失败: {str(e)}")
    
    def _execute_with_limit_check(self, operation):
        """执行带频率限制检查的操作"""
        if not self.rate_limiter.can_make_call():
            remaining = self.rate_limiter.get_remaining_calls()
            error_msg = f"API调用频率超限，剩余: {remaining}/小时"
            debug_logger.log('ERROR', error_msg)
            raise CosOperationError(error_msg)
        
        result = operation()
        self.rate_limiter.record_call()
        return result
    
    def upload_file(self, file_data: bytes, filename: str, compress: bool = True) -> Optional[str]:
        """上传文件到COS"""
        try:
            original_filename = filename
            filename = sanitize_filename(filename)
            
            # 压缩处理
            upload_data = file_data
            final_filename = filename
            
            if compress:
                compressed_data = self.compression.compress_data(file_data)
                if len(compressed_data) < len(file_data):
                    upload_data = compressed_data
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
            
            # 上传操作
            def upload_operation():
                return self.client.put_object(
                    Bucket=self.bucket_name,
                    Body=upload_data,
                    Key=final_filename,
                    ContentType='application/octet-stream'
                )
            
            retry_operation(lambda: self._execute_with_limit_check(upload_operation))
            
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
            raise CosOperationError(f"文件上传失败: {str(e)}")
    
    def download_file(self, filename: str, decompress: bool = True) -> Optional[bytes]:
        """从COS下载文件"""
        try:
            debug_logger.log('INFO', '开始下载文件', {'filename': filename})
            
            def download_operation():
                response = self.client.get_object(
                    Bucket=self.bucket_name,
                    Key=filename
                )
                return response['Body'].read()
            
            file_data = retry_operation(lambda: self._execute_with_limit_check(download_operation))
            
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
    
    def upload_json(self, data: dict, filename: str) -> bool:
        """上传JSON数据"""
        try:
            json_bytes = self.compression.compress_json(data)
            
            if not filename.endswith('.gz'):
                filename = filename + '.gz'
            
            result = self.upload_file(json_bytes, filename, compress=False)
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
        """下载JSON数据"""
        try:
            possible_filenames = [filename]
            if not filename.endswith('.gz'):
                possible_filenames.append(filename + '.gz')
            if filename.endswith('.gz'):
                possible_filenames.append(filename[:-3])
            
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
    
    def list_files(self, prefix: str = "") -> List[Dict]:
        """列出文件"""
        try:
            def list_operation():
                return self.client.list_objects(
                    Bucket=self.bucket_name,
                    Prefix=prefix,
                    MaxKeys=1000
                )
            
            response = retry_operation(lambda: self._execute_with_limit_check(list_operation))
            
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

# ===================== 主应用类 =====================
@st.cache_resource(show_spinner="连接腾讯云COS...")
def get_tencent_cos_client():
    """获取腾讯云COS客户端 - 使用缓存"""
    try:
        manager = TencentCOSManager()
        debug_logger.log('INFO', 'COS客户端缓存创建成功')
        logger.info("腾讯云COS客户端创建成功")
        return manager
    except Exception as e:
        debug_logger.log('ERROR', f'COS客户端创建失败: {str(e)}')
        logger.error(f"腾讯云COS客户端创建失败: {str(e)}")
        raise CosOperationError(f"连接失败: {str(e)}")

def clean_dataframe_for_storage(df: pd.DataFrame) -> pd.DataFrame:
    """清理DataFrame以便存储"""
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
        
        debug_logger.log('INFO', f'DataFrame清理完成: {len(df_cleaned)} 行 x {len(df_cleaned.columns)} 列')
        return df_cleaned
        
    except Exception as e:
        debug_logger.log('ERROR', f'清理DataFrame失败: {str(e)}')
        logger.error(f"清理DataFrame失败: {str(e)}")
        raise DataProcessingError(f"数据清理失败: {str(e)}")

def save_permissions_to_cos(df: pd.DataFrame, cos_manager: TencentCOSManager) -> bool:
    """保存权限数据到COS"""
    with error_handler("保存权限数据"):
        def _save_operation():
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            permissions_data = []
            for _, row in df.iterrows():
                permissions_data.append({
                    "store_name": str(row.iloc[0]).strip(),
                    "user_id": str(row.iloc[1]).strip(),
                    "created_at": current_time
                })
            
            data = {
                'permissions': permissions_data,
                'last_updated': current_time,
                'count': len(permissions_data)
            }
            
            success = cos_manager.upload_json(data, cos_manager.permissions_file)
            
            if success:
                # 清除相关缓存
                cache_key = get_cache_key("permissions", "load")
                if f"cache_{cache_key}" in st.session_state:
                    del st.session_state[f"cache_{cache_key}"]
                
                debug_logger.log('INFO', f'权限数据保存成功: {len(permissions_data)} 条记录')
                logger.info(f"权限数据保存成功: {len(permissions_data)} 条记录")
            
            return success
        
        return retry_operation(_save_operation)

def load_permissions_from_cos(cos_manager: TencentCOSManager) -> Optional[pd.DataFrame]:
    """从COS加载权限数据 - 使用缓存"""
    cache_key = get_cache_key("permissions", "load")
    cached_data = get_cache(cache_key)
    if cached_data is not None:
        debug_logger.log('INFO', "从缓存加载权限数据")
        logger.info("从缓存加载权限数据")
        return cached_data
    
    with error_handler("加载权限数据"):
        def _load_operation():
            data = cos_manager.download_json(cos_manager.permissions_file)
            
            if not data or 'permissions' not in data:
                debug_logger.log('INFO', "权限数据不存在或为空")
                logger.info("权限数据不存在或为空")
                return None
            
            permissions = data['permissions']
            
            # 转换为DataFrame
            df_data = []
            for perm in permissions:
                df_data.append({
                    '门店名称': perm.get('store_name', '').strip(),
                    '人员编号': perm.get('user_id', '').strip()
                })
            
            if not df_data:
                return None
            
            df = pd.DataFrame(df_data)
            
            # 移除空行
            df = df[
                (df['门店名称'] != '') & 
                (df['人员编号'] != '')
            ]
            
            debug_logger.log('INFO', f'权限数据加载成功: {len(df)} 条记录')
            logger.info(f"权限数据加载成功: {len(df)} 条记录")
            
            # 设置缓存
            set_cache(cache_key, df)
            return df
        
        return retry_operation(_load_operation)

def save_reports_to_cos(reports_dict: Dict[str, pd.DataFrame], cos_manager: TencentCOSManager, original_file_data: bytes) -> bool:
    """保存报表数据到COS - 混合存储：原始文件 + 预解析数据"""
    def _save_operation():
        # 加载现有元数据
        metadata = cos_manager.download_json(cos_manager.metadata_file) or {'reports': []}
        
        current_time = datetime.now().isoformat()
        timestamp = int(time.time())
        uploaded_files = []  # 跟踪已上传文件，用于回滚
        
        try:
            # 第一步：为每个门店保存原始文件和预解析数据
            for store_name, df in reports_dict.items():
                sanitized_name = sanitize_filename(store_name)
                file_hash = hashlib.md5(str(df.values.tolist()).encode()).hexdigest()[:8]
                base_name = f"{sanitized_name}_{timestamp}_{file_hash}"
                
                # 原始文件路径
                raw_filename = f"reports/raw/{base_name}.xlsx"
                parsed_filename = f"reports/parsed/{base_name}_data.json"
                
                debug_logger.log('INFO', f'开始处理门店: {store_name}', {
                    'raw_file': raw_filename,
                    'parsed_file': parsed_filename
                })
                
                # 1. 保存原始Excel文件
                raw_url = cos_manager.upload_file(original_file_data, raw_filename, compress=True)
                if not raw_url:
                    raise Exception(f"原始文件上传失败: {store_name}")
                
                uploaded_files.append(raw_filename + '.gz')
                debug_logger.log('INFO', f'原始文件上传成功: {store_name}')
                
                # 2. 立即验证原始文件
                verify_data = cos_manager.download_file(raw_filename + '.gz', decompress=True)
                if not verify_data:
                    raise Exception(f"原始文件验证失败: {store_name}")
                
                # 3. 生成预解析数据
                try:
                    # 分析应收-未收额
                    analysis_result = analyze_receivable_data(df)
                    
                    # 清理DataFrame准备JSON序列化
                    df_cleaned = clean_dataframe_for_storage(df)
                    
                    parsed_data = {
                        'store_name': store_name,
                        'data': df_cleaned.to_dict('records'),
                        'columns': list(df_cleaned.columns),
                        'analysis': analysis_result,
                        'row_count': len(df),
                        'col_count': len(df.columns),
                        'parsed_time': current_time
                    }
                    
                    # 4. 保存预解析数据
                    parsed_success = cos_manager.upload_json(parsed_data, parsed_filename)
                    if parsed_success:
                        uploaded_files.append(parsed_filename + '.gz')
                        debug_logger.log('INFO', f'预解析数据保存成功: {store_name}')
                    else:
                        debug_logger.log('WARNING', f'预解析数据保存失败，但原始文件已保存: {store_name}')
                
                except Exception as e:
                    debug_logger.log('WARNING', f'预解析失败但原始文件已保存: {store_name}, 错误: {str(e)}')
                    # 预解析失败不影响整体流程，因为有原始文件兜底
                
                # 5. 创建报表元数据
                report_metadata = {
                    "store_name": store_name.strip(),
                    "raw_filename": raw_filename + '.gz',
                    "parsed_filename": parsed_filename + '.gz' if parsed_success else None,
                    "file_url": raw_url,
                    "file_size_mb": len(original_file_data) / 1024 / 1024,
                    "upload_time": current_time,
                    "row_count": len(df),
                    "col_count": len(df.columns),
                    "analysis": analysis_result if 'analysis_result' in locals() else {},
                    "id": f"{store_name}_{timestamp}",
                    "has_parsed_data": parsed_success
                }
                
                # 移除同门店的旧记录
                old_reports = [r for r in metadata.get('reports', []) 
                             if normalize_store_name(r.get('store_name', '')) == normalize_store_name(store_name.strip())]
                
                metadata['reports'] = [r for r in metadata.get('reports', []) 
                                     if normalize_store_name(r.get('store_name', '')) != normalize_store_name(store_name.strip())]
                
                # 清理旧文件
                for old_report in old_reports:
                    try:
                        if old_report.get('raw_filename'):
                            cos_manager.delete_file(old_report['raw_filename'])
                        if old_report.get('parsed_filename'):
                            cos_manager.delete_file(old_report['parsed_filename'])
                    except:
                        pass  # 忽略清理错误
                
                # 添加新记录
                metadata.setdefault('reports', []).append(report_metadata)
                
                debug_logger.log('INFO', f'门店 {store_name} 处理完成', {
                    'raw_file_saved': True,
                    'parsed_file_saved': parsed_success,
                    'metadata_updated': True
                })
            
            # 第二步：保存元数据
            metadata['last_updated'] = current_time
            metadata_success = cos_manager.upload_json(metadata, cos_manager.metadata_file)
            
            if not metadata_success:
                raise Exception("元数据保存失败")
            
            # 第三步：验证元数据
            verify_metadata = cos_manager.download_json(cos_manager.metadata_file)
            if not verify_metadata:
                raise Exception("元数据验证失败")
            
            # 第四步：清除缓存
            cache_keys_to_clear = [
                get_cache_key("reports", "load"),
                get_cache_key("metadata", "load")
            ]
            
            for cache_key in cache_keys_to_clear:
                if f"cache_{cache_key}" in st.session_state:
                    del st.session_state[f"cache_{cache_key}"]
            
            debug_logger.log('INFO', f'报表数据保存完成', {
                'stores_processed': len(reports_dict),
                'files_uploaded': len(uploaded_files),
                'metadata_saved': True
            })
            
            return True
            
        except Exception as e:
            debug_logger.log('ERROR', f'保存过程失败，开始回滚: {str(e)}')
            
            # 回滚：删除已上传的文件
            for filename in uploaded_files:
                try:
                    cos_manager.delete_file(filename)
                    debug_logger.log('INFO', f'回滚删除文件: {filename}')
                except:
                    debug_logger.log('WARNING', f'回滚删除文件失败: {filename}')
            
            raise Exception(f"保存失败并已回滚: {str(e)}")
    
    try:
        return retry_operation(_save_operation)
    except Exception as e:
        logger.error(f"报表保存失败: {str(e)}")
        return False

def load_reports_from_cos(cos_manager: TencentCOSManager) -> Dict[str, pd.DataFrame]:
    """从COS加载报表数据 - 混合存储：优先使用预解析数据，否则解析原始文件"""
    cache_key = get_cache_key("reports", "load")
    cached_data = get_cache(cache_key)
    if cached_data is not None:
        debug_logger.log('INFO', "从缓存加载报表数据")
        logger.info("从缓存加载报表数据")
        return cached_data
    
    def _load_operation():
        metadata = cos_manager.download_json(cos_manager.metadata_file)
        
        if not metadata or 'reports' not in metadata:
            debug_logger.log('INFO', "报表元数据不存在或为空")
            logger.info("报表元数据不存在或为空")
            return {}
        
        reports_dict = {}
        reports = metadata['reports']
        
        for report in reports:
            store_name = report.get('store_name')
            raw_filename = report.get('raw_filename')
            parsed_filename = report.get('parsed_filename')
            has_parsed_data = report.get('has_parsed_data', False)
            
            if not store_name:
                continue
            
            debug_logger.log('INFO', f'加载报表: {store_name}', {
                'has_parsed_data': has_parsed_data,
                'raw_filename': raw_filename,
                'parsed_filename': parsed_filename
            })
            
            df = None
            
            # 策略1：优先尝试使用预解析数据
            if has_parsed_data and parsed_filename:
                try:
                    debug_logger.log('INFO', f'尝试加载预解析数据: {store_name}')
                    parsed_data = cos_manager.download_json(parsed_filename.replace('.gz', ''))
                    
                    if parsed_data and 'data' in parsed_data:
                        # 从预解析数据重建DataFrame
                        df = pd.DataFrame(parsed_data['data'])
                        if 'columns' in parsed_data:
                            df.columns = parsed_data['columns'][:len(df.columns)]
                        
                        debug_logger.log('INFO', f'预解析数据加载成功: {store_name}', {
                            'rows': len(df),
                            'cols': len(df.columns)
                        })
                
                except Exception as e:
                    debug_logger.log('WARNING', f'预解析数据加载失败: {store_name}, 错误: {str(e)}')
                    df = None
            
            # 策略2：预解析数据不可用时，解析原始文件
            if df is None and raw_filename:
                try:
                    debug_logger.log('INFO', f'尝试解析原始文件: {store_name}')
                    
                    # 下载原始Excel文件
                    excel_data = cos_manager.download_file(raw_filename, decompress=True)
                    
                    if excel_data:
                        # 解析Excel文件
                        excel_file = pd.ExcelFile(io.BytesIO(excel_data))
                        
                        # 查找合适的工作表
                        sheet_name = None
                        normalized_store = normalize_store_name(store_name)
                        
                        # 多层工作表匹配
                        for sheet in excel_file.sheet_names:
                            normalized_sheet = normalize_store_name(sheet)
                            if (sheet == store_name or 
                                normalized_sheet == normalized_store or
                                store_name in sheet or sheet in store_name or
                                normalized_store in normalized_sheet or 
                                normalized_sheet in normalized_store):
                                sheet_name = sheet
                                break
                        
                        # 如果没找到匹配的，使用第一个工作表
                        if not sheet_name and excel_file.sheet_names:
                            sheet_name = excel_file.sheet_names[0]
                        
                        if sheet_name:
                            df = pd.read_excel(io.BytesIO(excel_data), sheet_name=sheet_name)
                            
                            debug_logger.log('INFO', f'原始文件解析成功: {store_name}', {
                                'sheet_name': sheet_name,
                                'rows': len(df),
                                'cols': len(df.columns)
                            })
                        else:
                            debug_logger.log('ERROR', f'未找到合适的工作表: {store_name}')
                            continue
                
                except Exception as e:
                    debug_logger.log('ERROR', f'原始文件解析失败: {store_name}, 错误: {str(e)}')
                    logger.error(f"原始文件解析失败 {store_name}: {str(e)}")
                    continue
            
            # 成功加载数据
            if df is not None:
                reports_dict[store_name] = df
                debug_logger.log('INFO', f'报表 {store_name} 加载成功', {
                    'final_rows': len(df),
                    'final_cols': len(df.columns),
                    'load_method': 'parsed_data' if has_parsed_data and parsed_filename else 'raw_file'
                })
            else:
                debug_logger.log('ERROR', f'报表 {store_name} 加载完全失败')
        
        debug_logger.log('INFO', f'报表数据加载完成: {len(reports_dict)} 个门店')
        logger.info(f"报表数据加载完成: {len(reports_dict)} 个门店")
        
        # 设置缓存
        set_cache(cache_key, reports_dict)
        return reports_dict
    
    try:
        return retry_operation(_load_operation)
    except Exception as e:
        debug_logger.log('ERROR', f'加载报表数据失败: {str(e)}')
        logger.error(f"加载报表数据失败: {str(e)}")
        return {}

def analyze_receivable_data(df: pd.DataFrame) -> Dict[str, Any]:
    """分析应收未收额数据 - 专门查找第69行"""
    result = {}
    
    try:
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
            
            debug_logger.log('INFO', '分析第69行应收-未收额', {
                'first_col_value': first_col_value,
                'row_data_sample': [str(row.iloc[i]) if pd.notna(row.iloc[i]) else '' for i in range(min(5, len(row)))]
            })
            
            # 检查关键词
            keywords = ['应收-未收额', '应收未收额', '应收-未收', '应收未收']
            
            for keyword in keywords:
                if keyword in first_col_value:
                    debug_logger.log('INFO', f'找到关键词: {keyword}')
                    
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
                                    
                                    debug_logger.log('INFO', '成功分析应收-未收额', {
                                        'amount': amount,
                                        'column_name': str(df.columns[col_idx]),
                                        'column_index': col_idx
                                    })
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
                                            
                                            debug_logger.log('INFO', f'备用查找成功: 第{idx+1}行', {
                                                'amount': amount,
                                                'row_name': row_name
                                            })
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
        
        debug_logger.log('WARNING', '未找到应收-未收额数据', result['debug_info'])
        
    except Exception as e:
        debug_logger.log('ERROR', f'分析应收-未收额数据时出错: {str(e)}')
        logger.warning(f"分析应收-未收额数据时出错: {str(e)}")
    
    return result

def verify_user_permission(store_name: str, user_id: str, permissions_data: Optional[pd.DataFrame]) -> bool:
    """验证用户权限"""
    try:
        if permissions_data is None or len(permissions_data.columns) < 2:
            debug_logger.log('WARNING', '权限数据无效')
            return False
        
        store_col = permissions_data.columns[0]
        id_col = permissions_data.columns[1]
        
        normalized_target_store = normalize_store_name(store_name)
        user_id_str = str(user_id).strip()
        
        for _, row in permissions_data.iterrows():
            stored_store = str(row[store_col]).strip()
            stored_id = str(row[id_col]).strip()
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
            'normalized_store': normalized_target_store
        })
        return False
        
    except Exception as e:
        debug_logger.log('ERROR', f'权限验证异常: {str(e)}')
        logger.error(f"权限验证失败: {str(e)}")
        return False

def find_matching_reports(store_name: str, reports_data: Dict[str, pd.DataFrame]) -> List[str]:
    """查找匹配的报表"""
    try:
        matching = []
        normalized_target = normalize_store_name(store_name)
        
        for sheet_name in reports_data.keys():
            normalized_sheet = normalize_store_name(sheet_name)
            
            if (store_name == sheet_name or
                normalized_target == normalized_sheet or
                store_name in sheet_name or sheet_name in store_name or
                normalized_target in normalized_sheet or normalized_sheet in normalized_target):
                matching.append(sheet_name)
        
        debug_logger.log('INFO', f'查找匹配报表: {store_name}', {
            'matching_reports': matching,
            'total_reports': len(reports_data)
        })
        
        return matching
        
    except Exception as e:
        debug_logger.log('ERROR', f'查找匹配报表失败: {str(e)}')
        logger.error(f"查找匹配报表失败: {str(e)}")
        return []

def get_original_file_for_download(store_name: str, cos_manager: TencentCOSManager) -> Optional[bytes]:
    """获取门店的原始Excel文件用于下载"""
    try:
        # 获取元数据
        metadata = cos_manager.download_json(cos_manager.metadata_file)
        if not metadata or 'reports' not in metadata:
            return None
        
        # 查找匹配的报表
        normalized_target = normalize_store_name(store_name)
        matching_report = None
        
        for report in metadata['reports']:
            report_store_name = report.get('store_name', '').strip()
            normalized_report = normalize_store_name(report_store_name)
            
            if (report_store_name == store_name or 
                normalized_report == normalized_target or
                store_name in report_store_name or 
                report_store_name in store_name or
                normalized_target in normalized_report or
                normalized_report in normalized_target):
                matching_report = report
                break
        
        if not matching_report:
            debug_logger.log('WARNING', f'未找到门店 {store_name} 的原始文件')
            return None
        
        raw_filename = matching_report.get('raw_filename')
        if not raw_filename:
            debug_logger.log('WARNING', f'门店 {store_name} 没有原始文件记录')
            return None
        
        # 下载原始文件
        original_data = cos_manager.download_file(raw_filename, decompress=True)
        if original_data:
            debug_logger.log('INFO', f'原始文件下载成功: {store_name}', {
                'filename': raw_filename,
                'size': len(original_data)
            })
            return original_data
        else:
            debug_logger.log('ERROR', f'原始文件下载失败: {store_name}')
            return None
            
    except Exception as e:
        debug_logger.log('ERROR', f'获取原始文件失败: {store_name}, 错误: {str(e)}')
        logger.error(f"获取原始文件失败 {store_name}: {str(e)}")
        return None
    """查找匹配的报表"""
    try:
        matching = []
        normalized_target = normalize_store_name(store_name)
        
        for sheet_name in reports_data.keys():
            normalized_sheet = normalize_store_name(sheet_name)
            
            if (store_name == sheet_name or
                normalized_target == normalized_sheet or
                store_name in sheet_name or sheet_name in store_name or
                normalized_target in normalized_sheet or normalized_sheet in normalized_target):
                matching.append(sheet_name)
        
        debug_logger.log('INFO', f'查找匹配报表: {store_name}', {
            'matching_reports': matching,
            'total_reports': len(reports_data)
        })
        
        return matching
        
    except Exception as e:
        debug_logger.log('ERROR', f'查找匹配报表失败: {str(e)}')
        logger.error(f"查找匹配报表失败: {str(e)}")
        return []

def show_status_message(message: str, status_type: str = "info"):
    """显示状态消息"""
    css_class = f"status-{status_type}"
    st.markdown(f'<div class="{css_class}">{message}</div>', unsafe_allow_html=True)

def show_debug_info():
    """显示调试信息"""
    if st.session_state.get('debug_mode', False):
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

# ===================== 会话状态初始化 =====================
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
if 'store_name' not in st.session_state:
    st.session_state.store_name = ""
if 'user_id' not in st.session_state:
    st.session_state.user_id = ""
if 'is_admin' not in st.session_state:
    st.session_state.is_admin = False
if 'cos_manager' not in st.session_state:
    st.session_state.cos_manager = None
if 'operation_status' not in st.session_state:
    st.session_state.operation_status = []
if 'debug_mode' not in st.session_state:
    st.session_state.debug_mode = False

# ===================== 主程序 =====================
# 主标题
st.markdown('<h1 class="main-header">📊 门店报表查询系统</h1>', unsafe_allow_html=True)

# 初始化腾讯云COS客户端
if not st.session_state.cos_manager:
    try:
        with st.spinner("连接腾讯云COS..."):
            cos_manager = get_tencent_cos_client()
            st.session_state.cos_manager = cos_manager
            show_status_message("✅ 腾讯云COS连接成功！", "success")
            debug_logger.log('INFO', '系统初始化成功')
    except Exception as e:
        show_status_message(f"❌ 连接失败: {str(e)}", "error")
        debug_logger.log('ERROR', f'系统初始化失败: {str(e)}')
        st.stop()

cos_manager = st.session_state.cos_manager

# 显示系统状态和调试信息
try:
    storage_stats = cos_manager.get_storage_stats()
    
    st.markdown(f'''
    <div class="system-stats">
    <h4>☁️ 腾讯云COS状态监控</h4>
    <div style="display: flex; justify-content: space-between; flex-wrap: wrap;">
        <div style="text-align: center; flex: 1; min-width: 120px;">
            <div style="font-size: 1.5rem; font-weight: bold;">
                {storage_stats.get('total_files', 0)}
            </div>
            <div style="font-size: 0.9rem;">总文件数</div>
        </div>
        <div style="text-align: center; flex: 1; min-width: 120px;">
            <div style="font-size: 1.5rem; font-weight: bold;">
                {storage_stats.get('total_size_gb', 0):.1f}GB
            </div>
            <div style="font-size: 0.9rem;">存储使用</div>
        </div>
        <div style="text-align: center; flex: 1; min-width: 120px;">
            <div style="font-size: 1.5rem; font-weight: bold;">
                {storage_stats.get('usage_percent', 0):.1f}%
            </div>
            <div style="font-size: 0.9rem;">使用率</div>
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
    
except Exception as e:
    show_status_message(f"❌ 获取状态失败: {str(e)}", "error")

# 显示操作状态
for status in st.session_state.operation_status:
    show_status_message(status['message'], status['type'])

# 侧边栏
with st.sidebar:
    st.title("⚙️ 系统功能")
    
    # 系统状态
    st.subheader("📡 系统状态")
    if cos_manager:
        st.success("🟢 腾讯云COS已连接")
    else:
        st.error("🔴 腾讯云COS断开")
    
    # 调试模式
    st.session_state.debug_mode = st.checkbox("🔍 调试模式", value=st.session_state.debug_mode)
    
    user_type = st.radio("选择用户类型", ["普通用户", "管理员"])
    
    if user_type == "管理员":
        st.subheader("🔐 管理员登录")
        admin_password = st.text_input("管理员密码", type="password")
        
        if st.button("验证管理员身份"):
            if admin_password == ADMIN_PASSWORD:
                st.session_state.is_admin = True
                show_status_message("✅ 管理员验证成功！", "success")
                debug_logger.log('INFO', '管理员登录成功')
                st.rerun()
            else:
                show_status_message("❌ 密码错误！", "error")
                debug_logger.log('WARNING', '管理员登录失败：密码错误')
        
        if st.session_state.is_admin:
            st.subheader("📁 文件管理")
            
            # 上传权限表
            permissions_file = st.file_uploader("上传门店权限表", type=['xlsx', 'xls'])
            if permissions_file:
                try:
                    with st.spinner("处理权限表文件..."):
                        df = pd.read_excel(permissions_file)
                        if len(df.columns) >= 2:
                            with st.spinner("保存到腾讯云COS..."):
                                if save_permissions_to_cos(df, cos_manager):
                                    show_status_message(f"✅ 权限表已上传：{len(df)} 个用户", "success")
                                    st.balloons()
                                else:
                                    show_status_message("❌ 保存失败", "error")
                        else:
                            show_status_message("❌ 格式错误：需要至少两列（门店名称、人员编号）", "error")
                except Exception as e:
                    show_status_message(f"❌ 处理失败：{str(e)}", "error")
            
            # 上传财务报表
            reports_file = st.file_uploader("上传财务报表", type=['xlsx', 'xls'])
            if reports_file:
                try:
                    with st.spinner("处理报表文件..."):
                        # 获取原始文件数据
                        original_file_data = reports_file.getvalue()
                        
                        excel_file = pd.ExcelFile(reports_file)
                        reports_dict = {}
                        
                        for sheet in excel_file.sheet_names:
                            try:
                                df = pd.read_excel(reports_file, sheet_name=sheet)
                                if not df.empty:
                                    reports_dict[sheet] = df
                                    debug_logger.log('INFO', f'读取工作表 "{sheet}": {len(df)} 行')
                                    logger.info(f"读取工作表 '{sheet}': {len(df)} 行")
                            except Exception as e:
                                debug_logger.log('WARNING', f'跳过工作表 "{sheet}": {str(e)}')
                                logger.warning(f"跳过工作表 '{sheet}': {str(e)}")
                                continue
                        
                        if reports_dict:
                            with st.spinner("保存到腾讯云COS..."):
                                if save_reports_to_cos(reports_dict, cos_manager, original_file_data):
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
            cache_count = len([key for key in st.session_state.keys() if key.startswith('cache_')])
            st.info(f"当前缓存项目: {cache_count}")
            
            if st.button("清除所有缓存"):
                cache_keys = [key for key in st.session_state.keys() if key.startswith('cache_')]
                for key in cache_keys:
                    del st.session_state[key]
                show_status_message("✅ 缓存已清除", "success")
                debug_logger.log('INFO', '缓存已清除')
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
                debug_logger.log('INFO', '用户退出登录')
                st.rerun()

# 清除状态消息
st.session_state.operation_status = []

# 主界面
if user_type == "管理员" and st.session_state.is_admin:
    st.markdown('<div class="admin-panel"><h3>👨‍💼 管理员控制面板</h3><p>数据永久保存在腾讯云COS，支持高效压缩存储和缓存机制</p></div>', unsafe_allow_html=True)
    
    try:
        with st.spinner("加载数据统计..."):
            permissions_data = load_permissions_from_cos(cos_manager)
            reports_data = load_reports_from_cos(cos_manager)
        
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
            
            # 显示存储方式统计
            total_reports = len(reports_data)
            parsed_count = 0
            raw_only_count = 0
            
            # 获取元数据统计
            try:
                metadata = cos_manager.download_json(cos_manager.metadata_file)
                if metadata and 'reports' in metadata:
                    for report in metadata['reports']:
                        if report.get('has_parsed_data', False):
                            parsed_count += 1
                        else:
                            raw_only_count += 1
            except:
                pass
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("总报表数", total_reports)
            with col2:
                st.metric("预解析可用", parsed_count, f"{parsed_count/total_reports*100:.1f}%" if total_reports > 0 else "0%")
            with col3:
                st.metric("仅原始文件", raw_only_count, f"{raw_only_count/total_reports*100:.1f}%" if total_reports > 0 else "0%")
            
            # 显示报表详情
            report_names = list(reports_data.keys())[:5]  # 显示前5个
            for name in report_names:
                with st.expander(f"📋 {name}"):
                    df = reports_data[name]
                    st.write(f"数据规模: {len(df)} 行 × {len(df.columns)} 列")
                    
                    # 显示存储状态
                    try:
                        metadata = cos_manager.download_json(cos_manager.metadata_file)
                        if metadata and 'reports' in metadata:
                            for report in metadata['reports']:
                                if report.get('store_name') == name:
                                    has_parsed = report.get('has_parsed_data', False)
                                    has_raw = bool(report.get('raw_filename'))
                                    
                                    status_info = []
                                    if has_raw:
                                        status_info.append("✅ 原始文件")
                                    if has_parsed:
                                        status_info.append("⚡ 预解析数据")
                                    
                                    if status_info:
                                        st.info(f"存储状态: {' + '.join(status_info)}")
                                    break
                    except:
                        pass
                    
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
                permissions_data = load_permissions_from_cos(cos_manager)
            
            if permissions_data is None:
                st.warning("⚠️ 系统维护中，请联系管理员")
            else:
                stores = sorted(permissions_data[permissions_data.columns[0]].unique().tolist())
                
                with st.form("login_form"):
                    selected_store = st.selectbox("选择门店", stores)
                    user_id = st.text_input("人员编号")
                    submit = st.form_submit_button("🚀 登录")
                    
                    if submit and selected_store and user_id:
                        debug_logger.log('INFO', '用户尝试登录', {
                            'store': selected_store,
                            'user_id': user_id
                        })
                        
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
        st.markdown(f'<div class="store-info"><h3>🏪 {st.session_state.store_name}</h3><p>操作员：{st.session_state.user_id}</p><p>数据来源：腾讯云COS</p></div>', unsafe_allow_html=True)
        
        # 数据刷新按钮
        col1, col2 = st.columns([4, 1])
        with col2:
            if st.button("🔄 刷新数据"):
                debug_logger.log('INFO', '用户手动刷新数据')
                st.rerun()
        
        try:
            with st.spinner("加载报表数据..."):
                reports_data = load_reports_from_cos(cos_manager)
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
                                    <div style="background: linear-gradient(135deg, #a8edea 0%, #fed6e3 100%); color: #2e7d32; padding: 2rem; border-radius: 15px; text-align: center; box-shadow: 0 8px 20px rgba(0, 0, 0, 0.15);">
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
                
                col1, col2, col3 = st.columns(3)
                
                # 下载处理后的Excel
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
                            "📊 下载处理后Excel",
                            buffer.getvalue(),
                            f"{st.session_state.store_name}_处理数据_{datetime.now().strftime('%Y%m%d')}.xlsx",
                            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                    except Exception as e:
                        show_status_message(f"处理后Excel下载准备失败：{str(e)}", "error")
                
                # 下载原始Excel文件
                with col2:
                    try:
                        original_data = get_original_file_for_download(st.session_state.store_name, cos_manager)
                        if original_data:
                            st.download_button(
                                "📄 下载原始Excel",
                                original_data,
                                f"{st.session_state.store_name}_原始文件_{datetime.now().strftime('%Y%m%d')}.xlsx",
                                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                            )
                        else:
                            st.error("原始文件不可用")
                    except Exception as e:
                        show_status_message(f"原始文件下载失败：{str(e)}", "error")
                
                # 下载CSV格式
                with col3:
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
                            "📋 下载CSV格式",
                            csv,
                            f"{st.session_state.store_name}_报表_{datetime.now().strftime('%Y%m%d')}.csv",
                            "text/csv"
                        )
                    except Exception as e:
                        show_status_message(f"CSV下载准备失败：{str(e)}", "error")
            
            else:
                st.error(f"❌ 未找到门店 '{st.session_state.store_name}' 的报表")
                
                if st.session_state.debug_mode:
                    with st.expander("🔍 调试信息"):
                        st.write("**可用报表列表:**")
                        available_stores = list(reports_data.keys())
                        st.write(available_stores)
                        st.write(f"**查询门店:** '{st.session_state.store_name}'")
                        st.write(f"**标准化查询:** '{normalize_store_name(st.session_state.store_name)}'")
                
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
    st.caption("☁️ 腾讯云COS存储")
with col4:
    st.caption(f"🔧 版本: v2.1 (COS版) | API: {API_RATE_LIMIT}/h")
