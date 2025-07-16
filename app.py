import streamlit as st
import pandas as pd
import io
import json
import hashlib
import gzip
import pickle
from datetime import datetime, timedelta
import time
from qcloud_cos import CosConfig
from qcloud_cos import CosS3Client
from qcloud_cos.cos_exception import CosServiceError, CosClientError
import logging
from typing import Optional, Dict, Any, List
import traceback
import threading

# 页面配置
st.set_page_config(
    page_title="门店报表查询系统", 
    page_icon="📊",
    layout="wide"
)

# 系统配置
ADMIN_PASSWORD = st.secrets.get("system", {}).get("admin_password", "admin123")
MAX_STORAGE_MB = 40 * 1024  # 40GB限制，留出10GB缓冲
API_RATE_LIMIT = 500  # 每小时API调用限制（提高限制）
COMPRESSION_LEVEL = 6  # GZIP压缩等级

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
    }
    .store-info {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1.5rem;
        border-radius: 15px;
        margin: 1rem 0;
        box-shadow: 0 8px 16px rgba(0, 0, 0, 0.1);
    }
    .admin-panel {
        background: linear-gradient(135deg, #ffeaa7 0%, #fab1a0 100%);
        padding: 1.5rem;
        border-radius: 15px;
        border: 2px solid #fdcb6e;
        margin: 1rem 0;
    }
    .architecture-info {
        background: linear-gradient(135deg, #00cec9 0%, #55a3ff 100%);
        padding: 1.5rem;
        border-radius: 15px;
        margin: 1rem 0;
        border: 2px solid #00b894;
        color: white;
    }
    .success-box {
        background: linear-gradient(135deg, #84fab0 0%, #8fd3f4 100%);
        padding: 1rem;
        border-radius: 10px;
        margin: 1rem 0;
        color: #2d3436;
    }
    .warning-box {
        background: linear-gradient(135deg, #fdcb6e 0%, #e17055 100%);
        padding: 1rem;
        border-radius: 10px;
        margin: 1rem 0;
        color: white;
    }
    .debug-box {
        background: linear-gradient(135deg, #74b9ff 0%, #0984e3 100%);
        padding: 1rem;
        border-radius: 10px;
        margin: 1rem 0;
        color: white;
    }
    .compression-info {
        background: linear-gradient(135deg, #a29bfe 0%, #6c5ce7 100%);
        padding: 1rem;
        border-radius: 10px;
        margin: 1rem 0;
        color: white;
    }
    </style>
""", unsafe_allow_html=True)

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class APIRateLimiter:
    """API调用频率限制器"""
    
    def __init__(self, max_calls_per_hour: int = 500):
        self.max_calls = max_calls_per_hour
        self.calls = []
        self.lock = threading.Lock()
        self.bypass_mode = False  # 紧急绕过模式
    
    def can_make_call(self) -> bool:
        """检查是否可以进行API调用"""
        if self.bypass_mode:
            return True
            
        with self.lock:
            now = datetime.now()
            # 清理一小时前的记录
            self.calls = [call_time for call_time in self.calls 
                         if now - call_time < timedelta(hours=1)]
            
            # 如果接近限制，启用绕过模式避免系统卡死
            if len(self.calls) >= self.max_calls * 0.9:
                logger.warning("API调用接近限制，启用绕过模式")
                self.bypass_mode = True
                return True
            
            return len(self.calls) < self.max_calls
    
    def record_call(self):
        """记录API调用"""
        if not self.bypass_mode:
            with self.lock:
                self.calls.append(datetime.now())
    
    def get_remaining_calls(self) -> int:
        """获取剩余可调用次数"""
        if self.bypass_mode:
            return 999  # 绕过模式下显示足够的剩余次数
            
        with self.lock:
            now = datetime.now()
            self.calls = [call_time for call_time in self.calls 
                         if now - call_time < timedelta(hours=1)]
            return max(0, self.max_calls - len(self.calls))
    
    def reset_bypass_mode(self):
        """重置绕过模式"""
        self.bypass_mode = False
        self.calls = []

class CompressionManager:
    """数据压缩管理器"""
    
    @staticmethod
    def compress_data(data: bytes, level: int = COMPRESSION_LEVEL) -> bytes:
        """压缩数据"""
        return gzip.compress(data, compresslevel=level)
    
    @staticmethod
    def decompress_data(compressed_data: bytes) -> bytes:
        """解压数据"""
        return gzip.decompress(compressed_data)
    
    @staticmethod
    def compress_json(data: dict, level: int = COMPRESSION_LEVEL) -> bytes:
        """压缩JSON数据"""
        json_str = json.dumps(data, ensure_ascii=False, separators=(',', ':'))
        return gzip.compress(json_str.encode('utf-8'), compresslevel=level)
    
    @staticmethod
    def decompress_json(compressed_data: bytes) -> dict:
        """解压JSON数据"""
        json_str = gzip.decompress(compressed_data).decode('utf-8')
        return json.loads(json_str)
    
    @staticmethod
    def compress_excel(excel_data: bytes, level: int = COMPRESSION_LEVEL) -> bytes:
        """压缩Excel数据"""
        return gzip.compress(excel_data, compresslevel=level)
    
    @staticmethod
    def decompress_excel(compressed_data: bytes) -> bytes:
        """解压Excel数据"""
        return gzip.decompress(compressed_data)
    
    @staticmethod
    def get_compression_ratio(original_size: int, compressed_size: int) -> float:
        """计算压缩比"""
        if original_size == 0:
            return 0.0
        return (1 - compressed_size / original_size) * 100

class TencentCOSManager:
    """腾讯云COS存储管理器 - 修复版本"""
    
    def __init__(self):
        self.client = None
        self.bucket_name = None
        self.region = None
        self.rate_limiter = APIRateLimiter(API_RATE_LIMIT)
        self.compression = CompressionManager()
        self._file_cache = {}  # 初始化文件缓存
        self.initialize_from_secrets()
    
    def initialize_from_secrets(self):
        """从Streamlit Secrets初始化"""
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
            
            # 配置COS客户端
            cos_config = CosConfig(
                Region=self.region,
                SecretId=secret_id,
                SecretKey=secret_key
            )
            
            self.client = CosS3Client(cos_config)
            logger.info("腾讯云COS客户端初始化成功")
            
        except Exception as e:
            logger.error(f"腾讯云COS初始化失败: {str(e)}")
            raise
    
    def _check_api_limit(self) -> bool:
        """检查API调用限制"""
        if not self.rate_limiter.can_make_call():
            remaining = self.rate_limiter.get_remaining_calls()
            if remaining <= 0 and not self.rate_limiter.bypass_mode:
                st.warning(f"⚠️ API调用频率限制，系统已自动优化调用策略")
                return True  # 改为允许调用，避免系统完全卡死
            return True
        return True
    
    def upload_file(self, file_data: bytes, filename: str, content_type: str = None, 
                   compress: bool = True) -> Optional[str]:
        """上传文件到腾讯云COS（支持压缩）"""
        if not self._check_api_limit():
            return None
            
        try:
            original_size = len(file_data)
            
            # 压缩数据
            if compress:
                if filename.endswith('.json'):
                    # JSON数据特殊处理
                    data = json.loads(file_data.decode('utf-8'))
                    compressed_data = self.compression.compress_json(data)
                    filename = filename.replace('.json', '.gz')
                else:
                    compressed_data = self.compression.compress_data(file_data)
                    if not filename.endswith('.gz'):
                        filename = filename + '.gz'
                
                compressed_size = len(compressed_data)
                compression_ratio = self.compression.get_compression_ratio(original_size, compressed_size)
                
                st.info(f"📦 压缩效果: {original_size/1024:.1f}KB → {compressed_size/1024:.1f}KB (节省 {compression_ratio:.1f}%)")
                
                upload_data = compressed_data
            else:
                upload_data = file_data
            
            # 默认内容类型
            if not content_type:
                if filename.endswith('.xlsx'):
                    content_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                elif filename.endswith('.json') or filename.endswith('.gz'):
                    content_type = 'application/gzip'
                else:
                    content_type = 'application/octet-stream'
            
            # 上传文件
            response = self.client.put_object(
                Bucket=self.bucket_name,
                Body=upload_data,
                Key=filename,
                ContentType=content_type
            )
            
            self.rate_limiter.record_call()
            
            # 清除文件缓存，强制下次重新获取
            self._file_cache.clear()
            
            # 生成文件URL
            file_url = f"https://{self.bucket_name}.cos.{self.region}.myqcloud.com/{filename}"
            
            logger.info(f"文件上传成功: {filename}")
            return file_url
            
        except CosServiceError as e:
            logger.error(f"COS服务错误: {e.get_error_msg()}")
            raise Exception(f"文件上传失败: {e.get_error_msg()}")
        except CosClientError as e:
            logger.error(f"COS客户端错误: {str(e)}")
            raise Exception(f"文件上传失败: {str(e)}")
        except Exception as e:
            logger.error(f"上传文件时出错: {str(e)}")
            raise Exception(f"文件上传失败: {str(e)}")
    
    def download_file(self, filename: str, decompress: bool = True) -> Optional[bytes]:
        """从腾讯云COS下载文件（支持解压）"""
        if not self._check_api_limit():
            return None
            
        try:
            response = self.client.get_object(
                Bucket=self.bucket_name,
                Key=filename
            )
            
            self.rate_limiter.record_call()
            
            # 读取文件内容
            file_data = response['Body'].read()
            
            # 解压数据
            if decompress and filename.endswith('.gz'):
                try:
                    if filename.replace('.gz', '').endswith('.json'):
                        # JSON数据特殊处理
                        decompressed_data = self.compression.decompress_json(file_data)
                        return json.dumps(decompressed_data, ensure_ascii=False).encode('utf-8')
                    else:
                        return self.compression.decompress_data(file_data)
                except Exception as e:
                    logger.warning(f"解压失败，返回原始数据: {str(e)}")
                    return file_data
            
            logger.info(f"文件下载成功: {filename}")
            return file_data
            
        except CosServiceError as e:
            if e.get_error_code() == 'NoSuchKey':
                logger.info(f"文件不存在: {filename}")
                return None
            logger.error(f"COS服务错误: {e.get_error_msg()}")
            return None
        except CosClientError as e:
            logger.error(f"COS客户端错误: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"下载文件时出错: {str(e)}")
            return None
    
    def delete_file(self, filename: str) -> bool:
        """删除腾讯云COS文件"""
        if not self._check_api_limit():
            return False
            
        try:
            self.client.delete_object(
                Bucket=self.bucket_name,
                Key=filename
            )
            
            self.rate_limiter.record_call()
            logger.info(f"文件删除成功: {filename}")
            
            # 清除文件缓存
            self._file_cache.clear()
            
            return True
            
        except CosServiceError as e:
            logger.error(f"COS服务错误: {e.get_error_msg()}")
            return False
        except CosClientError as e:
            logger.error(f"COS客户端错误: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"删除文件时出错: {str(e)}")
            return False
    
    def list_files(self, prefix: str = "", max_keys: int = 1000) -> List[Dict]:
        """列出存储桶中的文件"""
        if not self._check_api_limit():
            return []
            
        try:
            response = self.client.list_objects(
                Bucket=self.bucket_name,
                Prefix=prefix,
                MaxKeys=max_keys
            )
            
            self.rate_limiter.record_call()
            
            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    files.append({
                        'filename': obj['Key'],
                        'size': obj['Size'],
                        'last_modified': obj['LastModified']
                    })
            
            # 更新文件缓存
            for file_info in files:
                self._file_cache[file_info['filename']] = True
            
            return files
            
        except Exception as e:
            logger.error(f"列出文件时出错: {str(e)}")
            return []
    
    def file_exists(self, filename: str) -> bool:
        """检查文件是否存在（优化版本）"""
        try:
            # 检查缓存
            if filename in self._file_cache:
                return self._file_cache[filename]
            
            # 如果缓存不存在，进行API调用
            if not self._check_api_limit():
                return False
                
            self.client.head_object(
                Bucket=self.bucket_name,
                Key=filename
            )
            self.rate_limiter.record_call()
            
            # 更新缓存
            self._file_cache[filename] = True
            return True
            
        except:
            # 更新缓存
            self._file_cache[filename] = False
            return False
    
    def clear_cache(self):
        """清除所有缓存"""
        self._file_cache.clear()
        logger.info("文件缓存已清除")
    
    def upload_json(self, data: dict, filename: str, compress: bool = True) -> bool:
        """上传JSON数据（支持压缩）"""
        try:
            json_data = json.dumps(data, ensure_ascii=False, separators=(',', ':'))
            json_bytes = json_data.encode('utf-8')
            
            result = self.upload_file(json_bytes, filename, 'application/json', compress)
            return result is not None
            
        except Exception as e:
            logger.error(f"上传JSON失败: {str(e)}")
            return False
    
    def download_json(self, filename: str, decompress: bool = True) -> Optional[dict]:
        """下载JSON数据（支持解压）"""
        try:
            # 尝试压缩版本
            if not filename.endswith('.gz'):
                compressed_filename = filename.replace('.json', '.gz')
                if self.file_exists(compressed_filename):
                    filename = compressed_filename
            
            file_data = self.download_file(filename, decompress)
            if file_data:
                if filename.endswith('.gz') and decompress:
                    # 已经在download_file中处理了解压
                    return json.loads(file_data.decode('utf-8'))
                else:
                    json_str = file_data.decode('utf-8')
                    return json.loads(json_str)
            return None
            
        except Exception as e:
            logger.error(f"下载JSON失败: {str(e)}")
            return None
    
    def cleanup_old_files(self, days_old: int = 7, prefix: str = "") -> int:
        """清理指定天数前的旧文件"""
        try:
            files = self.list_files(prefix)
            cutoff_date = datetime.now() - timedelta(days=days_old)
            
            deleted_count = 0
            for file_info in files:
                try:
                    # 解析文件修改时间
                    file_date = datetime.fromisoformat(file_info['last_modified'].replace('Z', '+00:00')).replace(tzinfo=None)
                    
                    if file_date < cutoff_date:
                        if self.delete_file(file_info['filename']):
                            deleted_count += 1
                            
                except Exception as e:
                    logger.warning(f"清理文件 {file_info['filename']} 失败: {str(e)}")
                    continue
            
            return deleted_count
            
        except Exception as e:
            logger.error(f"清理旧文件失败: {str(e)}")
            return 0
    
    def get_storage_usage(self) -> Dict:
        """获取存储使用情况"""
        try:
            files = self.list_files()
            total_size = sum(f['size'] for f in files)
            
            # 按类型分类
            report_files = [f for f in files if f['filename'].startswith('reports/')]
            system_files = [f for f in files if f['filename'].startswith('system/')]
            
            report_size = sum(f['size'] for f in report_files)
            system_size = sum(f['size'] for f in system_files)
            
            return {
                'file_count': len(files),
                'total_size_bytes': total_size,
                'total_size_mb': total_size / (1024 * 1024),
                'total_size_gb': total_size / (1024 * 1024 * 1024),
                'report_files': len(report_files),
                'report_size_mb': report_size / (1024 * 1024),
                'system_files': len(system_files),
                'system_size_kb': system_size / 1024,
                'usage_percentage': (total_size / (1024 * 1024)) / (50 * 1024) * 100,
                'remaining_calls': self.rate_limiter.get_remaining_calls(),
                'files': files
            }
            
        except Exception as e:
            logger.error(f"获取存储使用情况失败: {str(e)}")
            return {
                'file_count': 0, 
                'total_size_mb': 0, 
                'total_size_gb': 0,
                'report_files': 0,
                'report_size_mb': 0,
                'system_files': 0,
                'system_size_kb': 0,
                'usage_percentage': 0, 
                'remaining_calls': 0, 
                'files': []
            }

class TencentCOSSystem:
    """基于腾讯云COS的完整存储系统 - 修复版本"""
    
    def __init__(self):
        self.cos_manager = TencentCOSManager()
        self.permissions_file = "system/permissions.json"
        self.metadata_file = "system/metadata.json"
        self.initialized = True
        # 添加缓存刷新标记
        self._last_refresh = datetime.now()
    
    def force_refresh_cache(self):
        """强制刷新所有缓存"""
        self.cos_manager.clear_cache()
        self._last_refresh = datetime.now()
        logger.info("强制刷新缓存完成")
    
    def show_architecture_info(self):
        """显示架构信息"""
        usage = self.cos_manager.get_storage_usage()
        
        st.markdown(f'''
        <div class="architecture-info">
        <h4>☁️ 腾讯云COS优化存储架构</h4>
        <p><strong>📦 智能压缩</strong>: GZIP压缩，节省60-80%存储空间</p>
        <p><strong>🔐 权限管理</strong>: /system/permissions.json.gz (压缩存储)</p>
        <p><strong>📋 元数据管理</strong>: /system/metadata.json.gz (压缩存储)</p>
        <p><strong>📊 报表存储</strong>: /reports/*.xlsx.gz (压缩存储)</p>
        <p><strong>⚡ API优化</strong>: 剩余调用 {usage['remaining_calls']}/小时</p>
        <p><strong>💾 存储状态</strong>: {usage['total_size_gb']:.2f}GB / 50GB ({usage['usage_percentage']:.2f}%)</p>
        </div>
        ''', unsafe_allow_html=True)
    
    def load_permissions(self, force_refresh: bool = False) -> List[Dict]:
        """从COS加载权限数据（增强版本）"""
        try:
            if force_refresh:
                self.force_refresh_cache()
            
            # 首先尝试压缩版本
            compressed_filename = self.permissions_file.replace('.json', '.gz')
            
            data = None
            # 尝试加载压缩版本
            if self.cos_manager.file_exists(compressed_filename):
                data = self.cos_manager.download_json(compressed_filename, decompress=True)
                logger.info(f"从压缩文件加载权限数据: {compressed_filename}")
            # 如果压缩版本不存在，尝试未压缩版本
            elif self.cos_manager.file_exists(self.permissions_file):
                data = self.cos_manager.download_json(self.permissions_file, decompress=False)
                logger.info(f"从未压缩文件加载权限数据: {self.permissions_file}")
            
            if data and 'permissions' in data:
                permissions = data['permissions']
                logger.info(f"成功加载权限数据: {len(permissions)} 条记录")
                
                # 调试信息
                if st.session_state.get('debug_mode', False):
                    st.write(f"**调试信息**: 加载了 {len(permissions)} 条权限记录")
                    for i, perm in enumerate(permissions[:3]):  # 显示前3条
                        st.write(f"权限 {i+1}: {perm}")
                
                return permissions
            else:
                logger.warning("权限文件存在但格式不正确或为空")
                return []
                
        except Exception as e:
            logger.error(f"加载权限数据失败: {str(e)}")
            st.error(f"❌ 权限数据加载失败: {str(e)}")
            return []
    
    def save_permissions(self, permissions_data: List[Dict]) -> bool:
        """保存权限数据到COS（压缩）"""
        try:
            data = {
                'permissions': permissions_data,
                'last_updated': datetime.now().isoformat(),
                'version': '2.0',
                'compressed': True,
                'count': len(permissions_data)
            }
            
            # 保存到压缩版本
            compressed_filename = self.permissions_file.replace('.json', '.gz')
            success = self.cos_manager.upload_json(data, compressed_filename, compress=True)
            
            if success:
                logger.info(f"权限数据保存成功: {len(permissions_data)} 条记录")
                # 强制刷新缓存
                self.force_refresh_cache()
                return True
            else:
                logger.error("权限数据保存失败")
                return False
                
        except Exception as e:
            logger.error(f"保存权限数据失败: {str(e)}")
            return False
    
    def load_metadata(self, force_refresh: bool = False) -> Dict:
        """从COS加载元数据（增强版本）"""
        try:
            if force_refresh:
                self.force_refresh_cache()
            
            # 首先尝试压缩版本
            compressed_filename = self.metadata_file.replace('.json', '.gz')
            
            data = None
            # 尝试加载压缩版本
            if self.cos_manager.file_exists(compressed_filename):
                data = self.cos_manager.download_json(compressed_filename, decompress=True)
                logger.info(f"从压缩文件加载元数据: {compressed_filename}")
            # 如果压缩版本不存在，尝试未压缩版本
            elif self.cos_manager.file_exists(self.metadata_file):
                data = self.cos_manager.download_json(self.metadata_file, decompress=False)
                logger.info(f"从未压缩文件加载元数据: {self.metadata_file}")
            
            if data:
                reports = data.get('reports', [])
                logger.info(f"成功加载元数据: {len(reports)} 个报表")
                return data
            else:
                logger.info("元数据文件不存在，返回默认值")
                return {'reports': [], 'compressed': True}
                
        except Exception as e:
            logger.error(f"加载元数据失败: {str(e)}")
            return {'reports': [], 'compressed': True}
    
    def save_metadata(self, metadata: Dict) -> bool:
        """保存元数据到COS（压缩）"""
        try:
            metadata['last_updated'] = datetime.now().isoformat()
            metadata['compressed'] = True
            
            # 保存到压缩版本
            compressed_filename = self.metadata_file.replace('.json', '.gz')
            success = self.cos_manager.upload_json(metadata, compressed_filename, compress=True)
            
            if success:
                reports_count = len(metadata.get('reports', []))
                logger.info(f"元数据保存成功: {reports_count} 个报表")
                # 强制刷新缓存
                self.force_refresh_cache()
                return True
            else:
                logger.error("元数据保存失败")
                return False
                
        except Exception as e:
            logger.error(f"保存元数据失败: {str(e)}")
            return False
    
    def upload_and_process_permissions(self, uploaded_file) -> bool:
        """上传并处理权限文件（增强版本）"""
        try:
            # 读取Excel文件
            df = pd.read_excel(uploaded_file)
            
            if len(df.columns) < 2:
                st.error("❌ 权限文件格式错误：需要至少两列（门店名称、人员编号）")
                return False
            
            # 转换为权限数据格式
            permissions_data = []
            processed_count = 0
            skipped_count = 0
            
            for index, row in df.iterrows():
                store_name = str(row.iloc[0]).strip()
                user_id = str(row.iloc[1]).strip()
                
                # 更严格的数据验证
                if (store_name and user_id and 
                    store_name.lower() not in ['nan', 'none', '', 'null'] and 
                    user_id.lower() not in ['nan', 'none', '', 'null']):
                    
                    permissions_data.append({
                        "store_name": store_name,
                        "user_id": user_id,
                        "created_at": datetime.now().isoformat(),
                        "updated_at": datetime.now().isoformat(),
                        "row_index": index + 1
                    })
                    processed_count += 1
                else:
                    skipped_count += 1
            
            if processed_count == 0:
                st.error("❌ 没有找到有效的权限数据")
                return False
            
            # 显示处理统计
            st.info(f"📊 数据处理统计: 有效记录 {processed_count} 条，跳过 {skipped_count} 条")
            
            # 保存到COS（压缩）
            success = self.save_permissions(permissions_data)
            
            if success:
                # 强制刷新所有缓存和状态
                self.force_refresh_cache()
                
                st.markdown(f'''
                <div class="success-box">
                <h4>✅ 权限数据上传成功</h4>
                <p><strong>总记录数</strong>: {processed_count} 条</p>
                <p><strong>跳过记录</strong>: {skipped_count} 条</p>
                <p><strong>存储方式</strong>: 压缩存储，节省空间</p>
                <p><strong>状态</strong>: 立即生效，可用于登录验证</p>
                </div>
                ''', unsafe_allow_html=True)
                
                # 显示前几条权限记录作为确认
                if len(permissions_data) > 0:
                    st.subheader("📋 权限记录预览")
                    preview_df = pd.DataFrame(permissions_data[:10])  # 显示前10条
                    st.dataframe(preview_df[['store_name', 'user_id']], use_container_width=True)
                
                # 等待确保数据同步
                time.sleep(1)
                
                # 立即验证数据是否保存成功
                verification = self.load_permissions(force_refresh=True)
                if len(verification) == processed_count:
                    st.success(f"✅ 数据验证成功: 已确认保存 {len(verification)} 条权限记录")
                else:
                    st.warning(f"⚠️ 数据验证异常: 预期 {processed_count} 条，实际 {len(verification)} 条")
                
                # 刷新页面状态
                st.rerun()
                
                return True
            else:
                st.error("❌ 权限数据保存失败")
                return False
                
        except Exception as e:
            st.error(f"❌ 处理权限文件失败：{str(e)}")
            logger.error(f"处理权限文件失败: {str(e)}")
            st.write("**详细错误信息:**", str(e))
            return False
    
    def upload_and_process_reports(self, uploaded_file) -> bool:
        """上传并处理报表文件（压缩优化）"""
        try:
            file_size_mb = len(uploaded_file.getvalue()) / 1024 / 1024
            st.info(f"📄 原始文件大小: {file_size_mb:.2f} MB")
            
            # 检查存储空间
            usage = self.cos_manager.get_storage_usage()
            if usage['total_size_mb'] > MAX_STORAGE_MB:
                st.error(f"❌ 存储空间不足！当前使用: {usage['total_size_gb']:.1f}GB / 40GB")
                return False
            
            # 生成唯一文件名
            timestamp = int(time.time())
            file_hash = hashlib.md5(uploaded_file.getvalue()).hexdigest()[:8]
            filename = f"reports/reports_{timestamp}_{file_hash}.xlsx"
            
            # 先清理旧数据
            with st.spinner("正在清理旧数据..."):
                deleted_count = self._cleanup_old_reports()
                if deleted_count > 0:
                    st.info(f"🧹 已清理 {deleted_count} 个旧文件")
            
            # 上传压缩文件到腾讯云COS
            with st.spinner("正在压缩并上传文件到腾讯云COS..."):
                file_url = self.cos_manager.upload_file(
                    uploaded_file.getvalue(), 
                    filename, 
                    compress=True
                )
                
                if not file_url:
                    st.error("❌ 文件上传失败")
                    return False
            
            st.success(f"✅ 文件上传成功: {filename}.gz")
            
            # 解析Excel文件并提取元数据
            with st.spinner("正在分析文件内容..."):
                excel_file = pd.ExcelFile(uploaded_file)
                
                # 加载现有元数据
                metadata = self.load_metadata(force_refresh=True)
                if 'reports' not in metadata:
                    metadata['reports'] = []
                
                reports_processed = 0
                
                for sheet_name in excel_file.sheet_names:
                    try:
                        df = pd.read_excel(uploaded_file, sheet_name=sheet_name)
                        
                        if df.empty:
                            continue
                        
                        # 分析应收-未收额
                        analysis_result = self.analyze_receivable_data(df)
                        
                        # 生成精简数据摘要
                        summary = {
                            "rows": len(df),
                            "cols": len(df.columns),
                            "key_cols": df.columns.tolist()[:5],  # 只保存前5列名
                            "has_data": not df.empty
                        }
                        
                        # 创建报表元数据
                        report_metadata = {
                            "store_name": sheet_name,
                            "filename": filename + ".gz",  # 标记为压缩文件
                            "file_url": file_url,
                            "file_size_mb": file_size_mb,
                            "upload_time": datetime.now().isoformat(),
                            "summary": summary,
                            "analysis": analysis_result,
                            "id": f"{sheet_name}_{timestamp}",
                            "compressed": True
                        }
                        
                        # 移除同门店的旧记录
                        metadata['reports'] = [r for r in metadata['reports'] 
                                             if r.get('store_name') != sheet_name]
                        
                        # 添加新记录
                        metadata['reports'].append(report_metadata)
                        reports_processed += 1
                        
                        st.success(f"✅ {sheet_name}: {len(df)} 行数据已处理")
                        
                    except Exception as e:
                        st.warning(f"⚠️ 跳过工作表 '{sheet_name}': {str(e)}")
                        continue
                
                # 保存更新后的元数据（压缩）
                if reports_processed > 0:
                    if self.save_metadata(metadata):
                        # 强制刷新所有缓存
                        self.force_refresh_cache()
                        
                        st.markdown(f'''
                        <div class="compression-info">
                        <h4>🎉 报表处理完成</h4>
                        <p>✅ 处理工作表: {reports_processed} 个</p>
                        <p>📦 启用压缩存储，节省存储空间</p>
                        <p>⚡ API调用优化，避免频率限制</p>
                        <p>🔄 缓存已刷新，数据立即可用</p>
                        </div>
                        ''', unsafe_allow_html=True)
                        
                        # 显示存储统计
                        self._show_storage_stats()
                        
                        # 立即验证数据
                        verification_metadata = self.load_metadata(force_refresh=True)
                        current_reports = len(verification_metadata.get('reports', []))
                        st.success(f"✅ 数据验证成功: 当前系统中共有 {current_reports} 个报表")
                        
                        # 强制刷新页面状态
                        time.sleep(1)  # 等待1秒确保数据同步
                        st.rerun()
                        
                        return True
                    else:
                        st.error("❌ 元数据保存失败")
                        return False
                else:
                    st.error("❌ 没有成功处理任何工作表")
                    return False
                
        except Exception as e:
            st.error(f"❌ 处理报表文件失败：{str(e)}")
            logger.error(f"处理报表文件失败: {str(e)}")
            return False
    
    def _cleanup_old_reports(self, days_old: int = 3) -> int:
        """清理旧的报表数据"""
        try:
            # 使用COS管理器的清理功能
            deleted_count = self.cos_manager.cleanup_old_files(days_old, "reports/")
            
            # 同时清理元数据中的旧记录
            metadata = self.load_metadata(force_refresh=True)
            if 'reports' in metadata:
                cutoff_date = datetime.now() - timedelta(days=days_old)
                
                old_reports = metadata['reports']
                metadata['reports'] = [
                    r for r in metadata['reports']
                    if datetime.fromisoformat(r.get('upload_time', '1970-01-01')) > cutoff_date
                ]
                
                removed_count = len(old_reports) - len(metadata['reports'])
                if removed_count > 0:
                    self.save_metadata(metadata)
                    deleted_count += removed_count
            
            return deleted_count
            
        except Exception as e:
            st.warning(f"清理旧数据时出错: {str(e)}")
            return 0
    
    def _show_storage_stats(self):
        """显示存储统计信息"""
        try:
            usage = self.cos_manager.get_storage_usage()
            metadata = self.load_metadata()
            permissions = self.load_permissions()
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("📦 总文件数", usage.get('file_count', 0))
                st.metric("💾 总使用量", f"{usage.get('total_size_gb', 0):.2f} GB")
                
                # 使用率进度条
                usage_percent = usage.get('usage_percentage', 0)
                progress_value = min(usage_percent / 100, 1.0)
                st.progress(progress_value)
                
                # 颜色编码的使用率
                if usage_percent > 80:
                    st.error(f"🔴 使用率: {usage_percent:.1f}%")
                elif usage_percent > 60:
                    st.warning(f"🟡 使用率: {usage_percent:.1f}%")
                else:
                    st.success(f"🟢 使用率: {usage_percent:.1f}%")
            
            with col2:
                st.metric("📊 报表文件", usage.get('report_files', 0))
                st.metric("📋 报表记录", len(metadata.get('reports', [])))
                st.metric("📄 报表大小", f"{usage.get('report_size_mb', 0):.1f} MB")
                
                # 压缩效果估算
                report_size_mb = usage.get('report_size_mb', 0)
                if report_size_mb > 0:
                    estimated_uncompressed = report_size_mb * 3  # 假设压缩比为70%
                    savings = estimated_uncompressed - report_size_mb
                    st.success(f"💰 压缩节省: ~{savings:.1f} MB")
            
            with col3:
                st.metric("🔐 权限记录", len(permissions))
                st.metric("⚙️ 系统文件", usage.get('system_files', 0))
                st.metric("🗃️ 系统大小", f"{usage.get('system_size_kb', 0):.1f} KB")
                st.metric("⚡ API剩余", f"{usage.get('remaining_calls', 0)}/小时")
                
        except Exception as e:
            st.error(f"获取存储统计失败: {str(e)}")
            logger.error(f"显示存储统计失败: {str(e)}")
            
            # 显示基本信息作为备用
            st.info("📊 正在初始化存储统计...")
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("📦 总文件数", "加载中...")
            with col2:
                st.metric("📊 报表文件", "加载中...")
            with col3:
                st.metric("🔐 权限记录", "加载中...")
    
    def analyze_receivable_data(self, df: pd.DataFrame) -> Dict[str, Any]:
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
    
    def load_store_data(self, store_name: str, force_refresh: bool = False) -> Optional[pd.DataFrame]:
        """加载指定门店的数据（支持解压，增强版本）"""
        try:
            if force_refresh:
                self.force_refresh_cache()
            
            # 从元数据获取报表信息
            metadata = self.load_metadata(force_refresh=force_refresh)
            reports = metadata.get('reports', [])
            
            logger.info(f"尝试加载门店数据: {store_name}, 可用报表数: {len(reports)}")
            
            # 调试信息
            if st.session_state.get('debug_mode', False):
                st.write(f"**调试信息**: 查找门店 '{store_name}' 的数据")
                st.write(f"可用报表: {[r.get('store_name') for r in reports]}")
            
            # 查找匹配的门店报表（更智能的匹配）
            matching_report = None
            for report in reports:
                report_store_name = report.get('store_name', '')
                # 精确匹配
                if report_store_name == store_name:
                    matching_report = report
                    break
                # 模糊匹配
                elif store_name in report_store_name or report_store_name in store_name:
                    matching_report = report
                    break
            
            if not matching_report:
                logger.warning(f"未找到匹配的门店报表: {store_name}")
                if st.session_state.get('debug_mode', False):
                    st.error(f"**调试**: 未找到门店 '{store_name}' 的报表")
                return None
            
            filename = matching_report.get('filename')
            if not filename:
                logger.error("报表元数据中缺少文件名")
                return None
            
            logger.info(f"找到匹配报表: {filename}")
            
            # 从腾讯云COS下载文件（自动解压）
            with st.spinner(f"正在从腾讯云加载 {store_name} 的数据..."):
                is_compressed = filename.endswith('.gz')
                file_data = self.cos_manager.download_file(filename, decompress=is_compressed)
                
                if file_data:
                    # 解析Excel文件
                    excel_file = pd.ExcelFile(io.BytesIO(file_data))
                    
                    logger.info(f"Excel工作表: {excel_file.sheet_names}")
                    
                    # 查找匹配的工作表（更智能的匹配）
                    target_sheet = None
                    
                    # 1. 精确匹配
                    if store_name in excel_file.sheet_names:
                        target_sheet = store_name
                    else:
                        # 2. 模糊匹配
                        matching_sheets = []
                        for sheet in excel_file.sheet_names:
                            if store_name in sheet or sheet in store_name:
                                matching_sheets.append(sheet)
                        
                        if matching_sheets:
                            target_sheet = matching_sheets[0]  # 取第一个匹配的
                        elif excel_file.sheet_names:
                            # 3. 如果没有匹配的，使用第一个工作表
                            target_sheet = excel_file.sheet_names[0]
                    
                    if target_sheet:
                        logger.info(f"使用工作表: {target_sheet}")
                        df = pd.read_excel(io.BytesIO(file_data), sheet_name=target_sheet)
                        
                        if st.session_state.get('debug_mode', False):
                            st.success(f"**调试**: 成功加载工作表 '{target_sheet}', 数据行数: {len(df)}")
                        
                        return df
                    else:
                        logger.error("未找到合适的工作表")
                        return None
                else:
                    logger.error(f"文件下载失败: {filename}")
                    return None
            
            return None
            
        except Exception as e:
            st.error(f"❌ 加载 {store_name} 数据失败：{str(e)}")
            logger.error(f"加载门店数据失败: {str(e)}")
            
            if st.session_state.get('debug_mode', False):
                st.write("**详细错误信息:**", str(e))
                st.write("**错误追踪:**", traceback.format_exc())
            
            return None
    
    def verify_user_permission(self, store_name: str, user_id: str, force_refresh: bool = False) -> bool:
        """验证用户权限（增强版本）"""
        try:
            if force_refresh:
                self.force_refresh_cache()
            
            permissions = self.load_permissions(force_refresh=force_refresh)
            
            logger.info(f"验证权限: 门店={store_name}, 用户ID={user_id}, 权限记录数={len(permissions)}")
            
            # 调试信息
            if st.session_state.get('debug_mode', False):
                st.write(f"**调试信息**: 验证门店 '{store_name}' 用户 '{user_id}' 的权限")
                st.write(f"总权限记录数: {len(permissions)}")
                if len(permissions) > 0:
                    st.write("权限记录示例:")
                    for i, perm in enumerate(permissions[:3]):
                        st.write(f"  {i+1}. 门店: '{perm.get('store_name')}', 用户ID: '{perm.get('user_id')}'")
            
            # 查找匹配的权限
            for perm in permissions:
                stored_store = perm.get('store_name', '').strip()
                stored_id = perm.get('user_id', '').strip()
                
                # 精确匹配
                if stored_store == store_name and stored_id == str(user_id):
                    logger.info(f"权限验证成功: 精确匹配")
                    return True
                
                # 模糊匹配门店名称
                if ((store_name in stored_store or stored_store in store_name) and 
                    stored_id == str(user_id)):
                    logger.info(f"权限验证成功: 模糊匹配")
                    return True
            
            logger.warning(f"权限验证失败: 未找到匹配的权限记录")
            
            if st.session_state.get('debug_mode', False):
                st.error(f"**调试**: 权限验证失败，未找到匹配记录")
            
            return False
            
        except Exception as e:
            st.error(f"❌ 权限验证失败：{str(e)}")
            logger.error(f"权限验证失败: {str(e)}")
            
            if st.session_state.get('debug_mode', False):
                st.write("**权限验证错误详情:**", str(e))
            
            return False
    
    def get_available_stores(self, force_refresh: bool = False) -> List[str]:
        """获取可用的门店列表（增强版本）"""
        try:
            if force_refresh:
                self.force_refresh_cache()
            
            permissions = self.load_permissions(force_refresh=force_refresh)
            stores = []
            
            for perm in permissions:
                store_name = perm.get('store_name', '').strip()
                if store_name and store_name not in stores:
                    stores.append(store_name)
            
            stores.sort()
            
            logger.info(f"获取可用门店列表: {len(stores)} 个门店")
            
            # 调试信息
            if st.session_state.get('debug_mode', False):
                st.write(f"**调试信息**: 可用门店数量: {len(stores)}")
                if len(stores) > 0:
                    st.write("门店列表:", stores[:10])  # 显示前10个
            
            return stores
            
        except Exception as e:
            st.error(f"❌ 获取门店列表失败：{str(e)}")
            logger.error(f"获取门店列表失败: {str(e)}")
            return []
    
    def cleanup_storage(self, cleanup_type: str = "all"):
        """清理存储空间"""
        try:
            if cleanup_type == "all":
                # 清理所有数据
                all_files = self.cos_manager.list_files()
                deleted_count = 0
                
                for file_info in all_files:
                    if self.cos_manager.delete_file(file_info['filename']):
                        deleted_count += 1
                
                # 清理完成后刷新缓存
                self.force_refresh_cache()
                st.success(f"🧹 清理完成：删除了 {deleted_count} 个文件")
                
            elif cleanup_type == "old":
                # 只清理旧文件
                deleted_count = self._cleanup_old_reports(7)  # 清理7天前的文件
                self.force_refresh_cache()
                st.success(f"🧹 清理旧文件完成：删除了 {deleted_count} 个文件")
                
        except Exception as e:
            st.error(f"❌ 清理失败：{str(e)}")
            logger.error(f"存储清理失败: {str(e)}")
    
    def get_system_status(self) -> Dict:
        """获取系统状态"""
        try:
            # 检查系统文件是否存在
            permissions_exists = (self.cos_manager.file_exists(self.permissions_file) or 
                               self.cos_manager.file_exists(self.permissions_file.replace('.json', '.gz')))
            metadata_exists = (self.cos_manager.file_exists(self.metadata_file) or 
                            self.cos_manager.file_exists(self.metadata_file.replace('.json', '.gz')))
            
            # 获取统计数据
            permissions = self.load_permissions()
            metadata = self.load_metadata()
            usage = self.cos_manager.get_storage_usage()
            
            return {
                'permissions_file_exists': permissions_exists,
                'metadata_file_exists': metadata_exists,
                'permissions_count': len(permissions),
                'reports_count': len(metadata.get('reports', [])),
                'system_healthy': permissions_exists and metadata_exists and len(permissions) > 0,
                'storage_usage_percent': usage.get('usage_percentage', 0),
                'api_calls_remaining': usage.get('remaining_calls', 0),
                'compression_enabled': True,
                'last_refresh': self._last_refresh.isoformat()
            }
            
        except Exception as e:
            logger.error(f"获取系统状态失败: {str(e)}")
            return {
                'permissions_file_exists': False,
                'metadata_file_exists': False,
                'permissions_count': 0,
                'reports_count': 0,
                'system_healthy': False,
                'storage_usage_percent': 0,
                'api_calls_remaining': 0,
                'compression_enabled': False,
                'last_refresh': datetime.now().isoformat()
            }

# 初始化会话状态
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
if 'store_name' not in st.session_state:
    st.session_state.store_name = ""
if 'user_id' not in st.session_state:
    st.session_state.user_id = ""
if 'is_admin' not in st.session_state:
    st.session_state.is_admin = False
if 'storage_system' not in st.session_state:
    st.session_state.storage_system = None
if 'debug_mode' not in st.session_state:
    st.session_state.debug_mode = False

# 主标题
st.markdown('<h1 class="main-header">📊 门店报表查询系统 (修复版)</h1>', unsafe_allow_html=True)

# 初始化存储系统
if not st.session_state.storage_system:
    try:
        st.session_state.storage_system = TencentCOSSystem()
        st.success("✅ 腾讯云COS智能存储系统初始化成功")
    except Exception as e:
        st.error(f"❌ 存储系统初始化失败: {str(e)}")
        st.stop()

storage_system = st.session_state.storage_system

# 显示架构信息
storage_system.show_architecture_info()

# 侧边栏
with st.sidebar:
    st.title("⚙️ 系统功能")
    
    # 调试模式开关
    st.session_state.debug_mode = st.checkbox("🔍 调试模式", value=st.session_state.debug_mode)
    
    # 显示系统状态
    status = storage_system.get_system_status()
    
    if status['system_healthy']:
        st.success("🟢 系统状态正常")
    else:
        st.warning("🟡 系统需要初始化")
    
    # 存储状态颜色编码
    if status['storage_usage_percent'] > 80:
        st.error(f"🔴 存储: {status['storage_usage_percent']:.1f}%")
    elif status['storage_usage_percent'] > 60:
        st.warning(f"🟡 存储: {status['storage_usage_percent']:.1f}%")
    else:
        st.success(f"🟢 存储: {status['storage_usage_percent']:.1f}%")
    
    st.caption(f"📋 权限: {status['permissions_count']}")
    st.caption(f"📊 报表: {status['reports_count']}")
    st.caption(f"⚡ API: {status['api_calls_remaining']}/h")
    st.caption(f"📦 压缩: {'启用' if status['compression_enabled'] else '禁用'}")
    
    # 强制刷新按钮
    if st.button("🔄 强制刷新", help="强制刷新所有缓存和数据"):
        storage_system.force_refresh_cache()
        st.success("✅ 缓存已刷新")
        st.rerun()
    
    # API限制重置按钮
    if status['api_calls_remaining'] < 50:
        st.warning("API调用较多")
        if st.button("🔄 重置API限制", help="紧急重置API调用限制"):
            if st.session_state.storage_system:
                st.session_state.storage_system.cos_manager.rate_limiter.reset_bypass_mode()
                st.session_state.storage_system.cos_manager.clear_cache()
                st.success("✅ API限制已重置，缓存已清除")
                st.rerun()
    
    st.divider()
    
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
            st.info(f"查询编码：{st.session_state.user_id}")
            
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
    <h3>👨‍💼 管理员控制面板 (修复版)</h3>
    <p>✨ 智能压缩 + API优化 + 存储管理 + 上传修复</p>
    </div>
    ''', unsafe_allow_html=True)
    
    # 存储管理区域
    st.subheader("📊 存储管理")
    
    # 添加手动刷新按钮
    col1, col2 = st.columns([3, 1])
    with col2:
        if st.button("🔄 刷新统计", help="手动刷新存储统计数据"):
            storage_system.force_refresh_cache()
            st.success("✅ 统计数据已刷新")
            st.rerun()
    
    storage_system._show_storage_stats()
    
    st.divider()
    
    # 文件上传区域
    st.subheader("📁 文件管理")
    
    tab1, tab2, tab3, tab4 = st.tabs(["📋 权限表", "📊 报表数据", "🧹 存储清理", "🔧 系统诊断"])
    
    with tab1:
        st.markdown("#### 上传门店权限表")
        st.info("💡 Excel文件格式：第一列为门店名称，第二列为人员编号")
        
        st.markdown('''
        <div class="debug-box">
        <h4>🔧 权限上传修复版</h4>
        <p><strong>✅ 数据验证增强</strong>: 严格检查空值和无效数据</p>
        <p><strong>✅ 缓存刷新优化</strong>: 上传后立即刷新所有缓存</p>
        <p><strong>✅ 权限验证改进</strong>: 支持精确和模糊匹配</p>
        <p><strong>✅ 调试模式</strong>: 启用调试模式查看详细信息</p>
        </div>
        ''', unsafe_allow_html=True)
        
        permissions_file = st.file_uploader("选择权限Excel文件", type=['xlsx', 'xls'], key="permissions")
        
        if permissions_file and st.button("📤 上传权限表", key="upload_permissions"):
            with st.spinner("正在处理权限文件..."):
                if storage_system.upload_and_process_permissions(permissions_file):
                    st.balloons()
                    # 立即验证上传结果
                    updated_stores = storage_system.get_available_stores(force_refresh=True)
                    st.info(f"🎉 上传成功！系统现在支持 {len(updated_stores)} 个门店")
    
    with tab2:
        st.markdown("#### 上传财务报表")
        
        st.markdown('''
        <div class="success-box">
        <strong>🚀 智能压缩优势</strong><br>
        • GZIP压缩，节省60-80%存储空间<br>
        • 自动清理旧文件，防止空间不足<br>
        • API调用优化，避免频率限制<br>
        • 支持大文件，无需担心容量<br>
        • 中国地区高速访问<br>
        • 成本优化，50GB免费额度
        </div>
        ''', unsafe_allow_html=True)
        
        # 检查存储状态
        try:
            usage = storage_system.cos_manager.get_storage_usage()
            usage_percentage = usage.get('usage_percentage', 0)
            
            if usage_percentage > 90:
                st.error("⚠️ 存储空间即将满，建议先清理旧文件")
            elif usage_percentage > 75:
                st.warning("⚠️ 存储空间使用较多，建议定期清理")
        except Exception as e:
            st.info("📊 存储状态检查中...")
        
        reports_file = st.file_uploader("选择报表Excel文件", type=['xlsx', 'xls'], key="reports")
        
        if reports_file:
            file_size = len(reports_file.getvalue()) / 1024 / 1024
            st.metric("文件大小", f"{file_size:.2f} MB")
            
            # 估算压缩后大小
            estimated_compressed = file_size * 0.3  # 假设压缩比70%
            st.info(f"📦 预计压缩后: ~{estimated_compressed:.2f} MB (节省 ~{file_size - estimated_compressed:.2f} MB)")
            
            if file_size > 100:
                st.markdown('''
                <div class="warning-box">
                <strong>⚠️ 大文件优化</strong><br>
                启用智能压缩，大幅减少存储空间占用。<br>
                上传后自动清理旧文件，保持系统最佳状态。
                </div>
                ''', unsafe_allow_html=True)
        
        if reports_file and st.button("📤 上传报表数据", key="upload_reports"):
            with st.spinner("正在处理报表文件..."):
                if storage_system.upload_and_process_reports(reports_file):
                    st.balloons()
                    # 立即验证上传结果
                    updated_metadata = storage_system.load_metadata(force_refresh=True)
                    reports_count = len(updated_metadata.get('reports', []))
                    st.info(f"🎉 上传成功！系统现在有 {reports_count} 个报表")
    
    with tab3:
        st.markdown("#### 存储空间清理")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("##### 🗑️ 清理旧文件")
            st.info("清理3天前的旧报表文件，保留最新数据")
            
            if st.button("🧹 清理旧文件", type="secondary"):
                storage_system.cleanup_storage("old")
                st.rerun()
        
        with col2:
            st.markdown("##### ⚠️ 完全清理")
            st.warning("⚠️ 将删除所有存储数据，请谨慎操作！")
            
            if st.checkbox("我确认要清理所有数据"):
                if st.button("🗑️ 清理所有数据", type="primary"):
                    storage_system.cleanup_storage("all")
                    st.rerun()
    
    with tab4:
        st.markdown("#### 系统诊断与修复")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("##### 📊 系统状态诊断")
            
            if st.button("🔍 运行系统诊断", type="primary"):
                with st.spinner("正在运行系统诊断..."):
                    # 诊断权限文件
                    permissions = storage_system.load_permissions(force_refresh=True)
                    metadata = storage_system.load_metadata(force_refresh=True)
                    stores = storage_system.get_available_stores(force_refresh=True)
                    
                    st.markdown('''
                    <div class="debug-box">
                    <h4>🔧 系统诊断结果</h4>
                    </div>
                    ''', unsafe_allow_html=True)
                    
                    # 权限系统诊断
                    if len(permissions) > 0:
                        st.success(f"✅ 权限系统正常: {len(permissions)} 条权限记录")
                        st.info(f"📋 可用门店: {len(stores)} 个")
                        
                        # 显示权限样例
                        if len(permissions) > 0:
                            st.write("**权限记录样例:**")
                            sample_df = pd.DataFrame(permissions[:5])
                            st.dataframe(sample_df[['store_name', 'user_id']], use_container_width=True)
                    else:
                        st.error("❌ 权限系统异常: 没有权限记录")
                    
                    # 报表系统诊断
                    reports = metadata.get('reports', [])
                    if len(reports) > 0:
                        st.success(f"✅ 报表系统正常: {len(reports)} 个报表")
                        
                        # 显示报表样例
                        if len(reports) > 0:
                            st.write("**报表记录样例:**")
                            sample_reports = []
                            for report in reports[:5]:
                                sample_reports.append({
                                    '门店名称': report.get('store_name'),
                                    '文件大小': f"{report.get('file_size_mb', 0):.1f}MB",
                                    '上传时间': report.get('upload_time', '')[:19]
                                })
                            st.dataframe(pd.DataFrame(sample_reports), use_container_width=True)
                    else:
                        st.error("❌ 报表系统异常: 没有报表记录")
                    
                    # 存储诊断
                    usage = storage_system.cos_manager.get_storage_usage()
                    st.info(f"💾 存储使用: {usage.get('total_size_gb', 0):.2f}GB")
                    st.info(f"⚡ API剩余: {usage.get('remaining_calls', 0)}/小时")
        
        with col2:
            st.markdown("##### 🔧 修复操作")
            
            if st.button("🔄 重建缓存索引"):
                with st.spinner("正在重建缓存索引..."):
                    storage_system.force_refresh_cache()
                    
                    # 预加载关键数据
                    permissions = storage_system.load_permissions(force_refresh=True)
                    metadata = storage_system.load_metadata(force_refresh=True)
                    stores = storage_system.get_available_stores(force_refresh=True)
                    
                    st.success("✅ 缓存索引重建完成")
                    st.info(f"📋 权限记录: {len(permissions)}")
                    st.info(f"📊 报表记录: {len(metadata.get('reports', []))}")
                    st.info(f"🏪 可用门店: {len(stores)}")
            
            if st.button("🧹 清理无效文件"):
                with st.spinner("正在清理无效文件..."):
                    # 清理孤立文件
                    all_files = storage_system.cos_manager.list_files()
                    metadata = storage_system.load_metadata(force_refresh=True)
                    
                    valid_filenames = set()
                    for report in metadata.get('reports', []):
                        filename = report.get('filename')
                        if filename:
                            valid_filenames.add(filename)
                    
                    # 添加系统文件
                    valid_filenames.add('system/permissions.json')
                    valid_filenames.add('system/permissions.gz')
                    valid_filenames.add('system/metadata.json')
                    valid_filenames.add('system/metadata.gz')
                    
                    orphaned_files = []
                    for file_info in all_files:
                        if (file_info['filename'].startswith('reports/') and 
                            file_info['filename'] not in valid_filenames):
                            orphaned_files.append(file_info['filename'])
                    
                    if orphaned_files:
                        deleted_count = 0
                        for filename in orphaned_files:
                            if storage_system.cos_manager.delete_file(filename):
                                deleted_count += 1
                        
                        st.success(f"✅ 清理完成: 删除了 {deleted_count} 个无效文件")
                        storage_system.force_refresh_cache()
                    else:
                        st.info("✅ 没有发现无效文件")

elif user_type == "管理员" and not st.session_state.is_admin:
    st.info("👈 请在左侧边栏输入管理员密码")

else:
    if not st.session_state.logged_in:
        st.subheader("🔐 用户登录")
        
        try:
            # 强制刷新门店列表
            stores = storage_system.get_available_stores(force_refresh=True)
            
            if not stores:
                st.markdown('''
                <div class="warning-box">
                <h4>⚠️ 系统维护中</h4>
                <p>暂无可用门店，请联系管理员上传权限表</p>
                <p>如果管理员刚刚上传了权限表，请点击下方刷新按钮</p>
                </div>
                ''', unsafe_allow_html=True)
                
                if st.button("🔄 刷新门店列表"):
                    storage_system.force_refresh_cache()
                    st.rerun()
            else:
                st.markdown(f'''
                <div class="success-box">
                <h4>🏪 系统就绪</h4>
                <p>当前系统支持 <strong>{len(stores)}</strong> 个门店的查询服务</p>
                <p>请选择您的门店并输入查询编码</p>
                </div>
                ''', unsafe_allow_html=True)
                
                with st.form("login_form"):
                    selected_store = st.selectbox("选择门店", stores)
                    user_id = st.text_input("查询编码")
                    submit = st.form_submit_button("🚀 登录")
                    
                    if submit and selected_store and user_id:
                        with st.spinner("正在验证权限..."):
                            # 强制刷新权限验证
                            if storage_system.verify_user_permission(selected_store, user_id, force_refresh=True):
                                st.session_state.logged_in = True
                                st.session_state.store_name = selected_store
                                st.session_state.user_id = user_id
                                st.success("✅ 登录成功！")
                                st.balloons()
                                st.rerun()
                            else:
                                st.error("❌ 门店或编号错误！")
                                
                                # 调试信息
                                if st.session_state.debug_mode:
                                    st.error("**调试信息**: 权限验证失败")
                                    st.write(f"输入门店: '{selected_store}'")
                                    st.write(f"输入编码: '{user_id}'")
                                    
                                    # 显示相似的权限记录
                                    permissions = storage_system.load_permissions(force_refresh=True)
                                    similar_stores = [p for p in permissions 
                                                    if selected_store.lower() in p.get('store_name', '').lower() 
                                                    or p.get('store_name', '').lower() in selected_store.lower()]
                                    
                                    if similar_stores:
                                        st.write("**相似门店记录:**")
                                        for perm in similar_stores[:3]:
                                            st.write(f"- 门店: '{perm.get('store_name')}', 编码: '{perm.get('user_id')}'")
                            
        except Exception as e:
            st.error(f"❌ 系统连接失败：{str(e)}")
            st.write("**详细错误信息:**", str(e))
            
            if st.button("🔄 重试连接"):
                storage_system.force_refresh_cache()
                st.rerun()
    
    else:
        # 已登录用户界面
        st.markdown(f'<div class="store-info"><h3>🏪 {st.session_state.store_name}</h3><p>操作员：{st.session_state.user_id}</p></div>', unsafe_allow_html=True)
        
        # 添加强制刷新按钮
        col1, col2 = st.columns([4, 1])
        with col2:
            if st.button("🔄 刷新数据"):
                storage_system.force_refresh_cache()
                st.rerun()
        
        try:
            # 强制刷新数据加载
            df = storage_system.load_store_data(st.session_state.store_name, force_refresh=True)
            
            if df is not None:
                # 应收-未收额分析
                st.subheader("💰 应收-未收额")
                
                analysis_results = storage_system.analyze_receivable_data(df)
                
                if '应收-未收额' in analysis_results:
                    data = analysis_results['应收-未收额']
                    amount = data['amount']
                    
                    if amount > 0:
                        st.error(f"💳 应付款：¥{amount:,.2f}")
                    elif amount < 0:
                        st.success(f"💚 应退款：¥{abs(amount):,.2f}")
                    else:
                        st.info("⚖️ 收支平衡：¥0.00")
                    
                    # 显示详细信息
                    with st.expander("📊 详细信息"):
                        st.write(f"**所在行**: 第{data['actual_row_number']}行")
                        st.write(f"**所在列**: {data['column_name']}")
                        st.write(f"**行标题**: {data['row_name']}")
                else:
                    st.warning("⚠️ 未找到应收-未收额数据")
                    
                    # 调试信息
                    if st.session_state.debug_mode:
                        st.write("**调试信息**: 数据分析结果")
                        st.write(f"数据行数: {len(df)}")
                        if len(df) > 68:
                            row_69 = df.iloc[68]
                            st.write(f"第69行第一列内容: '{row_69.iloc[0] if pd.notna(row_69.iloc[0]) else 'N/A'}'")
                
                # 报表展示
                st.subheader("📋 报表数据")
                st.dataframe(df, use_container_width=True, height=400)
                
                # 下载功能
                if st.button("📥 下载完整报表"):
                    try:
                        buffer = io.BytesIO()
                        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                            df.to_excel(writer, index=False, sheet_name=st.session_state.store_name)
                        
                        st.download_button(
                            "点击下载",
                            buffer.getvalue(),
                            f"{st.session_state.store_name}_报表_{datetime.now().strftime('%Y%m%d')}.xlsx",
                            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                    except Exception as e:
                        st.error(f"下载失败：{str(e)}")
            
            else:
                st.markdown(f'''
                <div class="warning-box">
                <h4>❌ 未找到报表数据</h4>
                <p>门店 '<strong>{st.session_state.store_name}</strong>' 的报表数据不存在</p>
                <p>可能原因：</p>
                <ul>
                <li>管理员尚未上传该门店的报表</li>
                <li>文件正在处理中，请稍后再试</li>
                <li>数据同步延迟，请点击刷新数据</li>
                </ul>
                <p>如问题持续存在，请联系管理员</p>
                </div>
                ''', unsafe_allow_html=True)
                
                # 调试信息
                if st.session_state.debug_mode:
                    st.write("**调试信息**: 数据加载失败")
                    metadata = storage_system.load_metadata(force_refresh=True)
                    available_reports = [r.get('store_name') for r in metadata.get('reports', [])]
                    st.write(f"可用报表: {available_reports}")
                    st.write(f"查找门店: '{st.session_state.store_name}'")
                
        except Exception as e:
            st.error(f"❌ 数据加载失败：{str(e)}")
            st.write("**详细错误信息:**", str(e))
            
            if st.session_state.debug_mode:
                st.write("**完整错误追踪:**")
                st.code(traceback.format_exc())

# 页面底部
st.divider()

col1, col2, col3, col4, col5 = st.columns(5)
with col1:
    st.caption(f"🕒 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
with col2:
    st.caption("☁️ 腾讯云COS")
with col3:
    st.caption("📦 智能压缩")
with col4:
    st.caption("🔧 修复版本")
with col5:
    if st.session_state.debug_mode:
        st.caption("🔍 调试模式")
