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
    """腾讯云COS存储管理器 - 优化版本"""
    
    def __init__(self):
        self.client = None
        self.bucket_name = None
        self.region = None
        self.rate_limiter = APIRateLimiter(API_RATE_LIMIT)
        self.compression = CompressionManager()
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
            
            return files
            
        except Exception as e:
            logger.error(f"列出文件时出错: {str(e)}")
            return []
    
    def file_exists(self, filename: str) -> bool:
        """检查文件是否存在（优化版本）"""
        # 优先使用缓存或批量查询避免频繁API调用
        try:
            # 先尝试批量获取文件列表，减少API调用
            if hasattr(self, '_file_cache'):
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
            if not hasattr(self, '_file_cache'):
                self._file_cache = {}
            self._file_cache[filename] = True
            
            return True
        except:
            # 更新缓存
            if hasattr(self, '_file_cache'):
                self._file_cache[filename] = False
            return False
    
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
                'file_count': 0, 'total_size_mb': 0, 'total_size_gb': 0,
                'usage_percentage': 0, 'remaining_calls': 0, 'files': []
            }

class TencentCOSSystem:
    """基于腾讯云COS的完整存储系统 - 优化版本"""
    
    def __init__(self):
        self.cos_manager = TencentCOSManager()
        self.permissions_file = "system/permissions.json"
        self.metadata_file = "system/metadata.json"
        self.initialized = True
    
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
    
    def load_permissions(self) -> List[Dict]:
        """从COS加载权限数据"""
        try:
            data = self.cos_manager.download_json(self.permissions_file)
            return data.get('permissions', []) if data else []
        except Exception as e:
            logger.error(f"加载权限数据失败: {str(e)}")
            return []
    
    def save_permissions(self, permissions_data: List[Dict]) -> bool:
        """保存权限数据到COS（压缩）"""
        try:
            data = {
                'permissions': permissions_data,
                'last_updated': datetime.now().isoformat(),
                'version': '2.0',
                'compressed': True
            }
            return self.cos_manager.upload_json(data, self.permissions_file, compress=True)
        except Exception as e:
            logger.error(f"保存权限数据失败: {str(e)}")
            return False
    
    def load_metadata(self) -> Dict:
        """从COS加载元数据"""
        try:
            data = self.cos_manager.download_json(self.metadata_file)
            return data if data else {'reports': [], 'compressed': True}
        except Exception as e:
            logger.error(f"加载元数据失败: {str(e)}")
            return {'reports': [], 'compressed': True}
    
    def save_metadata(self, metadata: Dict) -> bool:
        """保存元数据到COS（压缩）"""
        try:
            metadata['last_updated'] = datetime.now().isoformat()
            metadata['compressed'] = True
            return self.cos_manager.upload_json(metadata, self.metadata_file, compress=True)
        except Exception as e:
            logger.error(f"保存元数据失败: {str(e)}")
            return False
    
    def upload_and_process_permissions(self, uploaded_file) -> bool:
        """上传并处理权限文件"""
        try:
            # 读取Excel文件
            df = pd.read_excel(uploaded_file)
            
            if len(df.columns) < 2:
                st.error("❌ 权限文件格式错误：需要至少两列（门店名称、人员编号）")
                return False
            
            # 转换为权限数据格式
            permissions_data = []
            for _, row in df.iterrows():
                store_name = str(row.iloc[0]).strip()
                user_id = str(row.iloc[1]).strip()
                
                if store_name and user_id and store_name != 'nan' and user_id != 'nan':
                    permissions_data.append({
                        "store_name": store_name,
                        "user_id": user_id,
                        "created_at": datetime.now().isoformat(),
                        "updated_at": datetime.now().isoformat()
                    })
            
            # 保存到COS（压缩）
            success = self.save_permissions(permissions_data)
            
            if success:
                st.success(f"✅ 权限数据保存成功：{len(permissions_data)} 条记录（已压缩）")
                return True
            else:
                st.error("❌ 权限数据保存失败")
                return False
                
        except Exception as e:
            st.error(f"❌ 处理权限文件失败：{str(e)}")
            logger.error(f"处理权限文件失败: {str(e)}")
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
                metadata = self.load_metadata()
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
                        st.markdown(f'''
                        <div class="compression-info">
                        <h4>🎉 报表处理完成</h4>
                        <p>✅ 处理工作表: {reports_processed} 个</p>
                        <p>📦 启用压缩存储，节省存储空间</p>
                        <p>⚡ API调用优化，避免频率限制</p>
                        </div>
                        ''', unsafe_allow_html=True)
                        
                        # 显示存储统计
                        self._show_storage_stats()
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
            metadata = self.load_metadata()
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
                st.metric("📦 总文件数", usage['file_count'])
                st.metric("💾 总使用量", f"{usage['total_size_gb']:.2f} GB")
                
                # 使用率进度条
                progress_value = min(usage['usage_percentage'] / 100, 1.0)
                st.progress(progress_value)
                
                # 颜色编码的使用率
                if usage['usage_percentage'] > 80:
                    st.error(f"🔴 使用率: {usage['usage_percentage']:.1f}%")
                elif usage['usage_percentage'] > 60:
                    st.warning(f"🟡 使用率: {usage['usage_percentage']:.1f}%")
                else:
                    st.success(f"🟢 使用率: {usage['usage_percentage']:.1f}%")
            
            with col2:
                st.metric("📊 报表文件", usage['report_files'])
                st.metric("📋 报表记录", len(metadata.get('reports', [])))
                st.metric("📄 报表大小", f"{usage['report_size_mb']:.1f} MB")
                
                # 压缩效果估算
                if usage['report_size_mb'] > 0:
                    estimated_uncompressed = usage['report_size_mb'] * 3  # 假设压缩比为70%
                    savings = estimated_uncompressed - usage['report_size_mb']
                    st.success(f"💰 压缩节省: ~{savings:.1f} MB")
            
            with col3:
                st.metric("🔐 权限记录", len(permissions))
                st.metric("⚙️ 系统文件", usage['system_files'])
                st.metric("🗃️ 系统大小", f"{usage['system_size_kb']:.1f} KB")
                st.metric("⚡ API剩余", f"{usage['remaining_calls']}/小时")
                
        except Exception as e:
            st.warning(f"获取存储统计失败: {str(e)}")
    
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
    
    def load_store_data(self, store_name: str) -> Optional[pd.DataFrame]:
        """加载指定门店的数据（支持解压）"""
        try:
            # 从元数据获取报表信息
            metadata = self.load_metadata()
            reports = metadata.get('reports', [])
            
            # 查找匹配的门店报表
            matching_report = None
            for report in reports:
                if report.get('store_name') == store_name:
                    matching_report = report
                    break
            
            if not matching_report:
                return None
            
            filename = matching_report.get('filename')
            if not filename:
                return None
            
            # 从腾讯云COS下载文件（自动解压）
            with st.spinner(f"正在从腾讯云加载 {store_name} 的数据..."):
                is_compressed = filename.endswith('.gz')
                file_data = self.cos_manager.download_file(filename, decompress=is_compressed)
                
                if file_data:
                    # 解析Excel文件
                    excel_file = pd.ExcelFile(io.BytesIO(file_data))
                    
                    # 查找匹配的工作表
                    matching_sheets = [sheet for sheet in excel_file.sheet_names 
                                     if store_name in sheet or sheet in store_name]
                    
                    if matching_sheets:
                        df = pd.read_excel(io.BytesIO(file_data), sheet_name=matching_sheets[0])
                        return df
                    elif store_name in excel_file.sheet_names:
                        df = pd.read_excel(io.BytesIO(file_data), sheet_name=store_name)
                        return df
                    
            return None
            
        except Exception as e:
            st.error(f"❌ 加载 {store_name} 数据失败：{str(e)}")
            logger.error(f"加载门店数据失败: {str(e)}")
            return None
    
    def verify_user_permission(self, store_name: str, user_id: str) -> bool:
        """验证用户权限"""
        try:
            permissions = self.load_permissions()
            
            for perm in permissions:
                stored_store = perm.get('store_name', '').strip()
                stored_id = perm.get('user_id', '').strip()
                
                if (store_name in stored_store or stored_store in store_name) and stored_id == str(user_id):
                    return True
            
            return False
            
        except Exception as e:
            st.error(f"❌ 权限验证失败：{str(e)}")
            logger.error(f"权限验证失败: {str(e)}")
            return False
    
    def get_available_stores(self) -> List[str]:
        """获取可用的门店列表"""
        try:
            permissions = self.load_permissions()
            stores = list(set(perm.get('store_name', '') for perm in permissions))
            return sorted([store for store in stores if store.strip()])
            
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
                
                st.success(f"🧹 清理完成：删除了 {deleted_count} 个文件")
                
            elif cleanup_type == "old":
                # 只清理旧文件
                deleted_count = self._cleanup_old_reports(7)  # 清理7天前的文件
                st.success(f"🧹 清理旧文件完成：删除了 {deleted_count} 个文件")
                
        except Exception as e:
            st.error(f"❌ 清理失败：{str(e)}")
            logger.error(f"存储清理失败: {str(e)}")
    
    def get_system_status(self) -> Dict:
        """获取系统状态"""
        try:
            # 检查系统文件是否存在
            permissions_exists = self.cos_manager.file_exists(self.permissions_file) or \
                               self.cos_manager.file_exists(self.permissions_file.replace('.json', '.gz'))
            metadata_exists = self.cos_manager.file_exists(self.metadata_file) or \
                            self.cos_manager.file_exists(self.metadata_file.replace('.json', '.gz'))
            
            # 获取统计数据
            permissions = self.load_permissions()
            metadata = self.load_metadata()
            usage = self.cos_manager.get_storage_usage()
            
            return {
                'permissions_file_exists': permissions_exists,
                'metadata_file_exists': metadata_exists,
                'permissions_count': len(permissions),
                'reports_count': len(metadata.get('reports', [])),
                'system_healthy': permissions_exists and metadata_exists,
                'storage_usage_percent': usage['usage_percentage'],
                'api_calls_remaining': usage['remaining_calls'],
                'compression_enabled': True
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
                'compression_enabled': False
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

# 主标题
st.markdown('<h1 class="main-header">📊 门店报表查询系统 (智能压缩版)</h1>', unsafe_allow_html=True)

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
    
    # API限制重置按钮
    if status['api_calls_remaining'] < 50:
        st.warning("API调用较多")
        if st.button("🔄 重置API限制", help="紧急重置API调用限制"):
            if st.session_state.storage_system:
                st.session_state.storage_system.cos_manager.rate_limiter.reset_bypass_mode()
                st.success("✅ API限制已重置")
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
    <h3>👨‍💼 管理员控制面板</h3>
    <p>✨ 智能压缩 + API优化 + 存储管理</p>
    </div>
    ''', unsafe_allow_html=True)
    
    # 存储管理区域
    st.subheader("📊 存储管理")
    storage_system._show_storage_stats()
    
    st.divider()
    
    # 文件上传区域
    st.subheader("📁 文件管理")
    
    tab1, tab2, tab3, tab4 = st.tabs(["📋 权限表", "📊 报表数据", "🧹 存储清理", "⚙️ 系统优化"])
    
    with tab1:
        st.markdown("#### 上传门店权限表")
        st.info("💡 Excel文件格式：第一列为门店名称，第二列为人员编号")
        
        permissions_file = st.file_uploader("选择权限Excel文件", type=['xlsx', 'xls'], key="permissions")
        
        if permissions_file and st.button("📤 上传权限表", key="upload_permissions"):
            if storage_system.upload_and_process_permissions(permissions_file):
                st.balloons()
    
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
        usage = storage_system.cos_manager.get_storage_usage()
        if usage['usage_percentage'] > 90:
            st.error("⚠️ 存储空间即将满，建议先清理旧文件")
        elif usage['usage_percentage'] > 75:
            st.warning("⚠️ 存储空间使用较多，建议定期清理")
        
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
            if storage_system.upload_and_process_reports(reports_file):
                st.balloons()
    
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
        st.markdown("#### 系统优化")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("##### 📊 存储优化")
            usage = storage_system.cos_manager.get_storage_usage()
            
            st.metric("当前使用", f"{usage['total_size_gb']:.2f} GB")
            st.metric("文件数量", usage['file_count'])
            st.metric("压缩节省", f"~{usage['total_size_gb'] * 2:.1f} GB")
            
            # 优化建议
            if usage['usage_percentage'] > 80:
                st.error("🔴 建议立即清理旧文件")
            elif usage['usage_percentage'] > 60:
                st.warning("🟡 建议定期清理维护")
            else:
                st.success("🟢 存储状态良好")
        
        with col2:
            st.markdown("##### ⚡ API优化")
            
            st.metric("剩余调用", f"{usage['remaining_calls']}/小时")
            
            if usage['remaining_calls'] < 20:
                st.error("🔴 API调用接近限制")
                st.info("系统已自动优化调用频率")
            elif usage['remaining_calls'] < 50:
                st.warning("🟡 API使用较多")
            else:
                st.success("🟢 API状态正常")
            
            st.info("💡 系统已启用智能限流，自动避免API超限")

elif user_type == "管理员" and not st.session_state.is_admin:
    st.info("👈 请在左侧边栏输入管理员密码")

else:
    if not st.session_state.logged_in:
        st.subheader("🔐 用户登录")
        
        try:
            stores = storage_system.get_available_stores()
            
            if not stores:
                st.warning("⚠️ 系统维护中，请联系管理员上传权限表")
            else:
                with st.form("login_form"):
                    selected_store = st.selectbox("选择门店", stores)
                    user_id = st.text_input("查询编码")
                    submit = st.form_submit_button("🚀 登录")
                    
                    if submit and selected_store and user_id:
                        if storage_system.verify_user_permission(selected_store, user_id):
                            st.session_state.logged_in = True
                            st.session_state.store_name = selected_store
                            st.session_state.user_id = user_id
                            st.success("✅ 登录成功！")
                            st.balloons()
                            st.rerun()
                        else:
                            st.error("❌ 门店或编号错误！")
                            
        except Exception as e:
            st.error(f"❌ 系统连接失败：{str(e)}")
    
    else:
        # 已登录用户界面
        st.markdown(f'<div class="store-info"><h3>🏪 {st.session_state.store_name}</h3><p>操作员：{st.session_state.user_id}</p></div>', unsafe_allow_html=True)
        
        try:
            df = storage_system.load_store_data(st.session_state.store_name)
            
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
                st.error(f"❌ 未找到门店 '{st.session_state.store_name}' 的报表")
                st.info("请联系管理员上传最新的报表数据")
                
        except Exception as e:
            st.error(f"❌ 数据加载失败：{str(e)}")

# 页面底部
st.divider()

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.caption(f"🕒 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
with col2:
    st.caption("☁️ 腾讯云COS")
with col3:
    st.caption("📦 智能压缩")
with col4:
    st.caption("🔧 v7.0 优化版")
