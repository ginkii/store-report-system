import os
import io
import hashlib
import time
from datetime import datetime
from typing import Optional, Callable, Dict, Any
import streamlit as st

try:
    from qcloud_cos import CosConfig, CosS3Client
    from qcloud_cos.cos_exception import CosServiceError, CosClientError
    COS_AVAILABLE = True
except ImportError:
    COS_AVAILABLE = False
    st.error("腾讯云COS SDK未安装，请运行: pip install cos-python-sdk-v5")

class COSHandler:
    """腾讯云COS存储处理器 - 修复配置获取版本"""
    
    def __init__(self):
        if not COS_AVAILABLE:
            raise ImportError("腾讯云COS SDK未安装")
        
        # 从 config.py 获取COS配置（修复关键问题）
        try:
            from config import get_cos_config, validate_config
            
            # 验证配置完整性
            is_valid, errors = validate_config()
            if not is_valid:
                error_msg = f"COS配置验证失败: {'; '.join(errors)}"
                st.error(error_msg)
                raise ValueError(error_msg)
            
            # 获取完整的COS配置
            cos_config = get_cos_config()
            
            self.secret_id = cos_config.get('secret_id', '')
            self.secret_key = cos_config.get('secret_key', '')
            self.region = cos_config.get('region', 'ap-guangzhou')
            self.bucket = cos_config.get('bucket', '')
            self.domain = cos_config.get('domain', '')
            
            # 高级配置
            self.timeout = cos_config.get('timeout', 60)
            self.max_retries = cos_config.get('max_retries', 3)
            self.chunk_size = cos_config.get('chunk_size', 1024 * 1024)
            self.multipart_threshold = cos_config.get('multipart_threshold', 5 * 1024 * 1024)
            self.use_https = cos_config.get('use_https', True)
            
        except ImportError as e:
            error_msg = "无法导入配置模块，请检查 config.py 文件"
            st.error(error_msg)
            raise ImportError(error_msg)
        except Exception as e:
            error_msg = f"获取COS配置失败: {str(e)}"
            st.error(error_msg)
            raise ValueError(error_msg)
        
        # 验证必需配置
        if not all([self.secret_id, self.secret_key, self.region, self.bucket]):
            missing_configs = []
            if not self.secret_id: missing_configs.append('secret_id')
            if not self.secret_key: missing_configs.append('secret_key')
            if not self.region: missing_configs.append('region')
            if not self.bucket: missing_configs.append('bucket')
            
            error_msg = f"COS配置不完整，缺少: {', '.join(missing_configs)}"
            st.error(error_msg)
            raise ValueError(error_msg)
        
        # 分片上传配置
        self.max_chunks = 10000        # 最大分片数
        
        # 初始化COS客户端
        self.client = None
        self._initialize_client()
        
        # 统计信息
        self.upload_stats = {
            'total_uploads': 0,
            'failed_uploads': 0,
            'total_bytes': 0,
            'last_upload_time': None
        }
    
    def _initialize_client(self):
        """初始化COS客户端"""
        try:
            st.info(f"正在初始化COS客户端 (区域: {self.region}, 存储桶: {self.bucket})")
            
            # 创建COS配置
            config = CosConfig(
                Region=self.region,
                SecretId=self.secret_id,
                SecretKey=self.secret_key,
                Token=None,  # 暂不使用临时密钥
                Scheme='https' if self.use_https else 'http'
            )
            
            # 创建客户端
            self.client = CosS3Client(config)
            
            # 测试连接
            if self._test_connection():
                st.success(f"✅ COS客户端初始化成功，已连接到 {self.bucket}")
            else:
                error_msg = "COS连接测试失败，请检查配置和网络"
                st.error(error_msg)
                raise ConnectionError(error_msg)
                
        except Exception as e:
            error_msg = f"COS初始化失败: {str(e)}"
            st.error(error_msg)
            self.client = None
            raise ConnectionError(error_msg)
    
    def _test_connection(self) -> bool:
        """测试COS连接"""
        try:
            if not self.client:
                return False
            
            # 尝试列出桶信息
            response = self.client.head_bucket(Bucket=self.bucket)
            return True
            
        except CosServiceError as e:
            st.error(f"COS服务错误: {e.get_error_msg()}")
            return False
        except Exception as e:
            st.error(f"COS连接测试失败: {str(e)}")
            return False
    
    def upload_file(
        self, 
        file_content: bytes, 
        filename: str, 
        folder: str = "uploads",
        progress_callback: Optional[Callable[[int, str], None]] = None
    ) -> Optional[str]:
        """上传文件到COS - 支持分片上传和进度回调"""
        
        if not self.client:
            error_msg = "❌ COS客户端未初始化，无法上传文件"
            if progress_callback:
                progress_callback(0, error_msg)
            st.error(error_msg)
            return None
        
        try:
            # 生成COS对象键
            cos_key = self._generate_cos_key(filename, folder)
            file_size = len(file_content)
            
            if progress_callback:
                progress_callback(5, f"📁 准备上传文件到COS: {filename} ({file_size/1024/1024:.1f}MB)")
            
            # 选择上传策略
            if file_size <= self.multipart_threshold:
                # 小文件：简单上传
                return self._simple_upload(file_content, cos_key, progress_callback)
            else:
                # 大文件：分片上传
                return self._multipart_upload(file_content, cos_key, progress_callback)
                
        except Exception as e:
            self.upload_stats['failed_uploads'] += 1
            error_msg = f"❌ 上传失败: {str(e)}"
            if progress_callback:
                progress_callback(0, error_msg)
            st.error(f"文件上传失败: {str(e)}")
            return None
    
    def _simple_upload(
        self, 
        file_content: bytes, 
        cos_key: str,
        progress_callback: Optional[Callable[[int, str], None]] = None
    ) -> Optional[str]:
        """简单上传（小文件）"""
        try:
            if progress_callback:
                progress_callback(25, "⬆️ 正在上传文件到腾讯云COS...")
            
            # 上传文件
            response = self.client.put_object(
                Bucket=self.bucket,
                Body=io.BytesIO(file_content),
                Key=cos_key,
                Metadata={
                    'upload-time': datetime.now().isoformat(),
                    'file-size': str(len(file_content)),
                    'upload-method': 'simple',
                    'upload-source': 'streamlit-app'
                }
            )
            
            if progress_callback:
                progress_callback(80, "✅ 文件上传完成，正在验证...")
            
            # 验证上传
            if self._verify_upload(cos_key, len(file_content)):
                if progress_callback:
                    progress_callback(100, "🎉 文件已成功保存到腾讯云COS！")
                
                self._update_upload_stats(len(file_content), success=True)
                st.success(f"文件已成功上传到COS: {cos_key}")
                return cos_key
            else:
                if progress_callback:
                    progress_callback(0, "❌ 上传验证失败")
                st.error("COS上传验证失败")
                return None
                
        except CosServiceError as e:
            error_msg = f"COS服务错误: {e.get_error_msg()}"
            if progress_callback:
                progress_callback(0, f"❌ {error_msg}")
            st.error(error_msg)
            return None
        except Exception as e:
            error_msg = f"❌ 上传异常: {str(e)}"
            if progress_callback:
                progress_callback(0, error_msg)
            st.error(error_msg)
            return None
    
    def _multipart_upload(
        self, 
        file_content: bytes, 
        cos_key: str,
        progress_callback: Optional[Callable[[int, str], None]] = None
    ) -> Optional[str]:
        """分片上传（大文件）"""
        upload_id = None
        try:
            file_size = len(file_content)
            
            if progress_callback:
                progress_callback(10, f"🔄 准备分片上传到COS ({file_size/1024/1024:.1f}MB)")
            
            # 初始化分片上传
            response = self.client.create_multipart_upload(
                Bucket=self.bucket,
                Key=cos_key,
                Metadata={
                    'upload-time': datetime.now().isoformat(),
                    'file-size': str(file_size),
                    'upload-method': 'multipart',
                    'upload-source': 'streamlit-app'
                }
            )
            
            upload_id = response['UploadId']
            
            if progress_callback:
                progress_callback(15, f"📦 开始分片上传到COS，ID: {upload_id[:8]}...")
            
            # 计算分片
            total_chunks = (file_size + self.chunk_size - 1) // self.chunk_size
            uploaded_parts = []
            
            # 逐片上传
            for chunk_num in range(total_chunks):
                start_pos = chunk_num * self.chunk_size
                end_pos = min(start_pos + self.chunk_size, file_size)
                chunk_data = file_content[start_pos:end_pos]
                
                # 计算进度
                progress = 15 + int((chunk_num / total_chunks) * 70)
                if progress_callback:
                    progress_callback(
                        progress, 
                        f"⬆️ 上传分片到COS {chunk_num + 1}/{total_chunks} ({end_pos/1024/1024:.1f}MB)"
                    )
                
                # 上传分片（带重试）
                part_response = self._upload_chunk_with_retry(
                    chunk_data, cos_key, upload_id, chunk_num + 1
                )
                
                if not part_response:
                    # 分片上传失败，取消整个上传
                    self._abort_multipart_upload(cos_key, upload_id)
                    if progress_callback:
                        progress_callback(0, f"❌ 分片 {chunk_num + 1} 上传到COS失败")
                    return None
                
                uploaded_parts.append({
                    'ETag': part_response['ETag'],
                    'PartNumber': chunk_num + 1
                })
            
            if progress_callback:
                progress_callback(90, "🔗 正在完成COS分片上传...")
            
            # 完成分片上传
            complete_response = self.client.complete_multipart_upload(
                Bucket=self.bucket,
                Key=cos_key,
                UploadId=upload_id,
                MultipartUpload={'Part': uploaded_parts}
            )
            
            if progress_callback:
                progress_callback(95, "✅ 正在验证COS上传结果...")
            
            # 验证上传
            if self._verify_upload(cos_key, file_size):
                if progress_callback:
                    progress_callback(100, "🎉 文件已成功分片上传到腾讯云COS！")
                
                self._update_upload_stats(file_size, success=True)
                st.success(f"文件已成功分片上传到COS: {cos_key}")
                return cos_key
            else:
                if progress_callback:
                    progress_callback(0, "❌ COS上传验证失败")
                st.error("COS分片上传验证失败")
                return None
                
        except CosServiceError as e:
            if upload_id:
                self._abort_multipart_upload(cos_key, upload_id)
            error_msg = f"COS服务错误: {e.get_error_msg()}"
            if progress_callback:
                progress_callback(0, f"❌ {error_msg}")
            st.error(error_msg)
            return None
        except Exception as e:
            if upload_id:
                self._abort_multipart_upload(cos_key, upload_id)
            error_msg = f"❌ 分片上传异常: {str(e)}"
            if progress_callback:
                progress_callback(0, error_msg)
            st.error(error_msg)
            return None
    
    def _upload_chunk_with_retry(
        self, 
        chunk_data: bytes, 
        cos_key: str, 
        upload_id: str, 
        part_number: int
    ) -> Optional[Dict]:
        """带重试的分片上传"""
        for attempt in range(self.max_retries):
            try:
                response = self.client.upload_part(
                    Bucket=self.bucket,
                    Key=cos_key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=io.BytesIO(chunk_data)
                )
                return response
                
            except Exception as e:
                if attempt < self.max_retries - 1:
                    wait_time = (attempt + 1) * 2  # 递增等待时间
                    st.warning(f"分片 {part_number} 上传失败，{wait_time}秒后重试... (第{attempt+1}次)")
                    time.sleep(wait_time)
                    continue
                else:
                    st.error(f"分片 {part_number} 上传失败（已重试{self.max_retries}次）: {str(e)}")
                    return None
    
    def _abort_multipart_upload(self, cos_key: str, upload_id: str):
        """取消分片上传"""
        try:
            self.client.abort_multipart_upload(
                Bucket=self.bucket,
                Key=cos_key,
                UploadId=upload_id
            )
            st.warning(f"已取消分片上传: {upload_id}")
        except Exception as e:
            st.warning(f"取消分片上传失败: {str(e)}")
    
    def _verify_upload(self, cos_key: str, expected_size: int) -> bool:
        """验证文件上传是否成功"""
        try:
            response = self.client.head_object(Bucket=self.bucket, Key=cos_key)
            actual_size = int(response.get('Content-Length', 0))
            return actual_size == expected_size
        except Exception as e:
            st.warning(f"COS上传验证失败: {str(e)}")
            return False
    
    def download_file(self, file_path: str) -> Optional[bytes]:
        """从COS下载文件"""
        if not self.client:
            st.error("COS客户端未初始化，无法下载文件")
            return None
        
        try:
            st.info(f"正在从COS下载文件: {file_path}")
            response = self.client.get_object(Bucket=self.bucket, Key=file_path)
            content = response['Body'].read()
            st.success(f"文件从COS下载成功: {file_path}")
            return content
            
        except CosServiceError as e:
            st.error(f"从COS下载文件失败: {e.get_error_msg()}")
            return None
        except Exception as e:
            st.error(f"COS下载异常: {str(e)}")
            return None
    
    def test_connection(self) -> bool:
        """测试COS连接和权限"""
        if not self.client:
            st.error("COS客户端未初始化")
            return False
        
        try:
            st.info("正在测试COS连接和权限...")
            
            # 测试桶访问权限
            self.client.head_bucket(Bucket=self.bucket)
            st.success("✅ COS桶访问权限正常")
            
            # 测试写权限（上传小文件）
            test_key = f"test/{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            test_content = b"connection test from streamlit app"
            
            self.client.put_object(
                Bucket=self.bucket,
                Key=test_key,
                Body=io.BytesIO(test_content)
            )
            st.success("✅ COS写权限测试成功")
            
            # 测试读权限
            response = self.client.get_object(Bucket=self.bucket, Key=test_key)
            content = response['Body'].read()
            st.success("✅ COS读权限测试成功")
            
            # 清理测试文件
            self.client.delete_object(Bucket=self.bucket, Key=test_key)
            st.success("✅ COS权限测试完成，测试文件已清理")
            
            return content == test_content
            
        except CosServiceError as e:
            st.error(f"COS权限测试失败: {e.get_error_msg()}")
            return False
        except Exception as e:
            st.error(f"COS连接测试失败: {str(e)}")
            return False
    
    def get_upload_url(self, cos_key: str, expires: int = 3600) -> Optional[str]:
        """生成预签名上传URL"""
        try:
            url = self.client.get_presigned_url(
                Method='PUT',
                Bucket=self.bucket,
                Key=cos_key,
                Expired=expires
            )
            return url
        except Exception as e:
            st.error(f"生成COS上传URL失败: {str(e)}")
            return None
    
    def get_download_url(self, cos_key: str, expires: int = 3600) -> Optional[str]:
        """生成预签名下载URL"""
        try:
            url = self.client.get_presigned_url(
                Method='GET',
                Bucket=self.bucket,
                Key=cos_key,
                Expired=expires
            )
            return url
        except Exception as e:
            st.error(f"生成COS下载URL失败: {str(e)}")
            return None
    
    def list_files(self, prefix: str = "uploads/") -> list:
        """列出COS中的文件"""
        try:
            response = self.client.list_objects(
                Bucket=self.bucket,
                Prefix=prefix,
                MaxKeys=1000
            )
            
            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    files.append({
                        'key': obj['Key'],
                        'size': obj['Size'],
                        'last_modified': obj['LastModified'],
                        'etag': obj['ETag'].strip('"')
                    })
            
            return files
            
        except Exception as e:
            st.error(f"列出COS文件失败: {str(e)}")
            return []
    
    def delete_file(self, cos_key: str) -> bool:
        """删除COS文件"""
        try:
            self.client.delete_object(Bucket=self.bucket, Key=cos_key)
            st.success(f"文件已从COS删除: {cos_key}")
            return True
        except Exception as e:
            st.error(f"删除COS文件失败: {str(e)}")
            return False
    
    def get_storage_info(self) -> Dict[str, Any]:
        """获取存储统计信息"""
        try:
            # 真实的COS状态检查
            connection_status = 'connected' if (self.client and self._test_connection()) else 'disconnected'
            
            if connection_status == 'connected':
                files = self.list_files()
                total_size = sum(f['size'] for f in files)
                
                return {
                    'total_files': len(files),
                    'total_size_mb': round(total_size / (1024 * 1024), 2),
                    'bucket': self.bucket,
                    'region': self.region,
                    'region_name': self._get_region_name(),
                    'storage_type': 'COS',
                    'upload_stats': self.upload_stats,
                    'connection_status': connection_status,
                    'client_initialized': self.client is not None,
                    'config_valid': True
                }
            else:
                return {
                    'total_files': 0,
                    'total_size_mb': 0,
                    'bucket': self.bucket,
                    'region': self.region,
                    'region_name': self._get_region_name(),
                    'storage_type': 'COS',
                    'upload_stats': self.upload_stats,
                    'connection_status': connection_status,
                    'client_initialized': self.client is not None,
                    'config_valid': False
                }
            
        except Exception as e:
            return {
                'error': str(e),
                'storage_type': 'COS',
                'connection_status': 'error',
                'client_initialized': self.client is not None,
                'config_valid': False
            }
    
    def _get_region_name(self) -> str:
        """获取地域中文名称"""
        try:
            from config import COS_REGIONS
            return COS_REGIONS.get(self.region, {}).get('name', self.region)
        except:
            return self.region
    
    def _generate_cos_key(self, filename: str, folder: str) -> str:
        """生成COS对象键"""
        # 清理文件名
        clean_filename = filename.replace(' ', '_').replace('(', '').replace(')', '')
        
        # 添加时间戳避免重名
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        name, ext = os.path.splitext(clean_filename)
        unique_filename = f"{name}_{timestamp}{ext}"
        
        # 构建完整路径
        cos_key = f"{folder.strip('/')}/{unique_filename}"
        
        return cos_key
    
    def _update_upload_stats(self, file_size: int, success: bool = True):
        """更新上传统计"""
        self.upload_stats['total_uploads'] += 1
        if success:
            self.upload_stats['total_bytes'] += file_size
            self.upload_stats['last_upload_time'] = datetime.now().isoformat()
        else:
            self.upload_stats['failed_uploads'] += 1
    
    def get_file_info(self, cos_key: str) -> Optional[Dict[str, Any]]:
        """获取文件详细信息"""
        try:
            response = self.client.head_object(Bucket=self.bucket, Key=cos_key)
            
            return {
                'key': cos_key,
                'size': int(response.get('Content-Length', 0)),
                'size_mb': round(int(response.get('Content-Length', 0)) / (1024 * 1024), 2),
                'last_modified': response.get('Last-Modified'),
                'etag': response.get('ETag', '').strip('"'),
                'content_type': response.get('Content-Type', ''),
                'metadata': response.get('Metadata', {}),
                'accessible': True,
                'storage_location': 'COS'
            }
            
        except Exception as e:
            return {
                'key': cos_key,
                'error': str(e),
                'accessible': False,
                'storage_location': 'COS'
            }
    
    def check_bucket_policy(self) -> Dict[str, Any]:
        """检查桶权限策略"""
        try:
            # 检查桶ACL
            acl_response = self.client.get_bucket_acl(Bucket=self.bucket)
            
            # 检查CORS配置
            try:
                cors_response = self.client.get_bucket_cors(Bucket=self.bucket)
                cors_configured = True
            except:
                cors_configured = False
            
            # 测试实际权限
            has_read = True  # 如果能执行head_bucket就有读权限
            has_write = self._test_write_permission()
            
            return {
                'acl_owner': acl_response.get('Owner', {}).get('DisplayName', 'Unknown'),
                'cors_configured': cors_configured,
                'bucket_accessible': True,
                'permissions': {
                    'read': has_read,
                    'write': has_write
                },
                'bucket_name': self.bucket,
                'region': self.region
            }
            
        except Exception as e:
            return {
                'error': str(e),
                'bucket_accessible': False,
                'permissions': {
                    'read': False,
                    'write': False
                },
                'bucket_name': self.bucket,
                'region': self.region
            }
    
    def _test_write_permission(self) -> bool:
        """测试写权限"""
        try:
            test_key = f"test/write_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            test_content = b"write permission test"
            
            # 尝试写入
            self.client.put_object(
                Bucket=self.bucket,
                Key=test_key,
                Body=io.BytesIO(test_content)
            )
            
            # 清理测试文件
            self.client.delete_object(Bucket=self.bucket, Key=test_key)
            
            return True
        except:
            return False
