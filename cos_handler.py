import os
import io
import streamlit as st
from datetime import datetime
from typing import Optional, Dict, Any

try:
    from qcloud_cos import CosConfig, CosS3Client
    HAS_COS_SDK = True
except ImportError:
    HAS_COS_SDK = False
    st.warning("⚠️ 腾讯云 COS SDK 未安装，文件上传功能将被禁用")

class COSHandler:
    def __init__(self):
        self.client = None
        self.config = None
        self.bucket_name = None
        
        if HAS_COS_SDK:
            self._initialize_cos()
    
    def _initialize_cos(self):
        """初始化COS客户端"""
        try:
            # 从 streamlit secrets 或环境变量获取配置
            secret_id = self._get_config('COS_SECRET_ID')
            secret_key = self._get_config('COS_SECRET_KEY')
            region = self._get_config('COS_REGION', 'ap-shanghai')
            bucket_name = self._get_config('COS_BUCKET')
            
            if not all([secret_id, secret_key, bucket_name]):
                st.error("COS 配置不完整，请检查 secrets 设置")
                return
            
            # 创建 COS 配置
            self.config = CosConfig(
                Region=region,
                SecretId=secret_id,
                SecretKey=secret_key,
                Scheme="https"
            )
            
            # 创建 COS 客户端
            self.client = CosS3Client(self.config)
            self.bucket_name = bucket_name
            
        except Exception as e:
            st.error(f"初始化 COS 客户端失败: {str(e)}")
            self.client = None
    
    def _get_config(self, key: str, default: str = None) -> Optional[str]:
        """从 Streamlit secrets 或环境变量获取配置"""
        # 优先从 Streamlit secrets 读取
        if hasattr(st, 'secrets') and key in st.secrets:
            return st.secrets[key]
        # 回退到环境变量
        return os.getenv(key, default)
    
    def test_connection(self) -> bool:
        """测试COS连接"""
        if not HAS_COS_SDK:
            return False
            
        if not self.client:
            return False
        
        try:
            # 尝试列出存储桶内容
            response = self.client.list_objects(
                Bucket=self.bucket_name,
                MaxKeys=1
            )
            return True
        except Exception as e:
            st.error(f"COS 连接测试失败: {str(e)}")
            return False
    
    def upload_file(self, file_content: bytes, file_name: str, folder: str = "") -> Optional[str]:
        """上传文件到COS"""
        if not HAS_COS_SDK:
            st.error("COS SDK 未安装，无法上传文件")
            return None
            
        if not self.client:
            st.error("COS 客户端未初始化")
            return None
        
        try:
            # 构造文件路径
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            file_extension = os.path.splitext(file_name)[1]
            cos_key = f"{folder}/{timestamp}_{file_name}"
            
            # 上传文件
            response = self.client.put_object(
                Bucket=self.bucket_name,
                Body=file_content,
                Key=cos_key,
                ContentType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
            
            return cos_key
            
        except Exception as e:
            st.error(f"文件上传失败: {str(e)}")
            return None
    
    def download_file(self, cos_key: str) -> Optional[bytes]:
        """从COS下载文件"""
        if not HAS_COS_SDK:
            st.error("COS SDK 未安装，无法下载文件")
            return None
            
        if not self.client:
            st.error("COS 客户端未初始化")
            return None
        
        try:
            response = self.client.get_object(
                Bucket=self.bucket_name,
                Key=cos_key
            )
            
            return response['Body'].read()
            
        except Exception as e:
            st.error(f"文件下载失败: {str(e)}")
            return None
    
    def file_exists(self, cos_key: str) -> bool:
        """检查文件是否存在"""
        if not HAS_COS_SDK or not self.client:
            return False
        
        try:
            self.client.head_object(
                Bucket=self.bucket_name,
                Key=cos_key
            )
            return True
        except:
            return False
    
    def delete_file(self, cos_key: str) -> bool:
        """删除文件"""
        if not HAS_COS_SDK or not self.client:
            return False
        
        try:
            self.client.delete_object(
                Bucket=self.bucket_name,
                Key=cos_key
            )
            return True
        except Exception as e:
            st.error(f"文件删除失败: {str(e)}")
            return False
    
    def list_files(self, prefix: str = "") -> list:
        """列出文件"""
        if not HAS_COS_SDK or not self.client:
            return []
        
        try:
            response = self.client.list_objects(
                Bucket=self.bucket_name,
                Prefix=prefix,
                MaxKeys=1000
            )
            
            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    files.append({
                        'key': obj['Key'],
                        'size': obj['Size'],
                        'last_modified': obj['LastModified']
                    })
            
            return files
            
        except Exception as e:
            st.error(f"列出文件失败: {str(e)}")
            return []
    
    def get_file_url(self, cos_key: str, expire_seconds: int = 3600) -> Optional[str]:
        """获取文件临时访问URL"""
        if not HAS_COS_SDK or not self.client:
            return None
        
        try:
            url = self.client.get_presigned_url(
                Method='GET',
                Bucket=self.bucket_name,
                Key=cos_key,
                Expired=expire_seconds
            )
            return url
        except Exception as e:
            st.error(f"获取文件URL失败: {str(e)}")
            return None
    
    def get_upload_status(self) -> Dict[str, Any]:
        """获取上传状态"""
        return {
            'has_sdk': HAS_COS_SDK,
            'client_initialized': self.client is not None,
            'bucket_name': self.bucket_name,
            'connection_ok': self.test_connection() if HAS_COS_SDK else False
        }
