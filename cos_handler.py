import os
import io
from typing import Optional, BinaryIO
from datetime import datetime
import streamlit as st
from qcloud_cos import CosConfig, CosS3Client
from qcloud_cos.cos_exception import CosClientError, CosServiceError
from config import COS_CONFIG

class COSHandler:
    def __init__(self):
        self.config = CosConfig(
            Region=COS_CONFIG['region'],
            SecretId=COS_CONFIG['secret_id'],
            SecretKey=COS_CONFIG['secret_key'],
        )
        self.client = CosS3Client(self.config)
        self.bucket = COS_CONFIG['bucket']
    
    def upload_file(self, file_content: bytes, file_name: str, folder: str = "reports") -> Optional[str]:
        """
        上传文件到COS
        
        Args:
            file_content: 文件内容
            file_name: 文件名
            folder: 存储文件夹
            
        Returns:
            文件的COS路径，失败返回None
        """
        try:
            # 生成唯一的文件路径
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            file_extension = os.path.splitext(file_name)[1]
            unique_name = f"{os.path.splitext(file_name)[0]}_{timestamp}{file_extension}"
            cos_path = f"{folder}/{unique_name}"
            
            # 上传文件
            response = self.client.put_object(
                Bucket=self.bucket,
                Body=file_content,
                Key=cos_path,
                ContentType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
            
            st.success(f"文件上传成功: {cos_path}")
            return cos_path
            
        except CosClientError as e:
            st.error(f"COS客户端错误: {e}")
            return None
        except CosServiceError as e:
            st.error(f"COS服务错误: {e}")
            return None
        except Exception as e:
            st.error(f"上传失败: {e}")
            return None
    
    def download_file(self, cos_path: str) -> Optional[bytes]:
        """
        从COS下载文件
        
        Args:
            cos_path: COS文件路径
            
        Returns:
            文件内容，失败返回None
        """
        try:
            response = self.client.get_object(
                Bucket=self.bucket,
                Key=cos_path
            )
            
            file_content = response['Body'].read()
            return file_content
            
        except CosClientError as e:
            st.error(f"COS客户端错误: {e}")
            return None
        except CosServiceError as e:
            st.error(f"COS服务错误: {e}")
            return None
        except Exception as e:
            st.error(f"下载失败: {e}")
            return None
    
    def delete_file(self, cos_path: str) -> bool:
        """
        删除COS文件
        
        Args:
            cos_path: COS文件路径
            
        Returns:
            成功返回True，失败返回False
        """
        try:
            self.client.delete_object(
                Bucket=self.bucket,
                Key=cos_path
            )
            st.success(f"文件删除成功: {cos_path}")
            return True
            
        except CosClientError as e:
            st.error(f"COS客户端错误: {e}")
            return False
        except CosServiceError as e:
            st.error(f"COS服务错误: {e}")
            return False
        except Exception as e:
            st.error(f"删除失败: {e}")
            return False
    
    def file_exists(self, cos_path: str) -> bool:
        """
        检查文件是否存在
        
        Args:
            cos_path: COS文件路径
            
        Returns:
            存在返回True，不存在返回False
        """
        try:
            self.client.head_object(
                Bucket=self.bucket,
                Key=cos_path
            )
            return True
            
        except CosServiceError as e:
            if e.get_status_code() == 404:
                return False
            st.error(f"检查文件存在性失败: {e}")
            return False
        except Exception as e:
            st.error(f"检查文件存在性失败: {e}")
            return False
    
    def get_file_info(self, cos_path: str) -> Optional[dict]:
        """
        获取文件信息
        
        Args:
            cos_path: COS文件路径
            
        Returns:
            文件信息字典，失败返回None
        """
        try:
            response = self.client.head_object(
                Bucket=self.bucket,
                Key=cos_path
            )
            
            return {
                'size': response.get('Content-Length', 0),
                'last_modified': response.get('Last-Modified', ''),
                'etag': response.get('ETag', ''),
                'content_type': response.get('Content-Type', ''),
            }
            
        except CosServiceError as e:
            if e.get_status_code() != 404:
                st.error(f"获取文件信息失败: {e}")
            return None
        except Exception as e:
            st.error(f"获取文件信息失败: {e}")
            return None
    
    def get_download_url(self, cos_path: str, expires: int = 3600) -> Optional[str]:
        """
        生成下载链接
        
        Args:
            cos_path: COS文件路径
            expires: 链接有效期（秒）
            
        Returns:
            下载链接，失败返回None
        """
        try:
            url = self.client.get_presigned_download_url(
                Bucket=self.bucket,
                Key=cos_path,
                Expired=expires
            )
            return url
            
        except Exception as e:
            st.error(f"生成下载链接失败: {e}")
            return None
    
    def test_connection(self) -> bool:
        """
        测试COS连接
        
        Returns:
            连接成功返回True，失败返回False
        """
        try:
            # 尝试列出bucket，验证连接和权限
            response = self.client.list_objects(
                Bucket=self.bucket,
                MaxKeys=1
            )
            return True
            
        except Exception as e:
            st.error(f"COS连接测试失败: {e}")
            return False
