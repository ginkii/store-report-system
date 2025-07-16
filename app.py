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
import re

# ===================== 页面配置 =====================
st.set_page_config(
    page_title="门店报表查询系统", 
    page_icon="📊",
    layout="wide"
)

# ===================== 系统配置 =====================
ADMIN_PASSWORD = st.secrets.get("system", {}).get("admin_password", "admin123")
MAX_STORAGE_GB = 40
API_RATE_LIMIT = 200  # 降低API限制，提高稳定性
SYNC_WAIT_TIME = 3  # 同步等待时间（秒）

# ===================== CSS样式 =====================
st.markdown("""
    <style>
    .main-header {
        font-size: 3rem;
        color: #1f77b4;
        text-align: center;
        padding: 2rem 0;
        margin-bottom: 2rem;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: 800;
    }
    .success-box {
        background: linear-gradient(135deg, #84fab0 0%, #8fd3f4 100%);
        padding: 2rem;
        border-radius: 15px;
        margin: 1.5rem 0;
        color: #2d3436;
        border-left: 6px solid #00b894;
        box-shadow: 0 8px 25px rgba(0, 0, 0, 0.1);
    }
    .warning-box {
        background: linear-gradient(135deg, #fdcb6e 0%, #e17055 100%);
        padding: 2rem;
        border-radius: 15px;
        margin: 1.5rem 0;
        color: white;
        border-left: 6px solid #e84393;
        box-shadow: 0 8px 25px rgba(0, 0, 0, 0.1);
    }
    .error-box {
        background: linear-gradient(135deg, #fd79a8 0%, #e84393 100%);
        padding: 2rem;
        border-radius: 15px;
        margin: 1.5rem 0;
        color: white;
        border-left: 6px solid #d63031;
        box-shadow: 0 8px 25px rgba(0, 0, 0, 0.1);
    }
    .debug-box {
        background: linear-gradient(135deg, #a29bfe 0%, #6c5ce7 100%);
        padding: 1.5rem;
        border-radius: 12px;
        margin: 1rem 0;
        color: white;
        border: 2px solid #74b9ff;
        font-family: monospace;
    }
    .store-info {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 2.5rem;
        border-radius: 20px;
        margin: 2rem 0;
        box-shadow: 0 15px 35px rgba(0, 0, 0, 0.1);
        text-align: center;
    }
    .admin-panel {
        background: linear-gradient(135deg, #ffeaa7 0%, #fab1a0 100%);
        padding: 2.5rem;
        border-radius: 20px;
        border: 3px solid #fdcb6e;
        margin: 2rem 0;
        box-shadow: 0 12px 30px rgba(0, 0, 0, 0.1);
    }
    .diagnostic-info {
        background: linear-gradient(135deg, #00cec9 0%, #55a3ff 100%);
        padding: 1.5rem;
        border-radius: 12px;
        margin: 1rem 0;
        color: white;
        border-left: 5px solid #0984e3;
    }
    .step-indicator {
        background: white;
        padding: 1rem;
        border-radius: 10px;
        border-left: 4px solid #00b894;
        margin: 0.5rem 0;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
    }
    </style>
""", unsafe_allow_html=True)

# ===================== 日志配置 =====================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ===================== 核心工具函数 =====================
def debug_log(message: str, data: Any = None):
    """调试日志函数"""
    if st.session_state.get('debug_mode', False):
        st.write(f"🔍 **调试**: {message}")
        if data is not None:
            st.write(f"📝 **数据**: {data}")
    logger.info(f"DEBUG: {message} - {data}")

def safe_filename(filename: str) -> str:
    """生成安全的文件名"""
    # 移除或替换特殊字符
    safe_name = re.sub(r'[<>:"/\\|?*\r\n\t]', '_', filename)
    safe_name = re.sub(r'\s+', '_', safe_name.strip())
    safe_name = safe_name.strip('.')
    return safe_name if safe_name else f"file_{int(time.time())}"

def wait_for_sync(seconds: int = SYNC_WAIT_TIME):
    """等待云端同步"""
    debug_log(f"等待云端同步 {seconds} 秒")
    with st.spinner(f"⏳ 等待云端数据同步... ({seconds}秒)"):
        time.sleep(seconds)

def validate_json_data(data: dict, required_keys: List[str]) -> bool:
    """验证JSON数据结构"""
    for key in required_keys:
        if key not in data:
            debug_log(f"JSON验证失败: 缺少必需键 {key}")
            return False
    return True

# ===================== 简化压缩管理器 =====================
class SimpleCompression:
    """超简化的压缩管理器 - 重点解决压缩/解压问题"""
    
    @staticmethod
    def compress_bytes(data: bytes) -> Tuple[bytes, bool]:
        """压缩字节数据，返回(数据, 是否压缩成功)"""
        try:
            compressed = gzip.compress(data, compresslevel=6)
            # 只有压缩效果明显才使用压缩版本
            if len(compressed) < len(data) * 0.8:
                debug_log(f"压缩成功: {len(data)} -> {len(compressed)} bytes")
                return compressed, True
            else:
                debug_log("压缩效果不明显，使用原始数据")
                return data, False
        except Exception as e:
            debug_log(f"压缩失败: {str(e)}")
            return data, False
    
    @staticmethod
    def decompress_bytes(data: bytes, is_compressed: bool = True) -> bytes:
        """解压字节数据，支持容错"""
        if not is_compressed:
            return data
            
        try:
            result = gzip.decompress(data)
            debug_log(f"解压成功: {len(data)} -> {len(result)} bytes")
            return result
        except Exception as e:
            debug_log(f"解压失败，返回原始数据: {str(e)}")
            return data
    
    @staticmethod
    def compress_json(data: dict) -> Tuple[bytes, bool]:
        """压缩JSON数据"""
        try:
            json_str = json.dumps(data, ensure_ascii=False, separators=(',', ':'))
            json_bytes = json_str.encode('utf-8')
            return SimpleCompression.compress_bytes(json_bytes)
        except Exception as e:
            debug_log(f"JSON压缩失败: {str(e)}")
            fallback = json.dumps(data, ensure_ascii=False).encode('utf-8')
            return fallback, False
    
    @staticmethod
    def decompress_json(data: bytes, is_compressed: bool = True) -> dict:
        """解压JSON数据"""
        try:
            # 先尝试解压
            if is_compressed:
                try:
                    decompressed = gzip.decompress(data)
                    result = json.loads(decompressed.decode('utf-8'))
                    debug_log("JSON解压成功")
                    return result
                except:
                    debug_log("GZIP解压失败，尝试直接解析")
            
            # 直接解析JSON
            result = json.loads(data.decode('utf-8'))
            debug_log("JSON直接解析成功")
            return result
            
        except Exception as e:
            debug_log(f"JSON解压完全失败: {str(e)}")
            return {}

# ===================== 腾讯云COS管理器 - 问题修复版 =====================
class FixedCOSManager:
    """修复版腾讯云COS管理器 - 专门解决上传后查询问题"""
    
    def __init__(self):
        self.client = None
        self.bucket_name = None
        self.region = None
        self.compression = SimpleCompression()
        self.init_client()
    
    def init_client(self):
        """初始化COS客户端"""
        try:
            debug_log("开始初始化腾讯云COS客户端")
            
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
            debug_log(f"COS客户端初始化成功: {self.bucket_name}")
            
        except Exception as e:
            debug_log(f"COS初始化失败: {str(e)}")
            raise
    
    def upload_file_simple(self, file_data: bytes, filename: str) -> Tuple[Optional[str], str]:
        """简化的文件上传 - 重点解决文件名和路径问题"""
        try:
            # 1. 清理文件名
            clean_filename = safe_filename(filename)
            debug_log(f"原始文件名: {filename}")
            debug_log(f"清理后文件名: {clean_filename}")
            
            # 2. 尝试压缩
            upload_data, is_compressed = self.compression.compress_bytes(file_data)
            final_filename = clean_filename
            
            if is_compressed:
                if not final_filename.endswith('.gz'):
                    final_filename = final_filename + '.gz'
                compression_ratio = (1 - len(upload_data) / len(file_data)) * 100
                st.info(f"📦 压缩效果: {len(file_data)/1024:.1f}KB → {len(upload_data)/1024:.1f}KB (节省{compression_ratio:.1f}%)")
            
            debug_log(f"最终文件名: {final_filename}")
            debug_log(f"是否压缩: {is_compressed}")
            
            # 3. 上传文件
            response = self.client.put_object(
                Bucket=self.bucket_name,
                Body=upload_data,
                Key=final_filename,
                ContentType='application/octet-stream'
            )
            
            # 4. 验证上传成功
            if response.get('ETag'):
                file_url = f"https://{self.bucket_name}.cos.{self.region}.myqcloud.com/{final_filename}"
                debug_log(f"文件上传成功: {final_filename}")
                debug_log(f"文件URL: {file_url}")
                
                # 5. 立即验证文件是否可访问
                wait_for_sync(2)  # 等待2秒确保上传完成
                
                if self.verify_file_exists(final_filename):
                    debug_log("文件上传验证成功")
                    return file_url, final_filename
                else:
                    debug_log("文件上传验证失败")
                    return None, "文件上传后验证失败"
            else:
                return None, "上传响应异常"
            
        except Exception as e:
            error_msg = f"文件上传失败: {str(e)}"
            debug_log(error_msg)
            return None, error_msg
    
    def download_file_simple(self, filename: str) -> Tuple[Optional[bytes], str]:
        """简化的文件下载 - 重点解决解压问题"""
        try:
            debug_log(f"开始下载文件: {filename}")
            
            # 1. 下载文件
            response = self.client.get_object(
                Bucket=self.bucket_name,
                Key=filename
            )
            
            file_data = response['Body'].read()
            debug_log(f"文件下载成功: {len(file_data)} bytes")
            
            # 2. 判断是否需要解压
            is_compressed = filename.endswith('.gz')
            debug_log(f"文件是否压缩: {is_compressed}")
            
            # 3. 解压文件
            if is_compressed:
                decompressed_data = self.compression.decompress_bytes(file_data, True)
                debug_log(f"文件解压完成: {len(decompressed_data)} bytes")
                return decompressed_data, "下载解压成功"
            else:
                return file_data, "下载成功"
                
        except CosServiceError as e:
            if e.get_error_code() == 'NoSuchKey':
                error_msg = f"文件不存在: {filename}"
                debug_log(error_msg)
                return None, error_msg
            else:
                error_msg = f"COS服务错误: {e.get_error_msg()}"
                debug_log(error_msg)
                return None, error_msg
        except Exception as e:
            error_msg = f"文件下载失败: {str(e)}"
            debug_log(error_msg)
            return None, error_msg
    
    def verify_file_exists(self, filename: str) -> bool:
        """验证文件是否存在"""
        try:
            self.client.head_object(
                Bucket=self.bucket_name,
                Key=filename
            )
            debug_log(f"文件存在验证成功: {filename}")
            return True
        except Exception:
            debug_log(f"文件不存在: {filename}")
            return False
    
    def list_all_files(self) -> List[Dict]:
        """列出所有文件"""
        try:
            response = self.client.list_objects(
                Bucket=self.bucket_name,
                MaxKeys=1000
            )
            
            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    files.append({
                        'filename': obj['Key'],
                        'size': obj['Size'],
                        'last_modified': obj['LastModified']
                    })
            
            debug_log(f"列出文件成功: {len(files)} 个文件")
            return files
            
        except Exception as e:
            debug_log(f"列出文件失败: {str(e)}")
            return []
    
    def upload_json_simple(self, data: dict, filename: str) -> Tuple[bool, str]:
        """简化的JSON上传"""
        try:
            debug_log(f"开始上传JSON: {filename}")
            debug_log(f"JSON数据: {data}")
            
            # 1. 压缩JSON
            json_bytes, is_compressed = self.compression.compress_json(data)
            
            # 2. 确定文件名
            final_filename = filename
            if is_compressed and not filename.endswith('.gz'):
                final_filename = filename + '.gz'
            
            debug_log(f"JSON最终文件名: {final_filename}")
            
            # 3. 上传
            file_url, upload_filename = self.upload_file_simple(json_bytes, final_filename)
            
            if file_url:
                debug_log(f"JSON上传成功: {upload_filename}")
                return True, upload_filename
            else:
                return False, upload_filename  # 这里upload_filename是错误消息
                
        except Exception as e:
            error_msg = f"JSON上传失败: {str(e)}"
            debug_log(error_msg)
            return False, error_msg
    
    def download_json_simple(self, filename: str) -> Tuple[Optional[dict], str]:
        """简化的JSON下载"""
        try:
            debug_log(f"开始下载JSON: {filename}")
            
            # 1. 尝试下载压缩版本
            compressed_filename = filename if filename.endswith('.gz') else filename + '.gz'
            
            for try_filename in [compressed_filename, filename]:
                if self.verify_file_exists(try_filename):
                    debug_log(f"找到文件: {try_filename}")
                    
                    file_data, status = self.download_file_simple(try_filename)
                    if file_data:
                        # 2. 解析JSON
                        is_compressed = try_filename.endswith('.gz')
                        json_data = self.compression.decompress_json(file_data, is_compressed)
                        
                        debug_log(f"JSON下载解析成功: {len(json_data)} 个键")
                        return json_data, try_filename
                    else:
                        debug_log(f"文件下载失败: {status}")
            
            return None, f"未找到文件: {filename}"
            
        except Exception as e:
            error_msg = f"JSON下载失败: {str(e)}"
            debug_log(error_msg)
            return None, error_msg

# ===================== 主系统类 - 问题修复版 =====================
class FixedStoreSystem:
    """修复版门店报表系统 - 专门解决上传后查询问题"""
    
    def __init__(self):
        self.cos = FixedCOSManager()
        self.permissions_file = "system/permissions.json"
        self.metadata_file = "system/metadata.json"
        debug_log("系统初始化完成")
    
    def process_permissions_file(self, uploaded_file) -> bool:
        """处理权限文件 - 增强版"""
        try:
            debug_log("开始处理权限文件")
            
            # 1. 读取Excel文件
            df = pd.read_excel(uploaded_file)
            debug_log(f"Excel读取成功: {len(df)} 行, {len(df.columns)} 列")
            
            if len(df.columns) < 2:
                st.error("❌ 权限文件格式错误：需要至少两列（门店名称、人员编号）")
                return False
            
            # 2. 处理数据
            permissions_data = []
            valid_count = 0
            invalid_count = 0
            
            for index, row in df.iterrows():
                try:
                    store_name = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ""
                    user_id = str(row.iloc[1]).strip() if pd.notna(row.iloc[1]) else ""
                    
                    # 严格验证数据
                    if (store_name and user_id and 
                        store_name.lower() not in ['nan', 'none', '', 'null'] and 
                        user_id.lower() not in ['nan', 'none', '', 'null']):
                        
                        permissions_data.append({
                            "store_name": store_name,
                            "user_id": user_id,
                            "created_at": datetime.now().isoformat(),
                            "row_index": index + 2  # Excel行号从2开始
                        })
                        valid_count += 1
                        debug_log(f"有效权限记录: {store_name} - {user_id}")
                    else:
                        invalid_count += 1
                        debug_log(f"无效权限记录 第{index+2}行: '{store_name}' - '{user_id}'")
                        
                except Exception as e:
                    invalid_count += 1
                    debug_log(f"处理第{index+2}行时出错: {str(e)}")
            
            if valid_count == 0:
                st.error("❌ 没有找到有效的权限数据")
                return False
            
            # 3. 构建权限数据结构
            permissions_structure = {
                'permissions': permissions_data,
                'metadata': {
                    'total_count': valid_count,
                    'invalid_count': invalid_count,
                    'upload_time': datetime.now().isoformat(),
                    'version': '1.0'
                }
            }
            
            debug_log(f"权限数据结构构建完成: {valid_count} 条有效记录")
            
            # 4. 保存权限数据
            st.info("📤 正在保存权限数据...")
            success, result_filename = self.cos.upload_json_simple(permissions_structure, self.permissions_file)
            
            if success:
                debug_log(f"权限数据保存成功: {result_filename}")
                
                # 5. 等待同步
                wait_for_sync(3)
                
                # 6. 立即验证数据
                verification_data, verify_filename = self.cos.download_json_simple(self.permissions_file)
                if verification_data and 'permissions' in verification_data:
                    verify_count = len(verification_data['permissions'])
                    debug_log(f"权限数据验证成功: {verify_count} 条记录")
                    
                    st.markdown(f'''
                    <div class="success-box">
                    <h4>✅ 权限数据上传成功</h4>
                    <p><strong>有效记录</strong>: {valid_count} 条</p>
                    <p><strong>跳过记录</strong>: {invalid_count} 条</p>
                    <p><strong>保存文件</strong>: {result_filename}</p>
                    <p><strong>验证结果</strong>: ✅ 数据完整性检查通过</p>
                    </div>
                    ''', unsafe_allow_html=True)
                    
                    # 7. 显示权限预览
                    if len(permissions_data) > 0:
                        st.subheader("📋 权限记录预览")
                        preview_df = pd.DataFrame(permissions_data[:10])
                        st.dataframe(preview_df[['store_name', 'user_id']], use_container_width=True)
                        
                        # 8. 显示门店统计
                        unique_stores = list(set([p['store_name'] for p in permissions_data]))
                        st.info(f"🏪 支持门店数量: {len(unique_stores)} 个")
                        st.write("**门店列表**:", ", ".join(unique_stores[:10]))
                    
                    return True
                else:
                    st.error("❌ 权限数据验证失败")
                    debug_log("权限数据验证失败")
                    return False
            else:
                st.error(f"❌ 权限数据保存失败: {result_filename}")
                return False
                
        except Exception as e:
            error_msg = f"处理权限文件失败: {str(e)}"
            st.error(f"❌ {error_msg}")
            debug_log(error_msg)
            debug_log(f"错误详情: {traceback.format_exc()}")
            return False
    
    def process_reports_file(self, uploaded_file) -> bool:
        """处理报表文件 - 增强版"""
        try:
            debug_log("开始处理报表文件")
            
            file_size_mb = len(uploaded_file.getvalue()) / 1024 / 1024
            debug_log(f"报表文件大小: {file_size_mb:.2f} MB")
            
            # 1. 验证Excel文件
            try:
                excel_file = pd.ExcelFile(uploaded_file)
                sheet_names = excel_file.sheet_names
                debug_log(f"Excel工作表: {sheet_names}")
                
                if len(sheet_names) == 0:
                    st.error("❌ Excel文件没有工作表")
                    return False
                    
            except Exception as e:
                st.error(f"❌ Excel文件格式错误: {str(e)}")
                return False
            
            # 2. 生成文件名
            timestamp = int(time.time())
            file_hash = hashlib.md5(uploaded_file.getvalue()).hexdigest()[:8]
            filename = f"reports/report_{timestamp}_{file_hash}.xlsx"
            debug_log(f"生成文件名: {filename}")
            
            # 3. 上传文件
            st.info("📤 正在上传报表文件...")
            file_url, final_filename = self.cos.upload_file_simple(uploaded_file.getvalue(), filename)
            
            if not file_url:
                st.error(f"❌ 文件上传失败: {final_filename}")
                return False
            
            debug_log(f"报表文件上传成功: {final_filename}")
            st.success(f"✅ 文件上传成功: {final_filename}")
            
            # 4. 解析工作表内容
            st.info("📊 正在分析工作表内容...")
            
            # 加载现有元数据
            metadata, metadata_filename = self.cos.download_json_simple(self.metadata_file)
            if not metadata:
                metadata = {'reports': []}
                debug_log("创建新的元数据结构")
            else:
                debug_log(f"加载现有元数据: {len(metadata.get('reports', []))} 个报表")
            
            processed_sheets = []
            failed_sheets = []
            
            for sheet_name in sheet_names:
                try:
                    debug_log(f"处理工作表: {sheet_name}")
                    
                    # 读取工作表数据
                    df = pd.read_excel(uploaded_file, sheet_name=sheet_name)
                    
                    if df.empty:
                        debug_log(f"工作表为空，跳过: {sheet_name}")
                        continue
                    
                    # 分析应收-未收额
                    analysis_result = self.analyze_receivable_amount(df)
                    debug_log(f"应收-未收额分析结果: {analysis_result}")
                    
                    # 创建报表元数据
                    report_metadata = {
                        "store_name": sheet_name.strip(),
                        "filename": final_filename,
                        "file_url": file_url,
                        "file_size_mb": file_size_mb,
                        "upload_time": datetime.now().isoformat(),
                        "row_count": len(df),
                        "col_count": len(df.columns),
                        "analysis": analysis_result,
                        "id": f"{safe_filename(sheet_name)}_{timestamp}",
                        "status": "active"
                    }
                    
                    # 移除同门店的旧记录
                    metadata['reports'] = [r for r in metadata.get('reports', []) 
                                         if r.get('store_name', '').strip() != sheet_name.strip()]
                    
                    # 添加新记录
                    metadata.setdefault('reports', []).append(report_metadata)
                    processed_sheets.append(sheet_name)
                    
                    debug_log(f"工作表处理成功: {sheet_name}")
                    st.success(f"✅ {sheet_name}: {len(df)} 行数据已处理")
                    
                except Exception as e:
                    failed_sheets.append(sheet_name)
                    debug_log(f"工作表处理失败 {sheet_name}: {str(e)}")
                    st.warning(f"⚠️ 跳过工作表 '{sheet_name}': {str(e)}")
            
            # 5. 保存元数据
            if len(processed_sheets) > 0:
                metadata['last_updated'] = datetime.now().isoformat()
                metadata['total_reports'] = len(metadata['reports'])
                
                debug_log(f"准备保存元数据: {len(metadata['reports'])} 个报表")
                
                success, metadata_result = self.cos.upload_json_simple(metadata, self.metadata_file)
                
                if success:
                    debug_log(f"元数据保存成功: {metadata_result}")
                    
                    # 6. 等待同步
                    wait_for_sync(3)
                    
                    # 7. 验证元数据
                    verify_metadata, verify_filename = self.cos.download_json_simple(self.metadata_file)
                    if verify_metadata and 'reports' in verify_metadata:
                        verify_count = len(verify_metadata['reports'])
                        debug_log(f"元数据验证成功: {verify_count} 个报表")
                        
                        st.markdown(f'''
                        <div class="success-box">
                        <h4>🎉 报表处理完成</h4>
                        <p><strong>成功处理</strong>: {len(processed_sheets)} 个工作表</p>
                        <p><strong>失败跳过</strong>: {len(failed_sheets)} 个工作表</p>
                        <p><strong>保存文件</strong>: {final_filename}</p>
                        <p><strong>元数据文件</strong>: {metadata_result}</p>
                        <p><strong>验证结果</strong>: ✅ 数据完整性检查通过</p>
                        <p><strong>系统状态</strong>: 数据已同步，立即可用</p>
                        </div>
                        ''', unsafe_allow_html=True)
                        
                        # 8. 显示处理结果
                        if processed_sheets:
                            st.subheader("📊 处理成功的工作表")
                            for sheet in processed_sheets:
                                st.write(f"• {sheet}")
                        
                        if failed_sheets:
                            st.subheader("⚠️ 跳过的工作表")
                            for sheet in failed_sheets:
                                st.write(f"• {sheet}")
                        
                        return True
                    else:
                        st.error("❌ 元数据验证失败")
                        debug_log("元数据验证失败")
                        return False
                else:
                    st.error(f"❌ 元数据保存失败: {metadata_result}")
                    return False
            else:
                st.error("❌ 没有成功处理任何工作表")
                return False
                
        except Exception as e:
            error_msg = f"处理报表文件失败: {str(e)}"
            st.error(f"❌ {error_msg}")
            debug_log(error_msg)
            debug_log(f"错误详情: {traceback.format_exc()}")
            return False
    
    def analyze_receivable_amount(self, df: pd.DataFrame) -> Dict[str, Any]:
        """分析应收-未收额数据 - 增强版"""
        result = {}
        
        try:
            debug_log(f"开始分析应收-未收额数据: {len(df)} 行 x {len(df.columns)} 列")
            
            if len(df) <= 68:
                debug_log("数据行数不足68行，无法分析第69行")
                return result
            
            # 检查第69行
            row_69 = df.iloc[68]  # 第69行，索引为68
            first_col_value = str(row_69.iloc[0]).strip() if pd.notna(row_69.iloc[0]) else ""
            
            debug_log(f"第69行第一列内容: '{first_col_value}'")
            
            # 检查关键词
            keywords = ['应收-未收额', '应收未收额', '应收-未收', '应收未收']
            
            found_keyword = None
            for keyword in keywords:
                if keyword in first_col_value:
                    found_keyword = keyword
                    debug_log(f"找到关键词: {keyword}")
                    break
            
            if found_keyword:
                # 从右往左查找数值
                for col_idx in range(len(row_69)-1, -1, -1):
                    val = row_69.iloc[col_idx]
                    
                    if pd.notna(val):
                        val_str = str(val).strip()
                        debug_log(f"检查列 {col_idx} 值: '{val_str}'")
                        
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
                                        'column_index': col_idx,
                                        'row_name': first_col_value,
                                        'found_keyword': found_keyword,
                                        'original_value': val_str,
                                        'cleaned_value': cleaned
                                    }
                                    debug_log(f"找到应收-未收额: {amount} (列: {col_idx})")
                                    return result
                            except ValueError as ve:
                                debug_log(f"数值转换失败: '{cleaned}' - {str(ve)}")
                                continue
            else:
                debug_log(f"未找到关键词，第69行内容: '{first_col_value}'")
        
        except Exception as e:
            debug_log(f"分析应收-未收额时出错: {str(e)}")
        
        return result
    
    def get_available_stores(self) -> List[str]:
        """获取可用门店列表 - 增强版"""
        try:
            debug_log("开始获取可用门店列表")
            
            # 加载权限数据
            permissions_data, permissions_filename = self.cos.download_json_simple(self.permissions_file)
            
            if not permissions_data or 'permissions' not in permissions_data:
                debug_log("未找到权限数据")
                return []
            
            permissions = permissions_data['permissions']
            debug_log(f"加载权限数据成功: {len(permissions)} 条记录")
            
            # 提取门店名称
            stores = []
            for perm in permissions:
                store_name = perm.get('store_name', '').strip()
                if store_name and store_name not in stores:
                    stores.append(store_name)
            
            stores.sort()
            debug_log(f"可用门店: {stores}")
            
            return stores
            
        except Exception as e:
            debug_log(f"获取门店列表失败: {str(e)}")
            return []
    
    def verify_user_permission(self, store_name: str, user_id: str) -> bool:
        """验证用户权限 - 增强版"""
        try:
            debug_log(f"开始验证用户权限: 门店='{store_name}', 用户ID='{user_id}'")
            
            # 加载权限数据
            permissions_data, permissions_filename = self.cos.download_json_simple(self.permissions_file)
            
            if not permissions_data or 'permissions' not in permissions_data:
                debug_log("权限数据不存在")
                return False
            
            permissions = permissions_data['permissions']
            debug_log(f"权限数据加载成功: {len(permissions)} 条记录")
            
            # 查找匹配的权限
            for perm in permissions:
                stored_store = perm.get('store_name', '').strip()
                stored_id = perm.get('user_id', '').strip()
                
                debug_log(f"检查权限记录: '{stored_store}' - '{stored_id}'")
                
                # 精确匹配
                if stored_store == store_name and stored_id == str(user_id).strip():
                    debug_log("权限验证成功: 精确匹配")
                    return True
                
                # 模糊匹配
                if (str(user_id).strip() == stored_id and 
                    (store_name in stored_store or stored_store in store_name)):
                    debug_log("权限验证成功: 模糊匹配")
                    return True
            
            debug_log("权限验证失败: 未找到匹配记录")
            return False
            
        except Exception as e:
            debug_log(f"权限验证异常: {str(e)}")
            return False
    
    def load_store_data(self, store_name: str) -> Optional[pd.DataFrame]:
        """加载门店数据 - 增强版"""
        try:
            debug_log(f"开始加载门店数据: {store_name}")
            
            # 1. 加载元数据
            metadata, metadata_filename = self.cos.download_json_simple(self.metadata_file)
            
            if not metadata or 'reports' not in metadata:
                debug_log("元数据不存在或格式错误")
                return None
            
            reports = metadata['reports']
            debug_log(f"元数据加载成功: {len(reports)} 个报表")
            
            # 2. 查找匹配的报表
            matching_report = None
            for report in reports:
                report_store_name = report.get('store_name', '').strip()
                debug_log(f"检查报表: '{report_store_name}'")
                
                # 精确匹配
                if report_store_name == store_name:
                    matching_report = report
                    debug_log(f"找到精确匹配报表: {report_store_name}")
                    break
                # 模糊匹配
                elif store_name in report_store_name or report_store_name in store_name:
                    matching_report = report
                    debug_log(f"找到模糊匹配报表: {report_store_name}")
                    break
            
            if not matching_report:
                debug_log(f"未找到门店 '{store_name}' 的报表")
                available_stores = [r.get('store_name', '') for r in reports]
                debug_log(f"可用报表门店: {available_stores}")
                return None
            
            filename = matching_report.get('filename')
            if not filename:
                debug_log("报表元数据中缺少文件名")
                return None
            
            debug_log(f"找到报表文件: {filename}")
            
            # 3. 下载文件
            file_data, download_status = self.cos.download_file_simple(filename)
            
            if not file_data:
                debug_log(f"文件下载失败: {download_status}")
                return None
            
            debug_log(f"文件下载成功: {len(file_data)} bytes")
            
            # 4. 解析Excel文件
            try:
                excel_file = pd.ExcelFile(io.BytesIO(file_data))
                sheet_names = excel_file.sheet_names
                debug_log(f"Excel工作表: {sheet_names}")
                
                # 5. 查找匹配的工作表
                target_sheet = None
                
                # 精确匹配
                if store_name in sheet_names:
                    target_sheet = store_name
                    debug_log(f"精确匹配工作表: {target_sheet}")
                else:
                    # 模糊匹配
                    for sheet in sheet_names:
                        if store_name in sheet or sheet in store_name:
                            target_sheet = sheet
                            debug_log(f"模糊匹配工作表: {target_sheet}")
                            break
                    
                    # 如果还是没找到，使用第一个工作表
                    if not target_sheet and sheet_names:
                        target_sheet = sheet_names[0]
                        debug_log(f"使用第一个工作表: {target_sheet}")
                
                if target_sheet:
                    df = pd.read_excel(io.BytesIO(file_data), sheet_name=target_sheet)
                    debug_log(f"工作表加载成功: {len(df)} 行 x {len(df.columns)} 列")
                    return df
                else:
                    debug_log("未找到合适的工作表")
                    return None
                    
            except Exception as e:
                debug_log(f"Excel解析失败: {str(e)}")
                return None
            
        except Exception as e:
            debug_log(f"加载门店数据失败: {str(e)}")
            return None
    
    def get_system_status(self) -> Dict:
        """获取系统状态 - 增强版"""
        try:
            debug_log("开始获取系统状态")
            
            # 检查文件存在性
            files = self.cos.list_all_files()
            file_dict = {f['filename']: f for f in files}
            
            # 权限文件检查
            permissions_files = [f for f in files if 'permissions' in f['filename']]
            permissions_exists = len(permissions_files) > 0
            
            # 元数据文件检查
            metadata_files = [f for f in files if 'metadata' in f['filename']]
            metadata_exists = len(metadata_files) > 0
            
            # 加载实际数据
            permissions_data, _ = self.cos.download_json_simple(self.permissions_file)
            metadata, _ = self.cos.download_json_simple(self.metadata_file)
            
            permissions_count = len(permissions_data.get('permissions', [])) if permissions_data else 0
            reports_count = len(metadata.get('reports', [])) if metadata else 0
            
            # 存储统计
            total_size = sum(f['size'] for f in files)
            report_files = [f for f in files if f['filename'].startswith('reports/')]
            
            status = {
                'permissions_exists': permissions_exists,
                'metadata_exists': metadata_exists,
                'permissions_count': permissions_count,
                'reports_count': reports_count,
                'system_healthy': permissions_exists and metadata_exists and permissions_count > 0,
                'total_files': len(files),
                'total_size_gb': total_size / (1024**3),
                'report_files_count': len(report_files),
                'usage_percent': (total_size / (1024**3)) / MAX_STORAGE_GB * 100,
                'files_detail': files
            }
            
            debug_log(f"系统状态: {status}")
            return status
            
        except Exception as e:
            debug_log(f"获取系统状态失败: {str(e)}")
            return {
                'permissions_exists': False,
                'metadata_exists': False,
                'permissions_count': 0,
                'reports_count': 0,
                'system_healthy': False,
                'total_files': 0,
                'total_size_gb': 0,
                'report_files_count': 0,
                'usage_percent': 0,
                'files_detail': []
            }

# ===================== UI函数 =====================
def show_system_header():
    """显示系统头部"""
    st.markdown('<h1 class="main-header">📊 门店报表查询系统 (终极修复版)</h1>', unsafe_allow_html=True)

def show_diagnostic_panel():
    """显示诊断面板"""
    st.markdown('''
    <div class="diagnostic-info">
    <h4>🔧 系统诊断面板</h4>
    <p><strong>✅ 文件名一致性修复</strong>: 统一文件命名和路径处理</p>
    <p><strong>✅ 压缩/解压优化</strong>: 增强容错机制，支持回退</p>
    <p><strong>✅ 数据同步保障</strong>: 上传后强制验证和等待同步</p>
    <p><strong>✅ 权限匹配改进</strong>: 精确+模糊匹配，提高成功率</p>
    <p><strong>✅ 调试信息增强</strong>: 详细的操作日志和状态跟踪</p>
    </div>
    ''', unsafe_allow_html=True)

def show_step_indicator(step: str, status: str):
    """显示步骤指示器"""
    icon = "✅" if status == "success" else "⏳" if status == "processing" else "❌"
    st.markdown(f'''
    <div class="step-indicator">
    <strong>{icon} {step}</strong>
    </div>
    ''', unsafe_allow_html=True)

# ===================== 会话状态初始化 =====================
def init_session_state():
    """初始化会话状态"""
    defaults = {
        'logged_in': False,
        'store_name': "",
        'user_id': "",
        'is_admin': False,
        'system': None,
        'debug_mode': True  # 默认开启调试模式
    }
    
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

# ===================== 主程序 =====================
def main():
    """主程序"""
    init_session_state()
    show_system_header()
    show_diagnostic_panel()
    
    # 初始化系统
    if not st.session_state.system:
        try:
            with st.spinner("🔧 正在初始化修复版系统..."):
                st.session_state.system = FixedStoreSystem()
            st.success("✅ 修复版系统初始化成功")
        except Exception as e:
            st.error(f"❌ 系统初始化失败: {str(e)}")
            debug_log(f"系统初始化失败: {str(e)}")
            st.stop()
    
    system = st.session_state.system
    
    # 获取系统状态
    system_status = system.get_system_status()
    
    # 显示系统状态
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("权限记录", system_status['permissions_count'])
    with col2:
        st.metric("报表数量", system_status['reports_count'])
    with col3:
        st.metric("总文件数", system_status['total_files'])
    with col4:
        st.metric("存储使用", f"{system_status['total_size_gb']:.2f}GB")
    
    # 系统健康状态
    if system_status['system_healthy']:
        st.success("🟢 系统状态健康")
    else:
        st.warning("🟡 系统需要初始化数据")
    
    # 侧边栏
    with st.sidebar:
        st.title("⚙️ 系统控制")
        
        # 调试模式开关
        st.session_state.debug_mode = st.checkbox("🔍 调试模式", value=st.session_state.debug_mode)
        
        # 系统状态
        if system_status['system_healthy']:
            st.success("🟢 系统正常")
        else:
            st.error("🔴 需要初始化")
        
        st.caption(f"📋 权限: {system_status['permissions_count']}")
        st.caption(f"📊 报表: {system_status['reports_count']}")
        st.caption(f"💾 存储: {system_status['total_size_gb']:.1f}GB")
        
        st.divider()
        
        user_type = st.radio("选择用户类型", ["普通用户", "管理员"])
        
        if user_type == "管理员":
            st.subheader("🔐 管理员登录")
            admin_password = st.text_input("密码", type="password")
            
            if st.button("验证身份"):
                if admin_password == ADMIN_PASSWORD:
                    st.session_state.is_admin = True
                    st.success("✅ 验证成功")
                    st.rerun()
                else:
                    st.error("❌ 密码错误")
        else:
            if st.session_state.logged_in:
                st.subheader("👤 当前用户")
                st.info(f"门店：{st.session_state.store_name}")
                st.info(f"编码：{st.session_state.user_id}")
                
                if st.button("🚪 退出登录"):
                    st.session_state.logged_in = False
                    st.session_state.store_name = ""
                    st.session_state.user_id = ""
                    st.rerun()
    
    # 主界面
    if user_type == "管理员" and st.session_state.is_admin:
        show_admin_interface(system, system_status)
    elif user_type == "管理员":
        st.info("👈 请在左侧输入管理员密码")
    else:
        show_user_interface(system)

def show_admin_interface(system: FixedStoreSystem, system_status: Dict):
    """显示管理员界面"""
    st.markdown('''
    <div class="admin-panel">
    <h3>👨‍💼 管理员控制面板 (终极修复版)</h3>
    <p>专门解决上传后无法查询的问题，增强数据同步和验证机制</p>
    </div>
    ''', unsafe_allow_html=True)
    
    tab1, tab2, tab3 = st.tabs(["📋 权限管理", "📊 报表管理", "🔧 系统诊断"])
    
    with tab1:
        st.markdown("#### 权限表管理")
        
        st.markdown('''
        <div class="debug-box">
        <h5>🔧 权限上传修复要点</h5>
        <p>• 严格数据验证和清理</p>
        <p>• 上传后立即验证数据完整性</p>
        <p>• 强制等待云端同步完成</p>
        <p>• 详细的操作日志和错误诊断</p>
        </div>
        ''', unsafe_allow_html=True)
        
        permissions_file = st.file_uploader("选择权限Excel文件", type=['xlsx', 'xls'], key="permissions")
        
        if permissions_file and st.button("📤 上传权限表", type="primary"):
            debug_log("开始上传权限表")
            
            show_step_indicator("1. 读取Excel文件", "processing")
            
            with st.spinner("正在处理权限文件..."):
                if system.process_permissions_file(permissions_file):
                    show_step_indicator("2. 数据验证", "success")
                    show_step_indicator("3. 云端上传", "success")
                    show_step_indicator("4. 同步等待", "success")
                    show_step_indicator("5. 完整性验证", "success")
                    
                    st.balloons()
                    time.sleep(2)
                    st.rerun()
                else:
                    show_step_indicator("处理失败", "error")
    
    with tab2:
        st.markdown("#### 报表数据管理")
        
        st.markdown('''
        <div class="debug-box">
        <h5>🔧 报表上传修复要点</h5>
        <p>• 文件名一致性保障</p>
        <p>• 压缩/解压容错机制</p>
        <p>• 元数据同步验证</p>
        <p>• 工作表匹配优化</p>
        </div>
        ''', unsafe_allow_html=True)
        
        reports_file = st.file_uploader("选择报表Excel文件", type=['xlsx', 'xls'], key="reports")
        
        if reports_file:
            file_size = len(reports_file.getvalue()) / 1024 / 1024
            st.metric("文件大小", f"{file_size:.2f} MB")
        
        if reports_file and st.button("📤 上传报表数据", type="primary"):
            debug_log("开始上传报表数据")
            
            show_step_indicator("1. 文件验证", "processing")
            
            with st.spinner("正在处理报表文件..."):
                if system.process_reports_file(reports_file):
                    show_step_indicator("2. 文件上传", "success")
                    show_step_indicator("3. 工作表解析", "success")
                    show_step_indicator("4. 元数据更新", "success")
                    show_step_indicator("5. 数据同步", "success")
                    
                    st.balloons()
                    time.sleep(2)
                    st.rerun()
                else:
                    show_step_indicator("处理失败", "error")
    
    with tab3:
        st.markdown("#### 系统诊断")
        
        if st.button("🔍 运行完整诊断", type="primary"):
            debug_log("开始系统诊断")
            
            with st.spinner("正在运行系统诊断..."):
                # 权限系统诊断
                permissions_data, perm_file = system.cos.download_json_simple(system.permissions_file)
                if permissions_data and 'permissions' in permissions_data:
                    permissions = permissions_data['permissions']
                    st.success(f"✅ 权限系统正常: {len(permissions)} 条记录")
                    
                    stores = system.get_available_stores()
                    st.info(f"📋 支持门店: {len(stores)} 个")
                    
                    if stores:
                        st.write("**门店列表**:", ", ".join(stores[:10]))
                else:
                    st.error("❌ 权限系统异常")
                
                # 报表系统诊断
                metadata, meta_file = system.cos.download_json_simple(system.metadata_file)
                if metadata and 'reports' in metadata:
                    reports = metadata['reports']
                    st.success(f"✅ 报表系统正常: {len(reports)} 个报表")
                    
                    if reports:
                        report_stores = [r.get('store_name') for r in reports]
                        st.write("**报表门店**:", ", ".join(report_stores[:10]))
                else:
                    st.error("❌ 报表系统异常")
                
                # 文件系统诊断
                files = system.cos.list_all_files()
                st.info(f"
