import streamlit as st
import pandas as pd
import io
import json
from datetime import datetime
import time
import logging
from typing import Optional, Dict, Any, List
import hashlib
import pickle
import traceback
from contextlib import contextmanager
from qcloud_cos import CosConfig, CosS3Client
from qcloud_cos.cos_exception import CosServiceError, CosClientError
import openpyxl

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
MAX_RETRIES = 3
RETRY_DELAY = 1
CACHE_DURATION = 300  # 缓存5分钟

# CSS样式
st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        padding: 1rem 0;
        margin-bottom: 2rem;
    }
    .store-info {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1.5rem;
        border-radius: 10px;
        margin: 1rem 0;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .admin-panel {
        background: linear-gradient(135deg, #ffeaa7 0%, #fab1a0 100%);
        padding: 1.5rem;
        border-radius: 10px;
        border: 2px solid #fdcb6e;
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
    }
    .receivable-negative {
        background: linear-gradient(135deg, #a8edea 0%, #d299c2 100%);
        color: #0c4128;
        padding: 2rem;
        border-radius: 15px;
        border: 3px solid #48cab2;
        margin: 1rem 0;
        text-align: center;
    }
    .status-success {
        background: #d4edda;
        color: #155724;
        padding: 0.75rem;
        border-radius: 5px;
        border: 1px solid #c3e6cb;
        margin: 0.5rem 0;
    }
    .status-error {
        background: #f8d7da;
        color: #721c24;
        padding: 0.75rem;
        border-radius: 5px;
        border: 1px solid #f5c6cb;
        margin: 0.5rem 0;
    }
    .status-warning {
        background: #fff3cd;
        color: #856404;
        padding: 0.75rem;
        border-radius: 5px;
        border: 1px solid #ffeaa7;
        margin: 0.5rem 0;
    }
    </style>
""", unsafe_allow_html=True)

class CosOperationError(Exception):
    """腾讯云COS操作异常"""
    pass

class DataProcessingError(Exception):
    """数据处理异常"""
    pass

@contextmanager
def error_handler(operation_name: str):
    """通用错误处理上下文管理器"""
    try:
        yield
    except Exception as e:
        logger.error(f"{operation_name} 失败: {str(e)}")
        logger.error(traceback.format_exc())
        st.error(f"❌ {operation_name} 失败: {str(e)}")
        raise

def retry_operation(func, *args, max_retries=MAX_RETRIES, delay=RETRY_DELAY, **kwargs):
    """重试操作装饰器"""
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if attempt == max_retries - 1:
                logger.error(f"操作失败，已重试 {max_retries} 次: {str(e)}")
                raise
            logger.warning(f"操作失败，第 {attempt + 1} 次重试: {str(e)}")
            time.sleep(delay * (attempt + 1))

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
        logger.info(f"缓存已设置: {key}")
    except Exception as e:
        logger.warning(f"设置缓存失败: {str(e)}")

def get_cache(key: str) -> Optional[Any]:
    """获取缓存"""
    try:
        cache_key = f"cache_{key}"
        if cache_key in st.session_state:
            cache_data = st.session_state[cache_key]
            if time.time() - cache_data['timestamp'] < cache_data['duration']:
                logger.info(f"缓存命中: {key}")
                return cache_data['data']
            else:
                del st.session_state[cache_key]
                logger.info(f"缓存过期: {key}")
    except Exception as e:
        logger.warning(f"获取缓存失败: {str(e)}")
    return None

def validate_cos_config(config: dict) -> bool:
    """验证腾讯云COS配置"""
    required_keys = ["region", "secret_id", "secret_key", "bucket_name", "permissions_file"]
    for key in required_keys:
        if key not in config or not config[key]:
            logger.error(f"COS配置缺少必要参数: {key}")
            return False
    return True

@st.cache_resource(show_spinner="连接腾讯云存储...")
def get_cos_client():
    """获取腾讯云COS客户端 - 使用缓存"""
    try:
        if "tencent_cloud" not in st.secrets:
            raise CosOperationError("未找到腾讯云配置，请检查 secrets.toml 文件")
        
        cos_config = st.secrets["tencent_cloud"]
        
        # 验证配置
        if not validate_cos_config(cos_config):
            raise CosOperationError("腾讯云配置不完整")
        
        config = CosConfig(
            Region=cos_config["region"],
            SecretId=cos_config["secret_id"],
            SecretKey=cos_config["secret_key"],
            Scheme='https'  # 使用HTTPS协议
        )
        
        client = CosS3Client(config)
        
        # 测试连接
        try:
            client.head_bucket(Bucket=cos_config["bucket_name"])
            logger.info("腾讯云COS客户端创建成功，连接测试通过")
        except CosServiceError as e:
            logger.error(f"COS连接测试失败: {e.get_error_code()} - {e.get_error_msg()}")
            raise CosOperationError(f"存储桶连接失败: {e.get_error_msg()}")
        
        return client, cos_config["bucket_name"], cos_config["permissions_file"]
    
    except Exception as e:
        logger.error(f"腾讯云COS客户端创建失败: {str(e)}")
        raise CosOperationError(f"连接失败: {str(e)}")

def safe_cos_operation(operation_func, *args, **kwargs):
    """安全的COS操作"""
    return retry_operation(operation_func, *args, **kwargs)

def safe_format_number(value, default_value=0):
    """安全的数字格式化函数"""
    try:
        if isinstance(value, str):
            # 尝试转换字符串为数字
            if value.isdigit():
                return int(value)
            else:
                return float(value)
        elif isinstance(value, (int, float)):
            return value
        else:
            return default_value
    except (ValueError, TypeError):
        return default_value

def create_excel_buffer(data, sheet_name="数据", file_type="general"):
    """统一的Excel文件创建函数"""
    try:
        if isinstance(data, pd.DataFrame):
            df = data
        elif isinstance(data, list) and len(data) > 0:
            # 如果是列表数据，转换为DataFrame
            if isinstance(data[0], list):
                # 第一行作为表头
                df = pd.DataFrame(data[1:], columns=data[0])
            else:
                df = pd.DataFrame(data)
        else:
            raise ValueError("数据格式不支持")
        
        # 创建Excel缓冲区
        excel_buffer = io.BytesIO()
        
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            # 工作表名限制31字符
            safe_sheet_name = sheet_name[:31] if len(sheet_name) <= 31 else sheet_name[:28] + "..."
            df.to_excel(writer, index=False, sheet_name=safe_sheet_name)
        
        excel_content = excel_buffer.getvalue()
        
        logger.info(f"Excel文件创建成功: {len(df)} 行 × {len(df.columns)} 列, 大小: {len(excel_content)} 字节")
        
        return excel_content, len(df), len(df.columns)
        
    except Exception as e:
        logger.error(f"Excel文件创建失败: {str(e)}")
        raise DataProcessingError(f"Excel文件创建失败: {str(e)}")

def _upload_cos_file_with_integrity_check(cos_client, bucket_name, file_key, excel_content, content_type, metadata):
    """
    辅助函数：上传文件到COS并验证完整性。
    如果验证失败，则抛出IOError。
    """
    # 上传文件
    cos_client.put_object(
        Bucket=bucket_name,
        Body=excel_content,
        Key=file_key,
        ContentType=content_type,
        Metadata=metadata
    )
    
    # 立即验证上传结果
    head_response = cos_client.head_object(Bucket=bucket_name, Key=file_key)
    uploaded_size = safe_format_number(head_response.get('Content-Length', 0), 0)
    
    if uploaded_size != len(excel_content):
        raise IOError(f"上传文件大小不匹配! 预期: {len(excel_content)}, 实际: {uploaded_size}. 请重试上传。")
    
    # 进一步验证：下载前几个字节检查文件头部
    verify_response = cos_client.get_object(Bucket=bucket_name, Key=file_key, Range='bytes=0-1023')
    verify_content = verify_response['Body'].read()
    
    if verify_content[:2] != b'PK':
        raise IOError("上传文件头部验证失败，文件可能损坏。")
    
    return True

def unified_upload_to_cos(cos_client, bucket_name: str, file_key: str, excel_content: bytes, 
                         metadata: Dict[str, str], file_type: str = "file") -> bool:
    """统一的COS上传函数 - 使用重试机制确保完整上传"""
    try:
        logger.info(f"开始上传 {file_type}: {file_key}, 大小: {len(excel_content)} 字节")
        
        # 显示上传信息
        st.info(f"📤 正在上传 {file_type}: {file_key}")
        st.write(f"- 文件大小: {len(excel_content):,} 字节")
        
        # 使用retry_operation确保完整上传
        with st.spinner("上传中..."):
            upload_success = retry_operation(
                _upload_cos_file_with_integrity_check,
                cos_client, bucket_name, file_key, excel_content, 
                'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                metadata,
                max_retries=MAX_RETRIES,
                delay=RETRY_DELAY
            )
        
        if upload_success:
            st.success(f"✅ 上传验证成功: {len(excel_content):,} 字节")
            logger.info(f"上传成功: {file_key}")
            return True
        else:
            st.error(f"❌ 文件 '{file_key}' 上传失败或不完整，经过 {MAX_RETRIES} 次重试仍无法成功。")
            logger.error(f"文件 '{file_key}' 经过 {MAX_RETRIES} 次重试仍无法上传完整")
            return False
        
    except Exception as e:
        logger.error(f"上传失败: {str(e)}")
        st.error(f"❌ 上传失败: {str(e)}")
        return False

def _fetch_cos_file_content_with_integrity_check(cos_client, bucket_name, key, expected_size):
    """
    辅助函数：从COS获取文件内容，并检查下载内容的完整性。
    如果大小不匹配，则抛出IOError。
    """
    file_response = cos_client.get_object(Bucket=bucket_name, Key=key)
    excel_content = file_response['Body'].read()
    
    actual_size = len(excel_content)
    if actual_size != expected_size:
        raise IOError(f"下载文件大小不匹配! 预期: {expected_size}, 实际: {actual_size}. 请重试下载。")
    
    return excel_content

def unified_download_from_cos(cos_client, bucket_name: str, file_key: str, file_type: str = "file") -> Optional[bytes]:
    """统一的COS下载函数 - 使用重试机制确保完整下载"""
    try:
        logger.info(f"开始下载 {file_type}: {file_key}")
        
        # 1. 获取文件元数据
        head_response = cos_client.head_object(Bucket=bucket_name, Key=file_key)
        expected_size = safe_format_number(head_response.get('Content-Length', 0), 0)
        content_type = head_response.get('Content-Type', '')
        last_modified = head_response.get('Last-Modified', '')
        
        st.info(f"📥 准备下载 {file_type}: {file_key}")
        st.write(f"- 预期大小: {expected_size:,} 字节")
        st.write(f"- Content-Type: {content_type}")
        st.write(f"- 最后修改: {last_modified}")
        
        # 2. 使用retry_operation确保完整下载
        with st.spinner("下载中..."):
            excel_content = retry_operation(
                _fetch_cos_file_content_with_integrity_check,
                cos_client, bucket_name, file_key, expected_size,
                max_retries=MAX_RETRIES,
                delay=RETRY_DELAY
            )
        
        if excel_content is None:
            st.error(f"❌ 文件 '{file_key}' 下载失败或不完整，请检查网络或COS文件。")
            logger.error(f"文件 '{file_key}' 经过 {MAX_RETRIES} 次重试仍无法获取完整内容。")
            return None
        
        # 3. 验证下载结果
        actual_size = len(excel_content)
        st.success(f"✅ 下载完成: {actual_size:,} 字节")
        
        # 验证文件头部
        if len(excel_content) >= 2 and excel_content[:2] == b'PK':
            st.success("✅ 文件格式验证通过")
            logger.info(f"下载成功: {file_key}, 大小: {actual_size} 字节")
            return excel_content
        else:
            st.error("❌ 文件格式验证失败")
            logger.warning(f"文件头部验证失败: {excel_content[:4].hex() if len(excel_content) >= 4 else 'N/A'}")
            return None
            
    except Exception as e:
        logger.error(f"下载失败: {str(e)}")
        st.error(f"❌ 下载失败: {str(e)}")
        return None

def unified_excel_parser(excel_content: bytes, file_type: str = "file") -> Optional[pd.DataFrame]:
    """统一的Excel解析函数"""
    try:
        st.info(f"🔍 解析Excel文件...")
        
        # 创建字节流
        excel_buffer = io.BytesIO(excel_content)
        excel_buffer.seek(0)
        
        # 解析Excel
        df = pd.read_excel(excel_buffer, engine='openpyxl')
        
        st.success(f"✅ Excel解析成功: {len(df)} 行 × {len(df.columns)} 列")
        
        # 显示列名
        st.write(f"**列名**: {df.columns.tolist()}")
        
        # 显示数据预览
        if len(df) > 0:
            with st.expander("📊 数据预览（前5行）", expanded=False):
                st.dataframe(df.head(), use_container_width=True)
        
        logger.info(f"Excel解析成功: {len(df)} 行 × {len(df.columns)} 列")
        return df
        
    except Exception as e:
        logger.error(f"Excel解析失败: {str(e)}")
        st.error(f"❌ Excel解析失败: {str(e)}")
        
        # 提供详细错误信息
        error_type = type(e).__name__
        st.write(f"**错误类型**: {error_type}")
        st.write(f"**错误详情**: {str(e)}")
        
        return None

def unified_file_processor(cos_client, bucket_name: str, file_key: str, file_type: str = "file") -> Optional[pd.DataFrame]:
    """统一的文件处理函数 - 下载和解析"""
    try:
        st.subheader(f"🔍 {file_type} 处理流程")
        
        # 1. 下载文件
        excel_content = unified_download_from_cos(cos_client, bucket_name, file_key, file_type)
        
        if excel_content is None:
            return None
        
        # 2. 解析Excel
        df = unified_excel_parser(excel_content, file_type)
        
        return df
        
    except Exception as e:
        logger.error(f"文件处理失败: {str(e)}")
        st.error(f"❌ {file_type} 处理失败: {str(e)}")
        return None

def unified_excel_reader(cos_client, bucket_name: str, file_key: str, file_type: str = "unknown") -> Optional[pd.DataFrame]:
    """统一的Excel文件读取器 - 用于权限表和报表文件"""
    try:
        logger.info(f"开始读取 {file_type} 文件: {file_key}")
        
        # 1. 获取文件元数据
        try:
            head_response = cos_client.head_object(Bucket=bucket_name, Key=file_key)
            file_size_raw = head_response.get('Content-Length', 0)
            content_type = head_response.get('Content-Type', '')
            last_modified = head_response.get('Last-Modified', '')
            
            # 安全格式化文件大小
            file_size = safe_format_number(file_size_raw, 0)
            
            logger.info(f"文件元数据 - 大小: {file_size} 字节, 类型: {content_type}, 修改时间: {last_modified}")
            
            # 在Streamlit中显示文件信息
            st.info(f"📁 {file_type} 文件信息: {file_key}")
            st.write(f"- 文件大小: {file_size:,} 字节")
            st.write(f"- Content-Type: {content_type}")
            st.write(f"- 最后修改: {last_modified}")
            
        except Exception as e:
            logger.error(f"获取文件元数据失败: {str(e)}")
            st.error(f"❌ 获取文件 {file_key} 的元数据失败: {str(e)}")
            return None
        
        # 2. 下载文件内容
        try:
            response = cos_client.get_object(Bucket=bucket_name, Key=file_key)
            raw_content = response['Body'].read()
            
            actual_size = len(raw_content)
            logger.info(f"下载完成 - 实际大小: {actual_size} 字节")
            
            # 验证下载大小
            if actual_size != file_size:
                logger.warning(f"文件大小不匹配! 预期: {file_size}, 实际: {actual_size}")
                st.warning(f"⚠️ 文件大小不匹配! 预期: {file_size:,}, 实际: {actual_size:,}")
            
            # 检查文件头部（Excel文件应该以PK开头）
            if len(raw_content) >= 2:
                file_header = raw_content[:2]
                hex_header = file_header.hex().upper()
                logger.info(f"文件头部: {hex_header}")
                
                # Excel文件应该以PK开头（zip格式）
                if file_header != b'PK':
                    logger.error(f"文件头部不是Excel格式! 头部: {hex_header}")
                    st.error(f"❌ 文件头部不是Excel格式! 头部: {hex_header}")
                    
                    # 显示文件前64个字节用于调试
                    preview_bytes = raw_content[:64]
                    st.code(f"文件前64字节: {preview_bytes.hex()}")
                    
                    # 尝试以文本形式显示
                    try:
                        preview_text = preview_bytes.decode('utf-8', errors='ignore')
                        st.code(f"文本预览: {preview_text}")
                    except:
                        pass
                    
                    return None
                else:
                    st.success(f"✅ 文件头部验证通过: {hex_header}")
            
        except Exception as e:
            logger.error(f"下载文件失败: {str(e)}")
            st.error(f"❌ 下载文件失败: {str(e)}")
            return None
        
        # 3. 读取Excel文件
        try:
            # 创建字节流并确保指针在开始位置
            excel_buffer = io.BytesIO(raw_content)
            excel_buffer.seek(0)
            
            logger.info("开始解析Excel文件...")
            
            # 使用pandas读取Excel
            df = pd.read_excel(excel_buffer, engine='openpyxl')
            
            logger.info(f"Excel解析成功: {len(df)} 行 × {len(df.columns)} 列")
            st.success(f"✅ Excel解析成功: {len(df)} 行 × {len(df.columns)} 列")
            
            # 显示列名
            st.write(f"**列名**: {df.columns.tolist()}")
            
            # 显示数据预览
            if len(df) > 0:
                with st.expander("📊 数据预览（前5行）", expanded=False):
                    st.dataframe(df.head(), use_container_width=True)
            
            return df
            
        except Exception as e:
            logger.error(f"Excel解析失败: {str(e)}")
            st.error(f"❌ Excel解析失败: {str(e)}")
            
            # 提供详细的错误信息
            error_type = type(e).__name__
            st.write(f"**错误类型**: {error_type}")
            st.write(f"**错误详情**: {str(e)}")
            
            # 如果是BadZipFile错误，提供更多调试信息
            if "zip" in str(e).lower():
                st.write("**可能原因**:")
                st.write("- 文件不是有效的Excel格式")
                st.write("- 文件在传输过程中损坏")
                st.write("- 文件实际上是其他格式但扩展名为.xlsx")
            
            return None
            
    except Exception as e:
        logger.error(f"统一Excel读取器出错: {str(e)}")
        st.error(f"❌ 读取 {file_type} 文件时出错: {str(e)}")
        return None

def load_permissions_from_cos_enhanced_v2(cos_client, bucket_name: str, permissions_file: str, force_reload: bool = False) -> Optional[pd.DataFrame]:
    """权限表读取器 - 使用统一的文件处理机制"""
    
    # 缓存处理
    if not force_reload:
        cache_key = get_cache_key("permissions", "load")
        cached_data = get_cache(cache_key)
        if cached_data is not None:
            logger.info("使用缓存数据")
            st.info("📦 使用缓存数据")
            return cached_data
    
    st.subheader("🔍 权限表读取诊断")
    
    with error_handler("加载权限数据"):
        def _load_operation():
            # 确定文件名 - 智能路径处理
            if permissions_file.endswith('.csv'):
                excel_permissions_file = permissions_file.replace('.csv', '.xlsx')
            elif permissions_file.endswith('.xlsx'):
                excel_permissions_file = permissions_file
            else:
                excel_permissions_file = permissions_file + '.xlsx'
            
            # 检查是否已经包含路径，如果没有则可能需要添加
            if '/' not in excel_permissions_file:
                # 尝试常见的路径前缀
                possible_paths = [
                    excel_permissions_file,  # 原始文件名
                    f"permissions/{excel_permissions_file}",  # permissions文件夹
                    f"user/{excel_permissions_file}",  # user文件夹
                    f"auth/{excel_permissions_file}"  # auth文件夹
                ]
            else:
                possible_paths = [excel_permissions_file]
            
            st.write(f"📁 配置文件名: `{permissions_file}`")
            st.write(f"📁 转换后文件名: `{excel_permissions_file}`")
            
            # 尝试查找文件
            df = None
            found_file = None
            
            for file_path in possible_paths:
                st.write(f"🔍 尝试查找: `{file_path}`")
                try:
                    # 先检查文件是否存在
                    cos_client.head_object(Bucket=bucket_name, Key=file_path)
                    found_file = file_path
                    st.success(f"✅ 找到文件: `{file_path}`")
                    break
                except Exception as e:
                    st.write(f"❌ 文件不存在: `{file_path}` - {str(e)}")
                    continue
            
            if found_file is None:
                st.error("❌ 未找到权限文件，请检查配置或上传文件")
                return None
            
            # 使用统一的文件处理函数
            df = unified_file_processor(cos_client, bucket_name, found_file, "权限表")
            
            if df is None:
                st.warning("⚠️ Excel格式读取失败，尝试CSV格式...")
                
                # 尝试CSV格式回退
                csv_file = permissions_file if permissions_file.endswith('.csv') else permissions_file.replace('.xlsx', '.csv')
                
                # 对CSV文件也尝试不同路径
                if '/' not in csv_file:
                    csv_paths = [csv_file, f"permissions/{csv_file}"]
                else:
                    csv_paths = [csv_file]
                
                for csv_path in csv_paths:
                    try:
                        st.write(f"🔍 尝试CSV格式: `{csv_path}`")
                        response = cos_client.get_object(Bucket=bucket_name, Key=csv_path)
                        csv_content = response['Body'].read().decode('utf-8-sig')
                        df = pd.read_csv(io.StringIO(csv_content))
                        st.success(f"✅ CSV格式读取成功: {len(df)} 行")
                        break
                    except Exception as e:
                        st.write(f"❌ CSV文件不存在: `{csv_path}` - {str(e)}")
                        continue
                
                if df is None:
                    st.error("❌ Excel和CSV格式都失败")
                    return None
            
            if df is None or len(df) == 0:
                st.warning("⚠️ 权限表为空或无效")
                return None
            
            # 权限表数据处理
            if len(df.columns) < 2:
                st.error("❌ 权限表格式错误：需要至少两列")
                return None
            
            # 标准化处理
            result_df = df.iloc[:, :2].copy()
            result_df.columns = ['门店名称', '人员编号']
            
            # 数据清理
            original_count = len(result_df)
            result_df['门店名称'] = result_df['门店名称'].astype(str).str.strip()
            result_df['人员编号'] = result_df['人员编号'].astype(str).str.strip()
            
            # 移除无效数据
            result_df = result_df[
                (result_df['门店名称'] != '') & 
                (result_df['人员编号'] != '') &
                (result_df['门店名称'] != 'nan') &
                (result_df['人员编号'] != 'nan')
            ]
            
            final_count = len(result_df)
            st.write(f"📊 数据清理: {original_count} → {final_count} 条记录")
            
            if final_count == 0:
                st.warning("⚠️ 清理后权限数据为空")
                return None
            
            # 显示处理后的数据预览
            with st.expander("📋 权限数据预览（前10行）", expanded=False):
                st.dataframe(result_df.head(10), use_container_width=True)
            
            logger.info(f"权限数据加载成功: {final_count} 条记录")
            
            # 设置缓存
            if not force_reload:
                cache_key = get_cache_key("permissions", "load")
                set_cache(cache_key, result_df)
            
            return result_df
        
        return safe_cos_operation(_load_operation)

def get_single_report_from_cos_v2(cos_client, bucket_name: str, store_name: str) -> Optional[pd.DataFrame]:
    """报表文件读取器 - 使用统一的Excel读取逻辑"""
    cache_key = get_cache_key("single_report", store_name)
    cached_data = get_cache(cache_key)
    if cached_data is not None:
        st.info("📦 使用缓存的报表数据")
        return cached_data
    
    st.subheader(f"🔍 报表文件读取诊断 - {store_name}")
    
    with error_handler(f"加载门店 {store_name} 的报表"):
        def _load_operation():
            # 查找文件
            safe_store_name = store_name.replace(' ', '_')
            
            try:
                response = cos_client.list_objects(
                    Bucket=bucket_name,
                    Prefix=f'reports/{safe_store_name}_',
                    MaxKeys=100
                )
                
                if 'Contents' not in response or len(response['Contents']) == 0:
                    st.warning(f"⚠️ 未找到门店 {store_name} 的报表文件")
                    return None
                
                # 查找最新文件
                latest_file = None
                latest_time = None
                
                st.write(f"📁 找到 {len(response['Contents'])} 个文件:")
                for obj in response['Contents']:
                    key = obj['Key']
                    file_time = obj['LastModified']
                    file_size_raw = obj['Size']
                    file_size = safe_format_number(file_size_raw, 0)
                    
                    st.write(f"- {key} ({file_size:,} 字节, {file_time})")
                    
                    if key.endswith('.xlsx'):
                        if latest_time is None or file_time > latest_time:
                            latest_time = file_time
                            latest_file = key
                
                if latest_file is None:
                    st.error(f"❌ 没有找到有效的Excel文件")
                    return None
                
                st.success(f"✅ 选择最新文件: {latest_file}")
                
                # 使用统一的Excel读取器
                df = unified_excel_reader(cos_client, bucket_name, latest_file, f"报表({store_name})")
                
                if df is not None:
                    # 报表数据处理
                    processed_df = process_report_dataframe(df)
                    
                    # 设置缓存
                    set_cache(cache_key, processed_df)
                    
                    return processed_df
                else:
                    return None
                
            except Exception as e:
                st.error(f"❌ 查找报表文件失败: {str(e)}")
                return None
        
        return safe_cos_operation(_load_operation)

def compare_file_properties(cos_client, bucket_name: str, permissions_file: str, store_name: str):
    """对比权限表和报表文件的属性"""
    st.subheader("🔍 文件属性对比")
    
    try:
        # 权限表文件 - 智能路径处理
        if permissions_file.endswith('.csv'):
            excel_permissions_file = permissions_file.replace('.csv', '.xlsx')
        elif permissions_file.endswith('.xlsx'):
            excel_permissions_file = permissions_file
        else:
            excel_permissions_file = permissions_file + '.xlsx'
        
        # 如果没有路径前缀，尝试添加
        if '/' not in excel_permissions_file:
            possible_paths = [excel_permissions_file, f"permissions/{excel_permissions_file}"]
        else:
            possible_paths = [excel_permissions_file]
        
        # 查找权限文件
        perm_info = None
        for file_path in possible_paths:
            try:
                perm_head = cos_client.head_object(Bucket=bucket_name, Key=file_path)
                perm_size_raw = perm_head.get('Content-Length', 0)
                perm_size = safe_format_number(perm_size_raw, 0)
                perm_info = {
                    'file': file_path,
                    'size': perm_size,
                    'type': perm_head.get('Content-Type', ''),
                    'modified': perm_head.get('Last-Modified', '')
                }
                break
            except Exception:
                continue
        
        if perm_info is None:
            perm_info = {'error': 'Permission file not found in any expected location'}
        
        # 报表文件
        safe_store_name = store_name.replace(' ', '_')
        try:
            list_response = cos_client.list_objects(
                Bucket=bucket_name,
                Prefix=f'reports/{safe_store_name}_',
                MaxKeys=1
            )
            
            if 'Contents' in list_response and len(list_response['Contents']) > 0:
                report_key = list_response['Contents'][0]['Key']
                report_head = cos_client.head_object(Bucket=bucket_name, Key=report_key)
                report_size_raw = report_head.get('Content-Length', 0)
                report_size = safe_format_number(report_size_raw, 0)
                report_info = {
                    'file': report_key,
                    'size': report_size,
                    'type': report_head.get('Content-Type', ''),
                    'modified': report_head.get('Last-Modified', '')
                }
            else:
                report_info = {'error': 'No report file found'}
        except Exception as e:
            report_info = {'error': str(e)}
        
        # 显示对比结果
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**权限表文件**")
            if 'error' in perm_info:
                st.error(f"❌ {perm_info['error']}")
            else:
                st.success(f"✅ {perm_info['file']}")
                st.write(f"- 大小: {perm_info['size']:,} 字节")
                st.write(f"- 类型: {perm_info['type']}")
                st.write(f"- 修改: {perm_info['modified']}")
        
        with col2:
            st.write("**报表文件**")
            if 'error' in report_info:
                st.error(f"❌ {report_info['error']}")
            else:
                st.success(f"✅ {report_info['file']}")
                st.write(f"- 大小: {report_info['size']:,} 字节")
                st.write(f"- 类型: {report_info['type']}")
                st.write(f"- 修改: {report_info['modified']}")
        
        # 对比分析
        if 'error' not in perm_info and 'error' not in report_info:
            st.write("**对比分析**:")
            if perm_info['type'] == report_info['type']:
                st.success("✅ Content-Type 一致")
            else:
                st.warning(f"⚠️ Content-Type 不一致: {perm_info['type']} vs {report_info['type']}")
            
            if perm_info['size'] > 0 and report_info['size'] > 0:
                st.success("✅ 文件大小都大于0")
            else:
                st.error("❌ 存在空文件")
    
    except Exception as e:
        st.error(f"❌ 文件属性对比失败: {str(e)}")

def save_permissions_to_cos(df: pd.DataFrame, cos_client, bucket_name: str, permissions_file: str) -> bool:
    """保存权限数据到COS - 使用统一的文件处理机制"""
    with error_handler("保存权限数据"):
        def _save_operation():
            # 数据验证
            if df is None or len(df) == 0:
                raise DataProcessingError("权限数据为空")
            
            if len(df.columns) < 2:
                raise DataProcessingError("权限数据格式错误：需要至少两列（门店名称、人员编号）")
            
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # 创建详细的处理报告
            processing_report = {
                'original_rows': len(df),
                'original_columns': len(df.columns),
                'processed_rows': 0,
                'skipped_rows': [],
                'error_rows': [],
                'duplicate_rows': [],
                'empty_rows': [],
                'step_by_step': []
            }
            
            # 步骤1：显示原始数据统计
            st.info(f"📊 步骤1: 接收到原始数据 {len(df)} 行 × {len(df.columns)} 列")
            
            # 显示原始数据预览
            with st.expander("🔍 步骤1: 原始数据预览（前10行）", expanded=True):
                st.dataframe(df.head(10), use_container_width=True)
                st.write(f"**列名**: {df.columns.tolist()}")
                
                # 显示数据类型
                st.write("**数据类型**:")
                for col in df.columns:
                    st.write(f"- {col}: {df[col].dtype}")
            
            # 步骤2：数据处理
            st.info(f"📊 步骤2: 开始逐行处理 {len(df)} 行数据")
            
            # 确保使用正确的列
            store_col = df.columns[0]
            user_col = df.columns[1]
            
            # 创建权限数据
            permissions_data = []
            permissions_data.append(['门店名称', '人员编号', '更新时间'])
            
            # 创建进度条
            progress_container = st.container()
            with progress_container:
                progress_bar = st.progress(0)
                status_text = st.empty()
            
            processed_count = 0
            seen_combinations = set()  # 用于检测重复数据
            
            # 逐行处理数据
            for idx, row in df.iterrows():
                try:
                    progress = (idx + 1) / len(df)
                    progress_bar.progress(progress)
                    status_text.text(f"处理第 {idx + 1}/{len(df)} 行...")
                    
                    # 获取原始值
                    raw_store = row[store_col] if pd.notna(row[store_col]) else ""
                    raw_user = row[user_col] if pd.notna(row[user_col]) else ""
                    
                    # 转换为字符串并清理
                    store_name = str(raw_store).strip()
                    user_id = str(raw_user).strip()
                    
                    # 数据验证和清理逻辑（保持原有逻辑）
                    if (not store_name or store_name in ['nan', 'None']) and \
                       (not user_id or user_id in ['nan', 'None']):
                        processing_report['empty_rows'].append({
                            'row': idx + 1,
                            'store': raw_store,
                            'user': raw_user,
                            'reason': '门店和编号都为空'
                        })
                        continue
                    
                    if not store_name or store_name in ['nan', 'None']:
                        processing_report['skipped_rows'].append({
                            'row': idx + 1,
                            'store': raw_store,
                            'user': raw_user,
                            'reason': '门店名称为空'
                        })
                        continue
                    
                    if not user_id or user_id in ['nan', 'None']:
                        processing_report['skipped_rows'].append({
                            'row': idx + 1,
                            'store': raw_store,
                            'user': raw_user,
                            'reason': '人员编号为空'
                        })
                        continue
                    
                    # 清理特殊字符
                    store_name = store_name.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
                    user_id = user_id.replace('\n', '').replace('\r', '').replace('\t', '')
                    
                    # 去除多余空格
                    store_name = ' '.join(store_name.split())
                    user_id = ' '.join(user_id.split())
                    
                    # 最终验证
                    if len(store_name) == 0 or len(user_id) == 0:
                        processing_report['skipped_rows'].append({
                            'row': idx + 1,
                            'store': raw_store,
                            'user': raw_user,
                            'reason': '清理后数据为空'
                        })
                        continue
                    
                    # 检查重复数据
                    combination = (store_name.lower(), user_id.lower())
                    if combination in seen_combinations:
                        processing_report['duplicate_rows'].append({
                            'row': idx + 1,
                            'store': store_name,
                            'user': user_id,
                            'reason': '重复的门店-编号组合'
                        })
                        continue
                    
                    seen_combinations.add(combination)
                    permissions_data.append([store_name, user_id, current_time])
                    processed_count += 1
                    
                except Exception as e:
                    processing_report['error_rows'].append({
                        'row': idx + 1,
                        'store': raw_store if 'raw_store' in locals() else 'N/A',
                        'user': raw_user if 'raw_user' in locals() else 'N/A',
                        'reason': f'处理错误: {str(e)}'
                    })
                    logger.warning(f"处理第{idx+1}行时出错: {str(e)}")
                    continue
            
            # 清除进度显示
            progress_container.empty()
            
            # 更新处理报告
            processing_report['processed_rows'] = processed_count
            
            # 显示处理结果
            st.success(f"✅ 步骤2: 数据处理完成！有效数据 {processed_count} 行")
            
            if processed_count == 0:
                raise DataProcessingError("没有有效的权限数据可以保存")
            
            # 步骤3：创建Excel文件 - 使用统一的Excel创建函数
            st.info(f"📊 步骤3: 创建Excel文件，共 {processed_count} 条权限记录")
            
            # 转换为DataFrame
            final_df = pd.DataFrame(permissions_data[1:], columns=permissions_data[0])
            
            # 使用统一的Excel创建函数
            excel_content, row_count, col_count = create_excel_buffer(final_df, "权限数据", "权限表")
            
            # 步骤4：上传到COS - 使用统一的上传函数
            st.info(f"📊 步骤4: 上传到腾讯云COS")
            
            # 确定最终文件路径
            if permissions_file.endswith('.csv'):
                excel_permissions_file = permissions_file.replace('.csv', '.xlsx')
            elif permissions_file.endswith('.xlsx'):
                excel_permissions_file = permissions_file
            else:
                excel_permissions_file = permissions_file + '.xlsx'
            
            # 如果没有路径前缀，添加permissions/
            if '/' not in excel_permissions_file:
                excel_permissions_file = f"permissions/{excel_permissions_file}"
            
            # 准备元数据
            metadata = {
                'upload-time': current_time,
                'record-count': str(processed_count),
                'original-count': str(processing_report['original_rows']),
                'file-format': 'excel',
                'file-type': 'permissions'
            }
            
            # 使用统一的上传函数
            upload_success = unified_upload_to_cos(
                cos_client, 
                bucket_name, 
                excel_permissions_file, 
                excel_content, 
                metadata, 
                "权限表"
            )
            
            if upload_success:
                st.success(f"✅ 步骤4: 权限表上传成功！")
                logger.info(f"权限数据保存成功: {processed_count} 条记录")
                
                # 清除相关缓存
                clear_permissions_cache()
                
                return True
            else:
                raise DataProcessingError("权限表上传失败")
        
        return safe_cos_operation(_save_operation)

def clear_permissions_cache():
    """清除权限相关缓存"""
    cache_keys_to_clear = [
        get_cache_key("permissions", "load"),
        get_cache_key("store_list", "load")
    ]
    
    for cache_key in cache_keys_to_clear:
        full_key = f"cache_{cache_key}"
        if full_key in st.session_state:
            del st.session_state[full_key]
            logger.info(f"已清除缓存: {cache_key}")

def load_permissions_from_cos(cos_client, bucket_name: str, permissions_file: str) -> Optional[pd.DataFrame]:
    """从COS加载权限数据 - 使用统一读取逻辑"""
    return load_permissions_from_cos_enhanced_v2(cos_client, bucket_name, permissions_file, force_reload=False)

def save_single_report_to_cos(df: pd.DataFrame, cos_client, bucket_name: str, store_name: str) -> bool:
    """保存单个门店报表到COS"""
    try:
        # 数据验证
        if df is None or len(df) == 0:
            logger.warning(f"门店 {store_name} 的数据为空，跳过保存")
            return False
        
        # 清理门店名称，用于文件名
        safe_store_name = "".join(c for c in store_name if c.isalnum() or c in (' ', '-', '_')).strip()
        safe_store_name = safe_store_name.replace(' ', '_')
        
        # 生成文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"reports/{safe_store_name}_{timestamp}.xlsx"
        
        # 创建Excel文件
        excel_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            # 工作表名限制31字符
            sheet_name = store_name[:31] if len(store_name) <= 31 else store_name[:28] + "..."
            df.to_excel(writer, index=False, sheet_name=sheet_name)
        
        excel_content = excel_buffer.getvalue()
        
        # 上传到COS
        cos_client.put_object(
            Bucket=bucket_name,
            Body=excel_content,
            Key=filename,
            ContentType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            Metadata={
                'store-name': store_name,
                'upload-time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'row-count': str(len(df)),
                'col-count': str(len(df.columns))
            }
        )
        
        logger.info(f"报表保存成功: {store_name} -> {filename}")
        return True
        
    except Exception as e:
        logger.error(f"保存 {store_name} 报表失败: {str(e)}")
        return False

def save_reports_to_cos(reports_dict: Dict[str, pd.DataFrame], cos_client, bucket_name: str) -> bool:
    """保存报表数据到COS"""
    with error_handler("保存报表数据"):
        def _save_operation():
            if not reports_dict:
                raise DataProcessingError("没有报表数据需要保存")
            
            success_count = 0
            total_count = len(reports_dict)
            
            # 显示进度
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            for idx, (store_name, df) in enumerate(reports_dict.items()):
                try:
                    status_text.text(f"正在保存 {store_name}... ({idx+1}/{total_count})")
                    
                    if save_single_report_to_cos(df, cos_client, bucket_name, store_name):
                        success_count += 1
                    
                    # 更新进度
                    progress = (idx + 1) / total_count
                    progress_bar.progress(progress)
                    
                    # API限制延迟
                    time.sleep(0.3)
                    
                except Exception as e:
                    logger.error(f"保存门店 {store_name} 时出错: {str(e)}")
                    continue
            
            progress_bar.empty()
            status_text.empty()
            
            # 清除相关缓存
            clear_reports_cache()
            
            logger.info(f"报表数据保存完成: {success_count}/{total_count}")
            return success_count > 0  # 只要有一个成功就算成功
        
        return safe_cos_operation(_save_operation)

def clear_reports_cache():
    """清除报表相关缓存"""
    cache_keys_to_clear = []
    
    # 清除所有以cache_开头的键
    for key in list(st.session_state.keys()):
        if key.startswith('cache_'):
            cache_keys_to_clear.append(key)
    
    for key in cache_keys_to_clear:
        del st.session_state[key]
        logger.info(f"已清除缓存: {key}")

def get_store_list_from_cos(cos_client, bucket_name: str) -> List[str]:
    """从COS获取门店列表 - 优化版"""
    cache_key = get_cache_key("store_list", "load")
    cached_data = get_cache(cache_key)
    if cached_data is not None:
        return cached_data
    
    with error_handler("加载门店列表"):
        def _load_operation():
            try:
                store_set = set()
                marker = ''
                
                # 分页获取所有文件
                while True:
                    list_params = {
                        'Bucket': bucket_name,
                        'Prefix': 'reports/',
                        'MaxKeys': 1000
                    }
                    
                    if marker:
                        list_params['Marker'] = marker
                    
                    response = cos_client.list_objects(**list_params)
                    
                    if 'Contents' not in response:
                        break
                    
                    # 从文件名提取门店名称
                    for obj in response['Contents']:
                        key = obj['Key']
                        if key.endswith('.xlsx') and '/' in key:
                            # 从文件名提取门店名称
                            filename = key.split('/')[-1]  # 获取文件名
                            if '_' in filename:
                                store_name = filename.split('_')[0]  # 提取门店名称
                                # 还原下划线为空格
                                store_name = store_name.replace('_', ' ')
                                if store_name:
                                    store_set.add(store_name)
                    
                    # 检查是否还有更多数据
                    if response.get('IsTruncated') == 'true':
                        marker = response.get('NextMarker', '')
                        if not marker and response['Contents']:
                            marker = response['Contents'][-1]['Key']
                    else:
                        break
                
                store_list = sorted(list(store_set))
                logger.info(f"门店列表加载成功: {len(store_list)} 个门店")
                
                # 设置缓存
                set_cache(cache_key, store_list)
                return store_list
                
            except CosServiceError as e:
                logger.error(f"COS操作失败: {e.get_error_code()} - {e.get_error_msg()}")
                return []
            except Exception as e:
                logger.error(f"获取门店列表失败: {str(e)}")
                return []
        
        return safe_cos_operation(_load_operation)

def get_single_report_from_cos(cos_client, bucket_name: str, store_name: str) -> Optional[pd.DataFrame]:
    """从COS获取单个门店的报表数据 - 使用统一读取逻辑"""
    return get_single_report_from_cos_v2(cos_client, bucket_name, store_name)

def process_report_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """处理报表DataFrame - 统一的数据清理逻辑"""
    if df is None or len(df) == 0:
        return pd.DataFrame()
    
    try:
        # 检查第一行是否是门店名称
        if len(df) > 0:
            first_row = df.iloc[0]
            non_empty_count = sum(1 for val in first_row if pd.notna(val) and str(val).strip() != '')
            
            # 如果第一行只有1-2个非空值，可能是门店名称行，跳过
            if non_empty_count <= 2 and len(df) > 1:
                df = df.iloc[1:].reset_index(drop=True)
        
        # 处理表头
        if len(df) > 1:
            header_row = df.iloc[0].fillna('').astype(str).tolist()
            data_rows = df.iloc[1:].copy()
            
            # 清理列名并处理重复
            cols = []
            for i, col in enumerate(header_row):
                col = str(col).strip()
                if col == '' or col == 'nan' or col == '0':
                    col = f'列{i+1}' if i > 0 else '项目名称'
                
                # 处理重复列名
                original_col = col
                counter = 1
                while col in cols:
                    col = f"{original_col}_{counter}"
                    counter += 1
                cols.append(col)
            
            # 确保列数匹配
            min_cols = min(len(data_rows.columns), len(cols))
            cols = cols[:min_cols]
            data_rows = data_rows.iloc[:, :min_cols]
            
            data_rows.columns = cols
            df = data_rows.reset_index(drop=True).fillna('')
        else:
            # 处理少于3行的数据
            df = df.fillna('')
            default_cols = []
            for i in range(len(df.columns)):
                col_name = f'列{i+1}' if i > 0 else '项目名称'
                default_cols.append(col_name)
            df.columns = default_cols
        
        return df
        
    except Exception as e:
        logger.error(f"处理DataFrame时出错: {str(e)}")
        return df.fillna('')

def analyze_receivable_data(df: pd.DataFrame) -> Dict[str, Any]:
    """分析应收未收额数据 - 专门查找第69行"""
    result = {}
    
    if len(df.columns) == 0 or len(df) == 0:
        return result
    
    # 使用处理后的数据
    processed_df = df.copy()
    
    # 查找第69行
    target_row_index = 68  # 第69行（从0开始索引）
    
    if len(processed_df) > target_row_index:
        row = processed_df.iloc[target_row_index]
        first_col_value = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
        
        # 检查关键词
        keywords = ['应收-未收额', '应收未收额', '应收-未收', '应收未收']
        
        for keyword in keywords:
            if keyword in first_col_value:
                # 查找数值（从右往左）
                for col_idx in range(len(row)-1, 0, -1):
                    val = row.iloc[col_idx]
                    if pd.notna(val) and str(val).strip() not in ['', 'None', 'nan']:
                        cleaned = str(val).replace(',', '').replace('¥', '').replace('￥', '').strip()
                        
                        # 处理括号表示负数
                        if cleaned.startswith('(') and cleaned.endswith(')'):
                            cleaned = '-' + cleaned[1:-1]
                        
                        try:
                            amount = float(cleaned)
                            if amount != 0:
                                result['应收-未收额'] = {
                                    'amount': amount,
                                    'column_name': str(processed_df.columns[col_idx]),
                                    'row_name': first_col_value,
                                    'row_index': target_row_index,
                                    'actual_row_number': target_row_index + 1
                                }
                                return result
                        except ValueError:
                            continue
                break
    
    # 备用查找策略
    if '应收-未收额' not in result:
        keywords = ['应收-未收额', '应收未收额', '应收-未收', '应收未收']
        
        for idx, row in processed_df.iterrows():
            try:
                row_name = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
                
                if not row_name.strip():
                    continue
                
                for keyword in keywords:
                    if keyword in row_name:
                        # 查找数值（从右往左）
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
                                            'column_name': str(processed_df.columns[col_idx]),
                                            'row_name': row_name,
                                            'row_index': idx,
                                            'actual_row_number': idx + 1,
                                            'note': f'在第{idx+1}行找到（非第69行）'
                                        }
                                        return result
                                except ValueError:
                                    continue
                        break
            except Exception as e:
                logger.warning(f"分析第{idx+1}行时出错: {str(e)}")
                continue
    
    # 调试信息
    result['debug_info'] = {
        'total_rows': len(processed_df),
        'checked_row_69': len(processed_df) > target_row_index,
        'row_69_content': str(processed_df.iloc[target_row_index].iloc[0]) if len(processed_df) > target_row_index else 'N/A'
    }
    
    return result

def verify_user_permission(store_name: str, user_id: str, permissions_data: Optional[pd.DataFrame]) -> bool:
    """验证用户权限"""
    if permissions_data is None or len(permissions_data.columns) < 2:
        logger.warning("权限数据无效")
        return False
    
    try:
        store_col = permissions_data.columns[0]
        id_col = permissions_data.columns[1]
        
        # 清理输入数据
        store_name = str(store_name).strip()
        user_id = str(user_id).strip()
        
        for _, row in permissions_data.iterrows():
            try:
                stored_store = str(row[store_col]).strip()
                stored_id = str(row[id_col]).strip()
                
                # 门店名称模糊匹配 + 用户ID精确匹配
                store_match = (store_name in stored_store or stored_store in store_name)
                id_match = (stored_id == user_id)
                
                if store_match and id_match:
                    logger.info(f"权限验证通过: {store_name} - {user_id}")
                    return True
                    
            except Exception as e:
                logger.warning(f"检查权限行时出错: {str(e)}")
                continue
        
        logger.warning(f"权限验证失败: {store_name} - {user_id}")
        return False
        
    except Exception as e:
        logger.error(f"权限验证出错: {str(e)}")
        return False

def find_matching_stores(store_name: str, store_list: List[str]) -> List[str]:
    """查找匹配的门店"""
    if not store_name or not store_list:
        return []
    
    matching = []
    store_name = str(store_name).strip()
    
    for store in store_list:
        store = str(store).strip()
        if store_name in store or store in store_name:
            matching.append(store)
    
    return sorted(matching)

def show_status_message(message: str, status_type: str = "info"):
    """显示状态消息"""
    css_class = f"status-{status_type}"
    st.markdown(f'<div class="{css_class}">{message}</div>', unsafe_allow_html=True)

def init_session_state():
    """初始化会话状态"""
    defaults = {
        'logged_in': False,
        'store_name': "",
        'user_id': "",
        'is_admin': False,
        'cos_client': None,
        'operation_status': [],
        'reports_uploader_key': 'initial_reports_uploader_key',
        'permissions_uploader_key': 'initial_permissions_uploader_key',
        'show_diagnosis': False,
        'debug_mode': False
    }
    
    for key, default_value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default_value

# 主程序开始
def main():
    try:
        # 初始化会话状态
        init_session_state()
        
        # 主标题
        st.markdown('<h1 class="main-header">📊 门店报表查询系统</h1>', unsafe_allow_html=True)
        
        # 初始化腾讯云COS客户端
        if not st.session_state.cos_client:
            try:
                with st.spinner("正在连接腾讯云存储..."):
                    cos_client, bucket_name, permissions_file = get_cos_client()
                    st.session_state.cos_client = (cos_client, bucket_name, permissions_file)
                    show_status_message("✅ 腾讯云存储连接成功！", "success")
            except Exception as e:
                show_status_message(f"❌ 连接失败: {str(e)}", "error")
                st.error("请检查 secrets.toml 中的腾讯云配置是否正确")
                st.stop()
        
        cos_client, bucket_name, permissions_file = st.session_state.cos_client
        
        # 显示操作状态
        for status in st.session_state.operation_status:
            show_status_message(status['message'], status['type'])
        
        # 侧边栏
        with st.sidebar:
            st.title("⚙️ 系统功能")
            
            # 系统状态
            st.subheader("📡 系统状态")
            if cos_client:
                st.success("🟢 腾讯云存储已连接")
            else:
                st.error("🔴 腾讯云存储断开")
            
            # 调试选项
            st.subheader("🔧 调试选项")
            debug_mode = st.checkbox("启用详细调试", value=st.session_state.debug_mode)
            st.session_state.debug_mode = debug_mode
            
            if debug_mode:
                if st.button("🔍 对比文件属性"):
                    if st.session_state.logged_in:
                        compare_file_properties(cos_client, bucket_name, permissions_file, st.session_state.store_name)
                
                if st.button("🔄 强制重新加载权限表"):
                    clear_permissions_cache()
                    with st.spinner("重新加载权限表..."):
                        new_data = load_permissions_from_cos_enhanced_v2(cos_client, bucket_name, permissions_file, force_reload=True)
                        if new_data is not None:
                            st.success(f"✅ 权限表重新加载成功: {len(new_data)} 条")
                        else:
                            st.error("❌ 权限表重新加载失败")
                
                if st.button("🔄 重新上传权限表"):
                    st.warning("⚠️ 此操作将使用统一的重试机制重新上传权限表，可能会解决文件损坏问题")
                    
                    # 创建确认按钮
                    confirm_key = f"confirm_reupload_{datetime.now().timestamp()}"
                    if st.button("✅ 确认重新上传", type="primary", key=confirm_key):
                        # 首先尝试读取现有数据
                        try:
                            st.info("📖 正在读取现有权限数据...")
                            
                            # 尝试直接从CSV格式读取（如果存在）
                            csv_file = permissions_file.replace('.xlsx', '.csv')
                            if '/' not in csv_file:
                                csv_paths = [csv_file, f"permissions/{csv_file}"]
                            else:
                                csv_paths = [csv_file]
                            
                            existing_data = None
                            for csv_path in csv_paths:
                                try:
                                    response = cos_client.get_object(Bucket=bucket_name, Key=csv_path)
                                    csv_content = response['Body'].read().decode('utf-8-sig')
                                    existing_data = pd.read_csv(io.StringIO(csv_content))
                                    st.success(f"✅ 从CSV格式读取成功: {csv_path}")
                                    break
                                except Exception:
                                    continue
                            
                            if existing_data is None:
                                # 如果CSV不存在，尝试强制读取Excel（可能会失败）
                                st.info("🔄 CSV不存在，尝试强制读取Excel...")
                                existing_data = load_permissions_from_cos_enhanced_v2(cos_client, bucket_name, permissions_file, force_reload=True)
                            
                            if existing_data is not None and len(existing_data) > 0:
                                # 使用现有数据重新上传
                                st.info("📤 使用现有数据重新上传...")
                                
                                # 确保数据格式正确
                                if len(existing_data.columns) >= 2:
                                    # 只取前两列
                                    upload_data = existing_data.iloc[:, :2].copy()
                                    upload_data.columns = ['门店名称', '人员编号']
                                    
                                    # 清理数据
                                    upload_data = upload_data.dropna().astype(str)
                                    upload_data = upload_data[
                                        (upload_data['门店名称'] != '') & 
                                        (upload_data['人员编号'] != '') &
                                        (upload_data['门店名称'] != 'nan') &
                                        (upload_data['人员编号'] != 'nan')
                                    ]
                                    
                                    if len(upload_data) > 0:
                                        st.info(f"🔄 准备重新上传 {len(upload_data)} 条权限记录...")
                                        
                                        if save_permissions_to_cos(upload_data, cos_client, bucket_name, permissions_file):
                                            st.success("✅ 权限表重新上传成功！文件损坏问题已解决。")
                                            # 清除缓存
                                            clear_permissions_cache()
                                            st.balloons()
                                        else:
                                            st.error("❌ 权限表重新上传失败")
                                    else:
                                        st.error("❌ 数据清理后为空，无法重新上传")
                                else:
                                    st.error("❌ 数据格式不正确，无法重新上传")
                            else:
                                st.error("❌ 无法读取现有权限数据，请手动上传新的权限表")
                                st.info("💡 建议：请在管理员面板中重新上传权限表文件")
                                
                        except Exception as e:
                            st.error(f"❌ 重新上传失败: {str(e)}")
                            st.info("💡 建议：请在管理员面板中重新上传权限表文件")
                
                if st.button("🗑️ 清除所有缓存"):
                    cache_keys = [key for key in st.session_state.keys() if key.startswith('cache_')]
                    for key in cache_keys:
                        del st.session_state[key]
                    st.success("✅ 所有缓存已清除")
                    st.rerun()
            
            # 用户类型选择
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
                    permissions_file_upload = st.file_uploader(
                        "上传门店权限表", 
                        type=['xlsx', 'xls'],
                        key=st.session_state.permissions_uploader_key,
                        help="Excel文件，需包含门店名称和人员编号两列"
                    )
                    
                    if permissions_file_upload:
                        try:
                            # 计算文件哈希
                            file_hash = hashlib.md5(permissions_file_upload.getvalue()).hexdigest()
                            
                            # 检查是否已处理过
                            if ("last_permissions_hash" in st.session_state and 
                                st.session_state.last_permissions_hash == file_hash and 
                                st.session_state.get("permissions_upload_successful", False)):
                                st.info("ℹ️ 该权限表已成功处理，无需重复操作。")
                            else:
                                st.session_state.last_permissions_hash = file_hash
                                st.session_state.permissions_upload_successful = False
                                
                                with st.spinner("分析Excel文件结构..."):
                                    # 尝试读取Excel文件的所有工作表
                                    excel_file = pd.ExcelFile(permissions_file_upload)
                                    st.info(f"📄 发现 {len(excel_file.sheet_names)} 个工作表：{excel_file.sheet_names}")
                                    
                                    # 让用户选择工作表（如果有多个）
                                    if len(excel_file.sheet_names) > 1:
                                        selected_sheet = st.selectbox(
                                            "选择包含权限数据的工作表：", 
                                            excel_file.sheet_names,
                                            key="permission_sheet_selector"
                                        )
                                    else:
                                        selected_sheet = excel_file.sheet_names[0]
                                    
                                    # 读取选定的工作表
                                    df_raw = pd.read_excel(permissions_file_upload, sheet_name=selected_sheet)
                                    st.info(f"📊 原始数据：{len(df_raw)} 行 × {len(df_raw.columns)} 列")
                                    
                                    # 显示原始数据的前几行，帮助用户确认数据结构
                                    st.subheader("🔍 原始数据预览")
                                    st.dataframe(df_raw.head(10), use_container_width=True)
                                    
                                    # 让用户选择包含权限数据的列
                                    if len(df_raw.columns) >= 2:
                                        col1, col2 = st.columns(2)
                                        with col1:
                                            store_column = st.selectbox(
                                                "选择门店名称列：",
                                                df_raw.columns.tolist(),
                                                index=0,
                                                key="store_column_selector"
                                            )
                                        with col2:
                                            user_column = st.selectbox(
                                                "选择人员编号列：",
                                                df_raw.columns.tolist(),
                                                index=1 if len(df_raw.columns) > 1 else 0,
                                                key="user_column_selector"
                                            )
                                        
                                        # 检查是否有表头行需要跳过
                                        header_row = st.number_input(
                                            "数据开始行（0表示第一行）：",
                                            min_value=0,
                                            max_value=len(df_raw)-1,
                                            value=0,
                                            key="header_row_selector"
                                        )
                                        
                                        if st.button("🚀 开始处理权限数据", key="process_permissions"):
                                            try:
                                                # 重新读取Excel，跳过指定的表头行
                                                if header_row > 0:
                                                    df_processed = pd.read_excel(
                                                        permissions_file_upload, 
                                                        sheet_name=selected_sheet,
                                                        skiprows=header_row
                                                    )
                                                else:
                                                    df_processed = df_raw.copy()
                                                
                                                # 重新排列列顺序，确保门店名称和人员编号在前两列
                                                df = pd.DataFrame({
                                                    '门店名称': df_processed[store_column],
                                                    '人员编号': df_processed[user_column]
                                                })
                                                
                                                st.info(f"🔄 处理后数据：{len(df)} 行 × {len(df.columns)} 列")
                                                
                                                with st.spinner("保存到腾讯云..."):
                                                    if save_permissions_to_cos(df, cos_client, bucket_name, permissions_file):
                                                        st.session_state.permissions_upload_successful = True
                                                        st.balloons()
                                                        
                                                        # 重置上传器
                                                        st.session_state.permissions_uploader_key = f"{datetime.now().timestamp()}_permissions"
                                                        st.rerun()
                                                    else:
                                                        show_status_message("❌ 保存失败", "error")
                                                        st.session_state.permissions_upload_successful = False
                                            
                                            except Exception as e:
                                                show_status_message(f"❌ 数据处理失败：{str(e)}", "error")
                                                st.session_state.permissions_upload_successful = False
                                    
                                    else:
                                        show_status_message("❌ 数据格式错误：需要至少两列数据", "error")
                                        st.session_state.permissions_upload_successful = False
                        
                        except Exception as e:
                            show_status_message(f"❌ 文件读取失败：{str(e)}", "error")
                            st.session_state.permissions_upload_successful = False
                    
                    # 上传财务报表
                    reports_file_upload = st.file_uploader(
                        "上传财务报表", 
                        type=['xlsx', 'xls'],
                        key=st.session_state.reports_uploader_key,
                        help="Excel文件，每个工作表代表一个门店的报表"
                    )
                    
                    if reports_file_upload:
                        try:
                            # 计算文件哈希
                            file_hash = hashlib.md5(reports_file_upload.getvalue()).hexdigest()
                            
                            # 检查是否已处理过
                            if ("last_reports_hash" in st.session_state and 
                                st.session_state.last_reports_hash == file_hash and 
                                st.session_state.get("reports_upload_successful", False)):
                                st.info("ℹ️ 该报表文件已成功处理，无需重复操作。")
                            else:
                                st.session_state.last_reports_hash = file_hash
                                st.session_state.reports_upload_successful = False
                                
                                with st.spinner("处理报表文件..."):
                                    excel_file = pd.ExcelFile(reports_file_upload)
                                    reports_dict = {}
                                    
                                    for sheet in excel_file.sheet_names:
                                        try:
                                            df = pd.read_excel(reports_file_upload, sheet_name=sheet)
                                            if not df.empty:
                                                reports_dict[sheet] = df
                                                logger.info(f"读取工作表 '{sheet}': {len(df)} 行")
                                        except Exception as e:
                                            logger.warning(f"跳过工作表 '{sheet}': {str(e)}")
                                            continue
                                    
                                    if reports_dict:
                                        with st.spinner("保存到腾讯云..."):
                                            if save_reports_to_cos(reports_dict, cos_client, bucket_name):
                                                show_status_message(f"✅ 报表已上传：{len(reports_dict)} 个门店", "success")
                                                st.session_state.reports_upload_successful = True
                                                st.balloons()
                                                
                                                # 重置上传器
                                                st.session_state.reports_uploader_key = f"{datetime.now().timestamp()}_reports"
                                                st.rerun()
                                            else:
                                                show_status_message("❌ 保存失败", "error")
                                                st.session_state.reports_upload_successful = False
                                    else:
                                        show_status_message("❌ 文件中没有有效的工作表", "error")
                                        st.session_state.reports_upload_successful = False
                                        
                        except Exception as e:
                            show_status_message(f"❌ 处理失败：{str(e)}", "error")
                            st.session_state.reports_upload_successful = False
                    
                    # 缓存管理
                    st.subheader("🗂️ 缓存管理")
                    if st.button("清除所有缓存"):
                        cache_keys = [key for key in st.session_state.keys() if key.startswith('cache_')]
                        for key in cache_keys:
                            del st.session_state[key]
                        show_status_message("✅ 所有缓存已清除", "success")
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
            st.markdown('<div class="admin-panel"><h3>👨‍💼 管理员控制面板</h3><p>数据永久保存在腾讯云，支持高效存储和缓存机制</p><p>🔄 <strong>新特性</strong>: 集成了重试机制，确保文件上传下载的完整性和可靠性</p></div>', unsafe_allow_html=True)
            
            try:
                with st.spinner("加载数据统计..."):
                    # 使用新的读取逻辑
                    if debug_mode:
                        st.subheader("🔍 权限表加载诊断")
                        permissions_data = load_permissions_from_cos_enhanced_v2(cos_client, bucket_name, permissions_file, force_reload=False)
                    else:
                        permissions_data = load_permissions_from_cos(cos_client, bucket_name, permissions_file)
                    
                    store_list = get_store_list_from_cos(cos_client, bucket_name)
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    perms_count = len(permissions_data) if permissions_data is not None else 0
                    st.metric("权限表用户数", perms_count)
                with col2:
                    reports_count = len(store_list)
                    st.metric("报表门店数", reports_count)
                with col3:
                    cache_count = len([key for key in st.session_state.keys() if key.startswith('cache_')])
                    st.metric("缓存项目数", cache_count)
                    
                # 数据预览
                if permissions_data is not None and len(permissions_data) > 0:
                    st.subheader("👥 权限数据预览")
                    st.dataframe(permissions_data.head(10), use_container_width=True)
                    
                    if len(permissions_data) > 10:
                        st.caption(f"显示前10条记录，共{len(permissions_data)}条")
                
                if store_list:
                    st.subheader("📊 门店列表预览")
                    st.write(f"共有 {len(store_list)} 个门店")
                    
                    # 显示前10个门店
                    display_stores = store_list[:10]
                    for i in range(0, len(display_stores), 5):
                        cols = st.columns(5)
                        for j, store in enumerate(display_stores[i:i+5]):
                            with cols[j]:
                                st.info(f"🏪 {store}")
                    
                    if len(store_list) > 10:
                        st.caption(f"...以及其他 {len(store_list) - 10} 个门店")
                        
            except Exception as e:
                show_status_message(f"❌ 数据加载失败：{str(e)}", "error")

        elif user_type == "管理员" and not st.session_state.is_admin:
            st.info("👈 请在左侧边栏输入管理员密码")

        else:
            if not st.session_state.logged_in:
                st.subheader("🔐 用户登录")
                
                try:
                    with st.spinner("加载权限数据..."):
                        # 使用新的读取逻辑
                        if debug_mode:
                            st.subheader("🔍 权限表加载诊断")
                            permissions_data = load_permissions_from_cos_enhanced_v2(cos_client, bucket_name, permissions_file, force_reload=False)
                        else:
                            permissions_data = load_permissions_from_cos(cos_client, bucket_name, permissions_file)
                    
                    if permissions_data is None:
                        st.warning("⚠️ 系统维护中，请联系管理员")
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
                    with st.spinner("加载门店列表..."):
                        store_list = get_store_list_from_cos(cos_client, bucket_name)
                        matching_stores = find_matching_stores(st.session_state.store_name, store_list)
                    
                    if matching_stores:
                        if len(matching_stores) > 1:
                            selected_store = st.selectbox("选择报表", matching_stores)
                        else:
                            selected_store = matching_stores[0]
                        
                        # 使用新的报表读取逻辑
                        if debug_mode:
                            st.subheader("🔍 报表加载诊断")
                            df = get_single_report_from_cos_v2(cos_client, bucket_name, selected_store)
                        else:
                            with st.spinner(f"加载 {selected_store} 的报表数据..."):
                                df = get_single_report_from_cos(cos_client, bucket_name, selected_store)
                        
                        if df is not None:
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
                                    
                                    if debug_mode:
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
                            st.error(f"❌ 无法加载门店 '{selected_store}' 的报表数据")
                        
                    else:
                        st.error(f"❌ 未找到门店 '{st.session_state.store_name}' 的报表")
                        
                except Exception as e:
                    show_status_message(f"❌ 报表加载失败：{str(e)}", "error")
        
        # 页面底部状态信息
        st.divider()
        col1, col2, col3 = st.columns(3)
        with col1:
            st.caption(f"🕒 当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        with col2:
            cache_count = len([key for key in st.session_state.keys() if key.startswith('cache_')])
            st.caption(f"💾 缓存项目: {cache_count}")
        with col3:
            st.caption("🔧 版本: v4.1 (重试机制增强版)")

    except Exception as e:
        st.error(f"系统运行时出错: {str(e)}")
        logger.error(f"Main function error: {str(e)}")
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    main()
