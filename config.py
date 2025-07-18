# Streamlit Secrets 专用配置文件
import streamlit as st
from typing import Dict, Any, Optional

def get_secret(section: str, key: str, default: Any = None) -> Any:
    """从 Streamlit Secrets 安全获取配置值"""
    try:
        if hasattr(st, 'secrets') and section in st.secrets:
            return st.secrets[section].get(key, default)
        else:
            return default
    except Exception:
        return default

def check_secrets_available() -> bool:
    """检查 Streamlit Secrets 是否可用"""
    try:
        return hasattr(st, 'secrets') and len(st.secrets) > 0
    except Exception:
        return False

# 应用配置 - 从 Streamlit Secrets 读取
APP_CONFIG = {
    'max_file_size': get_secret('APP', 'max_file_size', 10 * 1024 * 1024),
    'upload_folder': get_secret('APP', 'upload_folder', 'uploads'),
    'temp_folder': get_secret('APP', 'temp_folder', 'temp'),
    'backup_folder': get_secret('APP', 'backup_folder', 'backups'),
    'allowed_file_types': ['xlsx', 'xls'],
    'max_concurrent_uploads': get_secret('APP', 'max_concurrent_uploads', 3),
    'upload_timeout': get_secret('APP', 'upload_timeout', 300),
    'cache_ttl': get_secret('APP', 'cache_ttl', 3600),
}

# Streamlit 界面配置
STREAMLIT_CONFIG = {
    'page_title': get_secret('UI', 'page_title', '门店报表查询系统'),
    'page_icon': get_secret('UI', 'page_icon', '📊'),
    'layout': get_secret('UI', 'layout', 'wide'),
    'initial_sidebar_state': get_secret('UI', 'initial_sidebar_state', 'expanded'),
    'menu_items': {
        'Get Help': None,
        'Report a bug': None,
        'About': "门店报表查询系统 v2.0 - Streamlit Secrets版"
    }
}

# 管理员密码 - 从 Streamlit Secrets 读取
ADMIN_PASSWORD = get_secret('AUTH', 'admin_password', 'admin123')

# 腾讯云 COS 配置 - 完全基于 Streamlit Secrets
COS_CONFIG = {
    'secret_id': get_secret('COS', 'secret_id', ''),
    'secret_key': get_secret('COS', 'secret_key', ''),
    'region': get_secret('COS', 'region', 'ap-guangzhou'),
    'bucket': get_secret('COS', 'bucket', ''),
    'domain': get_secret('COS', 'domain', ''),
    
    # 高级配置
    'timeout': int(get_secret('COS', 'timeout', 60)),
    'max_retries': int(get_secret('COS', 'max_retries', 3)),
    'chunk_size': int(get_secret('COS', 'chunk_size', 1024 * 1024)),
    'enable_multipart': get_secret('COS', 'enable_multipart', True),
    'multipart_threshold': int(get_secret('COS', 'multipart_threshold', 5 * 1024 * 1024)),
    
    # CDN和加速配置
    'cdn_domain': get_secret('COS', 'cdn_domain', ''),
    'enable_cdn': get_secret('COS', 'enable_cdn', False),
    'signed_url_expires': int(get_secret('COS', 'signed_url_expires', 3600)),
    
    # 安全配置
    'use_https': get_secret('COS', 'use_https', True),
    'verify_ssl': get_secret('COS', 'verify_ssl', True),
}

# 地域配置优化
COS_REGIONS = {
    'ap-guangzhou': {'name': '广州', 'endpoint': 'cos.ap-guangzhou.myqcloud.com'},
    'ap-shanghai': {'name': '上海', 'endpoint': 'cos.ap-shanghai.myqcloud.com'},
    'ap-beijing': {'name': '北京', 'endpoint': 'cos.ap-beijing.myqcloud.com'},
    'ap-chengdu': {'name': '成都', 'endpoint': 'cos.ap-chengdu.myqcloud.com'},
    'ap-chongqing': {'name': '重庆', 'endpoint': 'cos.ap-chongqing.myqcloud.com'},
    'ap-nanjing': {'name': '南京', 'endpoint': 'cos.ap-nanjing.myqcloud.com'},
    'ap-hongkong': {'name': '香港', 'endpoint': 'cos.ap-hongkong.myqcloud.com'},
    'ap-singapore': {'name': '新加坡', 'endpoint': 'cos.ap-singapore.myqcloud.com'},
    'ap-tokyo': {'name': '东京', 'endpoint': 'cos.ap-tokyo.myqcloud.com'},
    'na-siliconvalley': {'name': '硅谷', 'endpoint': 'cos.na-siliconvalley.myqcloud.com'},
    'na-ashburn': {'name': '弗吉尼亚', 'endpoint': 'cos.na-ashburn.myqcloud.com'},
    'eu-frankfurt': {'name': '法兰克福', 'endpoint': 'cos.eu-frankfurt.myqcloud.com'},
}

# 数据库配置
DATABASE_CONFIG = {
    'type': 'json',
    'path': get_secret('DB', 'path', 'data'),
    'backup_interval': int(get_secret('DB', 'backup_interval', 24)),
    'max_backups': int(get_secret('DB', 'max_backups', 7)),
}

# 系统配置
SYSTEM_CONFIG = {
    'debug': get_secret('SYSTEM', 'debug', False),
    'log_level': get_secret('SYSTEM', 'log_level', 'INFO'),
    'performance_monitoring': get_secret('SYSTEM', 'performance_monitoring', True),
    'auto_cleanup': get_secret('SYSTEM', 'auto_cleanup', True),
    'memory_threshold': int(get_secret('SYSTEM', 'memory_threshold', 80)),
    'network_timeout': int(get_secret('SYSTEM', 'network_timeout', 30)),
}

# Excel 解析配置
EXCEL_CONFIG = {
    'max_rows_scan': int(get_secret('EXCEL', 'max_rows_scan', 1000)),
    'max_cols_scan': int(get_secret('EXCEL', 'max_cols_scan', 50)),
    'preview_rows': int(get_secret('EXCEL', 'preview_rows', 5)),
    'cache_timeout': int(get_secret('EXCEL', 'cache_timeout', 300)),
    'memory_limit': int(get_secret('EXCEL', 'memory_limit', 100)),
    'chunk_size': int(get_secret('EXCEL', 'chunk_size', 1000)),
    'enable_fast_scan': get_secret('EXCEL', 'enable_fast_scan', True),
    'optimize_for_cos': get_secret('EXCEL', 'optimize_for_cos', True),
}

# 搜索配置
SEARCH_CONFIG = {
    'max_results': int(get_secret('SEARCH', 'max_results', 100)),
    'fuzzy_threshold': float(get_secret('SEARCH', 'fuzzy_threshold', 0.8)),
    'search_timeout': int(get_secret('SEARCH', 'search_timeout', 30)),
    'cache_results': get_secret('SEARCH', 'cache_results', True),
}

# 网络配置
NETWORK_CONFIG = {
    'connection_timeout': int(get_secret('NETWORK', 'connection_timeout', 10)),
    'read_timeout': int(get_secret('NETWORK', 'read_timeout', 60)),
    'max_retries': int(get_secret('NETWORK', 'max_retries', 3)),
    'retry_delay': int(get_secret('NETWORK', 'retry_delay', 2)),
    'enable_keep_alive': get_secret('NETWORK', 'enable_keep_alive', True),
}

def validate_config() -> tuple[bool, list[str]]:
    """验证配置是否完整和有效"""
    errors = []
    
    # 检查 Streamlit Secrets 是否可用
    if not check_secrets_available():
        errors.append("Streamlit Secrets 未配置或不可用")
        return False, errors
    
    try:
        # 检查管理员密码
        if not ADMIN_PASSWORD or ADMIN_PASSWORD == 'admin123':
            errors.append("请在 Streamlit Secrets 中配置安全的管理员密码")
        
        # 检查文件大小限制
        max_size = APP_CONFIG.get('max_file_size', 0)
        if max_size < 1024 * 1024:
            errors.append("文件大小限制配置过小")
        
        # 检查COS配置
        cos_required = ['secret_id', 'secret_key', 'region', 'bucket']
        cos_missing = []
        
        for key in cos_required:
            if not COS_CONFIG.get(key):
                cos_missing.append(key.upper())
        
        if cos_missing:
            errors.append(f"COS配置缺失: {', '.join(cos_missing)}")
        
        # 验证COS地域
        if COS_CONFIG.get('region') and COS_CONFIG['region'] not in COS_REGIONS:
            errors.append(f"不支持的COS地域: {COS_CONFIG['region']}")
        
        return len(errors) == 0, errors
        
    except Exception as e:
        errors.append(f"配置验证异常: {str(e)}")
        return False, errors

def get_cos_config() -> Dict[str, Any]:
    """获取完整的COS配置"""
    config = COS_CONFIG.copy()
    
    # 添加地域信息
    region_info = COS_REGIONS.get(config.get('region', ''), {})
    config['region_name'] = region_info.get('name', config.get('region', ''))
    config['endpoint'] = region_info.get('endpoint', '')
    
    return config

def get_upload_config() -> dict:
    """获取上传相关配置"""
    return {
        'max_file_size': APP_CONFIG['max_file_size'],
        'allowed_types': APP_CONFIG['allowed_file_types'],
        'timeout': APP_CONFIG['upload_timeout'],
        'max_concurrent': APP_CONFIG['max_concurrent_uploads'],
        'cos_enabled': bool(COS_CONFIG.get('secret_id') and COS_CONFIG.get('secret_key')),
        'multipart_threshold': COS_CONFIG.get('multipart_threshold', 5 * 1024 * 1024),
    }

def get_performance_config() -> dict:
    """获取性能相关配置"""
    return {
        'max_rows_scan': EXCEL_CONFIG['max_rows_scan'],
        'max_cols_scan': EXCEL_CONFIG['max_cols_scan'],
        'cache_timeout': EXCEL_CONFIG['cache_timeout'],
        'memory_limit': EXCEL_CONFIG['memory_limit'],
        'chunk_size': EXCEL_CONFIG['chunk_size'],
        'cos_optimized': EXCEL_CONFIG['optimize_for_cos'],
    }

def get_network_config() -> dict:
    """获取网络配置"""
    return NETWORK_CONFIG.copy()

def check_cos_connectivity() -> Dict[str, Any]:
    """检查COS连通性"""
    result = {
        'connected': False,
        'latency': None,
        'error': None,
        'region_accessible': False
    }
    
    try:
        import time
        import socket
        
        # 检查COS地域端点连通性
        region = COS_CONFIG.get('region', 'ap-guangzhou')
        region_info = COS_REGIONS.get(region, {})
        endpoint = region_info.get('endpoint', f'cos.{region}.myqcloud.com')
        
        # 测试连接
        start_time = time.time()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        
        try:
            # 尝试连接COS端点的443端口
            sock.connect((endpoint, 443))
            latency = (time.time() - start_time) * 1000  # 毫秒
            
            result.update({
                'connected': True,
                'latency': round(latency, 2),
                'region_accessible': True
            })
            
        except socket.error as e:
            result['error'] = f"连接失败: {str(e)}"
        finally:
            sock.close()
            
    except Exception as e:
        result['error'] = f"连通性检查异常: {str(e)}"
    
    return result

def generate_cos_policy_example() -> Dict[str, Any]:
    """生成COS权限策略示例"""
    bucket = COS_CONFIG.get('bucket', 'your-bucket-name')
    
    return {
        "version": "2.0",
        "statement": [
            {
                "effect": "allow",
                "principal": {
                    "qcs": [
                        f"qcs::cam::uin/your-uin:uin/your-uin"
                    ]
                },
                "action": [
                    "cos:GetObject",
                    "cos:PutObject",
                    "cos:DeleteObject",
                    "cos:HeadObject",
                    "cos:ListParts",
                    "cos:InitiateMultipartUpload",
                    "cos:UploadPart",
                    "cos:CompleteMultipartUpload",
                    "cos:AbortMultipartUpload"
                ],
                "resource": [
                    f"qcs::cos:*:uid/*:{bucket}/*"
                ]
            }
        ]
    }

def export_config_template() -> str:
    """导出 Streamlit Secrets 配置模板"""
    template = f"""# Streamlit Secrets 配置模板
# 在 Streamlit Cloud 中：设置 > Secrets > 粘贴以下内容
# 本地开发：创建 .streamlit/secrets.toml 文件

[AUTH]
admin_password = "your_secure_password_here"

[COS]
secret_id = "your_secret_id"
secret_key = "your_secret_key"
region = "ap-guangzhou"
bucket = "your-bucket-name"
domain = "your-custom-domain.com"  # 可选
timeout = 60
max_retries = 3
chunk_size = 1048576  # 1MB
enable_multipart = true
multipart_threshold = 5242880  # 5MB

[APP]
max_file_size = 10485760  # 10MB
upload_folder = "uploads"
max_concurrent_uploads = 3
upload_timeout = 300

[EXCEL]
max_rows_scan = 1000
max_cols_scan = 50
cache_timeout = 300
memory_limit = 100
enable_fast_scan = true
optimize_for_cos = true

[SYSTEM]
debug = false
log_level = "INFO"
performance_monitoring = true
auto_cleanup = true
memory_threshold = 80

[UI]
page_title = "门店报表查询系统"
page_icon = "📊"
layout = "wide"

# 可选的高级配置
[COS.advanced]
cdn_domain = "your-cdn-domain.com"
enable_cdn = false
signed_url_expires = 3600
use_https = true
verify_ssl = true
"""
    
    return template

def get_secrets_status() -> Dict[str, Any]:
    """获取 Secrets 配置状态"""
    try:
        secrets_available = check_secrets_available()
        
        if not secrets_available:
            return {
                'available': False,
                'configured_sections': [],
                'missing_sections': ['AUTH', 'COS', 'APP'],
                'status': 'not_configured'
            }
        
        # 检查已配置的sections
        configured_sections = []
        missing_sections = []
        required_sections = ['AUTH', 'COS', 'APP']
        
        for section in required_sections:
            try:
                if section in st.secrets and len(st.secrets[section]) > 0:
                    configured_sections.append(section)
                else:
                    missing_sections.append(section)
            except:
                missing_sections.append(section)
        
        status = 'complete' if len(missing_sections) == 0 else 'partial'
        
        return {
            'available': True,
            'configured_sections': configured_sections,
            'missing_sections': missing_sections,
            'status': status
        }
        
    except Exception as e:
        return {
            'available': False,
            'configured_sections': [],
            'missing_sections': ['AUTH', 'COS', 'APP'],
            'status': 'error',
            'error': str(e)
        }

def detect_environment() -> str:
    """检测运行环境"""
    try:
        # 检查是否在 Streamlit Cloud
        if check_secrets_available():
            return 'streamlit_cloud'
        else:
            return 'local'
    except Exception:
        return 'unknown'

# 根据环境调整配置
ENVIRONMENT = detect_environment()
