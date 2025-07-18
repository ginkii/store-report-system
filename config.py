# 优化的配置文件 - COS集成版本
import os
import json
from typing import Dict, Any, Optional

# 应用配置
APP_CONFIG = {
    'max_file_size': 10 * 1024 * 1024,  # 10MB - 提升文件大小限制
    'upload_folder': 'uploads',
    'temp_folder': 'temp',
    'backup_folder': 'backups',
    'allowed_file_types': ['xlsx', 'xls'],
    'max_concurrent_uploads': 3,
    'upload_timeout': 300,  # 5分钟超时
    'cache_ttl': 3600,  # 1小时缓存
}

# Streamlit 界面配置
STREAMLIT_CONFIG = {
    'page_title': '门店报表查询系统',
    'page_icon': '📊',
    'layout': 'wide',
    'initial_sidebar_state': 'expanded',
    'menu_items': {
        'Get Help': None,
        'Report a bug': None,
        'About': "门店报表查询系统 v2.0 - COS云存储版"
    }
}

# 管理员密码
ADMIN_PASSWORD = os.getenv('ADMIN_PASSWORD', 'admin123')

# 腾讯云 COS 配置 - 增强版本
COS_CONFIG = {
    'secret_id': os.getenv('COS_SECRET_ID', ''),
    'secret_key': os.getenv('COS_SECRET_KEY', ''),
    'region': os.getenv('COS_REGION', 'ap-guangzhou'),
    'bucket': os.getenv('COS_BUCKET', ''),
    'domain': os.getenv('COS_DOMAIN', ''),
    
    # 高级配置
    'timeout': int(os.getenv('COS_TIMEOUT', '60')),
    'max_retries': int(os.getenv('COS_MAX_RETRIES', '3')),
    'chunk_size': int(os.getenv('COS_CHUNK_SIZE', str(1024 * 1024))),  # 1MB
    'enable_multipart': os.getenv('COS_ENABLE_MULTIPART', 'true').lower() == 'true',
    'multipart_threshold': int(os.getenv('COS_MULTIPART_THRESHOLD', str(5 * 1024 * 1024))),  # 5MB
    
    # CDN和加速配置
    'cdn_domain': os.getenv('COS_CDN_DOMAIN', ''),
    'enable_cdn': os.getenv('COS_ENABLE_CDN', 'false').lower() == 'true',
    'signed_url_expires': int(os.getenv('COS_SIGNED_URL_EXPIRES', '3600')),  # 1小时
    
    # 安全配置
    'use_https': os.getenv('COS_USE_HTTPS', 'true').lower() == 'true',
    'verify_ssl': os.getenv('COS_VERIFY_SSL', 'true').lower() == 'true',
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

# 数据库配置（如果使用）
DATABASE_CONFIG = {
    'type': 'json',  # 默认使用JSON文件
    'path': os.getenv('DB_PATH', 'data'),
    'backup_interval': 24,  # 24小时自动备份
    'max_backups': 7,  # 保留7个备份
}

# 系统配置
SYSTEM_CONFIG = {
    'debug': os.getenv('DEBUG', 'False').lower() == 'true',
    'log_level': os.getenv('LOG_LEVEL', 'INFO'),
    'performance_monitoring': True,
    'auto_cleanup': True,
    'memory_threshold': 80,  # 内存使用超过80%时清理缓存
    'network_timeout': 30,   # 网络超时设置
}

# Excel 解析配置 - 针对COS优化
EXCEL_CONFIG = {
    'max_rows_scan': 1000,      # 最大扫描行数
    'max_cols_scan': 50,        # 最大扫描列数
    'preview_rows': 5,          # 预览行数
    'cache_timeout': 300,       # 5分钟缓存
    'memory_limit': 100,        # 100MB内存限制
    'chunk_size': 1000,         # 分块处理大小
    'enable_fast_scan': True,   # 启用快速扫描
    'optimize_for_cos': True,   # COS优化模式
}

# 搜索配置
SEARCH_CONFIG = {
    'max_results': 100,         # 最大搜索结果数
    'fuzzy_threshold': 0.8,     # 模糊匹配阈值
    'search_timeout': 30,       # 30秒搜索超时
    'cache_results': True,      # 缓存搜索结果
}

# 网络配置
NETWORK_CONFIG = {
    'connection_timeout': 10,   # 连接超时
    'read_timeout': 60,         # 读取超时
    'max_retries': 3,           # 最大重试次数
    'retry_delay': 2,           # 重试延迟（秒）
    'enable_keep_alive': True,  # 保持连接
}

def load_env_file(env_path: str = '.env'):
    """加载.env文件中的环境变量"""
    try:
        if os.path.exists(env_path):
            with open(env_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        os.environ[key.strip()] = value.strip().strip('"\'')
            return True
    except Exception as e:
        print(f"加载.env文件失败: {e}")
    return False

def validate_config() -> tuple[bool, list[str]]:
    """验证配置是否完整和有效"""
    errors = []
    
    try:
        # 检查基础配置
        if not ADMIN_PASSWORD or ADMIN_PASSWORD == 'admin123':
            errors.append("建议修改默认管理员密码")
        
        # 检查文件大小限制是否合理
        max_size = APP_CONFIG.get('max_file_size', 0)
        if max_size < 1024 * 1024:  # 小于1MB
            errors.append("文件大小限制过小")
        
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
        
        # 检查文件类型配置
        if not APP_CONFIG.get('allowed_file_types'):
            errors.append("未配置允许的文件类型")
        
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

def update_config(section: str, key: str, value) -> bool:
    """动态更新配置"""
    try:
        config_map = {
            'app': APP_CONFIG,
            'streamlit': STREAMLIT_CONFIG,
            'cos': COS_CONFIG,
            'excel': EXCEL_CONFIG,
            'search': SEARCH_CONFIG,
            'system': SYSTEM_CONFIG,
            'network': NETWORK_CONFIG,
        }
        
        if section in config_map:
            config_map[section][key] = value
            return True
        
        return False
        
    except Exception:
        return False

def detect_environment() -> str:
    """检测运行环境"""
    if os.getenv('STREAMLIT_SHARING'):
        return 'streamlit_cloud'
    elif os.getenv('HEROKU'):
        return 'heroku'
    elif os.getenv('DOCKER'):
        return 'docker'
    elif os.getenv('KUBERNETES_SERVICE_HOST'):
        return 'kubernetes'
    else:
        return 'local'

def get_optimal_cos_region() -> str:
    """根据环境获取最优COS地域"""
    environment = detect_environment()
    
    # 根据部署环境推荐地域
    if environment == 'streamlit_cloud':
        return 'ap-singapore'  # Streamlit Cloud通常在海外
    elif environment in ['heroku', 'kubernetes']:
        return 'ap-hongkong'   # 国际访问友好
    else:
        return 'ap-guangzhou'  # 国内默认

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
    """导出配置模板"""
    template = f"""# 门店报表查询系统配置文件
# 请根据实际情况修改以下配置

# 管理员密码
ADMIN_PASSWORD=your_secure_password

# 腾讯云COS配置
COS_SECRET_ID=your_secret_id
COS_SECRET_KEY=your_secret_key
COS_REGION={get_optimal_cos_region()}
COS_BUCKET=your-bucket-name
COS_DOMAIN=your-custom-domain.com

# 高级COS配置（可选）
COS_TIMEOUT=60
COS_MAX_RETRIES=3
COS_CHUNK_SIZE={1024 * 1024}
COS_ENABLE_MULTIPART=true
COS_MULTIPART_THRESHOLD={5 * 1024 * 1024}

# CDN配置（可选）
COS_CDN_DOMAIN=your-cdn-domain.com
COS_ENABLE_CDN=false

# 系统配置
DEBUG=false
LOG_LEVEL=INFO
MAX_FILE_SIZE={APP_CONFIG['max_file_size']}
"""
    
    return template

# 根据环境调整配置
ENVIRONMENT = detect_environment()

if ENVIRONMENT == 'streamlit_cloud':
    # Streamlit Cloud 环境优化
    APP_CONFIG['max_file_size'] = 8 * 1024 * 1024  # 8MB
    EXCEL_CONFIG['max_rows_scan'] = 800
    EXCEL_CONFIG['memory_limit'] = 80  # 80MB
    COS_CONFIG['timeout'] = 90  # 增加超时时间
elif ENVIRONMENT == 'heroku':
    # Heroku 环境优化
    APP_CONFIG['max_file_size'] = 10 * 1024 * 1024  # 10MB
    EXCEL_CONFIG['max_rows_scan'] = 1000
    COS_CONFIG['timeout'] = 60
elif ENVIRONMENT == 'local':
    # 本地环境可以使用更高的限制
    APP_CONFIG['max_file_size'] = 20 * 1024 * 1024  # 20MB
    EXCEL_CONFIG['max_rows_scan'] = 2000
    EXCEL_CONFIG['memory_limit'] = 200  # 200MB

# 自动加载.env文件
load_env_file()
