import os
from typing import Dict, Any

# 管理员配置
ADMIN_PASSWORD = os.getenv('ADMIN_PASSWORD', 'admin123')  # 建议使用环境变量

# 腾讯云COS配置
COS_CONFIG = {
    'region': 'ap-shanghai',  # 上海地区
    'secret_id': 'AKIDARaYN4YpuqcDdqrfJkFnCQSYbVDi06zf',
    'secret_key': 'XszvmRt9C3iWHC6ymU2OXVIsGRPBk8LN',
    'bucket': 'store-reports-data-1369683907',
    'domain': os.getenv('COS_DOMAIN', ''),  # 可选，自定义域名
}

# 应用配置
APP_CONFIG = {
    'title': '门店报表查询系统',
    'data_file': 'data.json',
    'upload_folder': 'reports',
    'max_file_size': 50 * 1024 * 1024,  # 50MB
    'allowed_extensions': ['.xlsx', '.xls'],
    'session_timeout': 3600,  # 1小时
}

# Streamlit页面配置
STREAMLIT_CONFIG = {
    'page_title': '门店报表查询系统',
    'page_icon': '📊',
    'layout': 'wide',
    'initial_sidebar_state': 'expanded',
}

def validate_config() -> bool:
    """验证配置是否完整"""
    required_keys = ['secret_id', 'secret_key', 'bucket', 'region']
    for key in required_keys:
        if not COS_CONFIG.get(key):
            print(f"警告: COS配置缺失 {key}")
            return False
    return True
