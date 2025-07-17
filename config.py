import os
import streamlit as st

def get_secret(key, default=None):
    """从 Streamlit secrets 或环境变量中获取配置"""
    # 优先从 Streamlit secrets 读取
    if hasattr(st, 'secrets') and key in st.secrets:
        return st.secrets[key]
    # 回退到环境变量
    return os.getenv(key, default)

# 腾讯云COS配置
COS_CONFIG = {
    'SecretId': get_secret('COS_SECRET_ID', 'your-secret-id'),
    'SecretKey': get_secret('COS_SECRET_KEY', 'your-secret-key'),
    'Region': get_secret('COS_REGION', 'ap-shanghai'),
    'Bucket': get_secret('COS_BUCKET', 'your-bucket-name')
}

# 应用配置
APP_CONFIG = {
    'max_file_size': int(get_secret('MAX_FILE_SIZE', '52428800')),
    'session_timeout': int(get_secret('SESSION_TIMEOUT', '3600')),
    'upload_folder': get_secret('UPLOAD_FOLDER', 'reports'),
    'data_file': get_secret('DATA_FILE', 'data.json'),
    'allowed_extensions': ['xlsx', 'xls']
}

# Streamlit配置
STREAMLIT_CONFIG = {
    'page_title': get_secret('PAGE_TITLE', '门店报表查询系统'),
    'page_icon': get_secret('PAGE_ICON', '📊'),
    'layout': get_secret('LAYOUT', 'wide'),
    'initial_sidebar_state': get_secret('INITIAL_SIDEBAR_STATE', 'expanded')
}

# 管理员密码
ADMIN_PASSWORD = get_secret('ADMIN_PASSWORD', 'admin123')

def validate_config():
    """验证配置是否完整"""
    required_keys = ['SecretId', 'SecretKey', 'Region', 'Bucket']
    for key in required_keys:
        if not COS_CONFIG.get(key) or COS_CONFIG[key] in ['your-secret-id', 'your-secret-key', 'your-bucket-name']:
            print(f"配置项 {key} 未正确设置")
            return False
    return True
