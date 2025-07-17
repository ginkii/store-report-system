import os
import streamlit as st
from typing import Dict, Any

def get_admin_password() -> str:
    """获取管理员密码"""
    try:
        # 优先从streamlit secrets获取
        return st.secrets.get('ADMIN_PASSWORD', 'admin123')
    except (AttributeError, FileNotFoundError):
        # 降级到环境变量
        return os.getenv('ADMIN_PASSWORD', 'admin123')

def get_cos_config() -> Dict[str, str]:
    """获取腾讯云COS配置"""
    try:
        # 优先从streamlit secrets获取
        cos_secrets = st.secrets.get('tencent_cos', {})
        return {
            'region': cos_secrets.get('region', 'ap-shanghai'),
            'secret_id': cos_secrets.get('secret_id', ''),
            'secret_key': cos_secrets.get('secret_key', ''),
            'bucket': cos_secrets.get('bucket_name', ''),
            'domain': cos_secrets.get('domain', ''),
        }
    except (AttributeError, FileNotFoundError):
        # 降级到环境变量
        return {
            'region': os.getenv('COS_REGION', 'ap-shanghai'),
            'secret_id': os.getenv('COS_SECRET_ID', ''),
            'secret_key': os.getenv('COS_SECRET_KEY', ''),
            'bucket': os.getenv('COS_BUCKET', ''),
            'domain': os.getenv('COS_DOMAIN', ''),
        }

def get_app_config() -> Dict[str, Any]:
    """获取应用配置"""
    try:
        # 优先从streamlit secrets获取
        app_secrets = st.secrets.get('app', {})
        return {
            'title': '门店报表查询系统',
            'data_file': app_secrets.get('data_file', 'data.json'),
            'upload_folder': app_secrets.get('upload_folder', 'reports'),
            'max_file_size': app_secrets.get('max_file_size', 50 * 1024 * 1024),  # 50MB
            'allowed_extensions': ['.xlsx', '.xls'],
            'session_timeout': app_secrets.get('session_timeout', 3600),  # 1小时
        }
    except (AttributeError, FileNotFoundError):
        # 降级到默认配置
        return {
            'title': '门店报表查询系统',
            'data_file': 'data.json',
            'upload_folder': 'reports',
            'max_file_size': 50 * 1024 * 1024,  # 50MB
            'allowed_extensions': ['.xlsx', '.xls'],
            'session_timeout': 3600,  # 1小时
        }

def get_streamlit_config() -> Dict[str, str]:
    """获取Streamlit页面配置"""
    try:
        # 优先从streamlit secrets获取
        streamlit_secrets = st.secrets.get('streamlit', {})
        return {
            'page_title': streamlit_secrets.get('page_title', '门店报表查询系统'),
            'page_icon': streamlit_secrets.get('page_icon', '📊'),
            'layout': streamlit_secrets.get('layout', 'wide'),
            'initial_sidebar_state': streamlit_secrets.get('initial_sidebar_state', 'expanded'),
        }
    except (AttributeError, FileNotFoundError):
        # 降级到默认配置
        return {
            'page_title': '门店报表查询系统',
            'page_icon': '📊',
            'layout': 'wide',
            'initial_sidebar_state': 'expanded',
        }

# 导出配置变量（保持向后兼容）
try:
    ADMIN_PASSWORD = get_admin_password()
    COS_CONFIG = get_cos_config()
    APP_CONFIG = get_app_config()
    STREAMLIT_CONFIG = get_streamlit_config()
except Exception as e:
    # 如果在导入时出错，使用默认配置
    print(f"配置加载警告: {e}")
    ADMIN_PASSWORD = 'admin123'
    COS_CONFIG = {
        'region': 'ap-shanghai',
        'secret_id': '',
        'secret_key': '',
        'bucket': '',
        'domain': '',
    }
    APP_CONFIG = {
        'title': '门店报表查询系统',
        'data_file': 'data.json',
        'upload_folder': 'reports',
        'max_file_size': 50 * 1024 * 1024,
        'allowed_extensions': ['.xlsx', '.xls'],
        'session_timeout': 3600,
    }
    STREAMLIT_CONFIG = {
        'page_title': '门店报表查询系统',
        'page_icon': '📊',
        'layout': 'wide',
        'initial_sidebar_state': 'expanded',
    }

def validate_config() -> bool:
    """验证配置是否完整"""
    cos_config = get_cos_config()
    required_keys = ['secret_id', 'secret_key', 'bucket', 'region']
    
    for key in required_keys:
        if not cos_config.get(key):
            print(f"警告: COS配置缺失 {key}")
            return False
    return True

def get_config_info() -> Dict[str, Any]:
    """获取配置信息（用于调试）"""
    return {
        'config_source': 'streamlit_secrets' if hasattr(st, 'secrets') else 'environment',
        'admin_password_set': bool(get_admin_password()),
        'cos_config_complete': validate_config(),
        'app_config': get_app_config(),
    }

def reload_config():
    """重新加载配置（用于配置更新后刷新）"""
    global ADMIN_PASSWORD, COS_CONFIG, APP_CONFIG, STREAMLIT_CONFIG
    try:
        ADMIN_PASSWORD = get_admin_password()
        COS_CONFIG = get_cos_config()
        APP_CONFIG = get_app_config()
        STREAMLIT_CONFIG = get_streamlit_config()
        return True
    except Exception as e:
        print(f"重新加载配置失败: {e}")
        return False
