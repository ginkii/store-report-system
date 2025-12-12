# config_manager.py - 统一配置管理
import streamlit as st
import os
from typing import Dict, Optional

class ConfigManager:
    """统一的配置管理器"""
    
    @staticmethod
    def get_mongodb_config() -> Dict[str, str]:
        """获取MongoDB连接配置"""
        try:
            # 优先从Streamlit secrets获取
            if hasattr(st, 'secrets') and 'mongodb' in st.secrets:
                return {
                    'uri': st.secrets["mongodb"]["uri"],
                    'database_name': st.secrets["mongodb"]["database_name"]
                }
        except Exception:
            pass
        
        # 从环境变量获取
        return {
            'uri': os.getenv('MONGODB_URI', 'mongodb://localhost:27017/'),
            'database_name': os.getenv('DATABASE_NAME', 'store_reports')
        }
    
    @staticmethod
    def get_admin_password() -> str:
        """获取管理员密码"""
        try:
            if hasattr(st, 'secrets') and 'security' in st.secrets:
                return st.secrets["security"]["admin_password"]
        except Exception:
            pass
        
        return os.getenv('ADMIN_PASSWORD', 'admin123')
    
    @staticmethod
    def validate_config() -> bool:
        """验证配置是否完整"""
        try:
            config = ConfigManager.get_mongodb_config()
            return bool(config['uri'] and config['database_name'])
        except Exception:
            return False
