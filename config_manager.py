import streamlit as st
import os
from typing import Dict, Any

class ConfigManager:
    """配置管理器，统一处理Streamlit secrets和环境变量"""
    
    @staticmethod
    def get_mongodb_config() -> Dict[str, str]:
        """获取MongoDB配置"""
        config = {}
        
        # 优先使用Streamlit secrets
        if hasattr(st, 'secrets') and 'mongodb' in st.secrets:
            config['uri'] = st.secrets["mongodb"]["uri"]
            config['database_name'] = st.secrets["mongodb"]["database_name"]
        else:
            # 回退到环境变量
            config['uri'] = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
            config['database_name'] = os.getenv('DATABASE_NAME', 'store_reports')
        
        return config
    
    @staticmethod
    def get_app_config() -> Dict[str, Any]:
        """获取应用配置"""
        config = {}
        
        if hasattr(st, 'secrets') and 'app' in st.secrets:
            config['secret_key'] = st.secrets["app"]["secret_key"]
            config['debug'] = st.secrets["app"].get("debug", False)
        else:
            config['secret_key'] = os.getenv('SECRET_KEY', 'default_secret_key')
            config['debug'] = os.getenv('APP_DEBUG', 'False').lower() == 'true'
        
        return config
    
    @staticmethod
    def get_security_config() -> Dict[str, Any]:
        """获取安全配置"""
        config = {}
        
        if hasattr(st, 'secrets') and 'security' in st.secrets:
            config['admin_password'] = st.secrets["security"]["admin_password"]
            config['session_timeout'] = st.secrets["security"].get("session_timeout", 14400)
        else:
            config['admin_password'] = os.getenv('ADMIN_PASSWORD', 'admin123456')
            config['session_timeout'] = int(os.getenv('SESSION_TIMEOUT', '14400'))
        
        return config
    
    @staticmethod
    def validate_config() -> bool:
        """验证配置是否完整"""
        try:
            mongodb_config = ConfigManager.get_mongodb_config()
            
            # 检查必要的配置项
            if not mongodb_config.get('uri') or mongodb_config['uri'] == 'mongodb://localhost:27017/':
                st.warning("⚠️ 未配置MongoDB连接，使用本地数据库")
                return False
            
            if not mongodb_config.get('database_name'):
                st.error("❌ 数据库名称未配置")
                return False
            
            return True
            
        except Exception as e:
            st.error(f"❌ 配置验证失败: {e}")
            return False