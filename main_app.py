# main_app.py - 统一主入口（合并版本）
"""
门店报表查询系统 - 统一主入口
集成查询、上传、权限管理功能
"""

import streamlit as st
import sys
import traceback
from pathlib import Path

# 页面配置
st.set_page_config(
    page_title="门店报表系统",
    page_icon="🏪",
    layout="wide",
    initial_sidebar_state="expanded"
)

def load_app_module(module_name: str, app_function: str):
    """安全加载应用模块"""
    try:
        if module_name == "enhanced_app":
            from mongodb_store_system_fixed import main
            return main
        elif module_name == "bulk_uploader":
            from bulk_uploader_fixed import create_upload_interface
            return create_upload_interface
        elif module_name == "permission_manager":
            from permission_manager_fixed import create_permission_interface
            return create_permission_interface
        else:
            st.error(f"未知的应用模块: {module_name}")
            return None
    except ImportError as e:
        st.error(f"导入模块 {module_name} 失败: {e}")
        st.code(f"错误详情:\n{traceback.format_exc()}")
        return None
    except Exception as e:
        st.error(f"加载应用 {module_name} 时出现未知错误: {e}")
        st.code(f"错误详情:\n{traceback.format_exc()}")
        return None

def show_system_info():
    """显示系统信息"""
    from config import ConfigManager
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📊 系统信息")
    
    # 检查配置状态
    if ConfigManager.validate_config():
        st.sidebar.success("✅ 配置正常")
    else:
        st.sidebar.warning("⚠️ 配置待完善")
    
    # 检查数据库连接
    try:
        from database_manager import get_database
        db = get_database()
        if db:
            st.sidebar.success("✅ 数据库已连接")
        else:
            st.sidebar.error("❌ 数据库连接失败")
    except Exception:
        st.sidebar.error("❌ 数据库连接失败")

def main():
    """统一主应用入口"""
    
    # 侧边栏应用选择
    with st.sidebar:
        st.title("🏪 门店报表系统")
        
        app_choice = st.selectbox(
            "选择功能模块",
            [
                "门店查询系统", 
                "批量上传系统", 
                "权限管理系统"
            ],
            index=0,
            help="选择要使用的功能模块"
        )
        
        st.markdown("---")
        st.markdown("### 📋 功能说明")
        
        if app_choice == "门店查询系统":
            st.markdown("""
            **门店查询系统**
            - 🔐 查询编号登录
            - 📊 查看报表数据  
            - 💰 应收未收看板
            - 📥 报表下载
            """)
        elif app_choice == "批量上传系统":
            st.markdown("""
            **批量上传系统**
            - 👨‍💼 管理员登录
            - 📤 批量上传Excel
            - 🏪 自动门店创建
            - 📈 上传统计
            """)
        elif app_choice == "权限管理系统":
            st.markdown("""
            **权限管理系统**
            - 🔐 权限表管理
            - 🔗 查询编号分配
            - 🏪 门店权限配置
            - 📋 权限列表查看
            """)
        
        # 显示系统状态信息
        show_system_info()
    
    # 加载并运行对应的应用
    try:
        app_function = None
        
        if app_choice == "门店查询系统":
            app_function = load_app_module("enhanced_app", "main")
            
        elif app_choice == "批量上传系统":
            app_function = load_app_module("bulk_uploader", "create_upload_interface")
            
        elif app_choice == "权限管理系统":
            app_function = load_app_module("permission_manager", "create_permission_interface")
        
        # 运行应用
        if app_function:
            app_function()
        else:
            st.error("应用加载失败，请刷新页面重试")
            
    except Exception as e:
        st.error(f"应用运行时出错: {e}")
        
        # 显示详细错误信息（仅在调试模式下）
        from config import ConfigManager
        if ConfigManager.get_app_config().get('debug', False):
            st.code(f"详细错误信息:\n{traceback.format_exc()}")
        
        st.info("💡 解决建议：")
        st.markdown("""
        1. 检查所有依赖文件是否存在
        2. 确认数据库连接配置正确
        3. 尝试刷新页面
        4. 联系系统管理员
        """)

if __name__ == "__main__":
    main()
