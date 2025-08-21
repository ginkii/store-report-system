"""
门店报表查询系统 - 主入口文件
包含查询编号认证、批量上传、权限管理等完整功能
"""

import streamlit as st

# 页面配置
st.set_page_config(
    page_title="门店报表系统",
    page_icon="🏪",
    layout="wide",
    initial_sidebar_state="expanded"
)

def main():
    """主应用入口"""
    
    # 侧边栏选择应用
    with st.sidebar:
        st.title("🏪 门店报表系统")
        
        app_choice = st.selectbox(
            "选择应用",
            ["门店查询系统", "批量上传系统", "权限管理系统"],
            index=0
        )
        
        st.markdown("---")
        st.markdown("### 📋 系统说明")
        st.markdown("""
        - **门店查询系统**: 门店用户查询报表
        - **批量上传系统**: 管理员批量上传数据
        - **权限管理系统**: 管理查询编号和门店权限
        """)
    
    # 根据选择加载对应的应用
    try:
        if app_choice == "门店查询系统":
            # 导入并运行门店查询应用
            from enhanced_app import main as query_main
            query_main()
            
        elif app_choice == "批量上传系统":
            # 导入并运行批量上传应用
            from bulk_uploader import create_upload_interface
            create_upload_interface()
            
        elif app_choice == "权限管理系统":
            # 导入并运行权限管理应用
            from permission_manager import create_permission_interface
            create_permission_interface()
            
    except Exception as e:
        st.error(f"应用加载失败: {e}")
        st.info("请刷新页面或联系管理员")

if __name__ == "__main__":
    main()
