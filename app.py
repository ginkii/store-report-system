"""
门店报表查询系统 - Streamlit Cloud 部署入口
"""

import streamlit as st
import sys
from pathlib import Path

# 添加当前目录到Python路径
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

def main():
    """主应用入口"""
    
    # 页面配置
    st.set_page_config(
        page_title="门店报表系统",
        page_icon="🏪",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # 侧边栏选择应用
    with st.sidebar:
        st.title("🏪 门店报表系统")
        
        app_choice = st.selectbox(
            "选择应用",
            ["门店查询系统", "批量上传系统"],
            index=0
        )
        
        st.markdown("---")
        st.markdown("### 📋 系统说明")
        st.markdown("""
        - **门店查询系统**: 门店用户查询报表
        - **批量上传系统**: 管理员批量上传数据
        """)
    
    # 根据选择加载对应的应用
    if app_choice == "门店查询系统":
        # 导入并运行门店查询应用
        from enhanced_app import main as query_main
        query_main()
        
    elif app_choice == "批量上传系统":
        # 导入并运行批量上传应用
        from bulk_uploader import create_upload_interface
        create_upload_interface()

if __name__ == "__main__":
    main()
