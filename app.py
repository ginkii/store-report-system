import streamlit as st
import pandas as pd
import io
from datetime import datetime

# 页面配置
st.set_page_config(
    page_title="门店报表查询系统",
    page_icon="📊",
    layout="wide"
)

# 自定义CSS样式
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
        border-bottom: 3px solid #1f77b4;
        padding-bottom: 1rem;
    }
    .store-info {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 10px;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

# 数据加载函数
@st.cache_data
def load_permissions_data(file):
    """加载门店权限表"""
    if file is not None:
        df = pd.read_excel(file)
        return df
    return None

@st.cache_data
def load_reports_data(file):
    """加载财务报表数据"""
    if file is not None:
        excel_file = pd.ExcelFile(file)
        all_sheets = {}
        
        for sheet_name in excel_file.sheet_names:
            df = pd.read_excel(file, sheet_name=sheet_name)
            
            # 从标题行或sheet名提取门店名称
            store_name = extract_store_name_from_sheet(df, sheet_name)
            if store_name:
                all_sheets[store_name] = df
            else:
                all_sheets[sheet_name] = df
        
        return all_sheets
    return None

def extract_store_name_from_sheet(df, sheet_name):
    """从sheet数据或sheet名中提取门店名称"""
    # 方法1: 从第一行标题中提取（如：2025年犀牛百货（南通大学店）盈利情况表）
    if not df.empty and len(df.columns) > 0:
        first_cell = str(df.iloc[0, 0]) if not df.empty else ""
        if "（" in first_cell and "）" in first_cell:
            start = first_cell.find("（") + 1
            end = first_cell.find("）")
            if start > 0 and end > start:
                store_name = first_cell[start:end]
                return store_name
    
    # 方法2: 从sheet名中提取
    if "（" in sheet_name and "）" in sheet_name:
        start = sheet_name.find("（") + 1
        end = sheet_name.find("）")
        if start > 0 and end > start:
            return sheet_name[start:end]
    
    # 方法3: 直接使用sheet名
    return sheet_name

def authenticate_user(permissions_df, store_name, employee_id):
    """用户身份验证"""
    if permissions_df is not None:
        user_record = permissions_df[
            (permissions_df['门店名称'] == store_name) & 
            (permissions_df['人员编号'] == employee_id)
        ]
        return not user_record.empty, user_record
    return False, None

def get_store_report(reports_data, store_name):
    """获取指定门店的报表数据"""
    if reports_data and store_name in reports_data:
        return reports_data[store_name]
    return None

def main():
    st.markdown('<h1 class="main-header">📊 门店报表查询系统</h1>', unsafe_allow_html=True)
    
    # 检查session state
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
        st.session_state.current_store = None
        st.session_state.user_info = None
    
    # 侧边栏 - 文件上传
    with st.sidebar:
        st.header("📁 数据上传")
        
        permissions_file = st.file_uploader(
            "上传门店权限表",
            type=['xlsx', 'xls'],
            help="Excel文件，包含列：门店名称、人员编号"
        )
        
        reports_file = st.file_uploader(
            "上传财务报表总表",
            type=['xlsx', 'xls'],
            help="Excel文件，每个门店一个sheet"
        )
        
        # 数据格式说明
        with st.expander("📋 数据格式说明"):
            st.markdown("""
            **权限表格式：**
            - 列1：门店名称（如：南通大学店）
            - 列2：人员编号（如：001）
            
            **报表格式：**
            - 每个门店一个sheet页
            - 标题包含门店名称，如：2025年犀牛百货（南通大学店）盈利情况表
            - 或者sheet名包含门店名称
            """)
        
        if permissions_file and reports_file:
            st.success("✅ 文件上传成功！")
        
        st.markdown("---")
        
        # 登出按钮
        if st.session_state.authenticated:
            if st.button("🚪 退出登录", type="secondary"):
                st.session_state.authenticated = False
                st.session_state.current_store = None
                st.session_state.user_info = None
                st.rerun()
    
    # 加载数据
    permissions_df = load_permissions_data(permissions_file)
    reports_data = load_reports_data(reports_file)
    
    # 主要内容区域
    if not st.session_state.authenticated:
        # 登录界面
        st.markdown("### 🔐 用户登录")
        
        if permissions_df is not None:
            # 获取所有门店名称
            store_names = sorted(permissions_df['门店名称'].unique().tolist())
            
            col1, col2 = st.columns(2)
            
            with col1:
                selected_store = st.selectbox(
                    "选择门店",
                    options=store_names,
                    index=0 if store_names else None
                )
            
            with col2:
                employee_id = st.text_input(
                    "输入人员编号",
                    placeholder="请输入您的人员编号"
                )
            
            if st.button("🔑 登录", type="primary"):
                if selected_store and employee_id:
                    is_valid, user_record = authenticate_user(permissions_df, selected_store, employee_id)
                    
                    if is_valid:
                        st.session_state.authenticated = True
                        st.session_state.current_store = selected_store
                        st.session_state.user_info = user_record.iloc[0]
                        st.success(f"✅ 登录成功！欢迎来到 {selected_store}")
                        st.rerun()
                    else:
                        st.error("❌ 门店名称和人员编号不匹配，请重新输入！")
                else:
                    st.warning("⚠️ 请选择门店并输入人员编号！")
        else:
            st.info("📋 请先上传门店权限表文件")
    
    else:
        # 已登录 - 显示报表
        user_info = st.session_state.user_info
        current_store = st.session_state.current_store
        
        # 用户信息显示
        st.markdown(f"""
        <div class="store-info">
            <h3>👤 当前用户信息</h3>
            <p><strong>门店：</strong>{current_store}</p>
            <p><strong>人员编号：</strong>{user_info.get('人员编号', 'N/A')}</p>
            <p><strong>登录时间：</strong>{datetime.now().strftime('%Y-%m-%d %H:%M')}</p>
        </div>
        """, unsafe_allow_html=True)
        
        # 获取门店报表数据
        if reports_data:
            store_report = get_store_report(reports_data, current_store)
            
            if store_report is not None:
                st.markdown(f"### 📊 {current_store} 财务报表")
                
                # 显示完整报表
                st.dataframe(store_report, use_container_width=True)
                
                # 下载报表
                st.markdown("#### 💾 导出报表")
                
                # 创建Excel文件用于下载
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    store_report.to_excel(writer, sheet_name=current_store, index=False)
                
                excel_data = output.getvalue()
                
                st.download_button(
                    label=f"📥 下载 {current_store} 报表",
                    data=excel_data,
                    file_name=f"{current_store}_财务报表_{datetime.now().strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
                
            else:
                st.error(f"❌ 未找到门店 '{current_store}' 的报表数据，请检查Excel文件中是否包含对应的sheet页。")
        else:
            st.info("📋 请先上传财务报表总表文件")

if __name__ == "__main__":
    main()
