import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
import time

def quick_diagnostic_and_fix():
    """快速诊断和修复工具"""
    st.title("🔧 快速诊断和修复工具")
    
    st.markdown("""
    这个工具会快速检查当前配置状态，并尝试解决403错误。
    """)
    
    # 第1步：检查当前配置
    st.subheader("1️⃣ 检查当前配置")
    
    if "google_sheets" in st.secrets:
        config = st.secrets["google_sheets"]
        current_email = config.get("client_email", "未知")
        current_project = config.get("project_id", "未知")
        
        st.markdown(f"""
        **当前服务账户**: `{current_email}`
        
        **当前项目ID**: `{current_project}`
        """)
        
        # 检查是否是新账户
        if "mendian@rugged-future-466116-g7" in current_email:
            st.success("✅ 配置已更新为新服务账户")
            is_new_account = True
        else:
            st.error("❌ 仍在使用旧服务账户！需要重新更新Secrets配置")
            is_new_account = False
            
            st.markdown("""
            ### 🔧 如何重新更新配置：
            1. 在Streamlit Cloud中打开应用管理页面
            2. 点击 ⚙️ 设置
            3. 找到 "Secrets" 部分
            4. 点击 "Edit Secrets"
            5. **完全清空**现有内容
            6. **重新粘贴**新的配置
            7. 点击 "Save"
            8. **等待应用重启**
            """)
            return
    else:
        st.error("❌ 未找到配置")
        return
    
    # 第2步：如果是新账户，检查权限
    if is_new_account:
        st.subheader("2️⃣ 检查新项目权限")
        
        project_links = f"""
        **请手动检查以下链接：**
        
        1. **Google Sheets API**: https://console.cloud.google.com/apis/library/sheets.googleapis.com?project={current_project}
        2. **Google Drive API**: https://console.cloud.google.com/apis/library/drive.googleapis.com?project={current_project}
        3. **IAM权限**: https://console.cloud.google.com/iam-admin/iam?project={current_project}
        
        确认：
        - ✅ 两个API都已启用
        - ✅ 服务账户有Editor权限
        """
        
        st.markdown(project_links)
        
        # 第3步：测试连接
        st.subheader("3️⃣ 测试新账户连接")
        
        if st.button("🔍 测试新账户连接"):
            try:
                with st.spinner("测试连接中..."):
                    # 创建客户端
                    scopes = [
                        "https://www.googleapis.com/auth/spreadsheets",
                        "https://www.googleapis.com/auth/drive"
                    ]
                    
                    credentials = Credentials.from_service_account_info(config, scopes=scopes)
                    client = gspread.authorize(credentials)
                    
                    st.success("✅ 认证成功")
                    
                    # 尝试创建测试文件
                    test_name = f"连接测试_{int(time.time())}"
                    
                    try:
                        test_sheet = client.create(test_name)
                        st.success(f"✅ 文件创建成功: {test_sheet.id}")
                        
                        # 立即删除测试文件
                        try:
                            client.del_spreadsheet(test_sheet.id)
                            st.success("✅ 测试文件已清理")
                        except:
                            st.warning("⚠️ 测试文件清理失败，但不影响功能")
                        
                        st.success("🎉 新账户工作正常！403错误应该已解决")
                        
                    except Exception as e:
                        st.error(f"❌ 文件创建失败: {str(e)}")
                        
                        if "403" in str(e):
                            if "quota" in str(e).lower() or "storage" in str(e).lower():
                                st.error("🚨 即使新账户也遇到存储配额问题！")
                                st.markdown("""
                                ### 🔧 可能的解决方案：
                                
                                1. **检查项目计费状态**:
                                   - 访问: https://console.cloud.google.com/billing/projects
                                   - 确认项目没有被暂停
                                
                                2. **创建另一个新项目**:
                                   - 这个项目可能有隐藏的限制
                                   - 建议创建全新的项目
                                
                                3. **启用计费**:
                                   - 可能需要添加付款方式
                                   - 即使是$0.01也能解决配额问题
                                """)
                            else:
                                st.error("权限问题，请检查API启用状态和IAM权限")
                        else:
                            st.error(f"其他错误: {str(e)}")
            
            except Exception as e:
                st.error(f"❌ 连接失败: {str(e)}")
                
                if "400" in str(e):
                    st.error("配置文件格式错误，请检查private_key格式")
                elif "403" in str(e):
                    st.error("权限问题，请检查API启用状态")
    
    # 第4步：紧急解决方案
    st.subheader("4️⃣ 紧急解决方案")
    
    emergency_solutions = """
    如果新账户仍有问题，立即解决方案：
    
    ### 方案A：再创建一个新项目 ⭐
    1. 访问: https://console.cloud.google.com/
    2. 创建项目名称: `门店系统备用` 
    3. 启用API并创建服务账户
    4. 更新配置
    
    ### 方案B：启用计费（推荐）
    1. 访问: https://console.cloud.google.com/billing/projects
    2. 为当前项目启用计费
    3. 添加信用卡（即使余额为$0也有效）
    4. 立即解决配额问题
    
    ### 方案C：使用免费替代方案
    1. 改用Airtable作为数据存储
    2. 或使用GitHub作为数据库
    3. 完全避开Google Cloud存储限制
    """
    
    st.markdown(emergency_solutions)
    
    # 第5步：配置验证
    st.subheader("5️⃣ 配置验证助手")
    
    if st.button("📋 生成新项目配置模板"):
        template = f"""
**如果需要创建新项目，使用以下模板：**

1. **项目名称建议**: 
   - `store-system-backup-{int(time.time())}`
   - `mendian-system-v2`
   - `rugged-future-backup`

2. **服务账户名称**:
   - `store-service`
   - `mendian-service`

3. **必须启用的API**:
   - Google Sheets API
   - Google Drive API

4. **IAM权限**:
   - 服务账户必须有 Editor 角色
"""
        st.markdown(template)

# 在你的主应用中添加这个诊断功能
if __name__ == "__main__":
    quick_diagnostic_and_fix()
