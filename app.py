import streamlit as st
import pandas as pd
import io

# 页面设置
st.set_page_config(page_title="门店报表查询系统", page_icon="📊")

# 主标题
st.title("📊 门店报表查询系统")

# 初始化状态
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
    st.session_state.store_name = ""

# 侧边栏文件上传
st.sidebar.title("📁 文件上传")

permissions_file = st.sidebar.file_uploader(
    "上传门店权限表", 
    type=['xlsx', 'xls']
)

reports_file = st.sidebar.file_uploader(
    "上传财务报表", 
    type=['xlsx', 'xls']
)

# 登出按钮
if st.session_state.logged_in:
    if st.sidebar.button("退出登录"):
        st.session_state.logged_in = False
        st.session_state.store_name = ""
        st.rerun()

# 主界面
if not st.session_state.logged_in:
    st.subheader("🔐 用户登录")
    
    if permissions_file:
        try:
            # 读取权限表
            permissions_df = pd.read_excel(permissions_file)
            
            # 调试信息
            st.write("**调试信息 - 权限表结构：**")
            st.write(f"列名: {list(permissions_df.columns)}")
            st.write("前几行数据:")
            st.dataframe(permissions_df.head())
            
            if len(permissions_df.columns) >= 2:
                store_column = permissions_df.columns[0]  # 门店名称列
                id_column = permissions_df.columns[1]     # 人员编号列
                
                # 获取所有门店名称（保持原始格式）
                stores = permissions_df[store_column].dropna().unique().tolist()
                st.write(f"**发现的门店:** {stores}")
                
                selected_store = st.selectbox("选择门店", stores)
                user_id = st.text_input("输入人员编号")
                
                if st.button("登录"):
                    if selected_store and user_id:
                        # 将用户输入的ID转换为数字进行比较
                        try:
                            user_id_num = int(user_id)
                        except:
                            user_id_num = user_id
                        
                        # 检查权限 - 支持字符串和数字比较
                        user_check = permissions_df[
                            (permissions_df[store_column] == selected_store) & 
                            ((permissions_df[id_column] == user_id) | 
                             (permissions_df[id_column] == user_id_num))
                        ]
                        
                        st.write(f"**查找条件:** 门店='{selected_store}', 编号='{user_id}'")
                        st.write(f"**匹配结果:** {len(user_check)} 条记录")
                        
                        if len(user_check) > 0:
                            st.session_state.logged_in = True
                            st.session_state.store_name = selected_store
                            st.success("登录成功！")
                            st.rerun()
                        else:
                            st.error("门店或人员编号错误！")
                            st.write("**权限表中的所有数据：**")
                            st.dataframe(permissions_df)
                    else:
                        st.warning("请填写完整信息")
            else:
                st.error("权限表格式错误：至少需要2列")
                
        except Exception as e:
            st.error(f"读取权限表出错：{e}")
            st.write("请检查文件格式是否正确")
    else:
        st.info("请先上传门店权限表")

else:
    # 已登录状态
    st.success(f"✅ 当前门店：{st.session_state.store_name}")
    
    if reports_file:
        try:
            # 读取报表
            excel_file = pd.ExcelFile(reports_file)
            
            st.write(f"**报表文件包含的Sheet:** {excel_file.sheet_names}")
            
            # 找到对应门店的sheet - 更灵活的匹配
            target_sheet = None
            store_key = st.session_state.store_name
            
            # 尝试几种匹配方式
            for sheet in excel_file.sheet_names:
                # 完全匹配
                if store_key in sheet:
                    target_sheet = sheet
                    break
                # 去掉括号后匹配
                if "（" in store_key:
                    store_short = store_key.split("（")[0]
                    if store_short in sheet:
                        target_sheet = sheet
                        break
            
            st.write(f"**匹配的Sheet:** {target_sheet}")
            
            if target_sheet:
                df = pd.read_excel(reports_file, sheet_name=target_sheet)
                
                st.subheader(f"📊 {st.session_state.store_name} 财务报表")
                st.dataframe(df, use_container_width=True)
                
                # 下载按钮
                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False)
                
                st.download_button(
                    label="📥 下载报表",
                    data=buffer.getvalue(),
                    file_name=f"{st.session_state.store_name}_报表.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            else:
                st.error(f"❌ 未找到门店 '{st.session_state.store_name}' 的报表")
                st.info("💡 请检查Excel文件中的Sheet名称是否包含门店名称")
                
        except Exception as e:
            st.error(f"读取报表文件出错：{e}")
    else:
        st.info("请上传财务报表文件")

# 使用说明
st.markdown("---")
st.markdown("""
### 📋 使用说明
1. **上传权限表：** Excel文件，第一列为门店名称，第二列为人员编号
2. **上传报表文件：** Excel文件，每个门店一个Sheet页
3. **选择门店并输入编号进行登录**
4. **查看和下载对应的财务报表**
""")
