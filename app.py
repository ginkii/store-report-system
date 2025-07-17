import streamlit as st
import pandas as pd
from datetime import datetime
import hashlib
import os
from typing import Optional

# 导入自定义模块
from config import APP_CONFIG, STREAMLIT_CONFIG, ADMIN_PASSWORD, validate_config
from json_handler import JSONHandler
from cos_handler import COSHandler
from excel_parser import ExcelParser
from query_handler import QueryHandler

# 页面配置
st.set_page_config(
    page_title=STREAMLIT_CONFIG['page_title'],
    page_icon=STREAMLIT_CONFIG['page_icon'],
    layout=STREAMLIT_CONFIG['layout'],
    initial_sidebar_state=STREAMLIT_CONFIG['initial_sidebar_state']
)

class ReportQueryApp:
    def __init__(self):
        self.json_handler = JSONHandler()
        self.cos_handler = COSHandler()
        self.excel_parser = ExcelParser()
        self.query_handler = QueryHandler()
        
        # 初始化session state
        if 'admin_logged_in' not in st.session_state:
            st.session_state.admin_logged_in = False
        if 'selected_store' not in st.session_state:
            st.session_state.selected_store = None
        if 'search_history' not in st.session_state:
            st.session_state.search_history = []
    
    def check_admin_password(self, password: str) -> bool:
        """验证管理员密码"""
        return password == ADMIN_PASSWORD
    
    def admin_login(self):
        """管理员登录界面"""
        st.subheader("🔐 管理员登录")
        
        with st.form("admin_login_form"):
            password = st.text_input("请输入管理员密码", type="password")
            submitted = st.form_submit_button("登录")
            
            if submitted:
                if self.check_admin_password(password):
                    st.session_state.admin_logged_in = True
                    st.success("登录成功！")
                    st.rerun()
                else:
                    st.error("密码错误！")
    
    def admin_logout(self):
        """管理员登出"""
        if st.button("退出登录"):
            st.session_state.admin_logged_in = False
            st.rerun()
    
    def admin_panel(self):
        """管理员面板"""
        st.title("📊 管理员面板")
        
        # 导航标签
        tab1, tab2, tab3, tab4 = st.tabs(["📤 上传报表", "📋 报表管理", "📊 系统统计", "⚙️ 系统设置"])
        
        with tab1:
            self.admin_upload_report()
        
        with tab2:
            self.admin_manage_reports()
        
        with tab3:
            self.admin_system_stats()
        
        with tab4:
            self.admin_system_settings()
    
    def admin_upload_report(self):
        """管理员上传报表"""
        st.subheader("📤 上传汇总报表")
        
        # 文件上传
        uploaded_file = st.file_uploader(
            "选择汇总报表文件",
            type=['xlsx', 'xls'],
            help="请选择包含各门店数据的Excel汇总报表"
        )
        
        if uploaded_file is not None:
            # 显示文件信息
            st.info(f"文件名: {uploaded_file.name}")
            st.info(f"文件大小: {uploaded_file.size / 1024 / 1024:.2f} MB")
            
            # 文件大小检查
            if uploaded_file.size > APP_CONFIG['max_file_size']:
                st.error(f"文件大小超过限制 ({APP_CONFIG['max_file_size'] / 1024 / 1024:.0f}MB)")
                return
            
            # 读取文件内容
            file_content = uploaded_file.read()
            
            # 验证文件格式
            if not self.excel_parser.validate_excel_file(file_content):
                st.error("文件格式无效，请检查文件是否为有效的Excel文件")
                return
            
            # 获取文件统计信息
            stats = self.excel_parser.get_file_statistics(file_content)
            
            # 显示文件统计
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("工作表数量", stats['total_sheets'])
            with col2:
                st.metric("文件大小", f"{stats['file_size'] / 1024 / 1024:.2f} MB")
            with col3:
                st.metric("门店数量", len([s for s in stats['sheets_info'] if s['has_data']]))
            
            # 显示门店列表
            if stats['sheet_names']:
                st.subheader("检测到的门店列表")
                
                # 创建门店信息DataFrame
                store_df = pd.DataFrame(stats['sheets_info'])
                if not store_df.empty:
                    store_df = store_df.rename(columns={
                        'name': '门店名称',
                        'rows': '行数',
                        'columns': '列数',
                        'has_data': '有数据'
                    })
                    st.dataframe(store_df, use_container_width=True)
                
                # 上传配置
                st.subheader("上传配置")
                
                description = st.text_area(
                    "报表描述",
                    value=f"{datetime.now().strftime('%Y年%m月')}门店汇总报表",
                    help="请输入对此报表的描述"
                )
                
                if st.button("确认上传", type="primary"):
                    with st.spinner("正在上传文件到腾讯云..."):
                        # 上传文件到COS
                        cos_path = self.cos_handler.upload_file(
                            file_content,
                            uploaded_file.name,
                            APP_CONFIG['upload_folder']
                        )
                        
                        if cos_path:
                            # 更新JSON数据
                            report_info = {
                                'id': datetime.now().strftime('%Y%m%d_%H%M%S'),
                                'file_name': uploaded_file.name,
                                'cos_file_path': cos_path,
                                'description': description,
                                'file_size': uploaded_file.size,
                                'version': '1.0'
                            }
                            
                            if self.json_handler.update_current_report(report_info, stats['sheet_names']):
                                st.success("报表上传成功！")
                                st.success(f"共检测到 {len(stats['sheet_names'])} 个门店")
                                st.balloons()
                            else:
                                st.error("更新报表信息失败")
                        else:
                            st.error("文件上传失败")
    
    def admin_manage_reports(self):
        """管理员报表管理"""
        st.subheader("📋 报表管理")
        
        # 当前报表信息
        current_report = self.json_handler.get_current_report()
        
        if current_report:
            st.subheader("当前活跃报表")
            
            col1, col2 = st.columns(2)
            with col1:
                st.info(f"**文件名**: {current_report['file_name']}")
                st.info(f"**上传时间**: {current_report['upload_time']}")
                st.info(f"**描述**: {current_report['description']}")
            
            with col2:
                st.info(f"**文件大小**: {current_report.get('file_size', 0) / 1024 / 1024:.2f} MB")
                st.info(f"**版本**: {current_report.get('version', 'N/A')}")
                st.info(f"**存储路径**: {current_report['cos_file_path']}")
            
            # 门店列表
            store_sheets = self.json_handler.get_store_sheets()
            if store_sheets:
                st.subheader("门店列表")
                
                # 创建门店DataFrame
                store_data = []
                for store in store_sheets:
                    store_data.append({
                        '门店名称': store['sheet_name'],
                        '查询次数': store.get('query_count', 0),
                        '最后查询': store.get('last_query_time', '从未查询')
                    })
                
                df = pd.DataFrame(store_data)
                st.dataframe(df, use_container_width=True)
        else:
            st.warning("暂无活跃报表，请先上传报表文件")
        
        # 历史报表
        st.subheader("历史报表")
        report_history = self.json_handler.get_report_history()
        
        if report_history:
            history_data = []
            for report in report_history:
                history_data.append({
                    '文件名': report['file_name'],
                    '上传时间': report['upload_time'],
                    '归档时间': report.get('archived_time', 'N/A'),
                    '描述': report['description'],
                    '状态': report['status']
                })
            
            df = pd.DataFrame(history_data)
            st.dataframe(df, use_container_width=True)
        else:
            st.info("暂无历史报表")
    
    def admin_system_stats(self):
        """管理员系统统计"""
        st.subheader("📊 系统统计")
        
        # 获取系统状态
        status = self.query_handler.get_system_status()
        
        # 基础统计
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("门店数量", status['stores_count'])
        with col2:
            st.metric("总查询次数", status['total_queries'])
        with col3:
            st.metric("历史报表数", status['history_count'])
        with col4:
            st.metric("系统状态", "正常" if status['cos_connection'] else "异常")
        
        # 系统状态详情
        st.subheader("系统状态详情")
        
        col1, col2 = st.columns(2)
        with col1:
            st.success("✅ COS连接正常") if status['cos_connection'] else st.error("❌ COS连接异常")
            st.success("✅ 报表文件可访问") if status['file_accessible'] else st.error("❌ 报表文件不可访问")
        
        with col2:
            st.info(f"**最后更新时间**: {status['last_updated'] or '无'}")
            st.info(f"**系统时间**: {status['system_time']}")
        
        # 查询历史
        st.subheader("最近查询记录")
        query_history = self.query_handler.get_query_history(20)
        
        if query_history:
            df = pd.DataFrame(query_history)
            df = df.rename(columns={
                'store_name': '门店名称',
                'query_count': '查询次数',
                'last_query_time': '最后查询时间'
            })
            st.dataframe(df, use_container_width=True)
        else:
            st.info("暂无查询记录")
    
    def admin_system_settings(self):
        """管理员系统设置"""
        st.subheader("⚙️ 系统设置")
        
        # 配置验证
        st.subheader("配置验证")
        
        if validate_config():
            st.success("✅ 配置验证通过")
        else:
            st.error("❌ 配置验证失败，请检查腾讯云COS配置")
        
        # 连接测试
        st.subheader("连接测试")
        
        if st.button("测试COS连接"):
            with st.spinner("正在测试连接..."):
                if self.cos_handler.test_connection():
                    st.success("✅ COS连接测试成功")
                else:
                    st.error("❌ COS连接测试失败")
        
        # 数据管理
        st.subheader("数据管理")
        
        st.warning("⚠️ 以下操作会影响系统数据，请谨慎操作！")
        
        if st.button("清空查询统计", type="secondary"):
            # 这里可以添加清空统计的逻辑
            st.info("功能待实现")
    
    def user_query_interface(self):
        """用户查询界面"""
        st.title("🔍 门店报表查询")
        
        # 获取可用门店
        available_stores = self.query_handler.get_available_stores()
        
        if not available_stores:
            st.error("暂无可用门店数据，请联系管理员上传报表")
            return
        
        # 门店选择
        st.subheader("第一步：选择门店")
        
        selected_store = st.selectbox(
            "请选择要查询的门店",
            options=available_stores,
            index=0,
            help="选择您要查询的门店"
        )
        
        if selected_store:
            st.session_state.selected_store = selected_store
            st.success(f"已选择门店: {selected_store}")
            
            # 门店预览
            with st.expander("🔍 查看门店数据预览"):
                if st.button("加载预览"):
                    with st.spinner("正在加载预览..."):
                        preview_data = self.query_handler.get_store_preview(selected_store, 5)
                        
                        if preview_data:
                            st.info(f"总行数: {preview_data['total_rows']}, 总列数: {preview_data['total_columns']}")
                            
                            # 显示预览数据
                            if preview_data['preview_data']:
                                df = pd.DataFrame(preview_data['preview_data'])
                                st.dataframe(df, use_container_width=True)
                            else:
                                st.warning("该门店暂无数据")
            
            # 编码查询
            st.subheader("第二步：输入查询编码")
            
            col1, col2 = st.columns([3, 1])
            
            with col1:
                search_code = st.text_input(
                    "请输入查询编码",
                    placeholder="输入要查询的编码...",
                    help="支持数字、字母的混合编码"
                )
            
            with col2:
                fuzzy_match = st.checkbox("模糊匹配", value=True, help="启用模糊匹配可以找到包含查询编码的所有结果")
            
            # 查询按钮
            if st.button("🔍 开始查询", type="primary", disabled=not search_code):
                if not self.query_handler.validate_search_code(search_code):
                    st.error("请输入有效的查询编码")
                    return
                
                # 执行查询
                search_results = self.query_handler.search_code_in_store(
                    selected_store, search_code, fuzzy_match
                )
                
                if search_results:
                    # 保存到session state
                    st.session_state.search_results = search_results
                    
                    # 显示搜索结果
                    self.display_search_results(search_results)
                else:
                    st.info("未找到匹配的结果")
    
    def display_search_results(self, search_results):
        """显示搜索结果"""
        st.subheader("🎯 查询结果")
        
        # 结果汇总
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("匹配数量", search_results['match_count'])
        with col2:
            st.metric("门店", search_results['sheet_name'])
        with col3:
            st.metric("搜索编码", search_results['search_code'])
        
        # 匹配结果详情
        if search_results['matches']:
            st.subheader("匹配详情")
            
            for i, match in enumerate(search_results['matches']):
                with st.expander(f"匹配项 {i+1} - 第{match['row_index']+1}行，{match['column']}列"):
                    st.write(f"**匹配值**: {match['matched_value']}")
                    
                    # 显示行数据
                    row_data = match['row_data']
                    if row_data:
                        # 转换为DataFrame显示
                        df = pd.DataFrame([row_data])
                        st.dataframe(df, use_container_width=True)
        
        # 导出功能
        st.subheader("📥 导出结果")
        
        if st.button("导出为Excel"):
            with st.spinner("正在生成Excel文件..."):
                excel_content = self.query_handler.export_search_results(search_results)
                
                if excel_content:
                    # 生成文件名
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    filename = f"查询结果_{search_results['sheet_name']}_{search_results['search_code']}_{timestamp}.xlsx"
                    
                    st.download_button(
                        label="下载Excel文件",
                        data=excel_content,
                        file_name=filename,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                else:
                    st.error("生成Excel文件失败")
    
    def sidebar_info(self):
        """侧边栏信息"""
        st.sidebar.title("📊 系统信息")
        
        # 系统状态
        status = self.query_handler.get_system_status()
        
        st.sidebar.metric("可用门店", status['stores_count'])
        st.sidebar.metric("总查询次数", status['total_queries'])
        
        # 当前报表信息
        if status['current_report']:
            st.sidebar.subheader("当前报表")
            st.sidebar.info(f"文件: {status['current_report']['file_name']}")
            st.sidebar.info(f"更新: {status['last_updated'] or '未知'}")
        
        # 最近查询
        st.sidebar.subheader("最近查询")
        query_history = self.query_handler.get_query_history(5)
        
        if query_history:
            for record in query_history:
                st.sidebar.text(f"📍 {record['store_name']}")
                st.sidebar.text(f"   查询: {record['query_count']}次")
        else:
            st.sidebar.info("暂无查询记录")
    
    def run(self):
        """运行应用"""
        # 验证配置
        if not validate_config():
            st.error("系统配置不完整，请联系管理员")
            return
        
        # 侧边栏信息
        self.sidebar_info()
        
        # 管理员登录状态检查
        if st.session_state.admin_logged_in:
            # 管理员界面
            col1, col2 = st.columns([6, 1])
            with col1:
                st.success(f"👋 欢迎，管理员！")
            with col2:
                self.admin_logout()
            
            self.admin_panel()
        else:
            # 用户界面
            tab1, tab2 = st.tabs(["🔍 门店查询", "🔐 管理员登录"])
            
            with tab1:
                self.user_query_interface()
            
            with tab2:
                self.admin_login()

def main():
    """主函数"""
    app = ReportQueryApp()
    app.run()

if __name__ == "__main__":
    main()
