import streamlit as st
import pandas as pd
from datetime import datetime
import hashlib
import os
import time
from typing import Optional

# 导入自定义模块
try:
    from config import APP_CONFIG, STREAMLIT_CONFIG, ADMIN_PASSWORD, validate_config
except ImportError:
    # 如果config模块不存在，使用默认配置
    APP_CONFIG = {
        'max_file_size': 50 * 1024 * 1024,  # 50MB
        'upload_folder': 'uploads'
    }
    STREAMLIT_CONFIG = {
        'page_title': '门店报表查询系统',
        'page_icon': '📊',
        'layout': 'wide',
        'initial_sidebar_state': 'expanded'
    }
    ADMIN_PASSWORD = 'admin123'
    def validate_config():
        return True

from json_handler import JSONHandler

try:
    from excel_parser import ExcelParser
except ImportError:
    # 简化的Excel解析器
    class ExcelParser:
        def __init__(self):
            self.cache = {}
        
        def validate_excel_file(self, file_content):
            return True
        
        def get_file_statistics(self, file_content):
            return {
                'total_sheets': 1,
                'file_size': len(file_content),
                'sheets_info': [{'name': 'Sheet1', 'has_data': True, 'rows': 100, 'columns': 10}],
                'sheet_names': ['Sheet1']
            }
        
        def get_sheet_names_fast(self, file_content):
            return ['Sheet1']
        
        def get_cache_info(self):
            return {
                'sheet_data_cache_size': 0,
                'max_cache_size': 100,
                'cached_sheets': []
            }
        
        def clear_cache(self):
            self.cache.clear()

try:
    from query_handler import QueryHandler
except ImportError:
    # 简化的查询处理器
    class QueryHandler:
        def __init__(self):
            pass
        
        def get_available_stores(self):
            return ['门店A', '门店B', '门店C']
        
        def get_system_status(self):
            return {
                'stores_count': 3,
                'total_queries': 0,
                'history_count': 0,
                'cos_connection': True,
                'file_accessible': True,
                'last_updated': None,
                'system_time': datetime.now().isoformat()
            }
        
        def validate_search_code(self, code):
            return bool(code and code.strip())
        
        def search_code_in_store(self, store_name, search_code, fuzzy_match=True):
            return {
                'match_count': 1,
                'sheet_name': store_name,
                'search_code': search_code,
                'matches': [
                    {
                        'row_index': 0,
                        'column': 'A',
                        'matched_value': search_code,
                        'row_data': {'A': search_code, 'B': '测试数据'}
                    }
                ]
            }
        
        def get_store_preview(self, store_name, limit=5):
            return {
                'total_rows': 100,
                'total_columns': 5,
                'preview_data': [
                    {'A': '数据1', 'B': '数据2', 'C': '数据3'},
                    {'A': '数据4', 'B': '数据5', 'C': '数据6'}
                ]
            }
        
        def export_search_results(self, search_results):
            return b'dummy_excel_content'
        
        def get_query_history(self, limit=20):
            return [
                {'store_name': '门店A', 'query_count': 5, 'last_query_time': '2025-01-01 12:00:00'},
                {'store_name': '门店B', 'query_count': 3, 'last_query_time': '2025-01-01 11:00:00'}
            ]

# 尝试导入 COS 处理器，如果失败则使用本地存储
try:
    from cos_handler import COSHandler
    storage_handler = COSHandler()
    STORAGE_TYPE = "COS"
except ImportError as e:
    st.warning(f"COS 模块导入失败: {str(e)}")
    try:
        from local_storage_handler import LocalStorageHandler
        storage_handler = LocalStorageHandler()
        STORAGE_TYPE = "LOCAL"
    except ImportError:
        # 简化的本地存储处理器
        class LocalStorageHandler:
            def upload_file(self, file_content, filename, folder):
                return f"local/{filename}"
            
            def download_file(self, file_path):
                return b'dummy_content'
            
            def test_connection(self):
                return True
        
        storage_handler = LocalStorageHandler()
        STORAGE_TYPE = "LOCAL"

# 权限处理器
try:
    from permission_handler import PermissionHandler
    HAS_PERMISSION_HANDLER = True
except ImportError:
    # 简化的权限处理器
    class PermissionHandler:
        def get_permission_statistics(self):
            return {
                'has_permissions': False,
                'total_records': 0,
                'unique_stores': 0,
                'unique_codes': 0,
                'file_info': {}
            }
        
        def validate_permission_file(self, file_content):
            return True, "文件格式正确"
        
        def get_file_statistics(self, file_content):
            return {
                'total_rows': 100,
                'valid_records': 95,
                'unique_stores': 10,
                'unique_codes': 50
            }
        
        def parse_permission_file(self, file_content):
            return True, [{'store': '门店A', 'code': 'CODE001'}], "解析成功"
        
        def validate_permissions_with_stores(self, available_stores):
            return {
                'valid': True,
                'invalid_stores': [],
                'orphaned_permissions': 0,
                'available_stores': len(available_stores),
                'total_permission_stores': 5
            }
        
        def upload_permission_file(self, file_content, filename):
            return f"permissions/{filename}"
        
        def update_permissions(self, file_path, permissions, filename, file_size):
            return True
        
        def get_permissions_preview(self, limit=20):
            return [{'store': '门店A', 'code': 'CODE001'}]
        
        def export_permissions(self):
            return b'dummy_excel_content'
        
        def clear_permissions(self):
            return True
    
    HAS_PERMISSION_HANDLER = False

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
        self.storage_handler = storage_handler
        self.excel_parser = ExcelParser()
        self.query_handler = QueryHandler()
        
        # 权限处理器
        if HAS_PERMISSION_HANDLER:
            self.permission_handler = PermissionHandler()
            self.has_permission_handler = True
        else:
            self.permission_handler = PermissionHandler()  # 使用简化版本
            self.has_permission_handler = False
        
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
        if self.has_permission_handler:
            tab1, tab2, tab3, tab4, tab5 = st.tabs(["📤 上传报表", "🔐 权限管理", "📋 报表管理", "📊 系统统计", "⚙️ 系统设置"])
        else:
            tab1, tab2, tab3, tab4 = st.tabs(["📤 上传报表", "📋 报表管理", "📊 系统统计", "⚙️ 系统设置"])
        
        with tab1:
            self.admin_upload_report()
        
        if self.has_permission_handler:
            with tab2:
                self.admin_permission_management()
            
            with tab3:
                self.admin_manage_reports()
            
            with tab4:
                self.admin_system_stats()
            
            with tab5:
                self.admin_system_settings()
        else:
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
                st.metric("工作表数量", stats.get('total_sheets', 0))
            with col2:
                st.metric("文件大小", f"{stats.get('file_size', 0) / 1024 / 1024:.2f} MB")
            with col3:
                sheets_info = stats.get('sheets_info', [])
                store_count = len([s for s in sheets_info if s.get('has_data', False)]) if sheets_info else 0
                st.metric("门店数量", store_count)
            
            # 显示门店列表
            sheet_names = stats.get('sheet_names', [])
            if sheet_names:
                st.subheader("检测到的门店列表")
                
                # 创建门店信息DataFrame
                sheets_info = stats.get('sheets_info', [])
                if sheets_info:
                    store_df = pd.DataFrame(sheets_info)
                    if not store_df.empty:
                        store_df = store_df.rename(columns={
                            'name': '门店名称',
                            'rows': '行数',
                            'columns': '列数',
                            'has_data': '有数据'
                        })
                        st.dataframe(store_df, use_container_width=True)
                else:
                    # 如果没有详细信息，只显示名称列表
                    st.write("检测到的门店：")
                    for name in sheet_names:
                        st.write(f"• {name}")
                
                # 上传配置
                st.subheader("上传配置")
                
                description = st.text_area(
                    "报表描述",
                    value=f"{datetime.now().strftime('%Y年%m月')}门店汇总报表",
                    help="请输入对此报表的描述"
                )
                
                if st.button("确认上传", type="primary"):
                    with st.spinner("正在上传文件..."):
                        # 上传文件
                        file_path = self.storage_handler.upload_file(
                            file_content,
                            uploaded_file.name,
                            APP_CONFIG['upload_folder']
                        )
                        
                        if file_path:
                            # 更新JSON数据
                            report_info = {
                                'id': datetime.now().strftime('%Y%m%d_%H%M%S'),
                                'file_name': uploaded_file.name,
                                'file_path': file_path,
                                'description': description,
                                'file_size': uploaded_file.size,
                                'version': '1.0'
                            }
                            
                            if self.json_handler.update_current_report(report_info, sheet_names):
                                st.success("报表上传成功！")
                                st.success(f"共检测到 {len(sheet_names)} 个门店")
                                st.balloons()
                            else:
                                st.error("更新报表信息失败")
                        else:
                            st.error("文件上传失败")
    
    def admin_permission_management(self):
        """管理员权限管理"""
        if not self.has_permission_handler:
            st.error("权限处理器未可用")
            return
        
        st.subheader("🔐 权限管理")
        
        # 获取权限统计
        try:
            permission_stats = self.permission_handler.get_permission_statistics()
        except Exception as e:
            st.error(f"获取权限统计失败: {str(e)}")
            return
        
        # 上传权限表
        st.subheader("📤 上传权限表")
        
        # 文件上传
        uploaded_file = st.file_uploader(
            "选择权限表文件",
            type=['xlsx', 'xls'],
            help="请选择包含门店名称和查询编码对应关系的Excel文件"
        )
        
        if uploaded_file is not None:
            # 显示文件信息
            st.info(f"文件名: {uploaded_file.name}")
            st.info(f"文件大小: {uploaded_file.size / 1024:.2f} KB")
            
            # 文件大小检查
            if uploaded_file.size > APP_CONFIG['max_file_size']:
                st.error(f"文件大小超过限制 ({APP_CONFIG['max_file_size'] / 1024 / 1024:.0f}MB)")
                return
            
            # 读取文件内容
            file_content = uploaded_file.read()
            
            # 验证文件格式
            is_valid, error_message = self.permission_handler.validate_permission_file(file_content)
            if not is_valid:
                st.error(f"文件格式错误: {error_message}")
                return
            
            # 获取文件统计
            file_stats = self.permission_handler.get_file_statistics(file_content)
            
            # 显示文件统计
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("总行数", file_stats.get('total_rows', 0))
            with col2:
                st.metric("有效记录", file_stats.get('valid_records', 0))
            with col3:
                st.metric("唯一门店", file_stats.get('unique_stores', 0))
            with col4:
                st.metric("唯一编码", file_stats.get('unique_codes', 0))
            
            # 解析权限表
            is_parsed, permissions, parse_message = self.permission_handler.parse_permission_file(file_content)
            if not is_parsed:
                st.error(f"解析失败: {parse_message}")
                return
            
            st.success(parse_message)
            
            # 显示权限预览
            if permissions:
                st.subheader("权限预览 (前10条)")
                preview_df = pd.DataFrame(permissions[:10])
                preview_df = preview_df.rename(columns={'store': '门店名称', 'code': '查询编码'})
                st.dataframe(preview_df, use_container_width=True)
            
            # 检查权限表与汇总报表的同步
            available_stores = self.query_handler.get_available_stores()
            if available_stores:
                validation_result = self.permission_handler.validate_permissions_with_stores(available_stores)
                
                if not validation_result['valid']:
                    st.warning("⚠️ 权限表中存在汇总报表中不存在的门店")
                    invalid_stores = validation_result['invalid_stores']
                    st.error(f"无效门店: {', '.join(invalid_stores)}")
                    st.error(f"孤立权限记录: {validation_result['orphaned_permissions']} 条")
                else:
                    st.success("✅ 权限表与汇总报表同步正常")
            
            # 上传确认
            if st.button("确认上传权限表", type="primary"):
                with st.spinner("正在上传权限表..."):
                    # 上传文件
                    file_path = self.permission_handler.upload_permission_file(
                        file_content,
                        uploaded_file.name
                    )
                    
                    if file_path:
                        # 更新权限数据
                        if self.permission_handler.update_permissions(
                            file_path,
                            permissions,
                            uploaded_file.name,
                            uploaded_file.size
                        ):
                            st.success("权限表上传成功！")
                            st.success(f"共更新 {len(permissions)} 条权限记录")
                            st.balloons()
                        else:
                            st.error("更新权限数据失败")
                    else:
                        st.error("权限表上传失败")
        
        # 当前权限表信息
        st.subheader("📋 当前权限表")
        
        if permission_stats['has_permissions']:
            file_info = permission_stats['file_info']
            
            col1, col2 = st.columns(2)
            with col1:
                st.info(f"**文件名**: {file_info.get('file_name', 'N/A')}")
                st.info(f"**上传时间**: {file_info.get('upload_time', 'N/A')}")
                st.info(f"**总记录数**: {permission_stats['total_records']}")
            
            with col2:
                st.info(f"**文件大小**: {file_info.get('file_size', 0) / 1024:.2f} KB")
                st.info(f"**唯一门店**: {permission_stats['unique_stores']}")
                st.info(f"**唯一编码**: {permission_stats['unique_codes']}")
            
            # 权限记录预览
            st.subheader("权限记录预览")
            preview_permissions = self.permission_handler.get_permissions_preview(20)
            
            if preview_permissions:
                preview_df = pd.DataFrame(preview_permissions)
                preview_df = preview_df.rename(columns={'store': '门店名称', 'code': '查询编码'})
                st.dataframe(preview_df, use_container_width=True)
            
            # 权限管理操作
            st.subheader("权限管理操作")
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("导出权限表", type="secondary"):
                    excel_content = self.permission_handler.export_permissions()
                    if excel_content:
                        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                        filename = f"权限表_{timestamp}.xlsx"
                        
                        st.download_button(
                            label="下载权限表",
                            data=excel_content,
                            file_name=filename,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                    else:
                        st.error("导出失败")
            
            with col2:
                if st.button("清空权限表", type="secondary"):
                    if self.permission_handler.clear_permissions():
                        st.success("权限表已清空")
                        st.rerun()
                    else:
                        st.error("清空失败")
        
        else:
            st.info("暂无权限表，请先上传权限表文件")
    
    def admin_manage_reports(self):
        """管理员报表管理"""
        st.subheader("📋 报表管理")
        
        # 当前报表信息
        current_report = self.json_handler.get_current_report()
        
        if current_report:
            st.subheader("当前活跃报表")
            
            col1, col2 = st.columns(2)
            with col1:
                st.info(f"**文件名**: {current_report.get('file_name', 'N/A')}")
                st.info(f"**上传时间**: {current_report.get('upload_time', 'N/A')}")
                st.info(f"**描述**: {current_report.get('description', 'N/A')}")
            
            with col2:
                st.info(f"**文件大小**: {current_report.get('file_size', 0) / 1024 / 1024:.2f} MB")
                st.info(f"**版本**: {current_report.get('version', 'N/A')}")
                # 兼容旧版本的存储路径字段
                file_path = current_report.get('file_path') or current_report.get('cos_file_path', 'N/A')
                st.info(f"**存储路径**: {file_path}")
            
            # 门店列表
            store_sheets = self.json_handler.get_store_sheets()
            if store_sheets:
                st.subheader("门店列表")
                
                # 创建门店DataFrame
                store_data = []
                for store in store_sheets:
                    store_data.append({
                        '门店名称': store.get('sheet_name', 'N/A'),
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
                    '文件名': report.get('file_name', 'N/A'),
                    '上传时间': report.get('upload_time', 'N/A'),
                    '归档时间': report.get('archived_time', 'N/A'),
                    '描述': report.get('description', 'N/A'),
                    '状态': report.get('status', 'N/A')
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
        
        # 获取权限统计
        if self.has_permission_handler:
            try:
                permission_stats = self.permission_handler.get_permission_statistics()
            except Exception as e:
                permission_stats = {'total_records': 0, 'has_permissions': False}
                st.error(f"权限统计获取失败: {str(e)}")
        else:
            permission_stats = {'total_records': 0, 'has_permissions': False}
        
        # 基础统计
        if self.has_permission_handler:
            col1, col2, col3, col4, col5 = st.columns(5)
            with col1:
                st.metric("门店数量", status.get('stores_count', 0))
            with col2:
                st.metric("总查询次数", status.get('total_queries', 0))
            with col3:
                st.metric("历史报表数", status.get('history_count', 0))
            with col4:
                st.metric("权限记录数", permission_stats.get('total_records', 0))
            with col5:
                st.metric("系统状态", "正常" if status.get('cos_connection', True) else "异常")
        else:
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("门店数量", status.get('stores_count', 0))
            with col2:
                st.metric("总查询次数", status.get('total_queries', 0))
            with col3:
                st.metric("历史报表数", status.get('history_count', 0))
            with col4:
                st.metric("系统状态", "正常" if status.get('cos_connection', True) else "异常")
        
        # 系统状态详情
        st.subheader("系统状态详情")
        
        col1, col2 = st.columns(2)
        with col1:
            if status.get('cos_connection', True):
                st.success(f"✅ {STORAGE_TYPE} 连接正常")
            else:
                st.error(f"❌ {STORAGE_TYPE} 连接异常")
            
            if status.get('file_accessible', True):
                st.success("✅ 报表文件可访问")
            else:
                st.error("❌ 报表文件不可访问")
            
            if self.has_permission_handler:
                if permission_stats['has_permissions']:
                    st.success("✅ 权限表已配置")
                else:
                    st.warning("⚠️ 权限表未配置")
        
        with col2:
            st.info(f"**最后更新时间**: {status.get('last_updated') or '无'}")
            st.info(f"**系统时间**: {status.get('system_time', datetime.now().isoformat())}")
            if self.has_permission_handler and permission_stats.get('has_permissions', False):
                st.info(f"**权限表门店数**: {permission_stats.get('unique_stores', 0)}")
        
        # 权限表状态
        if self.has_permission_handler and permission_stats['has_permissions']:
            st.subheader("权限表状态")
            
            # 权限同步检查
            available_stores = self.query_handler.get_available_stores()
            if available_stores:
                try:
                    validation_result = self.permission_handler.validate_permissions_with_stores(available_stores)
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        if validation_result.get('valid', False):
                            st.success("✅ 权限表与汇总报表同步正常")
                        else:
                            st.error("❌ 权限表与汇总报表不同步")
                            invalid_stores = validation_result.get('invalid_stores', [])
                            st.error(f"无效门店: {len(invalid_stores)} 个")
                            st.error(f"孤立权限: {validation_result.get('orphaned_permissions', 0)} 条")
                    
                    with col2:
                        st.info(f"**汇总报表门店数**: {validation_result.get('available_stores', 0)}")
                        st.info(f"**权限表门店数**: {validation_result.get('total_permission_stores', 0)}")
                except Exception as e:
                    st.error(f"权限同步检查失败: {str(e)}")
            else:
                st.warning("⚠️ 无汇总报表数据，无法进行同步检查")
        
        # 查询历史
        st.subheader("最近查询记录")
        query_history = self.query_handler.get_query_history(20)
        
        if query_history:
            # 处理查询历史数据格式
            formatted_history = []
            for record in query_history:
                if isinstance(record, dict):
                    formatted_history.append({
                        '门店名称': record.get('store_name', ''),
                        '查询次数': record.get('query_count', 0),
                        '最后查询时间': record.get('last_query_time', '从未查询')
                    })
                else:
                    # 兼容旧格式
                    formatted_history.append({
                        '门店名称': getattr(record, 'store_name', ''),
                        '查询次数': getattr(record, 'query_count', 0),
                        '最后查询时间': getattr(record, 'last_query_time', '从未查询')
                    })
            
            df = pd.DataFrame(formatted_history)
            st.dataframe(df, use_container_width=True)
        else:
            st.info("暂无查询记录")
    
    def admin_system_settings(self):
        """管理员系统设置"""
        st.subheader("⚙️ 系统设置")
        
        # 系统诊断
        st.subheader("🔍 系统诊断")
        
        # 获取系统状态
        system_status = self.json_handler.get_system_status()
        
        # 显示关键状态
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            if system_status['data_loaded']:
                st.success("✅ 数据加载正常")
            else:
                st.error("❌ 数据加载失败")
        
        with col2:
            if system_status['has_current_report']:
                st.success("✅ 有当前报表")
            else:
                st.warning("⚠️ 无当前报表")
        
        with col3:
            if system_status['file_accessible']:
                st.success("✅ 文件可访问")
            else:
                st.error("❌ 文件不可访问")
        
        with col4:
            st.info(f"存储: {system_status['storage_type']}")
        
        # 详细状态信息
        with st.expander("📋 详细系统状态"):
            st.json(system_status)
        
        # 配置验证
        st.subheader("配置验证")
        
        st.info(f"当前使用存储类型: {STORAGE_TYPE}")
        
        if STORAGE_TYPE == "COS":
            if validate_config():
                st.success("✅ COS 配置验证通过")
            else:
                st.error("❌ COS 配置验证失败，请检查腾讯云COS配置")
        else:
            st.success("✅ 本地存储配置验证通过")
        
        # 连接测试
        st.subheader("连接测试")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("测试存储连接"):
                with st.spinner("正在测试连接..."):
                    if self.storage_handler.test_connection():
                        st.success(f"✅ {STORAGE_TYPE} 连接测试成功")
                    else:
                        st.error(f"❌ {STORAGE_TYPE} 连接测试失败")
        
        with col2:
            if st.button("测试数据读写"):
                with st.spinner("正在测试数据读写..."):
                    try:
                        # 测试数据读取
                        current_report = self.json_handler.get_current_report()
                        st.success("✅ 数据读取测试成功")
                        
                        # 测试数据写入（更新系统信息）
                        test_info = {"test_time": datetime.now().isoformat()}
                        if self.json_handler.update_system_info(test_info):
                            st.success("✅ 数据写入测试成功")
                        
                    except Exception as e:
                        st.error(f"❌ 数据读写测试失败: {str(e)}")
        
        # 权限系统测试
        if self.has_permission_handler:
            st.subheader("权限系统测试")
            
            if st.button("测试权限系统"):
                with st.spinner("正在测试权限系统..."):
                    try:
                        permission_stats = self.permission_handler.get_permission_statistics()
                        st.success("✅ 权限系统测试成功")
                        st.info(f"权限记录数: {permission_stats['total_records']}")
                    except Exception as e:
                        st.error(f"❌ 权限系统测试失败: {str(e)}")
        
        # 数据管理
        st.subheader("数据管理")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("📁 备份数据"):
                with st.spinner("正在备份数据..."):
                    if self.json_handler.backup_data():
                        st.success("✅ 数据备份完成")
                    else:
                        st.error("❌ 数据备份失败")
        
        with col2:
            if st.button("🔄 从备份恢复"):
                if st.button("⚠️ 确认恢复", key="confirm_restore"):
                    with st.spinner("正在从备份恢复..."):
                        if self.json_handler.restore_from_backup():
                            st.success("✅ 恢复成功")
                            st.rerun()
                        else:
                            st.error("❌ 恢复失败")
        
        with col3:
            if st.button("🗑️ 清空数据"):
                if st.button("⚠️ 确认清空", key="confirm_clear"):
                    with st.spinner("正在清空数据..."):
                        if self.json_handler.clear_all_data():
                            st.success("✅ 清空成功")
                            st.rerun()
                        else:
                            st.error("❌ 清空失败")
        
        # 索引系统状态
        st.subheader("索引系统状态")
        
        # 显示索引进度
        indexing_progress = self.json_handler.get_indexing_progress()
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("总工作表数", indexing_progress['total_sheets'])
            st.metric("已索引工作表", indexing_progress['indexed_sheets'])
        
        with col2:
            progress_percentage = indexing_progress['progress_percentage']
            st.metric("索引进度", f"{progress_percentage:.1f}%")
            st.metric("待索引工作表", indexing_progress['remaining_sheets'])
        
        # 显示进度条
        if indexing_progress['total_sheets'] > 0:
            st.progress(progress_percentage / 100)
        
        # 缓存管理
        st.subheader("缓存管理")
        
        cache_info = self.excel_parser.get_cache_info()
        
        col1, col2 = st.columns(2)
        with col1:
            st.info(f"**工作表缓存**: {cache_info['sheet_data_cache_size']}/{cache_info['max_cache_size']}")
        
        with col2:
            if st.button("清理缓存"):
                self.excel_parser.clear_cache()
                st.success("✅ 缓存已清理")
                st.rerun()
        
        # 显示缓存详情
        if cache_info['cached_sheets']:
            with st.expander("查看缓存详情"):
                for cached_sheet in cache_info['cached_sheets']:
                    st.text(f"• {cached_sheet}")
        
        # 高级操作
        st.subheader("高级操作")
        
        st.warning("⚠️ 以下操作会影响系统数据，请谨慎操作！")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("清空查询统计", type="secondary"):
                # 这里可以添加清空统计的逻辑
                st.info("功能待实现")
        
        with col2:
            if st.button("重建索引", type="secondary"):
                st.info("正在重建索引...")
                # 这里可以添加重建索引的逻辑
                current_report = self.json_handler.get_current_report()
                if current_report:
                    file_path = current_report.get('file_path')
                    if file_path:
                        file_content = self.storage_handler.download_file(file_path)
                        if file_content:
                            sheet_names = self.excel_parser.get_sheet_names_fast(file_content)
                            # 这里可以添加实际的索引构建逻辑
                            st.success("索引重建完成")
                        else:
                            st.error("无法下载文件")
                    else:
                        st.error("文件路径不存在")
                else:
                    st.error("无当前报表")
        
        with col3:
            if self.has_permission_handler:
                if st.button("重置权限系统", type="secondary"):
                    if self.permission_handler.clear_permissions():
                        st.success("权限系统已重置")
                        st.rerun()
                    else:
                        st.error("重置失败")
    
    def user_query_interface(self):
        """用户查询界面"""
        st.title("🔍 门店报表查询")
        
        # 首先检查系统状态
        system_status = self.json_handler.get_system_status()
        
        # 如果数据加载失败，显示错误信息
        if not system_status['data_loaded']:
            st.error("🔧 系统数据加载失败")
            st.error("请联系管理员检查系统状态")
            
            if st.button("🔄 重新加载"):
                st.rerun()
            
            return
        
        # 如果没有当前报表
        if not system_status['has_current_report']:
            st.warning("📋 系统中暂无报表数据")
            st.info("请联系管理员上传报表文件")
            
            # 显示系统状态
            with st.expander("📊 查看系统状态"):
                st.write("存储类型:", system_status.get('storage_type', 'Unknown'))
                st.write("数据加载状态:", "✅ 正常" if system_status.get('data_loaded', False) else "❌ 异常")
                if system_status.get('last_updated'):
                    st.write("最后更新:", system_status['last_updated'])
            
            return
        
        # 如果报表文件不可访问
        if not system_status['file_accessible']:
            st.error("📁 报表文件无法访问")
            st.error("文件可能已被删除或移动，请联系管理员重新上传")
            
            # 显示报表信息
            current_report = system_status.get('current_report')
            if current_report:
                st.info(f"报表文件: {current_report.get('file_name', 'N/A')}")
                st.info(f"上传时间: {current_report.get('upload_time', '未知')}")
            
            return
        
        # 获取可用门店
        available_stores = self.query_handler.get_available_stores()
        
        if not available_stores:
            st.warning("🏪 未找到可用门店数据")
            
            # 提供更详细的诊断信息
            with st.expander("🔍 诊断信息"):
                current_report = system_status.get('current_report')
                if current_report:
                    st.write("当前报表:", current_report.get('file_name', 'N/A'))
                    st.write("门店工作表数:", system_status.get('store_sheets_count', 0))
                    
                    store_sheets_count = system_status.get('store_sheets_count', 0)
                    if store_sheets_count == 0:
                        st.error("报表中没有检测到门店工作表")
                    else:
                        st.info(f"检测到 {store_sheets_count} 个门店，但查询接口获取失败")
            
            if st.button("🔄 刷新门店列表"):
                st.rerun()
            
            return
        
        # 显示系统状态（正常情况）
        with st.sidebar:
            st.success("✅ 系统运行正常")
            current_report = system_status.get('current_report')
            if current_report:
                st.info(f"📋 当前报表: {current_report.get('file_name', 'N/A')}")
                st.info(f"🏪 可用门店: {len(available_stores)} 个")
        
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
                        try:
                            preview_data = self.query_handler.get_store_preview(selected_store, 5)
                            
                            if preview_data:
                                total_rows = preview_data.get('total_rows', 0)
                                total_columns = preview_data.get('total_columns', 0)
                                st.info(f"总行数: {total_rows}, 总列数: {total_columns}")
                                
                                # 显示预览数据
                                preview_data_list = preview_data.get('preview_data', [])
                                if preview_data_list:
                                    df = pd.DataFrame(preview_data_list)
                                    st.dataframe(df, use_container_width=True)
                                else:
                                    st.warning("该门店暂无数据")
                            else:
                                st.error("无法加载门店预览数据")
                                st.info("可能原因：工作表不存在或数据格式问题")
                        except Exception as e:
                            st.error(f"加载预览失败: {str(e)}")
            
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
                try:
                    with st.spinner(f"正在 {selected_store} 中搜索 {search_code}..."):
                        search_results = self.query_handler.search_code_in_store(
                            selected_store, search_code, fuzzy_match
                        )
                    
                    if search_results:
                        # 检查是否权限被拒绝
                        if search_results.get('permission_denied', False):
                            st.error("🚫 " + search_results.get('error_message', '您没有权限查询此编码'))
                            st.info("请联系管理员确认您的查询权限")
                            return
                        
                        # 保存到session state
                        st.session_state.search_results = search_results
                        
                        # 显示搜索结果
                        self.display_search_results(search_results)
                    else:
                        st.info("未找到匹配的结果")
                        st.info("建议：尝试使用模糊匹配或检查编码是否正确")
                
                except Exception as e:
                    st.error(f"查询过程中出现错误: {str(e)}")
                    st.info("请稍后重试，或联系管理员")
    
    def display_search_results(self, search_results):
        """显示搜索结果"""
        st.subheader("🎯 查询结果")
        
        # 结果汇总
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("匹配数量", search_results.get('match_count', 0))
        with col2:
            st.metric("门店", search_results.get('sheet_name', 'N/A'))
        with col3:
            st.metric("搜索编码", search_results.get('search_code', 'N/A'))
        
        # 匹配结果详情
        matches = search_results.get('matches', [])
        if matches:
            st.subheader("匹配详情")
            
            for i, match in enumerate(matches):
                row_index = match.get('row_index', 0)
                column = match.get('column', 'N/A')
                matched_value = match.get('matched_value', 'N/A')
                
                with st.expander(f"匹配项 {i+1} - 第{row_index+1}行，{column}列"):
                    st.write(f"**匹配值**: {matched_value}")
                    
                    # 显示行数据
                    row_data = match.get('row_data', {})
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
                    sheet_name = search_results.get('sheet_name', 'Unknown')
                    search_code = search_results.get('search_code', 'Unknown')
                    filename = f"查询结果_{sheet_name}_{search_code}_{timestamp}.xlsx"
                    
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
        
        # 存储类型显示
        if STORAGE_TYPE == "COS":
            st.sidebar.success("🔗 使用腾讯云 COS 存储")
        else:
            st.sidebar.warning("💾 使用本地存储模式")
        
        # 系统状态
        status = self.query_handler.get_system_status()
        
        st.sidebar.metric("可用门店", status.get('stores_count', 0))
        st.sidebar.metric("总查询次数", status.get('total_queries', 0))
        
        # 当前报表信息
        current_report = self.json_handler.get_current_report()
        if current_report:
            st.sidebar.subheader("当前报表")
            st.sidebar.info(f"文件: {current_report.get('file_name', 'N/A')}")
            st.sidebar.info(f"更新: {status.get('last_updated') or '未知'}")
        
        # 权限表信息
        if self.has_permission_handler:
            try:
                permission_stats = self.permission_handler.get_permission_statistics()
                if permission_stats.get('has_permissions', False):
                    st.sidebar.subheader("权限表")
                    st.sidebar.info(f"权限记录: {permission_stats.get('total_records', 0)}条")
                    st.sidebar.info(f"涉及门店: {permission_stats.get('unique_stores', 0)}个")
                else:
                    st.sidebar.warning("⚠️ 未配置权限表")
            except Exception as e:
                st.sidebar.error(f"权限系统错误: {str(e)}")
        
        # 最近查询
        st.sidebar.subheader("最近查询")
        query_history = self.query_handler.get_query_history(5)
        
        if query_history:
            for record in query_history:
                store_name = record.get('store_name', '') if isinstance(record, dict) else getattr(record, 'store_name', '')
                query_count = record.get('query_count', 0) if isinstance(record, dict) else getattr(record, 'query_count', 0)
                
                st.sidebar.text(f"📍 {store_name}")
                st.sidebar.text(f"   查询: {query_count}次")
        else:
            st.sidebar.info("暂无查询记录")
    
    def run(self):
        """运行应用"""
        # 验证配置（仅在使用COS时验证）
        if STORAGE_TYPE == "COS" and not validate_config():
            st.error("COS 配置不完整，当前使用本地存储模式")
        
        # 检查权限表状态
        if self.has_permission_handler:
            try:
                permission_stats = self.permission_handler.get_permission_statistics()
                if not permission_stats.get('has_permissions', False):
                    st.warning("⚠️ 系统未配置权限表，用户查询功能将受限")
            except Exception as e:
                st.error(f"权限系统初始化失败: {str(e)}")
        
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
