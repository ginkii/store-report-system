import pandas as pd
import streamlit as st
from datetime import datetime
from typing import List, Dict, Any, Optional
import io
import re
import time

class QueryHandler:
    def __init__(self):
        # 导入时动态选择存储处理器
        try:
            from cos_handler import COSHandler
            self.storage_handler = COSHandler()
            self.storage_type = "COS"
        except ImportError:
            from local_storage_handler import LocalStorageHandler
            self.storage_handler = LocalStorageHandler()
            self.storage_type = "LOCAL"
        
        from json_handler import JSONHandler
        from excel_parser import ExcelParser
        from permission_handler import PermissionHandler
        
        self.json_handler = JSONHandler()
        self.excel_parser = ExcelParser()
        self.permission_handler = PermissionHandler()
    
    def get_available_stores(self) -> List[str]:
        """获取可用门店列表"""
        try:
            store_sheets = self.json_handler.get_store_sheets()
            return [store['sheet_name'] for store in store_sheets] if store_sheets else []
        except Exception as e:
            st.error(f"获取门店列表失败: {str(e)}")
            return []
    
    def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态"""
        try:
            current_report = self.json_handler.get_current_report()
            store_sheets = self.json_handler.get_store_sheets()
            report_history = self.json_handler.get_report_history()
            
            # 检查存储连接
            storage_connection = self.storage_handler.test_connection()
            
            # 检查文件可访问性
            file_accessible = False
            if current_report:
                file_path = current_report.get('file_path') or current_report.get('cos_file_path')
                if file_path:
                    file_accessible = self.storage_handler.file_exists(file_path)
            
            return {
                'stores_count': len(store_sheets) if store_sheets else 0,
                'total_queries': sum(store.get('query_count', 0) for store in store_sheets) if store_sheets else 0,
                'history_count': len(report_history) if report_history else 0,
                'cos_connection': storage_connection,  # 保持原名称兼容性
                'file_accessible': file_accessible,
                'current_report': current_report,
                'last_updated': current_report.get('upload_time') if current_report else None,
                'system_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'storage_type': self.storage_type
            }
        except Exception as e:
            st.error(f"获取系统状态失败: {str(e)}")
            return {
                'stores_count': 0,
                'total_queries': 0,
                'history_count': 0,
                'cos_connection': False,
                'file_accessible': False,
                'current_report': None,
                'last_updated': None,
                'system_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'storage_type': self.storage_type
            }
    
    def get_query_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """获取查询历史"""
        try:
            store_sheets = self.json_handler.get_store_sheets()
            if not store_sheets:
                return []
            
            # 按查询次数排序
            sorted_stores = sorted(
                store_sheets,
                key=lambda x: x.get('query_count', 0),
                reverse=True
            )
            
            return sorted_stores[:limit]
        except Exception as e:
            st.error(f"获取查询历史失败: {str(e)}")
            return []
    
    def get_store_preview(self, store_name: str, rows: int = 5) -> Optional[Dict[str, Any]]:
        """获取门店数据预览（按需加载）"""
        try:
            # 获取当前报表文件
            current_report = self.json_handler.get_current_report()
            if not current_report:
                return None
            
            # 获取文件路径
            file_path = current_report.get('file_path') or current_report.get('cos_file_path')
            if not file_path:
                return None
            
            # 从存储中下载文件
            file_content = self.storage_handler.download_file(file_path)
            if not file_content:
                return None
            
            # 使用按需加载获取预览
            preview_data = self.excel_parser.get_sheet_preview_on_demand(file_content, store_name, rows)
            return preview_data
                
        except Exception as e:
            st.error(f"获取门店预览失败: {str(e)}")
            return None
    
    def validate_search_code(self, code: str) -> bool:
        """验证搜索编码"""
        if not code or not code.strip():
            return False
        
        # 基本验证：不能只包含空格
        if len(code.strip()) < 1:
            return False
        
        return True
    
    def search_code_in_store(self, store_name: str, search_code: str, fuzzy_match: bool = True) -> Optional[Dict[str, Any]]:
        """在指定门店中搜索编码（按需加载）"""
        try:
            # 权限验证
            if not self.permission_handler.check_permission(store_name, search_code):
                return {
                    'sheet_name': store_name,
                    'search_code': search_code,
                    'match_count': 0,
                    'matches': [],
                    'permission_denied': True,
                    'error_message': '您没有权限查询此编码',
                    'search_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
            
            # 获取当前报表文件
            current_report = self.json_handler.get_current_report()
            if not current_report:
                st.error("未找到当前报表文件")
                return None
            
            # 获取文件路径
            file_path = current_report.get('file_path') or current_report.get('cos_file_path')
            if not file_path:
                st.error("报表文件路径不存在")
                return None
            
            # 从存储中下载文件
            file_content = self.storage_handler.download_file(file_path)
            if not file_content:
                st.error("无法下载报表文件")
                return None
            
            # 验证工作表是否存在
            if not self.excel_parser.validate_sheet_exists(file_content, store_name):
                st.error(f"工作表 {store_name} 不存在")
                return None
            
            # 使用按需搜索方法
            with st.spinner(f"正在 {store_name} 中搜索 {search_code}..."):
                matches = self.excel_parser.search_in_sheet_on_demand(file_content, store_name, search_code, fuzzy_match)
            
            # 更新查询统计
            self._update_query_stats(store_name)
            
            return {
                'sheet_name': store_name,
                'search_code': search_code,
                'match_count': len(matches),
                'matches': matches,
                'fuzzy_match': fuzzy_match,
                'permission_denied': False,
                'search_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
        except Exception as e:
            st.error(f"搜索失败: {str(e)}")
            return None
    
    def _update_query_stats(self, store_name: str):
        """更新查询统计"""
        try:
            # 使用新的访问统计方法
            self.json_handler.update_sheet_access_stats(store_name)
            
        except Exception as e:
            st.error(f"更新查询统计失败: {str(e)}")
    
    def export_search_results(self, search_results: Dict[str, Any]) -> Optional[bytes]:
        """导出搜索结果为Excel"""
        try:
            # 创建Excel文件
            output = io.BytesIO()
            
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                # 搜索信息工作表
                search_info = pd.DataFrame([{
                    '门店名称': search_results['sheet_name'],
                    '搜索编码': search_results['search_code'],
                    '匹配数量': search_results['match_count'],
                    '搜索时间': search_results['search_time'],
                    '匹配模式': '模糊匹配' if search_results['fuzzy_match'] else '精确匹配'
                }])
                search_info.to_excel(writer, sheet_name='搜索信息', index=False)
                
                # 匹配结果工作表
                if search_results['matches']:
                    matches_data = []
                    for i, match in enumerate(search_results['matches']):
                        match_info = {
                            '序号': i + 1,
                            '行号': match['row_index'] + 1,
                            '列名': match['column'],
                            '匹配值': match['matched_value']
                        }
                        
                        # 添加行数据
                        row_data = match['row_data']
                        for col, value in row_data.items():
                            match_info[f'数据_{col}'] = value
                        
                        matches_data.append(match_info)
                    
                    matches_df = pd.DataFrame(matches_data)
                    matches_df.to_excel(writer, sheet_name='匹配结果', index=False)
                else:
                    # 空结果
                    empty_df = pd.DataFrame([{'说明': '未找到匹配结果'}])
                    empty_df.to_excel(writer, sheet_name='匹配结果', index=False)
            
            return output.getvalue()
            
        except Exception as e:
            st.error(f"导出Excel失败: {str(e)}")
            return None
    
    def get_storage_info(self) -> Dict[str, Any]:
        """获取存储信息"""
        try:
            return self.storage_handler.get_upload_status()
        except Exception as e:
            st.error(f"获取存储信息失败: {str(e)}")
            return {
                'has_sdk': False,
                'client_initialized': False,
                'bucket_name': None,
                'connection_ok': False,
                'storage_type': self.storage_type
            }
