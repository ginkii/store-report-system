from typing import Optional, Dict, Any
import streamlit as st
from datetime import datetime
import time

from json_handler import JSONHandler
from cos_handler import COSHandler
from excel_parser import ExcelParser

class QueryHandler:
    def __init__(self):
        self.json_handler = JSONHandler()
        self.cos_handler = COSHandler()
        self.excel_parser = ExcelParser()
    
    def get_available_stores(self) -> list:
        """
        获取可用的门店列表
        
        Returns:
            门店名称列表
        """
        try:
            return self.json_handler.get_store_names()
        except Exception as e:
            st.error(f"获取门店列表失败: {e}")
            return []
    
    def validate_store_selection(self, store_name: str) -> bool:
        """
        验证门店选择是否有效
        
        Args:
            store_name: 门店名称
            
        Returns:
            有效返回True，无效返回False
        """
        available_stores = self.get_available_stores()
        return store_name in available_stores
    
    def search_code_in_store(self, store_name: str, search_code: str, 
                           fuzzy_match: bool = True) -> Optional[Dict[str, Any]]:
        """
        在指定门店中搜索编码
        
        Args:
            store_name: 门店名称
            search_code: 搜索编码
            fuzzy_match: 是否模糊匹配
            
        Returns:
            搜索结果字典，失败返回None
        """
        try:
            # 验证门店是否存在
            if not self.validate_store_selection(store_name):
                st.error(f"门店 '{store_name}' 不存在")
                return None
            
            # 获取当前报表信息
            current_report = self.json_handler.get_current_report()
            if not current_report:
                st.error("暂无可用报表，请联系管理员上传")
                return None
            
            # 显示进度条
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            # 第一步：从COS下载文件
            status_text.text("正在下载报表文件...")
            progress_bar.progress(25)
            
            file_content = self.cos_handler.download_file(current_report['cos_file_path'])
            if not file_content:
                st.error("下载报表文件失败")
                return None
            
            # 第二步：验证文件
            status_text.text("正在验证文件...")
            progress_bar.progress(50)
            
            if not self.excel_parser.validate_excel_file(file_content):
                st.error("报表文件格式无效")
                return None
            
            # 第三步：搜索编码
            status_text.text(f"正在 '{store_name}' 中搜索编码...")
            progress_bar.progress(75)
            
            search_results = self.excel_parser.search_code_in_sheet(
                file_content, store_name, search_code, fuzzy_match
            )
            
            # 第四步：更新统计信息
            status_text.text("正在更新统计信息...")
            progress_bar.progress(100)
            
            # 更新查询统计
            self.json_handler.update_query_stats(store_name)
            
            # 清除进度条
            progress_bar.empty()
            status_text.empty()
            
            if search_results:
                # 添加额外信息
                search_results['query_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                search_results['report_info'] = current_report
                
                st.success(f"搜索完成！在 '{store_name}' 中找到 {search_results['match_count']} 个匹配项")
                return search_results
            else:
                st.warning(f"在 '{store_name}' 中未找到编码 '{search_code}' 的匹配项")
                return None
                
        except Exception as e:
            st.error(f"搜索过程中发生错误: {e}")
            return None
    
    def get_store_preview(self, store_name: str, max_rows: int = 10) -> Optional[Dict[str, Any]]:
        """
        获取门店数据预览
        
        Args:
            store_name: 门店名称
            max_rows: 最大预览行数
            
        Returns:
            预览数据字典，失败返回None
        """
        try:
            # 验证门店是否存在
            if not self.validate_store_selection(store_name):
                st.error(f"门店 '{store_name}' 不存在")
                return None
            
            # 获取当前报表信息
            current_report = self.json_handler.get_current_report()
            if not current_report:
                st.error("暂无可用报表")
                return None
            
            # 从COS下载文件
            file_content = self.cos_handler.download_file(current_report['cos_file_path'])
            if not file_content:
                st.error("下载报表文件失败")
                return None
            
            # 获取预览数据
            preview_data = self.excel_parser.get_sheet_preview(file_content, store_name, max_rows)
            
            if preview_data:
                preview_data['report_info'] = current_report
                return preview_data
            else:
                st.error(f"无法获取门店 '{store_name}' 的预览数据")
                return None
                
        except Exception as e:
            st.error(f"获取预览失败: {e}")
            return None
    
    def export_search_results(self, search_results: Dict[str, Any]) -> Optional[bytes]:
        """
        导出搜索结果
        
        Args:
            search_results: 搜索结果字典
            
        Returns:
            Excel文件内容，失败返回None
        """
        try:
            return self.excel_parser.export_search_results(search_results)
        except Exception as e:
            st.error(f"导出失败: {e}")
            return None
    
    def get_query_history(self, limit: int = 10) -> list:
        """
        获取查询历史（基于门店的查询统计）
        
        Args:
            limit: 返回记录数量限制
            
        Returns:
            查询历史列表
        """
        try:
            store_sheets = self.json_handler.get_store_sheets()
            
            # 按查询时间排序
            history = []
            for store in store_sheets:
                if store.get('last_query_time'):
                    history.append({
                        'store_name': store['sheet_name'],
                        'last_query_time': store['last_query_time'],
                        'query_count': store.get('query_count', 0)
                    })
            
            # 按最后查询时间排序
            history.sort(key=lambda x: x['last_query_time'], reverse=True)
            
            return history[:limit]
            
        except Exception as e:
            st.error(f"获取查询历史失败: {e}")
            return []
    
    def get_system_status(self) -> Dict[str, Any]:
        """
        获取系统状态信息
        
        Returns:
            系统状态字典
        """
        try:
            # 获取基础统计信息
            stats = self.json_handler.get_system_stats()
            
            # 检查COS连接状态
            cos_status = self.cos_handler.test_connection()
            
            # 检查当前报表文件状态
            current_report = stats.get('current_report')
            file_status = False
            if current_report:
                file_status = self.cos_handler.file_exists(current_report['cos_file_path'])
            
            return {
                'stores_count': stats['total_stores'],
                'total_queries': stats['total_queries'],
                'current_report': current_report,
                'last_updated': stats['last_updated'],
                'history_count': stats['history_count'],
                'cos_connection': cos_status,
                'file_accessible': file_status,
                'system_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
        except Exception as e:
            st.error(f"获取系统状态失败: {e}")
            return {
                'stores_count': 0,
                'total_queries': 0,
                'current_report': None,
                'last_updated': None,
                'history_count': 0,
                'cos_connection': False,
                'file_accessible': False,
                'system_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
    
    def validate_search_code(self, search_code: str) -> bool:
        """
        验证搜索编码格式
        
        Args:
            search_code: 搜索编码
            
        Returns:
            有效返回True，无效返回False
        """
        if not search_code or not search_code.strip():
            return False
        
        # 基本长度检查
        if len(search_code.strip()) < 1 or len(search_code.strip()) > 50:
            return False
        
        return True
