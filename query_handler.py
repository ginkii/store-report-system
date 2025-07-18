import pandas as pd
import streamlit as st
from datetime import datetime
from typing import List, Dict, Any, Optional
import io
import re

class QueryHandler:
    def __init__(self):
        # 导入其他处理器
        from json_handler import JSONHandler
        try:
            from excel_parser import ExcelParser
        except ImportError:
            from excel_parser import ExcelParser  # 将使用我们修复的版本
        
        try:
            # 尝试导入存储处理器
            try:
                from cos_handler import COSHandler
                self.storage_handler = COSHandler()
                self.storage_type = "COS"
            except ImportError:
                from local_storage_handler import LocalStorageHandler
                self.storage_handler = LocalStorageHandler()
                self.storage_type = "LOCAL"
        except ImportError:
            # 如果都没有，使用简化版本
            self.storage_handler = None
            self.storage_type = "LOCAL"
        
        self.json_handler = JSONHandler()
        self.excel_parser = ExcelParser()
        
        # 缓存
        self._store_cache = {}
        self._cache_timestamp = None
    
    def get_available_stores(self) -> List[str]:
        """获取可用门店列表 - 从JSON数据中读取真实门店信息"""
        try:
            # 检查缓存是否有效（5分钟内）
            now = datetime.now()
            if (self._cache_timestamp and 
                (now - self._cache_timestamp).seconds < 300 and 
                self._store_cache.get('stores')):
                return self._store_cache['stores']
            
            # 从JSON处理器获取门店工作表列表
            store_sheets = self.json_handler.get_store_sheets()
            
            if not store_sheets:
                st.warning("未找到门店工作表数据，请检查是否已上传报表文件")
                return []
            
            # 提取门店名称
            store_names = []
            for sheet in store_sheets:
                sheet_name = sheet.get('sheet_name', '')
                if sheet_name:
                    store_names.append(sheet_name)
            
            # 更新缓存
            self._store_cache = {
                'stores': store_names,
                'count': len(store_names)
            }
            self._cache_timestamp = now
            
            return store_names
            
        except Exception as e:
            st.error(f"获取门店列表失败: {str(e)}")
            return []
    
    def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态 - 基于真实数据"""
        try:
            # 获取系统状态基础信息
            system_status = self.json_handler.get_system_status()
            
            # 获取门店数据
            store_sheets = self.json_handler.get_store_sheets()
            stores_count = len(store_sheets) if store_sheets else 0
            
            # 计算总查询次数
            total_queries = 0
            if store_sheets:
                total_queries = sum(sheet.get('query_count', 0) for sheet in store_sheets)
            
            # 获取历史报表数量
            report_history = self.json_handler.get_report_history()
            history_count = len(report_history) if report_history else 0
            
            # 获取当前报表信息
            current_report = self.json_handler.get_current_report()
            
            # 检查文件可访问性
            file_accessible = False
            if current_report and self.storage_handler:
                file_path = current_report.get('file_path') or current_report.get('cos_file_path')
                if file_path:
                    try:
                        # 尝试下载文件头部来验证可访问性
                        file_content = self.storage_handler.download_file(file_path)
                        file_accessible = bool(file_content)
                    except Exception:
                        file_accessible = False
                else:
                    file_accessible = False
            
            # 更新系统状态
            enhanced_status = {
                **system_status,  # 包含基础状态信息
                'stores_count': stores_count,
                'total_queries': total_queries,
                'history_count': history_count,
                'file_accessible': file_accessible,
                'current_report': current_report,
                'store_sheets_count': stores_count,
                'storage_type': self.storage_type
            }
            
            return enhanced_status
            
        except Exception as e:
            st.error(f"获取系统状态失败: {str(e)}")
            return {
                'data_loaded': False,
                'has_current_report': False,
                'file_accessible': False,
                'cos_connection': False,
                'storage_connection': False,
                'stores_count': 0,
                'total_queries': 0,
                'history_count': 0,
                'last_updated': None,
                'system_time': datetime.now().isoformat(),
                'storage_type': self.storage_type,
                'current_report': None,
                'store_sheets_count': 0
            }
    
    def validate_search_code(self, code: str) -> bool:
        """验证搜索编码格式"""
        if not code or not code.strip():
            return False
        
        # 基本验证：不能为空，长度合理
        code = code.strip()
        if len(code) < 1 or len(code) > 50:
            return False
        
        # 可以包含字母、数字、常见符号
        pattern = r'^[a-zA-Z0-9\-_\.\/\\\s]+$'
        return bool(re.match(pattern, code))
    
    def search_code_in_store(self, store_name: str, search_code: str, fuzzy_match: bool = True) -> Optional[Dict[str, Any]]:
        """在指定门店中搜索编码"""
        try:
            # 更新访问统计
            self.json_handler.update_sheet_access_stats(store_name)
            
            # 获取当前报表
            current_report = self.json_handler.get_current_report()
            if not current_report:
                return {
                    'error': True,
                    'error_message': '未找到当前报表',
                    'match_count': 0,
                    'matches': []
                }
            
            # 获取文件内容
            file_path = current_report.get('file_path') or current_report.get('cos_file_path')
            if not file_path or not self.storage_handler:
                return {
                    'error': True,
                    'error_message': '无法访问报表文件',
                    'match_count': 0,
                    'matches': []
                }
            
            try:
                file_content = self.storage_handler.download_file(file_path)
                if not file_content:
                    return {
                        'error': True,
                        'error_message': '文件下载失败',
                        'match_count': 0,
                        'matches': []
                    }
            except Exception as e:
                return {
                    'error': True,
                    'error_message': f'文件访问错误: {str(e)}',
                    'match_count': 0,
                    'matches': []
                }
            
            # 解析Excel并搜索
            try:
                search_results = self.excel_parser.search_in_sheet(
                    file_content, store_name, search_code, fuzzy_match
                )
                
                if search_results:
                    return {
                        'match_count': search_results['match_count'],
                        'sheet_name': store_name,
                        'search_code': search_code,
                        'matches': search_results['matches'],
                        'search_time': datetime.now().isoformat()
                    }
                else:
                    return {
                        'match_count': 0,
                        'sheet_name': store_name,
                        'search_code': search_code,
                        'matches': [],
                        'search_time': datetime.now().isoformat()
                    }
                    
            except Exception as e:
                return {
                    'error': True,
                    'error_message': f'搜索过程中出错: {str(e)}',
                    'match_count': 0,
                    'matches': []
                }
                
        except Exception as e:
            return {
                'error': True,
                'error_message': f'查询失败: {str(e)}',
                'match_count': 0,
                'matches': []
            }
    
    def get_store_preview(self, store_name: str, limit: int = 5) -> Optional[Dict[str, Any]]:
        """获取门店数据预览"""
        try:
            # 获取当前报表
            current_report = self.json_handler.get_current_report()
            if not current_report:
                return None
            
            file_path = current_report.get('file_path') or current_report.get('cos_file_path')
            if not file_path or not self.storage_handler:
                return None
            
            # 下载文件
            file_content = self.storage_handler.download_file(file_path)
            if not file_content:
                return None
            
            # 获取预览数据
            preview_data = self.excel_parser.get_sheet_preview(file_content, store_name, limit)
            return preview_data
            
        except Exception as e:
            st.error(f"获取门店预览失败: {str(e)}")
            return None
    
    def export_search_results(self, search_results: Dict[str, Any]) -> Optional[bytes]:
        """导出搜索结果为Excel"""
        try:
            # 创建Excel数据
            data_rows = []
            
            # 添加搜索信息头部
            data_rows.append({
                '搜索信息': '门店名称',
                '值': search_results.get('sheet_name', 'N/A')
            })
            data_rows.append({
                '搜索信息': '搜索编码',
                '值': search_results.get('search_code', 'N/A')
            })
            data_rows.append({
                '搜索信息': '匹配数量',
                '值': search_results.get('match_count', 0)
            })
            data_rows.append({
                '搜索信息': '搜索时间',
                '值': search_results.get('search_time', datetime.now().isoformat())
            })
            data_rows.append({})  # 空行
            
            # 添加匹配结果
            matches = search_results.get('matches', [])
            if matches:
                for i, match in enumerate(matches):
                    data_rows.append({
                        '匹配项': f'匹配项 {i+1}',
                        '行号': match.get('row_index', 0) + 1,
                        '列名': match.get('column', 'N/A'),
                        '匹配值': match.get('matched_value', 'N/A')
                    })
                    
                    # 添加行数据
                    row_data = match.get('row_data', {})
                    if row_data:
                        for key, value in row_data.items():
                            data_rows.append({
                                '字段名': key,
                                '字段值': str(value)
                            })
                    
                    data_rows.append({})  # 空行分隔
            
            # 创建DataFrame并导出Excel
            df = pd.DataFrame(data_rows)
            
            # 使用BytesIO创建Excel
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='搜索结果', index=False)
            
            output.seek(0)
            return output.getvalue()
            
        except Exception as e:
            st.error(f"导出搜索结果失败: {str(e)}")
            return None
    
    def get_query_history(self, limit: int = 20) -> List[Dict[str, Any]]:
        """获取查询历史记录"""
        try:
            store_sheets = self.json_handler.get_store_sheets()
            if not store_sheets:
                return []
            
            # 按查询次数和最后查询时间排序
            sorted_sheets = sorted(
                store_sheets,
                key=lambda x: (x.get('query_count', 0), x.get('last_query_time', '')),
                reverse=True
            )
            
            # 只返回有查询记录的门店
            query_history = []
            for sheet in sorted_sheets[:limit]:
                if sheet.get('query_count', 0) > 0:
                    query_history.append({
                        'store_name': sheet.get('sheet_name', 'N/A'),
                        'query_count': sheet.get('query_count', 0),
                        'last_query_time': sheet.get('last_query_time', '从未查询')
                    })
            
            return query_history
            
        except Exception as e:
            st.error(f"获取查询历史失败: {str(e)}")
            return []
    
    def clear_cache(self):
        """清除缓存"""
        self._store_cache.clear()
        self._cache_timestamp = None
    
    def refresh_store_data(self):
        """刷新门店数据"""
        self.clear_cache()
        return self.get_available_stores()
