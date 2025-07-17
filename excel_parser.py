import pandas as pd
import openpyxl
import streamlit as st
import io
from typing import Dict, List, Any, Optional, Tuple, Generator
from datetime import datetime
import time

class ExcelParser:
    def __init__(self):
        self.workbook_cache = {}
        self.sheet_data_cache = {}
        self.max_cache_size = 10  # 最多缓存10个工作表数据
    
    def validate_excel_file(self, file_content: bytes) -> bool:
        """快速验证Excel文件格式"""
        try:
            # 使用openpyxl快速验证
            workbook = openpyxl.load_workbook(io.BytesIO(file_content), read_only=True)
            workbook.close()
            return True
        except Exception as e:
            st.error(f"Excel文件验证失败: {str(e)}")
            return False
    
    def get_sheet_names_fast(self, file_content: bytes) -> List[str]:
        """快速获取所有工作表名称（上传时使用）"""
        try:
            workbook = openpyxl.load_workbook(io.BytesIO(file_content), read_only=True)
            sheet_names = workbook.sheetnames
            workbook.close()
            return sheet_names
        except Exception as e:
            st.error(f"获取工作表名称失败: {str(e)}")
            return []
    
    def get_file_statistics_for_upload(self, file_content: bytes) -> Dict[str, Any]:
        """上传时的快速文件统计（只获取基本信息，不解析工作表内容）"""
        try:
            file_size = len(file_content)
            
            # 快速获取工作表名称
            sheet_names = self.get_sheet_names_fast(file_content)
            
            if not sheet_names:
                return {
                    'file_size': file_size,
                    'total_sheets': 0,
                    'sheet_names': [],
                    'error': '未找到工作表'
                }
            
            # 返回基本统计信息，不解析工作表内容
            return {
                'file_size': file_size,
                'total_sheets': len(sheet_names),
                'sheet_names': sheet_names,
                'processing_mode': 'fast_upload',
                'message': f'检测到 {len(sheet_names)} 个门店工作表，数据将在查询时按需加载'
            }
            
        except Exception as e:
            st.error(f"获取文件统计失败: {str(e)}")
            return {
                'file_size': len(file_content),
                'total_sheets': 0,
                'sheet_names': [],
                'error': str(e)
            }
    
    def get_file_statistics(self, file_content: bytes) -> Dict[str, Any]:
        """获取详细的文件统计信息（兼容旧版本，现在也使用快速模式）"""
        return self.get_file_statistics_for_upload(file_content)
    
    def load_single_sheet_on_demand(self, file_content: bytes, sheet_name: str) -> Optional[pd.DataFrame]:
        """按需加载单个工作表数据（查询时使用）"""
        try:
            # 生成缓存键
            cache_key = f"{hash(file_content)}_{sheet_name}"
            
            # 检查缓存
            if cache_key in self.sheet_data_cache:
                st.info(f"从缓存加载工作表: {sheet_name}")
                return self.sheet_data_cache[cache_key]
            
            # 按需加载数据
            st.info(f"正在加载工作表: {sheet_name}")
            
            # 使用pandas直接读取指定工作表
            df = pd.read_excel(io.BytesIO(file_content), sheet_name=sheet_name)
            
            # 缓存数据（如果缓存未满）
            if len(self.sheet_data_cache) < self.max_cache_size:
                self.sheet_data_cache[cache_key] = df
            elif len(self.sheet_data_cache) >= self.max_cache_size:
                # 清理最旧的缓存（简单的FIFO策略）
                oldest_key = next(iter(self.sheet_data_cache))
                del self.sheet_data_cache[oldest_key]
                self.sheet_data_cache[cache_key] = df
            
            return df
            
        except Exception as e:
            st.error(f"加载工作表 {sheet_name} 失败: {str(e)}")
            return None
    
    def get_sheet_preview_on_demand(self, file_content: bytes, sheet_name: str, rows: int = 5) -> Optional[Dict[str, Any]]:
        """按需获取工作表预览数据"""
        try:
            df = self.load_single_sheet_on_demand(file_content, sheet_name)
            if df is None:
                return None
            
            preview_data = df.head(rows).to_dict('records')
            
            return {
                'total_rows': len(df),
                'total_columns': len(df.columns),
                'preview_data': preview_data,
                'columns': list(df.columns)
            }
        except Exception as e:
            st.error(f"获取工作表预览失败: {str(e)}")
            return None
    
    def search_in_sheet_on_demand(self, file_content: bytes, sheet_name: str, search_code: str, fuzzy_match: bool = True) -> List[Dict[str, Any]]:
        """按需在工作表中搜索数据"""
        try:
            df = self.load_single_sheet_on_demand(file_content, sheet_name)
            if df is None:
                return []
            
            matches = []
            search_code_lower = search_code.lower().strip()
            
            for row_index, row in df.iterrows():
                for col_name, cell_value in row.items():
                    if pd.isna(cell_value):
                        continue
                    
                    cell_str = str(cell_value).lower().strip()
                    
                    # 根据匹配模式搜索
                    is_match = False
                    if fuzzy_match:
                        is_match = search_code_lower in cell_str
                    else:
                        is_match = search_code_lower == cell_str
                    
                    if is_match:
                        matches.append({
                            'row_index': row_index,
                            'column': col_name,
                            'matched_value': cell_value,
                            'row_data': row.to_dict()
                        })
            
            return matches
            
        except Exception as e:
            st.error(f"搜索失败: {str(e)}")
            return []
    
    def validate_sheet_exists(self, file_content: bytes, sheet_name: str) -> bool:
        """验证工作表是否存在"""
        try:
            sheet_names = self.get_sheet_names_fast(file_content)
            return sheet_name in sheet_names
        except Exception:
            return False
    
    def get_sheet_basic_info_on_demand(self, file_content: bytes, sheet_name: str) -> Optional[Dict[str, Any]]:
        """按需获取工作表基本信息（不加载完整数据）"""
        try:
            workbook = openpyxl.load_workbook(io.BytesIO(file_content), read_only=True)
            
            if sheet_name not in workbook.sheetnames:
                workbook.close()
                return None
            
            sheet = workbook[sheet_name]
            
            info = {
                'name': sheet_name,
                'rows': sheet.max_row,
                'columns': sheet.max_column,
                'has_data': sheet.max_row > 0 and sheet.max_column > 0
            }
            
            workbook.close()
            return info
            
        except Exception as e:
            st.error(f"获取工作表信息失败: {str(e)}")
            return None
    
    def clear_cache(self):
        """清理缓存"""
        self.workbook_cache.clear()
        self.sheet_data_cache.clear()
        st.success("缓存已清理")
    
    def get_cache_info(self) -> Dict[str, Any]:
        """获取缓存信息"""
        return {
            'workbook_cache_size': len(self.workbook_cache),
            'sheet_data_cache_size': len(self.sheet_data_cache),
            'max_cache_size': self.max_cache_size,
            'cached_sheets': list(self.sheet_data_cache.keys())
        }
    
    def preload_sheet(self, file_content: bytes, sheet_name: str):
        """预加载指定工作表"""
        try:
            st.info(f"预加载工作表: {sheet_name}")
            self.load_single_sheet_on_demand(file_content, sheet_name)
        except Exception as e:
            st.error(f"预加载失败: {str(e)}")
    
    # 保持向后兼容的方法
    def load_sheet_data(self, file_content: bytes, sheet_name: str, use_cache: bool = True) -> Optional[pd.DataFrame]:
        """向后兼容的方法"""
        return self.load_single_sheet_on_demand(file_content, sheet_name)
    
    def get_sheet_preview(self, file_content: bytes, sheet_name: str, rows: int = 5) -> Optional[Dict[str, Any]]:
        """向后兼容的方法"""
        return self.get_sheet_preview_on_demand(file_content, sheet_name, rows)
    
    def search_in_sheet(self, file_content: bytes, sheet_name: str, search_code: str, fuzzy_match: bool = True) -> List[Dict[str, Any]]:
        """向后兼容的方法"""
        return self.search_in_sheet_on_demand(file_content, sheet_name, search_code, fuzzy_match)
    
    def get_sheet_basic_info(self, file_content: bytes, sheet_name: str) -> Optional[Dict[str, Any]]:
        """向后兼容的方法"""
        return self.get_sheet_basic_info_on_demand(file_content, sheet_name)
        
    # 废弃的方法（保持兼容性）
    def build_sheet_index(self, file_content: bytes) -> Generator[Dict[str, Any], None, None]:
        """已废弃：现在使用按需加载"""
        st.warning("build_sheet_index 方法已废弃，现在使用按需加载模式")
        return iter([])
    
    def get_file_statistics_fast(self, file_content: bytes) -> Dict[str, Any]:
        """重定向到新的快速统计方法"""
        return self.get_file_statistics_for_upload(file_content)
    
    def _get_detailed_statistics(self, file_content: bytes, sheet_names: List[str]) -> Dict[str, Any]:
        """已废弃：现在使用快速模式"""
        return self.get_file_statistics_for_upload(file_content)
