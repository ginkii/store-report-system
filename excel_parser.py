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
        """快速获取所有工作表名称"""
        try:
            workbook = openpyxl.load_workbook(io.BytesIO(file_content), read_only=True)
            sheet_names = workbook.sheetnames
            workbook.close()
            return sheet_names
        except Exception as e:
            st.error(f"获取工作表名称失败: {str(e)}")
            return []
    
    def build_sheet_index(self, file_content: bytes) -> Generator[Dict[str, Any], None, None]:
        """逐个构建工作表索引信息"""
        try:
            workbook = openpyxl.load_workbook(io.BytesIO(file_content), read_only=True)
            
            for sheet_name in workbook.sheetnames:
                try:
                    sheet = workbook[sheet_name]
                    
                    # 快速获取工作表基本信息
                    max_row = sheet.max_row
                    max_col = sheet.max_column
                    
                    # 快速判断是否有数据（检查前几行）
                    has_data = False
                    check_rows = min(5, max_row)  # 只检查前5行
                    
                    for row in range(1, check_rows + 1):
                        for col in range(1, min(10, max_col + 1)):  # 只检查前10列
                            cell_value = sheet.cell(row=row, column=col).value
                            if cell_value is not None and str(cell_value).strip():
                                has_data = True
                                break
                        if has_data:
                            break
                    
                    sheet_info = {
                        'name': sheet_name,
                        'rows': max_row,
                        'columns': max_col,
                        'has_data': has_data,
                        'indexed_time': datetime.now().isoformat()
                    }
                    
                    yield sheet_info
                    
                except Exception as e:
                    st.warning(f"处理工作表 {sheet_name} 时出错: {str(e)}")
                    yield {
                        'name': sheet_name,
                        'rows': 0,
                        'columns': 0,
                        'has_data': False,
                        'error': str(e),
                        'indexed_time': datetime.now().isoformat()
                    }
            
            workbook.close()
            
        except Exception as e:
            st.error(f"构建工作表索引失败: {str(e)}")
            return
    
    def get_file_statistics_fast(self, file_content: bytes) -> Dict[str, Any]:
        """快速获取文件统计信息（只计算总数，不处理每个工作表）"""
        try:
            file_size = len(file_content)
            sheet_names = self.get_sheet_names_fast(file_content)
            
            return {
                'file_size': file_size,
                'total_sheets': len(sheet_names),
                'sheet_names': sheet_names,
                'processing_required': True,  # 标记需要进一步处理
                'indexed': False
            }
        except Exception as e:
            st.error(f"获取文件统计失败: {str(e)}")
            return {
                'file_size': len(file_content),
                'total_sheets': 0,
                'sheet_names': [],
                'processing_required': False,
                'indexed': False
            }
    
    def get_file_statistics(self, file_content: bytes) -> Dict[str, Any]:
        """获取详细的文件统计信息（兼容旧版本）"""
        try:
            file_size = len(file_content)
            sheet_names = self.get_sheet_names_fast(file_content)
            
            # 如果工作表数量较少，直接处理
            if len(sheet_names) <= 10:
                return self._get_detailed_statistics(file_content, sheet_names)
            
            # 工作表数量较多，返回基本信息
            return {
                'file_size': file_size,
                'total_sheets': len(sheet_names),
                'sheet_names': sheet_names,
                'sheets_info': [{'name': name, 'rows': 0, 'columns': 0, 'has_data': True} for name in sheet_names],
                'processing_required': True,
                'indexed': False
            }
        except Exception as e:
            st.error(f"获取文件统计失败: {str(e)}")
            return {
                'file_size': len(file_content),
                'total_sheets': 0,
                'sheet_names': [],
                'sheets_info': [],
                'processing_required': False,
                'indexed': False
            }
    
    def _get_detailed_statistics(self, file_content: bytes, sheet_names: List[str]) -> Dict[str, Any]:
        """获取详细统计信息（用于少量工作表）"""
        try:
            workbook = openpyxl.load_workbook(io.BytesIO(file_content), read_only=True)
            
            sheets_info = []
            for sheet_name in sheet_names:
                sheet = workbook[sheet_name]
                sheets_info.append({
                    'name': sheet_name,
                    'rows': sheet.max_row,
                    'columns': sheet.max_column,
                    'has_data': sheet.max_row > 0 and sheet.max_column > 0
                })
            
            workbook.close()
            
            return {
                'file_size': len(file_content),
                'total_sheets': len(sheet_names),
                'sheet_names': sheet_names,
                'sheets_info': sheets_info,
                'processing_required': False,
                'indexed': True
            }
        except Exception as e:
            st.error(f"获取详细统计失败: {str(e)}")
            return {
                'file_size': len(file_content),
                'total_sheets': 0,
                'sheet_names': [],
                'sheets_info': [],
                'processing_required': False,
                'indexed': False
            }
    
    def load_sheet_data(self, file_content: bytes, sheet_name: str, use_cache: bool = True) -> Optional[pd.DataFrame]:
        """按需加载工作表数据"""
        try:
            # 生成缓存键
            cache_key = f"{hash(file_content)}_{sheet_name}"
            
            # 检查缓存
            if use_cache and cache_key in self.sheet_data_cache:
                st.info(f"从缓存加载工作表: {sheet_name}")
                return self.sheet_data_cache[cache_key]
            
            # 加载数据
            st.info(f"正在加载工作表: {sheet_name}")
            df = pd.read_excel(io.BytesIO(file_content), sheet_name=sheet_name)
            
            # 缓存数据（如果缓存未满）
            if use_cache and len(self.sheet_data_cache) < self.max_cache_size:
                self.sheet_data_cache[cache_key] = df
            elif use_cache and len(self.sheet_data_cache) >= self.max_cache_size:
                # 清理最旧的缓存
                oldest_key = next(iter(self.sheet_data_cache))
                del self.sheet_data_cache[oldest_key]
                self.sheet_data_cache[cache_key] = df
            
            return df
            
        except Exception as e:
            st.error(f"加载工作表 {sheet_name} 失败: {str(e)}")
            return None
    
    def get_sheet_preview(self, file_content: bytes, sheet_name: str, rows: int = 5) -> Optional[Dict[str, Any]]:
        """获取工作表预览数据"""
        try:
            df = self.load_sheet_data(file_content, sheet_name)
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
    
    def search_in_sheet(self, file_content: bytes, sheet_name: str, search_code: str, fuzzy_match: bool = True) -> List[Dict[str, Any]]:
        """在工作表中搜索数据"""
        try:
            df = self.load_sheet_data(file_content, sheet_name)
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
    
    def preload_common_sheets(self, file_content: bytes, sheet_names: List[str]):
        """预加载常用工作表"""
        try:
            st.info("正在预加载常用工作表...")
            
            # 限制预加载数量
            preload_limit = min(5, len(sheet_names))
            
            for i, sheet_name in enumerate(sheet_names[:preload_limit]):
                st.info(f"预加载工作表 {i+1}/{preload_limit}: {sheet_name}")
                self.load_sheet_data(file_content, sheet_name, use_cache=True)
                time.sleep(0.1)  # 避免过快处理
            
            st.success(f"已预加载 {preload_limit} 个工作表")
            
        except Exception as e:
            st.error(f"预加载失败: {str(e)}")
    
    def validate_sheet_exists(self, file_content: bytes, sheet_name: str) -> bool:
        """验证工作表是否存在"""
        try:
            sheet_names = self.get_sheet_names_fast(file_content)
            return sheet_name in sheet_names
        except Exception:
            return False
    
    def get_sheet_basic_info(self, file_content: bytes, sheet_name: str) -> Optional[Dict[str, Any]]:
        """获取工作表基本信息（不加载数据）"""
        try:
            workbook = openpyxl.load_workbook(io.BytesIO(file_content), read_only=True)
            
            if sheet_name not in workbook.sheetnames:
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
