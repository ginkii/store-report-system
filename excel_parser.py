import pandas as pd
import io
from typing import List, Dict, Optional, Any, Tuple
import streamlit as st
from openpyxl import load_workbook
import re

class ExcelParser:
    def __init__(self):
        self.supported_formats = ['.xlsx', '.xls']
    
    def get_sheet_names(self, file_content: bytes) -> List[str]:
        """
        获取Excel文件中所有sheet名称
        
        Args:
            file_content: Excel文件内容
            
        Returns:
            sheet名称列表
        """
        try:
            # 使用openpyxl读取sheet名称（更稳定）
            workbook = load_workbook(io.BytesIO(file_content), read_only=True)
            sheet_names = workbook.sheetnames
            workbook.close()
            return sheet_names
            
        except Exception as e:
            st.error(f"读取Excel文件失败: {e}")
            return []
    
    def validate_excel_file(self, file_content: bytes) -> bool:
        """
        验证Excel文件是否有效
        
        Args:
            file_content: Excel文件内容
            
        Returns:
            有效返回True，无效返回False
        """
        try:
            sheet_names = self.get_sheet_names(file_content)
            if not sheet_names:
                st.error("Excel文件中没有找到任何工作表")
                return False
            
            # 检查是否有合理的sheet数量
            if len(sheet_names) > 1000:
                st.error("工作表数量过多，请检查文件格式")
                return False
            
            return True
            
        except Exception as e:
            st.error(f"文件验证失败: {e}")
            return False
    
    def read_sheet_data(self, file_content: bytes, sheet_name: str) -> Optional[pd.DataFrame]:
        """
        读取指定sheet的数据
        
        Args:
            file_content: Excel文件内容
            sheet_name: 工作表名称
            
        Returns:
            DataFrame对象，失败返回None
        """
        try:
            df = pd.read_excel(
                io.BytesIO(file_content),
                sheet_name=sheet_name,
                engine='openpyxl'
            )
            return df
            
        except Exception as e:
            st.error(f"读取工作表 '{sheet_name}' 失败: {e}")
            return None
    
    def search_code_in_sheet(self, file_content: bytes, sheet_name: str, 
                           search_code: str, fuzzy_match: bool = True) -> Optional[Dict[str, Any]]:
        """
        在指定sheet中搜索编码
        
        Args:
            file_content: Excel文件内容
            sheet_name: 工作表名称
            search_code: 搜索的编码
            fuzzy_match: 是否模糊匹配
            
        Returns:
            搜索结果字典，包含匹配的行数据
        """
        try:
            df = self.read_sheet_data(file_content, sheet_name)
            if df is None:
                return None
            
            # 转换所有列为字符串进行搜索
            df_str = df.astype(str)
            
            # 搜索结果
            matches = []
            
            if fuzzy_match:
                # 模糊搜索：在所有列中查找包含搜索码的单元格
                for idx, row in df_str.iterrows():
                    for col_name, cell_value in row.items():
                        if search_code.lower() in str(cell_value).lower():
                            matches.append({
                                'row_index': idx,
                                'column': col_name,
                                'matched_value': cell_value,
                                'row_data': df.iloc[idx].to_dict()
                            })
            else:
                # 精确搜索：查找完全匹配的单元格
                for idx, row in df_str.iterrows():
                    for col_name, cell_value in row.items():
                        if str(cell_value).strip() == search_code.strip():
                            matches.append({
                                'row_index': idx,
                                'column': col_name,
                                'matched_value': cell_value,
                                'row_data': df.iloc[idx].to_dict()
                            })
            
            if not matches:
                return None
            
            return {
                'sheet_name': sheet_name,
                'search_code': search_code,
                'match_count': len(matches),
                'matches': matches,
                'sheet_preview': df.head(5).to_dict('records')  # 预览前5行
            }
            
        except Exception as e:
            st.error(f"搜索失败: {e}")
            return None
    
    def get_sheet_preview(self, file_content: bytes, sheet_name: str, 
                         max_rows: int = 10) -> Optional[Dict[str, Any]]:
        """
        获取工作表预览
        
        Args:
            file_content: Excel文件内容
            sheet_name: 工作表名称
            max_rows: 最大预览行数
            
        Returns:
            预览数据字典
        """
        try:
            df = self.read_sheet_data(file_content, sheet_name)
            if df is None:
                return None
            
            preview_data = df.head(max_rows)
            
            return {
                'sheet_name': sheet_name,
                'total_rows': len(df),
                'total_columns': len(df.columns),
                'columns': list(df.columns),
                'preview_data': preview_data.to_dict('records'),
                'data_types': df.dtypes.to_dict()
            }
            
        except Exception as e:
            st.error(f"获取预览失败: {e}")
            return None
    
    def export_search_results(self, search_results: Dict[str, Any]) -> Optional[bytes]:
        """
        导出搜索结果为Excel文件
        
        Args:
            search_results: 搜索结果字典
            
        Returns:
            Excel文件内容，失败返回None
        """
        try:
            output = io.BytesIO()
            
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                # 写入搜索结果汇总
                summary_data = {
                    '搜索编码': [search_results['search_code']],
                    '门店名称': [search_results['sheet_name']],
                    '匹配数量': [search_results['match_count']],
                    '导出时间': [pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')]
                }
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='搜索汇总', index=False)
                
                # 写入匹配的行数据
                if search_results['matches']:
                    matches_data = []
                    for match in search_results['matches']:
                        row_data = match['row_data'].copy()
                        row_data['_匹配列'] = match['column']
                        row_data['_匹配值'] = match['matched_value']
                        matches_data.append(row_data)
                    
                    matches_df = pd.DataFrame(matches_data)
                    matches_df.to_excel(writer, sheet_name='匹配结果', index=False)
                
                # 写入原始数据预览
                if search_results.get('sheet_preview'):
                    preview_df = pd.DataFrame(search_results['sheet_preview'])
                    preview_df.to_excel(writer, sheet_name='原始数据预览', index=False)
            
            output.seek(0)
            return output.read()
            
        except Exception as e:
            st.error(f"导出失败: {e}")
            return None
    
    def get_file_statistics(self, file_content: bytes) -> Dict[str, Any]:
        """
        获取Excel文件统计信息
        
        Args:
            file_content: Excel文件内容
            
        Returns:
            统计信息字典
        """
        try:
            sheet_names = self.get_sheet_names(file_content)
            total_sheets = len(sheet_names)
            
            stats = {
                'total_sheets': total_sheets,
                'sheet_names': sheet_names,
                'file_size': len(file_content),
                'sheets_info': []
            }
            
            # 获取每个sheet的基本信息
            for sheet_name in sheet_names:
                try:
                    df = self.read_sheet_data(file_content, sheet_name)
                    if df is not None:
                        sheet_info = {
                            'name': sheet_name,
                            'rows': len(df),
                            'columns': len(df.columns),
                            'has_data': not df.empty
                        }
                        stats['sheets_info'].append(sheet_info)
                except Exception:
                    # 如果某个sheet读取失败，继续处理其他sheet
                    stats['sheets_info'].append({
                        'name': sheet_name,
                        'rows': 0,
                        'columns': 0,
                        'has_data': False,
                        'error': True
                    })
            
            return stats
            
        except Exception as e:
            st.error(f"获取统计信息失败: {e}")
            return {
                'total_sheets': 0,
                'sheet_names': [],
                'file_size': 0,
                'sheets_info': []
            }
