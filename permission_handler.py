import pandas as pd
import streamlit as st
import io
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

class PermissionHandler:
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
        self.json_handler = JSONHandler()
    
    def validate_permission_file(self, file_content: bytes) -> Tuple[bool, str]:
        """验证权限表文件格式"""
        try:
            # 尝试读取Excel文件
            df = pd.read_excel(io.BytesIO(file_content))
            
            # 检查是否有数据
            if df.empty:
                return False, "文件为空"
            
            # 检查列数
            if len(df.columns) < 2:
                return False, "文件必须包含至少两列数据"
            
            # 获取前两列作为门店名称和查询编码
            store_col = df.iloc[:, 0]
            code_col = df.iloc[:, 1]
            
            # 检查是否有空值
            if store_col.isnull().any():
                return False, "门店名称列存在空值"
            
            if code_col.isnull().any():
                return False, "查询编码列存在空值"
            
            # 检查数据类型
            if not all(isinstance(x, (str, int, float)) for x in store_col):
                return False, "门店名称格式不正确"
            
            if not all(isinstance(x, (str, int, float)) for x in code_col):
                return False, "查询编码格式不正确"
            
            return True, "验证通过"
            
        except Exception as e:
            return False, f"文件解析失败: {str(e)}"
    
    def parse_permission_file(self, file_content: bytes) -> Tuple[bool, List[Dict[str, str]], str]:
        """解析权限表文件"""
        try:
            # 读取Excel文件
            df = pd.read_excel(io.BytesIO(file_content))
            
            # 获取前两列
            store_col = df.iloc[:, 0]
            code_col = df.iloc[:, 1]
            
            # 转换为权限记录列表
            permissions = []
            for i in range(len(df)):
                store_name = str(store_col.iloc[i]).strip()
                query_code = str(code_col.iloc[i]).strip()
                
                # 跳过空值
                if not store_name or not query_code or store_name == 'nan' or query_code == 'nan':
                    continue
                
                permissions.append({
                    'store': store_name,
                    'code': query_code
                })
            
            if not permissions:
                return False, [], "解析后没有有效的权限记录"
            
            return True, permissions, f"成功解析 {len(permissions)} 条权限记录"
            
        except Exception as e:
            return False, [], f"解析失败: {str(e)}"
    
    def get_file_statistics(self, file_content: bytes) -> Dict[str, Any]:
        """获取权限表文件统计信息"""
        try:
            df = pd.read_excel(io.BytesIO(file_content))
            
            # 基本统计
            total_rows = len(df)
            
            # 获取前两列
            store_col = df.iloc[:, 0]
            code_col = df.iloc[:, 1]
            
            # 去重统计
            unique_stores = store_col.nunique()
            unique_codes = code_col.nunique()
            
            # 有效记录数（排除空值）
            valid_records = len(df.dropna(subset=[df.columns[0], df.columns[1]]))
            
            return {
                'total_rows': total_rows,
                'valid_records': valid_records,
                'unique_stores': unique_stores,
                'unique_codes': unique_codes,
                'file_size': len(file_content),
                'columns': list(df.columns)
            }
            
        except Exception as e:
            st.error(f"获取文件统计失败: {str(e)}")
            return {}
    
    def upload_permission_file(self, file_content: bytes, file_name: str) -> Optional[str]:
        """上传权限表文件"""
        try:
            # 构造存储路径
            folder = "permissions"
            file_path = self.storage_handler.upload_file(file_content, file_name, folder)
            
            if file_path:
                st.success(f"权限表文件上传成功: {file_path}")
                return file_path
            else:
                st.error("权限表文件上传失败")
                return None
                
        except Exception as e:
            st.error(f"上传权限表文件失败: {str(e)}")
            return None
    
    def update_permissions(self, file_path: str, permissions: List[Dict[str, str]], 
                          file_name: str, file_size: int) -> bool:
        """更新权限表数据"""
        try:
            # 构造权限表信息
            permission_info = {
                'file_info': {
                    'file_name': file_name,
                    'file_path': file_path,
                    'upload_time': datetime.now().isoformat(),
                    'file_size': file_size,
                    'total_records': len(permissions)
                },
                'permissions': permissions
            }
            
            # 保存到JSON
            return self.json_handler.update_permissions(permission_info)
            
        except Exception as e:
            st.error(f"更新权限数据失败: {str(e)}")
            return False
    
    def get_current_permissions(self) -> Optional[Dict[str, Any]]:
        """获取当前权限表信息"""
        try:
            return self.json_handler.get_permissions()
        except Exception as e:
            st.error(f"获取权限信息失败: {str(e)}")
            return None
    
    def check_permission(self, store_name: str, query_code: str) -> bool:
        """检查门店-编码组合是否有权限"""
        try:
            permissions_data = self.get_current_permissions()
            if not permissions_data:
                return False
            
            permissions = permissions_data.get('permissions', [])
            
            # 检查是否存在匹配的权限记录
            for permission in permissions:
                if permission.get('store') == store_name and permission.get('code') == query_code:
                    return True
            
            return False
            
        except Exception as e:
            st.error(f"权限检查失败: {str(e)}")
            return False
    
    def get_permission_statistics(self) -> Dict[str, Any]:
        """获取权限统计信息"""
        try:
            permissions_data = self.get_current_permissions()
            if not permissions_data:
                return {
                    'total_records': 0,
                    'unique_stores': 0,
                    'unique_codes': 0,
                    'file_info': None,
                    'has_permissions': False
                }
            
            permissions = permissions_data.get('permissions', [])
            file_info = permissions_data.get('file_info', {})
            
            if not permissions:
                return {
                    'total_records': 0,
                    'unique_stores': 0,
                    'unique_codes': 0,
                    'file_info': file_info,
                    'has_permissions': False
                }
            
            # 统计唯一门店和编码
            stores = set(p.get('store') for p in permissions)
            codes = set(p.get('code') for p in permissions)
            
            return {
                'total_records': len(permissions),
                'unique_stores': len(stores),
                'unique_codes': len(codes),
                'file_info': file_info,
                'has_permissions': True,
                'stores': list(stores),
                'codes': list(codes)
            }
            
        except Exception as e:
            st.error(f"获取权限统计失败: {str(e)}")
            return {
                'total_records': 0,
                'unique_stores': 0,
                'unique_codes': 0,
                'file_info': None,
                'has_permissions': False
            }
    
    def get_permissions_preview(self, limit: int = 10) -> List[Dict[str, str]]:
        """获取权限记录预览"""
        try:
            permissions_data = self.get_current_permissions()
            if not permissions_data:
                return []
            
            permissions = permissions_data.get('permissions', [])
            return permissions[:limit]
            
        except Exception as e:
            st.error(f"获取权限预览失败: {str(e)}")
            return []
    
    def validate_permissions_with_stores(self, available_stores: List[str]) -> Dict[str, Any]:
        """验证权限表中的门店是否在汇总报表中存在"""
        try:
            permissions_data = self.get_current_permissions()
            if not permissions_data:
                return {
                    'valid': True,
                    'invalid_stores': [],
                    'orphaned_permissions': 0
                }
            
            permissions = permissions_data.get('permissions', [])
            
            # 检查权限表中的门店
            permission_stores = set(p.get('store') for p in permissions)
            available_stores_set = set(available_stores)
            
            # 找出不在汇总报表中的门店
            invalid_stores = permission_stores - available_stores_set
            
            # 统计孤立的权限记录
            orphaned_count = sum(1 for p in permissions 
                               if p.get('store') in invalid_stores)
            
            return {
                'valid': len(invalid_stores) == 0,
                'invalid_stores': list(invalid_stores),
                'orphaned_permissions': orphaned_count,
                'total_permission_stores': len(permission_stores),
                'available_stores': len(available_stores_set)
            }
            
        except Exception as e:
            st.error(f"权限验证失败: {str(e)}")
            return {
                'valid': False,
                'invalid_stores': [],
                'orphaned_permissions': 0
            }
    
    def clear_permissions(self) -> bool:
        """清空权限表"""
        try:
            return self.json_handler.clear_permissions()
        except Exception as e:
            st.error(f"清空权限失败: {str(e)}")
            return False
    
    def export_permissions(self) -> Optional[bytes]:
        """导出权限表为Excel"""
        try:
            permissions_data = self.get_current_permissions()
            if not permissions_data:
                return None
            
            permissions = permissions_data.get('permissions', [])
            if not permissions:
                return None
            
            # 转换为DataFrame
            df = pd.DataFrame(permissions)
            df = df.rename(columns={'store': '门店名称', 'code': '查询编码'})
            
            # 导出为Excel
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='权限表', index=False)
            
            return output.getvalue()
            
        except Exception as e:
            st.error(f"导出权限表失败: {str(e)}")
            return None
