import json
import os
import streamlit as st
from datetime import datetime
from typing import Dict, List, Any, Optional
import tempfile

class JSONHandler:
    def __init__(self):
        # 使用临时目录存储 JSON 数据
        self.temp_dir = tempfile.mkdtemp(prefix="store_reports_json_")
        self.data_file = os.path.join(self.temp_dir, "data.json")
        
        # 初始化数据文件
        self._initialize_data_file()
    
    def _initialize_data_file(self):
        """初始化数据文件"""
        if not os.path.exists(self.data_file):
            initial_data = {
                "current_report": None,
                "store_sheets": [],
                "report_history": [],
                "permission_table": None,
                "system_info": {
                    "created_time": datetime.now().isoformat(),
                    "last_updated": None,
                    "version": "1.0"
                }
            }
            self._save_data(initial_data)
    
    def _load_data(self) -> Dict[str, Any]:
        """加载数据"""
        try:
            if os.path.exists(self.data_file):
                with open(self.data_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                self._initialize_data_file()
                return self._load_data()
        except Exception as e:
            st.error(f"加载数据失败: {str(e)}")
            return {}
    
    def _save_data(self, data: Dict[str, Any]) -> bool:
        """保存数据"""
        try:
            # 更新最后修改时间
            if "system_info" in data:
                data["system_info"]["last_updated"] = datetime.now().isoformat()
            
            with open(self.data_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            return True
        except Exception as e:
            st.error(f"保存数据失败: {str(e)}")
            return False
    
    def get_current_report(self) -> Optional[Dict[str, Any]]:
        """获取当前报表信息"""
        try:
            data = self._load_data()
            return data.get("current_report")
        except Exception as e:
            st.error(f"获取当前报表失败: {str(e)}")
            return None
    
    def update_current_report(self, report_info: Dict[str, Any], store_sheets: List[str]) -> bool:
        """更新当前报表信息"""
        try:
            data = self._load_data()
            
            # 归档当前报表到历史记录
            if data.get("current_report"):
                old_report = data["current_report"].copy()
                old_report["status"] = "archived"
                old_report["archived_time"] = datetime.now().isoformat()
                
                if "report_history" not in data:
                    data["report_history"] = []
                data["report_history"].append(old_report)
            
            # 设置新的当前报表
            report_info["upload_time"] = datetime.now().isoformat()
            report_info["status"] = "active"
            data["current_report"] = report_info
            
            # 更新门店列表
            store_sheet_list = []
            for sheet_name in store_sheets:
                store_sheet_list.append({
                    "sheet_name": sheet_name,
                    "query_count": 0,
                    "last_query_time": None
                })
            
            data["store_sheets"] = store_sheet_list
            
            return self._save_data(data)
            
        except Exception as e:
            st.error(f"更新当前报表失败: {str(e)}")
            return False
    
    def get_store_sheets(self) -> List[Dict[str, Any]]:
        """获取门店工作表列表"""
        try:
            data = self._load_data()
            return data.get("store_sheets", [])
        except Exception as e:
            st.error(f"获取门店列表失败: {str(e)}")
            return []
    
    def update_store_sheets(self, store_sheets: List[Dict[str, Any]]) -> bool:
        """更新门店工作表列表"""
        try:
            data = self._load_data()
            data["store_sheets"] = store_sheets
            return self._save_data(data)
        except Exception as e:
            st.error(f"更新门店列表失败: {str(e)}")
            return False
    
    def get_report_history(self) -> List[Dict[str, Any]]:
        """获取报表历史记录"""
        try:
            data = self._load_data()
            return data.get("report_history", [])
        except Exception as e:
            st.error(f"获取报表历史失败: {str(e)}")
            return []
    
    def add_report_to_history(self, report_info: Dict[str, Any]) -> bool:
        """添加报表到历史记录"""
        try:
            data = self._load_data()
            
            if "report_history" not in data:
                data["report_history"] = []
            
            report_info["archived_time"] = datetime.now().isoformat()
            report_info["status"] = "archived"
            data["report_history"].append(report_info)
            
            return self._save_data(data)
            
        except Exception as e:
            st.error(f"添加历史记录失败: {str(e)}")
            return False
    
    def get_system_info(self) -> Dict[str, Any]:
        """获取系统信息"""
        try:
            data = self._load_data()
            return data.get("system_info", {})
        except Exception as e:
            st.error(f"获取系统信息失败: {str(e)}")
            return {}
    
    def update_system_info(self, system_info: Dict[str, Any]) -> bool:
        """更新系统信息"""
        try:
            data = self._load_data()
            
            if "system_info" not in data:
                data["system_info"] = {}
            
            data["system_info"].update(system_info)
            return self._save_data(data)
            
        except Exception as e:
            st.error(f"更新系统信息失败: {str(e)}")
            return False
    
    def clear_all_data(self) -> bool:
        """清空所有数据"""
        try:
            if os.path.exists(self.data_file):
                os.remove(self.data_file)
            self._initialize_data_file()
            return True
        except Exception as e:
            st.error(f"清空数据失败: {str(e)}")
            return False
    
    def export_data(self) -> Optional[Dict[str, Any]]:
        """导出所有数据"""
        try:
            return self._load_data()
        except Exception as e:
            st.error(f"导出数据失败: {str(e)}")
            return None
    
    def import_data(self, data: Dict[str, Any]) -> bool:
        """导入数据"""
        try:
            # 验证数据结构
            required_keys = ["current_report", "store_sheets", "report_history", "system_info"]
            for key in required_keys:
                if key not in data:
                    data[key] = [] if key != "system_info" else {}
            
            return self._save_data(data)
            
        except Exception as e:
            st.error(f"导入数据失败: {str(e)}")
            return False
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取数据统计"""
        try:
            data = self._load_data()
            
            store_sheets = data.get("store_sheets", [])
            report_history = data.get("report_history", [])
            
            total_queries = sum(store.get("query_count", 0) for store in store_sheets)
            
            return {
                "total_stores": len(store_sheets),
                "total_queries": total_queries,
                "total_reports": len(report_history) + (1 if data.get("current_report") else 0),
                "active_stores": len([s for s in store_sheets if s.get("query_count", 0) > 0]),
                "last_updated": data.get("system_info", {}).get("last_updated"),
                "data_file_size": os.path.getsize(self.data_file) if os.path.exists(self.data_file) else 0
            }
            
        except Exception as e:
            st.error(f"获取统计信息失败: {str(e)}")
            return {}
    
    def backup_data(self) -> Optional[str]:
        """备份数据"""
        try:
            data = self._load_data()
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_filename = f"data_backup_{timestamp}.json"
            backup_path = os.path.join(self.temp_dir, backup_filename)
            
            with open(backup_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            return backup_path
            
        except Exception as e:
            st.error(f"备份数据失败: {str(e)}")
            return None
    
    def restore_data(self, backup_path: str) -> bool:
        """恢复数据"""
        try:
            if not os.path.exists(backup_path):
                st.error("备份文件不存在")
                return False
            
            with open(backup_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            return self._save_data(data)
            
        except Exception as e:
            st.error(f"恢复数据失败: {str(e)}")
            return False
    
    def get_permissions(self) -> Optional[Dict[str, Any]]:
        """获取权限表数据"""
        try:
            data = self._load_data()
            return data.get("permission_table")
        except Exception as e:
            st.error(f"获取权限数据失败: {str(e)}")
            return None
    
    def update_permissions(self, permission_info: Dict[str, Any]) -> bool:
        """更新权限表数据"""
        try:
            data = self._load_data()
            data["permission_table"] = permission_info
            return self._save_data(data)
        except Exception as e:
            st.error(f"更新权限数据失败: {str(e)}")
            return False
    
    def clear_permissions(self) -> bool:
        """清空权限表数据"""
        try:
            data = self._load_data()
            data["permission_table"] = None
            return self._save_data(data)
        except Exception as e:
            st.error(f"清空权限数据失败: {str(e)}")
            return False
    
    def get_permissions_summary(self) -> Dict[str, Any]:
        """获取权限表摘要信息"""
        try:
            data = self._load_data()
            permission_table = data.get("permission_table")
            
            if not permission_table:
                return {
                    "has_permissions": False,
                    "total_records": 0,
                    "file_info": None
                }
            
            permissions = permission_table.get("permissions", [])
            file_info = permission_table.get("file_info", {})
            
            return {
                "has_permissions": True,
                "total_records": len(permissions),
                "file_info": file_info
            }
        except Exception as e:
            st.error(f"获取权限摘要失败: {str(e)}")
            return {
                "has_permissions": False,
                "total_records": 0,
                "file_info": None
            }
