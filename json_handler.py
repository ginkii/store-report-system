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
            
            # 更新门店列表（只保存基本信息）
            store_sheet_list = []
            for sheet_name in store_sheets:
                store_sheet_list.append({
                    "sheet_name": sheet_name,
                    "query_count": 0,
                    "last_query_time": None,
                    "indexed": False,  # 标记是否已建立索引
                    "cache_hits": 0,   # 缓存命中次数
                    "last_accessed": None  # 最后访问时间
                })
            
            data["store_sheets"] = store_sheet_list
            
            return self._save_data(data)
            
        except Exception as e:
            st.error(f"更新当前报表失败: {str(e)}")
            return False
    
    def update_sheet_index(self, sheet_name: str, index_info: Dict[str, Any]) -> bool:
        """更新工作表索引信息"""
        try:
            data = self._load_data()
            store_sheets = data.get("store_sheets", [])
            
            # 找到对应的工作表并更新索引信息
            for sheet in store_sheets:
                if sheet["sheet_name"] == sheet_name:
                    sheet["indexed"] = True
                    sheet["index_info"] = index_info
                    sheet["indexed_time"] = datetime.now().isoformat()
                    break
            
            data["store_sheets"] = store_sheets
            return self._save_data(data)
            
        except Exception as e:
            st.error(f"更新工作表索引失败: {str(e)}")
            return False
    
    def get_sheet_index(self, sheet_name: str) -> Optional[Dict[str, Any]]:
        """获取工作表索引信息"""
        try:
            data = self._load_data()
            store_sheets = data.get("store_sheets", [])
            
            for sheet in store_sheets:
                if sheet["sheet_name"] == sheet_name:
                    return sheet.get("index_info")
            
            return None
            
        except Exception as e:
            st.error(f"获取工作表索引失败: {str(e)}")
            return None
    
    def update_sheet_access_stats(self, sheet_name: str) -> bool:
        """更新工作表访问统计"""
        try:
            data = self._load_data()
            store_sheets = data.get("store_sheets", [])
            
            for sheet in store_sheets:
                if sheet["sheet_name"] == sheet_name:
                    sheet["query_count"] = sheet.get("query_count", 0) + 1
                    sheet["last_query_time"] = datetime.now().isoformat()
                    sheet["last_accessed"] = datetime.now().isoformat()
                    break
            
            data["store_sheets"] = store_sheets
            return self._save_data(data)
            
        except Exception as e:
            st.error(f"更新工作表访问统计失败: {str(e)}")
            return False
    
    def get_most_accessed_sheets(self, limit: int = 10) -> List[Dict[str, Any]]:
        """获取最常访问的工作表"""
        try:
            data = self._load_data()
            store_sheets = data.get("store_sheets", [])
            
            # 按查询次数排序
            sorted_sheets = sorted(
                store_sheets,
                key=lambda x: x.get("query_count", 0),
                reverse=True
            )
            
            return sorted_sheets[:limit]
            
        except Exception as e:
            st.error(f"获取最常访问工作表失败: {str(e)}")
            return []
    
    def get_indexing_progress(self) -> Dict[str, Any]:
        """获取索引进度"""
        try:
            data = self._load_data()
            store_sheets = data.get("store_sheets", [])
            
            total_sheets = len(store_sheets)
            indexed_sheets = sum(1 for sheet in store_sheets if sheet.get("indexed", False))
            
            return {
                "total_sheets": total_sheets,
                "indexed_sheets": indexed_sheets,
                "progress_percentage": (indexed_sheets / total_sheets * 100) if total_sheets > 0 else 0,
                "remaining_sheets": total_sheets - indexed_sheets
            }
            
        except Exception as e:
            st.error(f"获取索引进度失败: {str(e)}")
            return {
                "total_sheets": 0,
                "indexed_sheets": 0,
                "progress_percentage": 0,
                "remaining_sheets": 0
            }
    
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
    
    def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态（包括数据加载状态、文件可访问性等）"""
        try:
            data = self._load_data()
            current_report = data.get("current_report")
            store_sheets = data.get("store_sheets", [])
            
            # 检查数据是否加载成功
            data_loaded = bool(data)
            
            # 检查是否有当前报表
            has_current_report = current_report is not None
            
            # 检查文件是否可访问（这里简化处理，实际可能需要调用storage_handler）
            file_accessible = has_current_report
            if has_current_report:
                # 检查文件路径是否存在
                file_path = current_report.get('file_path') or current_report.get('cos_file_path')
                file_accessible = bool(file_path)
            
            # 检查存储连接状态（这里简化为文件系统可用性）
            storage_connection = os.path.exists(self.data_file)
            
            return {
                "data_loaded": data_loaded,
                "has_current_report": has_current_report,
                "file_accessible": file_accessible,
                "cos_connection": storage_connection,  # 保持兼容性
                "storage_connection": storage_connection,
                "current_report": current_report,
                "store_sheets_count": len(store_sheets),
                "stores_count": len(store_sheets),
                "total_queries": sum(sheet.get("query_count", 0) for sheet in store_sheets),
                "history_count": len(data.get("report_history", [])),
                "last_updated": data.get("system_info", {}).get("last_updated"),
                "system_time": datetime.now().isoformat(),
                "storage_type": "LOCAL"  # 默认本地存储
            }
            
        except Exception as e:
            st.error(f"获取系统状态失败: {str(e)}")
            return {
                "data_loaded": False,
                "has_current_report": False,
                "file_accessible": False,
                "cos_connection": False,
                "storage_connection": False,
                "current_report": None,
                "store_sheets_count": 0,
                "stores_count": 0,
                "total_queries": 0,
                "history_count": 0,
                "last_updated": None,
                "system_time": datetime.now().isoformat(),
                "storage_type": "LOCAL"
            }
    
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
    
    def backup_data(self) -> bool:
        """备份数据"""
        try:
            data = self._load_data()
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_filename = f"data_backup_{timestamp}.json"
            backup_path = os.path.join(self.temp_dir, backup_filename)
            
            with open(backup_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            # 更新系统信息记录备份
            self.update_system_info({"last_backup": datetime.now().isoformat()})
            
            return True
            
        except Exception as e:
            st.error(f"备份数据失败: {str(e)}")
            return False
    
    def restore_from_backup(self) -> bool:
        """从最新备份恢复数据"""
        try:
            # 查找最新的备份文件
            backup_files = []
            for filename in os.listdir(self.temp_dir):
                if filename.startswith("data_backup_") and filename.endswith(".json"):
                    backup_path = os.path.join(self.temp_dir, filename)
                    backup_files.append((backup_path, os.path.getmtime(backup_path)))
            
            if not backup_files:
                st.error("未找到备份文件")
                return False
            
            # 按修改时间排序，获取最新的备份
            backup_files.sort(key=lambda x: x[1], reverse=True)
            latest_backup = backup_files[0][0]
            
            # 恢复数据
            with open(latest_backup, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if self._save_data(data):
                st.success(f"已从备份恢复数据: {os.path.basename(latest_backup)}")
                return True
            else:
                return False
            
        except Exception as e:
            st.error(f"恢复数据失败: {str(e)}")
            return False
    
    def restore_data(self, backup_path: str) -> bool:
        """从指定备份恢复数据"""
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
