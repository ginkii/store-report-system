import json
import fcntl
import os
import time
from contextlib import contextmanager
from typing import Dict, List, Any, Optional
from datetime import datetime
import streamlit as st
from config import APP_CONFIG

class JSONHandler:
    def __init__(self, data_file: str = None):
        self.data_file = data_file or APP_CONFIG['data_file']
        self.init_data_file()
    
    def init_data_file(self):
        """初始化数据文件"""
        if not os.path.exists(self.data_file):
            initial_data = {
                "current_report": None,
                "store_sheets": [],
                "report_history": [],
                "last_updated": None
            }
            self.save_data(initial_data)
    
    @contextmanager
    def _file_lock(self, mode='r'):
        """文件锁上下文管理器"""
        max_retries = 5
        retry_delay = 0.1
        
        for attempt in range(max_retries):
            try:
                with open(self.data_file, mode, encoding='utf-8') as f:
                    fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                    yield f
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)
                return
            except (OSError, IOError) as e:
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (2 ** attempt))
                    continue
                else:
                    st.error(f"文件操作失败: {e}")
                    raise
    
    def load_data(self) -> Dict[str, Any]:
        """加载数据"""
        try:
            with self._file_lock('r') as f:
                data = json.load(f)
                return data
        except (FileNotFoundError, json.JSONDecodeError) as e:
            st.error(f"加载数据失败: {e}")
            return {
                "current_report": None,
                "store_sheets": [],
                "report_history": [],
                "last_updated": None
            }
    
    def save_data(self, data: Dict[str, Any]) -> bool:
        """保存数据"""
        try:
            with self._file_lock('w') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            return True
        except Exception as e:
            st.error(f"保存数据失败: {e}")
            return False
    
    def get_current_report(self) -> Optional[Dict[str, Any]]:
        """获取当前活跃报表"""
        data = self.load_data()
        return data.get('current_report')
    
    def get_store_sheets(self) -> List[Dict[str, Any]]:
        """获取门店sheet列表"""
        data = self.load_data()
        return data.get('store_sheets', [])
    
    def get_store_names(self) -> List[str]:
        """获取门店名称列表"""
        store_sheets = self.get_store_sheets()
        return [store['sheet_name'] for store in store_sheets]
    
    def update_current_report(self, report_info: Dict[str, Any], store_sheets: List[str]) -> bool:
        """更新当前报表信息"""
        data = self.load_data()
        
        # 如果有现有报表，移到历史记录
        if data['current_report']:
            historical_report = data['current_report'].copy()
            historical_report['status'] = 'archived'
            historical_report['archived_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            data['report_history'].append(historical_report)
        
        # 设置新的当前报表
        data['current_report'] = report_info
        data['current_report']['upload_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        data['current_report']['status'] = 'active'
        
        # 更新门店sheet列表
        data['store_sheets'] = [
            {
                'sheet_name': sheet_name,
                'last_query_time': None,
                'query_count': 0
            }
            for sheet_name in store_sheets
        ]
        
        data['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        return self.save_data(data)
    
    def update_query_stats(self, store_name: str) -> bool:
        """更新查询统计"""
        data = self.load_data()
        
        for store in data['store_sheets']:
            if store['sheet_name'] == store_name:
                store['last_query_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                store['query_count'] = store.get('query_count', 0) + 1
                break
        
        return self.save_data(data)
    
    def get_report_history(self) -> List[Dict[str, Any]]:
        """获取报表历史记录"""
        data = self.load_data()
        return data.get('report_history', [])
    
    def delete_report(self, report_id: str) -> bool:
        """删除报表（仅从历史记录中删除）"""
        data = self.load_data()
        
        # 不能删除当前活跃报表
        if data['current_report'] and data['current_report'].get('id') == report_id:
            return False
        
        # 从历史记录中删除
        data['report_history'] = [
            report for report in data['report_history'] 
            if report.get('id') != report_id
        ]
        
        return self.save_data(data)
    
    def get_system_stats(self) -> Dict[str, Any]:
        """获取系统统计信息"""
        data = self.load_data()
        
        total_queries = sum(store.get('query_count', 0) for store in data['store_sheets'])
        store_count = len(data['store_sheets'])
        
        return {
            'total_stores': store_count,
            'total_queries': total_queries,
            'current_report': data['current_report'],
            'last_updated': data.get('last_updated'),
            'history_count': len(data['report_history'])
        }
