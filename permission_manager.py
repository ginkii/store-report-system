"""
权限管理器 - 管理查询编号和门店访问权限
"""

import streamlit as st
import pandas as pd
import pymongo
from pymongo import MongoClient
from typing import List, Dict, Optional
import io
from config_manager import ConfigManager

class PermissionManager:
    """权限管理器"""
    
    def __init__(self, db):
        self.db = db
        self.permissions_collection = db['permissions']
        self.stores_collection = db['stores']
    
    def upload_permission_table(self, uploaded_file) -> Dict:
        """上传权限表"""
        try:
            # 读取Excel文件
            if uploaded_file.name.endswith('.xlsx') or uploaded_file.name.endswith('.xls'):
                df = pd.read_excel(uploaded_file)
            elif uploaded_file.name.endswith('.csv'):
                df = pd.read_csv(uploaded_file)
            else:
                return {"success": False, "message": "不支持的文件格式，请上传Excel(.xlsx/.xls)或CSV文件"}
            
            # 验证必要的列
            required_columns = ['查询编号', '门店名称']
            if not all(col in df.columns for col in required_columns):
                return {"success": False, "message": f"权限表必须包含以下列: {', '.join(required_columns)}"}
            
            # 处理权限数据
            results = {
                "success": True,
                "processed": 0,
                "created": 0,
                "updated": 0,
                "errors": []
            }
            
            # 处理每行权限数据（一对一关系）
            for _, row in df.iterrows():
                try:
                    query_code = str(row['查询编号']).strip()
                    store_name = str(row['门店名称']).strip()
                    
                    # 查找门店
                    store = self._find_store_by_name(store_name)
                    if not store:
                        results["errors"].append(f"未找到门店: {store_name}")
                        continue
                    
                    # 检查查询编号是否已被使用
                    existing = self.permissions_collection.find_one({'query_code': query_code})
                    
                    if existing:
                        # 更新现有记录
                        permission_doc = {
                            'query_code': query_code,
                            'store_id': store['_id'],
                            'store_name': store['store_name'],
                            'created_at': existing.get('created_at', pd.Timestamp.now()),
                            'updated_at': pd.Timestamp.now()
                        }
                        
                        self.permissions_collection.replace_one(
                            {'query_code': query_code},
                            permission_doc
                        )
                        results["updated"] += 1
                    else:
                        # 创建新记录
                        permission_doc = {
                            'query_code': query_code,
                            'store_id': store['_id'],
                            'store_name': store['store_name'],
                            'created_at': pd.Timestamp.now(),
                            'updated_at': pd.Timestamp.now()
                        }
                        
                        self.permissions_collection.insert_one(permission_doc)
                        results["created"] += 1
                    
                    results["processed"] += 1
                
                except Exception as e:
                    results["errors"].append(f"处理行数据时出错: {str(e)}")
            
            return results
            
        except Exception as e:
            return {"success": False, "message": f"处理文件时出错: {str(e)}"}
    
    def _find_store_by_name(self, store_name: str) -> Optional[Dict]:
        """根据门店名称查找门店"""
        try:
            # 精确匹配门店名称
            store = self.stores_collection.find_one({'store_name': store_name})
            if store:
                return store
            
            # 模糊匹配（去掉可能的前缀后缀）
            clean_name = store_name.replace('犀牛百货', '').replace('门店', '').replace('店', '').strip()
            if clean_name:
                # 使用正则表达式进行模糊匹配
                stores = list(self.stores_collection.find({
                    '$or': [
                        {'store_name': {'$regex': clean_name, '$options': 'i'}},
                        {'aliases': {'$in': [store_name, clean_name]}}
                    ]
                }))
                if stores:
                    return stores[0]  # 返回第一个匹配的
            
            return None
            
        except Exception as e:
            st.error(f"查找门店时出错: {e}")
            return None
    
    def get_all_permissions(self) -> List[Dict]:
        """获取所有权限配置"""
        try:
            permissions = list(self.permissions_collection.find().sort('query_code', 1))
            return permissions
        except Exception as e:
            st.error(f"获取权限配置失败: {e}")
            return []
    
    def delete_permission(self, query_code: str) -> bool:
        """删除权限配置"""
        try:
            result = self.permissions_collection.delete_one({'query_code': query_code})
            return result.deleted_count > 0
        except Exception as e:
            st.error(f"删除权限配置失败: {e}")
            return False
    
    def create_sample_permission_table(self) -> bytes:
        """创建示例权限表（一对一关系）"""
        sample_data = {
            '查询编号': ['QC001', 'QC002', 'QC003', 'QC004'],
            '门店名称': ['犀牛百货滨江店', '犀牛百货西湖店', '犀牛百货萧山店', '犀牛百货余杭店'],
            '说明': ['滨江店查询编号', '西湖店查询编号', '萧山店查询编号', '余杭店查询编号']
        }
        
        df = pd.DataFrame(sample_data)
        
        # 创建Excel文件
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='权限表', index=False)
        
        return output.getvalue()

def create_permission_interface():
    """创建权限管理界面"""
    st.title("🔐 权限管理系统")
    
    # 初始化数据库连接
    @st.cache_resource
    def init_mongodb():
        try:
            mongodb_config = ConfigManager.get_mongodb_config()
            client = MongoClient(mongodb_config['uri'])
            db = client[mongodb_config['database_name']]
            db.command('ping')
            return db
        except Exception as e:
            st.error(f"数据库连接失败: {e}")
            return None
    
    db = init_mongodb()
    if db is None:
        st.stop()
    
    permission_manager = PermissionManager(db)
    
    # 创建标签页
    tab1, tab2, tab3 = st.tabs(["📤 上传权限表", "📋 权限配置", "📥 下载模板"])
    
    with tab1:
        st.subheader("上传权限表")
        st.info("上传包含查询编号和门店名称对应关系的Excel或CSV文件")
        
        uploaded_file = st.file_uploader(
            "选择权限表文件",
            type=['xlsx', 'xls', 'csv'],
            help="文件必须包含'查询编号'和'门店名称'两列"
        )
        
        if uploaded_file is not None:
            # 显示文件预览
            try:
                if uploaded_file.name.endswith('.csv'):
                    preview_df = pd.read_csv(uploaded_file)
                else:
                    preview_df = pd.read_excel(uploaded_file)
                
                st.subheader("文件预览")
                st.dataframe(preview_df.head(10))
                
                # 上传按钮
                if st.button("开始上传", type="primary"):
                    with st.spinner("正在处理权限表..."):
                        # 重置文件指针
                        uploaded_file.seek(0)
                        result = permission_manager.upload_permission_table(uploaded_file)
                    
                    if result["success"]:
                        st.success("权限表上传成功！")
                        
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("处理记录数", result["processed"])
                        with col2:
                            st.metric("新建权限", result["created"])
                        with col3:
                            st.metric("更新权限", result["updated"])
                        
                        if result["errors"]:
                            st.warning("处理过程中出现以下问题：")
                            for error in result["errors"]:
                                st.write(f"- {error}")
                    else:
                        st.error(f"上传失败: {result['message']}")
                        
            except Exception as e:
                st.error(f"文件预览失败: {e}")
    
    with tab2:
        st.subheader("当前权限配置")
        
        permissions = permission_manager.get_all_permissions()
        
        if permissions:
            for perm in permissions:
                with st.expander(f"查询编号: {perm['query_code']} → {perm['store_name']}"):
                    st.write(f"**门店名称:** {perm['store_name']}")
                    st.write(f"**门店ID:** {perm['store_id']}")
                    st.write(f"**创建时间:** {perm.get('created_at', 'N/A')}")
                    st.write(f"**更新时间:** {perm.get('updated_at', 'N/A')}")
                    
                    if st.button(f"删除权限", key=f"delete_{perm['query_code']}"):
                        if permission_manager.delete_permission(perm['query_code']):
                            st.success("权限配置已删除")
                            st.rerun()
                        else:
                            st.error("删除失败")
        else:
            st.info("暂无权限配置")
    
    with tab3:
        st.subheader("下载权限表模板")
        st.info("下载示例权限表模板，了解正确的文件格式")
        
        if st.button("生成模板文件"):
            sample_file = permission_manager.create_sample_permission_table()
            st.download_button(
                label="📥 下载权限表模板",
                data=sample_file,
                file_name="权限表模板.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        
        st.subheader("文件格式说明")
        st.markdown("""
        权限表应包含以下列：
        - **查询编号**: 用户输入的查询编号
        - **门店名称**: 对应的门店名称
        - **说明** (可选): 权限说明
        
        **注意事项:**
        - 一个查询编号只能对应一个门店（一对一关系）
        - 门店名称必须与系统中的门店名称完全匹配
        - 支持Excel(.xlsx/.xls)和CSV格式
        - 如果查询编号重复，后面的记录会覆盖前面的记录
        """)

if __name__ == "__main__":
    create_permission_interface()