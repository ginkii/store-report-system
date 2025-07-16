import streamlit as st
import pandas as pd
import io
import json
import hashlib
from datetime import datetime, timedelta
import time
from qcloud_cos import CosConfig
from qcloud_cos import CosS3Client
from qcloud_cos.cos_exception import CosServiceError, CosClientError
from supabase import create_client, Client
import logging
from typing import Optional, Dict, Any, List
import traceback

# 页面配置
st.set_page_config(
    page_title="门店报表查询系统", 
    page_icon="📊",
    layout="wide"
)

# 系统配置
ADMIN_PASSWORD = st.secrets.get("system", {}).get("admin_password", "admin123")

# CSS样式
st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        padding: 1rem 0;
        margin-bottom: 2rem;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .store-info {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1.5rem;
        border-radius: 15px;
        margin: 1rem 0;
        box-shadow: 0 8px 16px rgba(0, 0, 0, 0.1);
    }
    .admin-panel {
        background: linear-gradient(135deg, #ffeaa7 0%, #fab1a0 100%);
        padding: 1.5rem;
        border-radius: 15px;
        border: 2px solid #fdcb6e;
        margin: 1rem 0;
    }
    .architecture-info {
        background: linear-gradient(135deg, #a8edea 0%, #fed6e3 100%);
        padding: 1.5rem;
        border-radius: 15px;
        margin: 1rem 0;
        border: 2px solid #48cab2;
    }
    .success-box {
        background: linear-gradient(135deg, #84fab0 0%, #8fd3f4 100%);
        padding: 1rem;
        border-radius: 10px;
        margin: 1rem 0;
        color: #2d3436;
    }
    .warning-box {
        background: linear-gradient(135deg, #fdcb6e 0%, #e17055 100%);
        padding: 1rem;
        border-radius: 10px;
        margin: 1rem 0;
        color: white;
    }
    </style>
""", unsafe_allow_html=True)

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TencentCOSManager:
    """腾讯云COS存储管理器"""
    
    def __init__(self):
        self.client = None
        self.bucket_name = None
        self.region = None
        self.initialize_from_secrets()
    
    def initialize_from_secrets(self):
        """从Streamlit Secrets初始化"""
        try:
            if "tencent_cos" not in st.secrets:
                raise Exception("未找到腾讯云COS配置")
            
            config = st.secrets["tencent_cos"]
            secret_id = config.get("secret_id")
            secret_key = config.get("secret_key")
            self.region = config.get("region", "ap-beijing")
            self.bucket_name = config.get("bucket_name")
            
            if not all([secret_id, secret_key, self.bucket_name]):
                raise Exception("腾讯云COS配置不完整")
            
            # 配置COS客户端
            cos_config = CosConfig(
                Region=self.region,
                SecretId=secret_id,
                SecretKey=secret_key
            )
            
            self.client = CosS3Client(cos_config)
            logger.info("腾讯云COS客户端初始化成功")
            
        except Exception as e:
            logger.error(f"腾讯云COS初始化失败: {str(e)}")
            raise
    
    def upload_file(self, file_data: bytes, filename: str) -> Optional[str]:
        """上传文件到腾讯云COS"""
        try:
            # 上传文件
            response = self.client.put_object(
                Bucket=self.bucket_name,
                Body=file_data,
                Key=filename,
                ContentType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
            
            # 生成文件URL
            file_url = f"https://{self.bucket_name}.cos.{self.region}.myqcloud.com/{filename}"
            
            logger.info(f"文件上传成功: {filename}")
            return file_url
            
        except CosServiceError as e:
            logger.error(f"COS服务错误: {e.get_error_msg()}")
            raise Exception(f"文件上传失败: {e.get_error_msg()}")
        except CosClientError as e:
            logger.error(f"COS客户端错误: {str(e)}")
            raise Exception(f"文件上传失败: {str(e)}")
        except Exception as e:
            logger.error(f"上传文件时出错: {str(e)}")
            raise Exception(f"文件上传失败: {str(e)}")
    
    def download_file(self, filename: str) -> Optional[bytes]:
        """从腾讯云COS下载文件"""
        try:
            response = self.client.get_object(
                Bucket=self.bucket_name,
                Key=filename
            )
            
            # 读取文件内容
            file_data = response['Body'].read()
            logger.info(f"文件下载成功: {filename}")
            return file_data
            
        except CosServiceError as e:
            logger.error(f"COS服务错误: {e.get_error_msg()}")
            return None
        except CosClientError as e:
            logger.error(f"COS客户端错误: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"下载文件时出错: {str(e)}")
            return None
    
    def delete_file(self, filename: str) -> bool:
        """删除腾讯云COS文件"""
        try:
            self.client.delete_object(
                Bucket=self.bucket_name,
                Key=filename
            )
            
            logger.info(f"文件删除成功: {filename}")
            return True
            
        except CosServiceError as e:
            logger.error(f"COS服务错误: {e.get_error_msg()}")
            return False
        except CosClientError as e:
            logger.error(f"COS客户端错误: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"删除文件时出错: {str(e)}")
            return False
    
    def list_files(self) -> List[Dict]:
        """列出存储桶中的所有文件"""
        try:
            response = self.client.list_objects(
                Bucket=self.bucket_name,
                MaxKeys=1000
            )
            
            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    files.append({
                        'filename': obj['Key'],
                        'size': obj['Size'],
                        'last_modified': obj['LastModified']
                    })
            
            return files
            
        except Exception as e:
            logger.error(f"列出文件时出错: {str(e)}")
            return []
    
    def get_storage_usage(self) -> Dict:
        """获取存储使用情况"""
        try:
            files = self.list_files()
            total_size = sum(f['size'] for f in files)
            
            return {
                'file_count': len(files),
                'total_size_bytes': total_size,
                'total_size_mb': total_size / (1024 * 1024),
                'files': files
            }
            
        except Exception as e:
            logger.error(f"获取存储使用情况失败: {str(e)}")
            return {'file_count': 0, 'total_size_mb': 0, 'files': []}

class SupabaseManager:
    """Supabase数据库管理器"""
    
    def __init__(self):
        self.supabase: Optional[Client] = None
        self.initialize_from_secrets()
    
    def initialize_from_secrets(self):
        """从Streamlit Secrets初始化"""
        try:
            if "supabase" not in st.secrets:
                raise Exception("未找到Supabase配置")
            
            config = st.secrets["supabase"]
            url = config.get("url")
            key = config.get("anon_key")
            
            if not url or not key:
                raise Exception("Supabase配置不完整")
            
            self.supabase = create_client(url, key)
            logger.info("Supabase客户端初始化成功")
            
        except Exception as e:
            logger.error(f"Supabase初始化失败: {str(e)}")
            raise
    
    def save_permissions(self, permissions_data: List[Dict]) -> bool:
        """保存权限数据"""
        try:
            # 清空现有数据
            self.supabase.table("permissions").delete().neq("id", 0).execute()
            
            # 插入新数据
            if permissions_data:
                result = self.supabase.table("permissions").insert(permissions_data).execute()
                return len(result.data) > 0
            return True
            
        except Exception as e:
            logger.error(f"保存权限数据失败: {str(e)}")
            return False
    
    def load_permissions(self) -> List[Dict]:
        """加载权限数据"""
        try:
            result = self.supabase.table("permissions").select("*").execute()
            return result.data
            
        except Exception as e:
            logger.error(f"加载权限数据失败: {str(e)}")
            return []
    
    def save_report_metadata(self, report_data: Dict) -> bool:
        """保存报表元数据"""
        try:
            # 检查是否已存在
            existing = self.supabase.table("reports").select("*").eq("store_name", report_data["store_name"]).execute()
            
            if existing.data:
                # 更新现有记录
                result = self.supabase.table("reports").update(report_data).eq("store_name", report_data["store_name"]).execute()
            else:
                # 插入新记录
                result = self.supabase.table("reports").insert(report_data).execute()
            
            return len(result.data) > 0
            
        except Exception as e:
            logger.error(f"保存报表元数据失败: {str(e)}")
            return False
    
    def load_report_metadata(self, store_name: str = None) -> List[Dict]:
        """加载报表元数据"""
        try:
            query = self.supabase.table("reports").select("*")
            
            if store_name:
                query = query.eq("store_name", store_name)
            
            result = query.execute()
            return result.data
            
        except Exception as e:
            logger.error(f"加载报表元数据失败: {str(e)}")
            return []
    
    def delete_report_metadata(self, report_id: int) -> bool:
        """删除报表元数据"""
        try:
            result = self.supabase.table("reports").delete().eq("id", report_id).execute()
            return len(result.data) > 0
            
        except Exception as e:
            logger.error(f"删除报表元数据失败: {str(e)}")
            return False

class TencentSupabaseSystem:
    """腾讯云+Supabase混合存储系统"""
    
    def __init__(self):
        self.cos_manager = TencentCOSManager()
        self.database = SupabaseManager()
        self.initialized = True
    
    def show_architecture_info(self):
        """显示架构信息"""
        st.markdown('''
        <div class="architecture-info">
        <h4>🏗️ 腾讯云 + Supabase 混合架构</h4>
        <p><strong>📦 腾讯云COS</strong>: 存储Excel文件 (50GB永久免费)</p>
        <p><strong>🗄️ Supabase</strong>: 存储权限、元数据、分析结果 (500MB免费)</p>
        <p><strong>💫 优势</strong>: 中国用户优化 + 大文件支持 + 快速查询 + 微信支付</p>
        </div>
        ''', unsafe_allow_html=True)
    
    def upload_and_process_permissions(self, uploaded_file) -> bool:
        """上传并处理权限文件"""
        try:
            # 读取Excel文件
            df = pd.read_excel(uploaded_file)
            
            if len(df.columns) < 2:
                st.error("❌ 权限文件格式错误：需要至少两列（门店名称、人员编号）")
                return False
            
            # 转换为数据库格式
            permissions_data = []
            for _, row in df.iterrows():
                store_name = str(row.iloc[0]).strip()
                user_id = str(row.iloc[1]).strip()
                
                if store_name and user_id and store_name != 'nan' and user_id != 'nan':
                    permissions_data.append({
                        "store_name": store_name,
                        "user_id": user_id,
                        "created_at": datetime.now().isoformat(),
                        "updated_at": datetime.now().isoformat()
                    })
            
            # 保存到数据库
            success = self.database.save_permissions(permissions_data)
            
            if success:
                st.success(f"✅ 权限数据保存成功：{len(permissions_data)} 条记录")
                return True
            else:
                st.error("❌ 权限数据保存失败")
                return False
                
        except Exception as e:
            st.error(f"❌ 处理权限文件失败：{str(e)}")
            logger.error(f"处理权限文件失败: {str(e)}")
            return False
    
    def upload_and_process_reports(self, uploaded_file) -> bool:
        """上传并处理报表文件"""
        try:
            file_size_mb = len(uploaded_file.getvalue()) / 1024 / 1024
            st.info(f"📄 文件大小: {file_size_mb:.2f} MB")
            
            # 生成唯一文件名
            timestamp = int(time.time())
            file_hash = hashlib.md5(uploaded_file.getvalue()).hexdigest()[:8]
            filename = f"reports_{timestamp}_{file_hash}.xlsx"
            
            # 先清理旧数据
            with st.spinner("正在清理旧数据..."):
                self._cleanup_old_reports()
            
            # 上传原始文件到腾讯云COS
            with st.spinner("正在上传文件到腾讯云COS..."):
                file_url = self.cos_manager.upload_file(uploaded_file.getvalue(), filename)
                
                if not file_url:
                    st.error("❌ 文件上传失败")
                    return False
            
            st.success(f"✅ 文件上传成功: {filename}")
            
            # 解析Excel文件并提取元数据
            with st.spinner("正在分析文件内容..."):
                excel_file = pd.ExcelFile(uploaded_file)
                
                reports_processed = 0
                
                for sheet_name in excel_file.sheet_names:
                    try:
                        df = pd.read_excel(uploaded_file, sheet_name=sheet_name)
                        
                        if df.empty:
                            continue
                        
                        # 分析应收-未收额
                        analysis_result = self.analyze_receivable_data(df)
                        
                        # 生成数据摘要
                        summary = {
                            "total_rows": len(df),
                            "total_columns": len(df.columns),
                            "columns": df.columns.tolist()[:10],  # 只保存前10列名
                            "has_data": not df.empty
                        }
                        
                        # 保存报表元数据到数据库
                        report_metadata = {
                            "store_name": sheet_name,
                            "filename": filename,
                            "file_url": file_url,
                            "file_size_mb": file_size_mb,
                            "upload_time": datetime.now().isoformat(),
                            "summary": json.dumps(summary),
                            "analysis_result": json.dumps(analysis_result),
                            "row_count": len(df),
                            "column_count": len(df.columns)
                        }
                        
                        if self.database.save_report_metadata(report_metadata):
                            reports_processed += 1
                            st.success(f"✅ {sheet_name}: {len(df)} 行数据已处理")
                        else:
                            st.warning(f"⚠️ {sheet_name}: 元数据保存失败")
                            
                    except Exception as e:
                        st.warning(f"⚠️ 跳过工作表 '{sheet_name}': {str(e)}")
                        continue
                
                if reports_processed > 0:
                    st.success(f"🎉 报表处理完成：{reports_processed} 个工作表")
                    
                    # 显示存储统计
                    self._show_storage_stats()
                    return True
                else:
                    st.error("❌ 没有成功处理任何工作表")
                    return False
                
        except Exception as e:
            st.error(f"❌ 处理报表文件失败：{str(e)}")
            logger.error(f"处理报表文件失败: {str(e)}")
            return False
    
    def _cleanup_old_reports(self):
        """清理旧的报表数据"""
        try:
            # 获取所有报表元数据
            all_reports = self.database.load_report_metadata()
            
            # 删除腾讯云COS中的旧文件
            deleted_count = 0
            for report in all_reports:
                try:
                    filename = report.get("filename")
                    if filename and self.cos_manager.delete_file(filename):
                        deleted_count += 1
                except:
                    continue
            
            # 清空数据库中的报表元数据
            self.database.supabase.table("reports").delete().neq("id", 0).execute()
            
            if deleted_count > 0:
                st.info(f"🧹 已清理 {deleted_count} 个旧文件")
                
        except Exception as e:
            st.warning(f"清理旧数据时出错: {str(e)}")
    
    def _show_storage_stats(self):
        """显示存储统计信息"""
        try:
            # 获取COS使用情况
            cos_usage = self.cos_manager.get_storage_usage()
            
            # 获取数据库记录数
            reports_count = len(self.database.load_report_metadata())
            permissions_count = len(self.database.load_permissions())
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("📦 COS文件数", cos_usage['file_count'])
                st.metric("💾 COS使用", f"{cos_usage['total_size_mb']:.2f} MB")
                
                # 使用率计算
                usage_percent = (cos_usage['total_size_mb'] / (50 * 1024)) * 100
                st.progress(min(usage_percent / 100, 1.0))
                st.caption(f"使用率: {usage_percent:.1f}% / 50GB免费")
            
            with col2:
                st.metric("🗄️ 报表记录", reports_count)
                st.metric("👥 权限记录", permissions_count)
            
            with col3:
                st.metric("📊 总门店数", reports_count)
                st.metric("🚀 系统状态", "正常运行")
                
        except Exception as e:
            st.warning(f"获取存储统计失败: {str(e)}")
    
    def analyze_receivable_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """分析应收未收额数据"""
        result = {}
        
        if len(df.columns) == 0 or len(df) == 0:
            return result
        
        # 查找第69行
        target_row_index = 68  # 第69行
        
        if len(df) > target_row_index:
            row = df.iloc[target_row_index]
            first_col_value = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
            
            # 检查关键词
            keywords = ['应收-未收额', '应收未收额', '应收-未收', '应收未收']
            
            for keyword in keywords:
                if keyword in first_col_value:
                    # 查找数值
                    for col_idx in range(len(row)-1, 0, -1):
                        val = row.iloc[col_idx]
                        if pd.notna(val) and str(val).strip() not in ['', 'None', 'nan']:
                            cleaned = str(val).replace(',', '').replace('¥', '').replace('￥', '').strip()
                            
                            if cleaned.startswith('(') and cleaned.endswith(')'):
                                cleaned = '-' + cleaned[1:-1]
                            
                            try:
                                amount = float(cleaned)
                                if amount != 0:
                                    result['应收-未收额'] = {
                                        'amount': amount,
                                        'column_name': str(df.columns[col_idx]),
                                        'row_name': first_col_value,
                                        'row_index': target_row_index,
                                        'actual_row_number': target_row_index + 1
                                    }
                                    return result
                            except ValueError:
                                continue
                    break
        
        return result
    
    def load_store_data(self, store_name: str) -> Optional[pd.DataFrame]:
        """加载指定门店的数据"""
        try:
            # 从数据库获取报表元数据
            reports = self.database.load_report_metadata(store_name)
            
            if not reports:
                return None
            
            # 获取最新的报表
            latest_report = max(reports, key=lambda x: x.get('upload_time', ''))
            filename = latest_report.get('filename')
            
            if not filename:
                return None
            
            # 从腾讯云COS下载文件
            with st.spinner(f"正在从腾讯云加载 {store_name} 的数据..."):
                file_data = self.cos_manager.download_file(filename)
                
                if file_data:
                    # 解析Excel文件
                    excel_file = pd.ExcelFile(io.BytesIO(file_data))
                    
                    # 查找匹配的工作表
                    matching_sheets = [sheet for sheet in excel_file.sheet_names 
                                     if store_name in sheet or sheet in store_name]
                    
                    if matching_sheets:
                        df = pd.read_excel(io.BytesIO(file_data), sheet_name=matching_sheets[0])
                        return df
                    
            return None
            
        except Exception as e:
            st.error(f"❌ 加载 {store_name} 数据失败：{str(e)}")
            logger.error(f"加载门店数据失败: {str(e)}")
            return None
    
    def verify_user_permission(self, store_name: str, user_id: str) -> bool:
        """验证用户权限"""
        try:
            permissions = self.database.load_permissions()
            
            for perm in permissions:
                stored_store = perm.get('store_name', '').strip()
                stored_id = perm.get('user_id', '').strip()
                
                if (store_name in stored_store or stored_store in store_name) and stored_id == str(user_id):
                    return True
            
            return False
            
        except Exception as e:
            st.error(f"❌ 权限验证失败：{str(e)}")
            logger.error(f"权限验证失败: {str(e)}")
            return False
    
    def get_available_stores(self) -> List[str]:
        """获取可用的门店列表"""
        try:
            permissions = self.database.load_permissions()
            stores = list(set(perm.get('store_name', '') for perm in permissions))
            return sorted([store for store in stores if store.strip()])
            
        except Exception as e:
            st.error(f"❌ 获取门店列表失败：{str(e)}")
            logger.error(f"获取门店列表失败: {str(e)}")
            return []
    
    def cleanup_storage(self, cleanup_type: str = "all"):
        """清理存储空间"""
        try:
            if cleanup_type == "all":
                # 清理所有数据
                cos_files = self.cos_manager.list_files()
                deleted_cos = 0
                
                for file_info in cos_files:
                    if self.cos_manager.delete_file(file_info['filename']):
                        deleted_cos += 1
                
                # 清理数据库
                self.database.supabase.table("reports").delete().neq("id", 0).execute()
                self.database.supabase.table("permissions").delete().neq("id", 0).execute()
                
                st.success(f"🧹 清理完成：删除了 {deleted_cos} 个COS文件和所有数据库记录")
                
        except Exception as e:
            st.error(f"❌ 清理失败：{str(e)}")
            logger.error(f"存储清理失败: {str(e)}")

# 初始化会话状态
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
if 'store_name' not in st.session_state:
    st.session_state.store_name = ""
if 'user_id' not in st.session_state:
    st.session_state.user_id = ""
if 'is_admin' not in st.session_state:
    st.session_state.is_admin = False
if 'storage_system' not in st.session_state:
    st.session_state.storage_system = None

# 主标题
st.markdown('<h1 class="main-header">📊 门店报表查询系统 </h1>', unsafe_allow_html=True)

# 初始化存储系统
if not st.session_state.storage_system:
    try:
        st.session_state.storage_system = TencentSupabaseSystem()
        st.success("✅ 腾讯云+Supabase存储系统初始化成功")
    except Exception as e:
        st.error(f"❌ 存储系统初始化失败: {str(e)}")
        st.stop()

storage_system = st.session_state.storage_system

# 显示架构信息
storage_system.show_architecture_info()

# 侧边栏
with st.sidebar:
    st.title("⚙️ 系统功能")
    
    user_type = st.radio("选择用户类型", ["普通用户", "管理员"])
    
    if user_type == "管理员":
        st.subheader("🔐 管理员登录")
        admin_password = st.text_input("管理员密码", type="password")
        
        if st.button("验证管理员身份"):
            if admin_password == ADMIN_PASSWORD:
                st.session_state.is_admin = True
                st.success("✅ 管理员验证成功！")
                st.rerun()
            else:
                st.error("❌ 密码错误！")
    
    else:
        if st.session_state.logged_in:
            st.subheader("👤 当前登录")
            st.info(f"门店：{st.session_state.store_name}")
            st.info(f"查询编码：{st.session_state.user_id}")
            
            if st.button("🚪 退出登录"):
                st.session_state.logged_in = False
                st.session_state.store_name = ""
                st.session_state.user_id = ""
                st.success("👋 已退出登录")
                st.rerun()

# 主界面
if user_type == "管理员" and st.session_state.is_admin:
    st.markdown('''
    <div class="admin-panel">
    <h3>👨‍💼 管理员控制面板 </h3>
    <p>✨ </p>
    </div>
    ''', unsafe_allow_html=True)
    
    # 存储管理区域
    st.subheader("📊 存储管理")
    storage_system._show_storage_stats()
    
    st.divider()
    
    # 文件上传区域
    st.subheader("📁 文件管理")
    
    tab1, tab2, tab3 = st.tabs(["📋 权限表", "📊 报表数据", "🧹 存储清理"])
    
    with tab1:
        st.markdown("#### 上传门店权限表")
        st.info("💡 Excel文件格式：第一列为门店名称，第二列为人员编号")
        
        permissions_file = st.file_uploader("选择权限Excel文件", type=['xlsx', 'xls'], key="permissions")
        
        if permissions_file and st.button("📤 上传权限表", key="upload_permissions"):
            if storage_system.upload_and_process_permissions(permissions_file):
                st.balloons()
    
    with tab2:
        st.markdown("#### 上传财务报表")
        
        st.markdown('''
        <div class="success-box">
        <strong>🚀 腾讯云COS优势</strong><br>
        • 50GB永久免费存储<br>
        • 支持任意大小Excel文件<br>
        • 中国地区访问速度快<br>
        • 微信支付便捷管理
        </div>
        ''', unsafe_allow_html=True)
        
        reports_file = st.file_uploader("选择报表Excel文件", type=['xlsx', 'xls'], key="reports")
        
        if reports_file:
            file_size = len(reports_file.getvalue()) / 1024 / 1024
            st.metric("文件大小", f"{file_size:.2f} MB")
            
            if file_size > 100:
                st.markdown('''
                <div class="warning-box">
                <strong>⚠️ 大文件提醒</strong><br>
                文件较大，上传可能需要较长时间，请耐心等待。<br>
                腾讯云COS支持大文件上传，无需担心大小限制。
                </div>
                ''', unsafe_allow_html=True)
        
        if reports_file and st.button("📤 上传报表数据", key="upload_reports"):
            if storage_system.upload_and_process_reports(reports_file):
                st.balloons()
    
    with tab3:
        st.markdown("#### 存储空间清理")
        
        st.warning("⚠️ 清理操作将删除所有存储的数据，请谨慎操作！")
        
        if st.checkbox("我确认要清理所有数据"):
            if st.button("🗑️ 清理所有存储数据", type="primary"):
                storage_system.cleanup_storage("all")
                st.rerun()

elif user_type == "管理员" and not st.session_state.is_admin:
    st.info("👈 请在左侧边栏输入管理员密码")

else:
    if not st.session_state.logged_in:
        st.subheader("🔐 用户登录")
        
        try:
            stores = storage_system.get_available_stores()
            
            if not stores:
                st.warning("⚠️ 系统维护中，请联系管理员上传权限表")
            else:
                with st.form("login_form"):
                    selected_store = st.selectbox("选择门店", stores)
                    user_id = st.text_input("人员编号")
                    submit = st.form_submit_button("🚀 登录")
                    
                    if submit and selected_store and user_id:
                        if storage_system.verify_user_permission(selected_store, user_id):
                            st.session_state.logged_in = True
                            st.session_state.store_name = selected_store
                            st.session_state.user_id = user_id
                            st.success("✅ 登录成功！")
                            st.balloons()
                            st.rerun()
                        else:
                            st.error("❌ 门店或编号错误！")
                            
        except Exception as e:
            st.error(f"❌ 系统连接失败：{str(e)}")
    
    else:
        # 已登录用户界面
        st.markdown(f'<div class="store-info"><h3>🏪 {st.session_state.store_name}</h3><p>操作员：{st.session_state.user_id}</p></div>', unsafe_allow_html=True)
        
        try:
            df = storage_system.load_store_data(st.session_state.store_name)
            
            if df is not None:
                # 应收-未收额分析
                st.subheader("💰 应收-未收额")
                
                analysis_results = storage_system.analyze_receivable_data(df)
                
                if '应收-未收额' in analysis_results:
                    data = analysis_results['应收-未收额']
                    amount = data['amount']
                    
                    if amount > 0:
                        st.error(f"💳 应付款：¥{amount:,.2f}")
                    elif amount < 0:
                        st.success(f"💚 应退款：¥{abs(amount):,.2f}")
                    else:
                        st.info("⚖️ 收支平衡：¥0.00")
                    
                    # 显示详细信息
                    with st.expander("📊 详细信息"):
                        st.write(f"**所在行**: 第{data['actual_row_number']}行")
                        st.write(f"**所在列**: {data['column_name']}")
                        st.write(f"**行标题**: {data['row_name']}")
                else:
                    st.warning("⚠️ 未找到应收-未收额数据")
                
                # 报表展示
                st.subheader("📋 报表数据")
                st.dataframe(df, use_container_width=True, height=400)
                
                # 下载功能
                if st.button("📥 下载完整报表"):
                    try:
                        buffer = io.BytesIO()
                        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                            df.to_excel(writer, index=False, sheet_name=st.session_state.store_name)
                        
                        st.download_button(
                            "点击下载",
                            buffer.getvalue(),
                            f"{st.session_state.store_name}_报表_{datetime.now().strftime('%Y%m%d')}.xlsx",
                            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                    except Exception as e:
                        st.error(f"下载失败：{str(e)}")
            
            else:
                st.error(f"❌ 未找到门店 '{st.session_state.store_name}' 的报表")
                st.info("请联系管理员上传最新的报表数据")
                
        except Exception as e:
            st.error(f"❌ 数据加载失败：{str(e)}")

# 页面底部
st.divider()

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.caption(f"🕒 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
with col2:
    st.caption("🏢 存储")
with col3:
    st.caption("🗄️ ")
with col4:
    st.caption("🔧 v5.0 ")
