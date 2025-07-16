import streamlit as st
import pandas as pd
import io
import json
from datetime import datetime
import time
import gspread
from google.oauth2.credentials import Credentials as OAuthCredentials
from google.oauth2.service_account import Credentials as ServiceCredentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import Flow
import logging
from typing import Optional, Dict, Any, List
import hashlib
import traceback
import zlib
import base64
import urllib.parse as urlparse

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 页面配置
st.set_page_config(
    page_title="门店报表查询系统", 
    page_icon="📊",
    layout="wide"
)

# 系统配置
ADMIN_PASSWORD = st.secrets.get("system", {}).get("admin_password", "admin123")
PERMISSIONS_SHEET_NAME = "store_permissions"
REPORTS_SHEET_NAME = "store_reports"
MAX_RETRIES = 3
RETRY_DELAY = 1
MAX_CHUNK_SIZE = 25000
CACHE_DURATION = 300
COMPRESSION_LEVEL = 9

# 简化的OAuth配置 - 只请求必要权限
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# CSS样式
st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        padding: 1rem 0;
        margin-bottom: 2rem;
    }
    .auth-selector {
        background: linear-gradient(135deg, #a8edea 0%, #fed6e3 100%);
        padding: 1.5rem;
        border-radius: 12px;
        margin: 1rem 0;
        border: 2px solid #48cab2;
    }
    .oauth-panel {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1.5rem;
        border-radius: 10px;
        margin: 1rem 0;
    }
    .oauth-success {
        background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
        color: white;
        padding: 1.5rem;
        border-radius: 10px;
        margin: 1rem 0;
        text-align: center;
    }
    .oauth-error {
        background: linear-gradient(135deg, #ff416c 0%, #ff4b2b 100%);
        color: white;
        padding: 1.5rem;
        border-radius: 10px;
        margin: 1rem 0;
    }
    .oauth-steps {
        background: rgba(255, 255, 255, 0.1);
        padding: 1rem;
        border-radius: 8px;
        margin: 1rem 0;
    }
    .service-panel {
        background: linear-gradient(135deg, #ffeaa7 0%, #fab1a0 100%);
        padding: 1.5rem;
        border-radius: 10px;
        margin: 1rem 0;
    }
    .status-success {
        background: #d4edda;
        color: #155724;
        padding: 1rem;
        border-radius: 8px;
        border: 1px solid #c3e6cb;
        margin: 1rem 0;
    }
    .status-error {
        background: #f8d7da;
        color: #721c24;
        padding: 1rem;
        border-radius: 8px;
        border: 1px solid #f5c6cb;
        margin: 1rem 0;
    }
    .status-warning {
        background: #fff3cd;
        color: #856404;
        padding: 1rem;
        border-radius: 8px;
        border: 1px solid #ffeaa7;
        margin: 1rem 0;
    }
    .receivable-positive {
        background: linear-gradient(135deg, #ff9a9e 0%, #fecfef 100%);
        color: #721c24;
        padding: 2rem;
        border-radius: 15px;
        border: 3px solid #f093fb;
        margin: 1rem 0;
        text-align: center;
    }
    .receivable-negative {
        background: linear-gradient(135deg, #a8edea 0%, #d299c2 100%);
        color: #0c4128;
        padding: 2rem;
        border-radius: 15px;
        border: 3px solid #48cab2;
        margin: 1rem 0;
        text-align: center;
    }
    .store-info {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1.5rem;
        border-radius: 10px;
        margin: 1rem 0;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .admin-panel {
        background: linear-gradient(135deg, #ffeaa7 0%, #fab1a0 100%);
        padding: 1.5rem;
        border-radius: 10px;
        border: 2px solid #fdcb6e;
        margin: 1rem 0;
    }
    .auth-button {
        display: inline-block;
        padding: 12px 24px;
        background: linear-gradient(135deg, #4285f4 0%, #34a853 100%);
        color: white;
        text-decoration: none;
        border-radius: 8px;
        font-weight: bold;
        font-size: 16px;
        border: none;
        cursor: pointer;
        transition: all 0.3s ease;
        text-align: center;
        min-width: 200px;
    }
    .auth-button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.2);
        text-decoration: none;
        color: white;
    }
    .diagnostic-panel {
        background: #f8f9fa;
        border: 1px solid #dee2e6;
        border-radius: 8px;
        padding: 1rem;
        margin: 1rem 0;
        font-family: monospace;
        font-size: 0.9rem;
    }
    .config-guide {
        background: #e3f2fd;
        border: 1px solid #2196f3;
        border-radius: 8px;
        padding: 1rem;
        margin: 1rem 0;
    }
    </style>
""", unsafe_allow_html=True)

class AuthenticationError(Exception):
    """认证异常"""
    pass

class OAuthManager:
    """OAuth管理器 - 稳定版本"""
    
    @staticmethod
    def validate_oauth_config():
        """验证OAuth配置的完整性"""
        if "google_oauth" not in st.secrets:
            return False, "OAuth配置缺失，请检查secrets.toml文件"
        
        oauth_config = st.secrets["google_oauth"]
        required_keys = ["client_id", "client_secret", "redirect_uri"]
        
        for key in required_keys:
            if key not in oauth_config:
                return False, f"缺少配置项: {key}"
            if not oauth_config[key].strip():
                return False, f"配置项 {key} 为空"
        
        # 验证client_id格式
        client_id = oauth_config["client_id"]
        if not client_id.endswith(".apps.googleusercontent.com"):
            return False, "client_id格式不正确，应该以.apps.googleusercontent.com结尾"
        
        # 验证redirect_uri格式
        redirect_uri = oauth_config["redirect_uri"]
        if not redirect_uri.startswith("https://"):
            return False, "redirect_uri必须使用HTTPS协议"
        
        if not redirect_uri.endswith(".streamlit.app/"):
            return False, "redirect_uri应该是Streamlit应用的完整URL，以.streamlit.app/结尾"
        
        return True, "OAuth配置验证通过"
    
    @staticmethod
    def create_oauth_flow():
        """创建OAuth流程"""
        try:
            # 验证配置
            is_valid, error_msg = OAuthManager.validate_oauth_config()
            if not is_valid:
                return None, error_msg
            
            oauth_config = st.secrets["google_oauth"]
            
            # 创建客户端配置
            client_config = {
                "web": {
                    "client_id": oauth_config["client_id"],
                    "client_secret": oauth_config["client_secret"],
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "redirect_uris": [oauth_config["redirect_uri"]]
                }
            }
            
            # 创建流程
            flow = Flow.from_client_config(
                client_config,
                scopes=SCOPES,
                redirect_uri=oauth_config["redirect_uri"]
            )
            
            return flow, None
            
        except Exception as e:
            logger.error(f"OAuth流程创建失败: {str(e)}")
            return None, f"OAuth流程创建失败: {str(e)}"
    
    @staticmethod
    def generate_auth_url(flow):
        """生成授权URL"""
        try:
            auth_url, _ = flow.authorization_url(
                access_type='offline',
                include_granted_scopes='true',
                prompt='consent'
            )
            return auth_url, None
        except Exception as e:
            logger.error(f"生成授权URL失败: {str(e)}")
            return None, f"生成授权URL失败: {str(e)}"
    
    @staticmethod
    def exchange_code_for_token(auth_code):
        """交换授权码为访问令牌"""
        try:
            # 重新创建flow以确保一致性
            flow, error = OAuthManager.create_oauth_flow()
            if error:
                return None, error
            
            # 交换token
            flow.fetch_token(code=auth_code)
            credentials = flow.credentials
            
            # 创建凭据字典
            cred_dict = {
                'token': credentials.token,
                'refresh_token': credentials.refresh_token,
                'id_token': getattr(credentials, 'id_token', None),
                'token_uri': credentials.token_uri,
                'client_id': credentials.client_id,
                'client_secret': credentials.client_secret,
                'scopes': SCOPES,
                'expiry': credentials.expiry.isoformat() if credentials.expiry else None
            }
            
            return cred_dict, None
            
        except Exception as e:
            logger.error(f"Token交换失败: {str(e)}")
            return None, f"Token交换失败: {str(e)}"
    
    @staticmethod
    def create_credentials_from_dict(cred_dict):
        """从字典创建凭据对象"""
        try:
            # 处理过期时间
            expiry = None
            if cred_dict.get('expiry'):
                try:
                    expiry = datetime.fromisoformat(cred_dict['expiry'].replace('Z', '+00:00'))
                except:
                    pass
            
            credentials = OAuthCredentials(
                token=cred_dict['token'],
                refresh_token=cred_dict.get('refresh_token'),
                id_token=cred_dict.get('id_token'),
                token_uri=cred_dict['token_uri'],
                client_id=cred_dict['client_id'],
                client_secret=cred_dict['client_secret'],
                scopes=cred_dict.get('scopes', SCOPES),
                expiry=expiry
            )
            
            return credentials, None
            
        except Exception as e:
            logger.error(f"凭据创建失败: {str(e)}")
            return None, f"凭据创建失败: {str(e)}"
    
    @staticmethod
    def test_credentials(credentials):
        """测试凭据有效性"""
        try:
            client = gspread.authorize(credentials)
            # 简单测试 - 尝试列出文件（限制数量以避免配额问题）
            client.list_spreadsheet_files()
            return True, None
        except Exception as e:
            error_msg = str(e).lower()
            if any(keyword in error_msg for keyword in ['unauthorized', 'invalid', 'expired']):
                return False, "凭据已过期或无效"
            elif 'quota' in error_msg:
                return False, "API配额不足"
            elif 'forbidden' in error_msg:
                return False, "权限被拒绝，请检查OAuth应用配置"
            return False, f"连接测试失败: {str(e)}"

def show_oauth_config_guide():
    """显示OAuth配置指南"""
    with st.expander("📋 OAuth配置指南", expanded=False):
        st.markdown('''
        <div class="config-guide">
        <h4>🔧 Google Cloud Console 配置步骤</h4>
        <ol>
            <li><strong>创建项目</strong>：在 <a href="https://console.cloud.google.com/" target="_blank">Google Cloud Console</a> 创建新项目</li>
            <li><strong>启用API</strong>：启用 "Google Sheets API"</li>
            <li><strong>创建OAuth 2.0凭据</strong>：
                <ul>
                    <li>选择 "凭据" → "创建凭据" → "OAuth 2.0 客户端ID"</li>
                    <li>应用类型选择 "Web应用"</li>
                    <li>添加授权重定向URI：<code>https://你的应用名.streamlit.app/</code></li>
                </ul>
            </li>
            <li><strong>添加测试用户</strong>：在 "OAuth同意屏幕" 中添加要使用的Google账号邮箱</li>
        </ol>
        
        <h4>📝 Secrets.toml 配置格式</h4>
        <pre>
[google_oauth]
client_id = "你的客户端ID.apps.googleusercontent.com"
client_secret = "你的客户端密钥"
redirect_uri = "https://你的应用名.streamlit.app/"

[system]
admin_password = "你的管理员密码"
        </pre>
        
        <h4>⚠️ 重要提示</h4>
        <ul>
            <li>redirect_uri 必须与 Streamlit 应用的实际URL完全一致</li>
            <li>确保在Google Cloud Console的测试用户中添加了你的邮箱</li>
            <li>OAuth应用需要通过Google审核后才能被任意用户使用</li>
        </ul>
        </div>
        ''', unsafe_allow_html=True)

def show_oauth_diagnostics():
    """显示OAuth诊断信息"""
    with st.expander("🔧 OAuth诊断信息", expanded=False):
        st.markdown('<div class="diagnostic-panel">', unsafe_allow_html=True)
        
        # 配置验证
        is_valid, validation_msg = OAuthManager.validate_oauth_config()
        if is_valid:
            st.markdown("✅ **OAuth配置验证通过**")
            oauth_config = st.secrets["google_oauth"]
            st.write(f"📧 Client ID: {oauth_config['client_id'][:30]}...")
            st.write(f"🔗 Redirect URI: {oauth_config['redirect_uri']}")
        else:
            st.markdown(f"❌ **OAuth配置错误**: {validation_msg}")
        
        # 当前认证状态
        if 'google_credentials' in st.session_state:
            st.markdown("✅ **本地凭据存在**")
            cred = st.session_state['google_credentials']
            st.write(f"🎫 Token存在: {'是' if cred.get('token') else '否'}")
            st.write(f"🔄 Refresh Token存在: {'是' if cred.get('refresh_token') else '否'}")
            if cred.get('expiry'):
                try:
                    expiry_time = datetime.fromisoformat(cred['expiry'].replace('Z', '+00:00'))
                    is_expired = expiry_time < datetime.now(expiry_time.tzinfo)
                    st.write(f"⏰ Token状态: {'已过期' if is_expired else '有效'}")
                except:
                    st.write("⏰ Token状态: 无法解析")
        else:
            st.markdown("❌ **本地凭据不存在**")
        
        # URL参数检查
        query_params = st.query_params
        if query_params:
            st.markdown("📋 **当前URL参数**:")
            for key, value in query_params.items():
                if key == 'code':
                    st.write(f"  {key}: {value[:20]}... (截断显示)")
                else:
                    st.write(f"  {key}: {value}")
        else:
            st.write("📋 无URL参数")
        
        # 当前应用URL
        try:
            current_url = st.query_params.get('_stale', 'unknown')
            if current_url == 'unknown':
                current_url = "请在浏览器地址栏查看完整URL"
            st.write(f"🌐 当前应用URL: {current_url}")
        except:
            st.write("🌐 当前应用URL: 无法获取")
        
        st.markdown('</div>', unsafe_allow_html=True)

def handle_oauth_authorization():
    """处理OAuth授权流程"""
    st.markdown("### 🔐 OAuth个人账号授权")
    
    # 显示配置指南和诊断信息
    show_oauth_config_guide()
    show_oauth_diagnostics()
    
    # 验证配置
    is_valid, validation_msg = OAuthManager.validate_oauth_config()
    if not is_valid:
        st.markdown(f'''
        <div class="oauth-error">
            <h4>❌ 配置错误</h4>
            <p>{validation_msg}</p>
            <p>请参考上方的配置指南进行设置</p>
        </div>
        ''', unsafe_allow_html=True)
        return False
    
    # 检查授权回调
    query_params = st.query_params
    
    if 'code' in query_params:
        st.markdown("### 🔄 正在处理授权...")
        
        with st.status("处理OAuth授权中...", expanded=True) as status:
            try:
                auth_code = query_params['code']
                st.write("✅ 收到授权码")
                
                # 交换token
                st.write("🔄 正在获取访问令牌...")
                cred_dict, error = OAuthManager.exchange_code_for_token(auth_code)
                
                if error:
                    st.write(f"❌ Token交换失败: {error}")
                    status.update(label="授权失败", state="error")
                    
                    # 提供重试选项
                    if st.button("🔄 重新尝试授权", key="retry_auth"):
                        st.query_params.clear()
                        st.rerun()
                    return False
                
                st.write("✅ 获取访问令牌成功")
                
                # 创建并测试凭据
                st.write("🔍 正在测试凭据...")
                credentials, cred_error = OAuthManager.create_credentials_from_dict(cred_dict)
                if cred_error:
                    st.write(f"❌ 凭据创建失败: {cred_error}")
                    status.update(label="凭据创建失败", state="error")
                    return False
                
                # 测试连接
                test_result, test_error = OAuthManager.test_credentials(credentials)
                if not test_result:
                    st.write(f"❌ 凭据测试失败: {test_error}")
                    status.update(label="凭据测试失败", state="error")
                    
                    # 提供详细错误信息
                    if "权限被拒绝" in test_error:
                        st.error("🚫 权限被拒绝。可能的原因：")
                        st.error("• OAuth应用还在测试模式，需要添加你的邮箱为测试用户")
                        st.error("• redirect_uri配置不正确")
                        st.error("• 权限范围配置问题")
                    
                    return False
                
                st.write("✅ 凭据测试通过")
                
                # 保存凭据
                st.session_state['google_credentials'] = cred_dict
                st.session_state['auth_method'] = 'oauth'
                st.session_state['auth_timestamp'] = time.time()
                
                # 清理URL
                st.query_params.clear()
                
                status.update(label="OAuth授权成功！", state="complete")
                st.write("🎉 授权完成，正在跳转...")
                
                # 显示成功信息
                st.markdown("""
                <div class="oauth-success">
                    <h3>🎉 OAuth授权成功！</h3>
                    <p>✅ 已获得Google账号访问权限</p>
                    <p>📊 可以使用15GB个人存储空间</p>
                    <p>🔄 正在自动刷新页面...</p>
                </div>
                """, unsafe_allow_html=True)
                
                time.sleep(3)
                st.rerun()
                
        except Exception as e:
            logger.error(f"OAuth处理异常: {str(e)}")
            st.markdown(f"""
            <div class="oauth-error">
                <h4>❌ 授权处理失败</h4>
                <p>错误详情: {str(e)}</p>
                <p>请尝试重新授权或检查配置</p>
            </div>
            """, unsafe_allow_html=True)
            
            # 清理状态
            st.query_params.clear()
            if st.button("🔄 重新开始授权", key="restart_auth"):
                st.rerun()
    
    else:
        # 显示授权界面
        st.markdown("#### 📝 授权步骤")
        st.markdown("""
        <div class="oauth-steps">
            <ol>
                <li>🖱️ 点击下方授权按钮</li>
                <li>🔑 在新窗口中选择你的Google账号</li>
                <li>✅ 同意应用访问权限</li>
                <li>⏳ 等待自动返回并完成授权</li>
            </ol>
        </div>
        """, unsafe_allow_html=True)
        
        # 创建OAuth流程并生成授权URL
        flow, flow_error = OAuthManager.create_oauth_flow()
        if flow_error:
            st.error(f"❌ OAuth流程创建失败: {flow_error}")
            return False
        
        auth_url, url_error = OAuthManager.generate_auth_url(flow)
        if url_error:
            st.error(f"❌ 生成授权URL失败: {url_error}")
            return False
        
        # 授权按钮
        st.markdown(f"""
        <div style="text-align: center; margin: 2rem 0;">
            <a href="{auth_url}" target="_self" class="auth-button">
                🚀 点击授权Google账号
            </a>
        </div>
        """, unsafe_allow_html=True)
        
        # 提示信息
        st.markdown("""
        <div class="status-warning">
            <h5>💡 重要提示</h5>
            <ul>
                <li>🔒 确保在Google Cloud Console中添加了你的邮箱为测试用户</li>
                <li>📱 建议使用桌面浏览器完成授权</li>
                <li>🔗 redirect_uri必须与应用URL完全一致</li>
                <li>⚡ 授权成功后享受15GB个人存储空间</li>
                <li>🔄 如遇到问题，可尝试使用下方的服务账号模式</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    return False

def get_oauth_client():
    """获取OAuth客户端"""
    if 'google_credentials' not in st.session_state:
        return None, "未找到OAuth凭据"
    
    try:
        cred_dict = st.session_state['google_credentials']
        
        # 创建凭据对象
        credentials, error = OAuthManager.create_credentials_from_dict(cred_dict)
        if error:
            return None, error
        
        # 检查并刷新token
        if credentials.expired and credentials.refresh_token:
            try:
                request = Request()
                credentials.refresh(request)
                
                # 更新存储的凭据
                st.session_state['google_credentials'].update({
                    'token': credentials.token,
                    'expiry': credentials.expiry.isoformat() if credentials.expiry else None
                })
                
                logger.info("Token已自动刷新")
                
            except Exception as refresh_error:
                logger.error(f"Token刷新失败: {str(refresh_error)}")
                # 清除无效凭据
                if 'google_credentials' in st.session_state:
                    del st.session_state['google_credentials']
                return None, f"Token刷新失败，请重新授权: {str(refresh_error)}"
        
        # 创建客户端
        client = gspread.authorize(credentials)
        
        # 简单测试连接
        try:
            client.list_spreadsheet_files()
        except Exception as test_error:
            error_msg = str(test_error).lower()
            if any(keyword in error_msg for keyword in ['unauthorized', 'invalid', 'expired']):
                # 清除无效凭据
                if 'google_credentials' in st.session_state:
                    del st.session_state['google_credentials']
                return None, "凭据已失效，请重新授权"
            return None, f"连接测试失败: {str(test_error)}"
        
        return client, None
        
    except Exception as e:
        logger.error(f"OAuth客户端创建失败: {str(e)}")
        # 清除可能损坏的凭据
        if 'google_credentials' in st.session_state:
            del st.session_state['google_credentials']
        return None, f"客户端创建失败，请重新授权: {str(e)}"

def get_service_account_client():
    """获取服务账号客户端"""
    try:
        if "google_sheets" not in st.secrets:
            return None, "服务账号配置缺失"
        
        credentials_info = st.secrets["google_sheets"]
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        credentials = ServiceCredentials.from_service_account_info(credentials_info, scopes=scopes)
        client = gspread.authorize(credentials)
        
        # 测试连接
        try:
            client.list_spreadsheet_files()
        except Exception as test_error:
            return None, f"服务账号连接测试失败: {str(test_error)}"
        
        return client, None
        
    except Exception as e:
        logger.error(f"服务账号客户端创建失败: {str(e)}")
        return None, f"服务账号客户端创建失败: {str(e)}"

def show_authentication_selector():
    """显示认证方式选择器"""
    st.markdown('<div class="auth-selector">', unsafe_allow_html=True)
    st.markdown("## 🔐 选择认证方式")
    
    # 检查可用的认证方式
    oauth_available = "google_oauth" in st.secrets
    service_available = "google_sheets" in st.secrets
    
    if not oauth_available and not service_available:
        st.error("❌ 未配置任何认证方式，请检查secrets配置")
        st.markdown('</div>', unsafe_allow_html=True)
        return None
    
    # 显示当前状态
    current_auth = st.session_state.get('auth_method', None)
    if current_auth:
        auth_time = st.session_state.get('auth_timestamp', 0)
        time_ago = int(time.time() - auth_time) if auth_time > 0 else 0
        st.success(f"🔗 当前认证方式: {current_auth} (连接时长: {time_ago//60}分钟)")
    
    # 认证选项
    auth_options = []
    if oauth_available:
        auth_options.append("OAuth个人账号 (推荐)")
    if service_available:
        auth_options.append("服务账号")
    
    selected_auth = st.radio(
        "选择认证方式：",
        auth_options,
        help="OAuth使用个人Google账号（15GB空间），服务账号使用项目配额",
        index=0 if oauth_available else None
    )
    
    # 管理按钮
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("🔄 重置认证"):
            auth_keys = ['google_credentials', 'auth_method', 'auth_timestamp']
            for key in auth_keys:
                if key in st.session_state:
                    del st.session_state[key]
            st.success("认证状态已重置")
            st.rerun()
    
    with col2:
        if st.button("🗑️ 清理缓存"):
            cache_keys = [key for key in st.session_state.keys() if key.startswith('cache_')]
            for key in cache_keys:
                del st.session_state[key]
            st.success("缓存已清理")
    
    with col3:
        if st.button("🔍 测试连接"):
            if current_auth == 'oauth':
                client, error = get_oauth_client()
                if client:
                    st.success("✅ OAuth连接正常")
                else:
                    st.error(f"❌ OAuth连接失败: {error}")
            elif current_auth == 'service':
                client, error = get_service_account_client()
                if client:
                    st.success("✅ 服务账号连接正常")
                else:
                    st.error(f"❌ 服务账号连接失败: {error}")
            else:
                st.warning("⚠️ 请先选择认证方式")
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # 根据选择显示对应面板
    if selected_auth == "OAuth个人账号 (推荐)":
        return show_oauth_auth_panel()
    else:
        return show_service_auth_panel()

def show_oauth_auth_panel():
    """显示OAuth认证面板"""
    st.markdown('<div class="oauth-panel">', unsafe_allow_html=True)
    st.markdown("### 👤 OAuth个人账号模式")
    st.markdown("✅ 使用你的个人Google账号和15GB存储空间")
    
    # 检查现有认证
    if ('google_credentials' in st.session_state and 
        st.session_state.get('auth_method') == 'oauth'):
        
        client, error = get_oauth_client()
        if client:
            # 显示成功状态
            auth_time = st.session_state.get('auth_timestamp', 0)
            time_ago = int(time.time() - auth_time)
            
            st.markdown(f"""
            <div class="oauth-success">
                <h4>🎉 OAuth认证成功！</h4>
                <p>✅ 使用个人Google账号</p>
                <p>⏰ 认证时间: {time_ago//60}分钟前</p>
                <p>📊 享受15GB个人存储空间</p>
                <p>🔗 权限范围: Google Sheets</p>
            </div>
            """, unsafe_allow_html=True)
            
            st.markdown('</div>', unsafe_allow_html=True)
            return client
        else:
            st.markdown(f"""
            <div class="oauth-error">
                <h4>❌ OAuth认证失败</h4>
                <p>错误: {error}</p>
                <p>将清除无效凭据并重新授权</p>
            </div>
            """, unsafe_allow_html=True)
            
            # 清除无效凭据
            if 'google_credentials' in st.session_state:
                del st.session_state['google_credentials']
            if 'auth_method' in st.session_state:
                del st.session_state['auth_method']
            
            time.sleep(2)
            st.rerun()
    
    # 显示授权界面
    handle_oauth_authorization()
    st.markdown('</div>', unsafe_allow_html=True)
    return None

def show_service_auth_panel():
    """显示服务账号认证面板"""
    st.markdown('<div class="service-panel">', unsafe_allow_html=True)
    st.markdown("### 🏢 服务账号模式")
    st.markdown("✅ 使用Google Cloud项目配额（稳定，无需用户授权）")
    
    client, error = get_service_account_client()
    if client:
        st.session_state['auth_method'] = 'service'
        st.session_state['auth_timestamp'] = time.time()
        
        st.markdown("""
        <div class="status-success">
            <h4>✅ 服务账号认证成功！</h4>
            <p>🏢 使用Google Cloud项目配额</p>
            <p>🔒 无需用户授权，自动连接</p>
            <p>🔗 权限范围: Google Sheets</p>
        </div>
        """, unsafe_allow_html=True)
        
        # 显示服务账号信息
        try:
            service_email = st.secrets["google_sheets"]["client_email"]
            st.info(f"📧 服务账号: {service_email}")
        except:
            pass
        
        st.markdown('</div>', unsafe_allow_html=True)
        return client
    else:
        st.markdown(f"""
        <div class="status-error">
            <h4>❌ 服务账号配置错误</h4>
            <p>错误: {error}</p>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("""
        **需要配置服务账号secrets：**
        ```toml
        [google_sheets]
        type = "service_account"
        project_id = "你的项目ID"
        private_key_id = "密钥ID"
        private_key = "-----BEGIN PRIVATE KEY-----\\n...\\n-----END PRIVATE KEY-----\\n"
        client_email = "service-account@project.iam.gserviceaccount.com"
        client_id = "客户端ID"
        auth_uri = "https://accounts.google.com/o/oauth2/auth"
        token_uri = "https://oauth2.googleapis.com/token"
        auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
        client_x509_cert_url = "https://www.googleapis.com/robot/v1/metadata/x509/..."
        ```
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        return None

def compress_data(data: str) -> str:
    """压缩数据"""
    try:
        compressed = zlib.compress(data.encode('utf-8'), COMPRESSION_LEVEL)
        encoded = base64.b64encode(compressed).decode('ascii')
        return encoded
    except Exception as e:
        logger.error(f"数据压缩失败: {str(e)}")
        return data

def decompress_data(compressed_data: str) -> str:
    """解压数据"""
    try:
        decoded = base64.b64decode(compressed_data.encode('ascii'))
        decompressed = zlib.decompress(decoded).decode('utf-8')
        return decompressed
    except Exception:
        return compressed_data

def get_or_create_spreadsheet(gc, name="门店报表系统数据"):
    """获取或创建表格"""
    try:
        spreadsheet = gc.open(name)
        logger.info(f"表格 '{name}' 已存在")
        return spreadsheet
    except gspread.SpreadsheetNotFound:
        logger.info(f"创建新表格 '{name}'")
        spreadsheet = gc.create(name)
        return spreadsheet

def get_or_create_worksheet(spreadsheet, name, rows=500, cols=10):
    """获取或创建工作表"""
    try:
        worksheet = spreadsheet.worksheet(name)
        logger.info(f"工作表 '{name}' 已存在")
        return worksheet
    except gspread.WorksheetNotFound:
        logger.info(f"创建新工作表 '{name}'")
        worksheet = spreadsheet.add_worksheet(title=name, rows=rows, cols=cols)
        return worksheet

def clean_and_compress_dataframe(df: pd.DataFrame) -> str:
    """清理并压缩DataFrame"""
    try:
        df_cleaned = df.copy()
        for col in df_cleaned.columns:
            df_cleaned[col] = df_cleaned[col].astype(str)
            df_cleaned[col] = df_cleaned[col].replace({
                'nan': '', 'None': '', 'NaT': '', 'null': '', '<NA>': ''
            })
            df_cleaned[col] = df_cleaned[col].apply(
                lambda x: x[:800] + '...' if len(str(x)) > 800 else x
            )
        
        json_data = df_cleaned.to_json(orient='records', force_ascii=False, ensure_ascii=False)
        compressed_data = compress_data(json_data)
        
        logger.info(f"数据处理完成: {len(df_cleaned)} 行 -> {len(compressed_data)} 字符")
        return compressed_data
        
    except Exception as e:
        logger.error(f"清理压缩DataFrame失败: {str(e)}")
        raise

def save_permissions_to_sheets(df: pd.DataFrame, gc) -> bool:
    """保存权限数据"""
    try:
        spreadsheet = get_or_create_spreadsheet(gc)
        worksheet = get_or_create_worksheet(spreadsheet, PERMISSIONS_SHEET_NAME, rows=100, cols=5)
        
        worksheet.clear()
        time.sleep(1)
        
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # 压缩权限数据
        compressed_data = []
        for _, row in df.iterrows():
            store_name = str(row.iloc[0]).strip()
            user_id = str(row.iloc[1]).strip()
            compressed_data.append(f"{store_name}|{user_id}")
        
        all_permissions = ";".join(compressed_data)
        compressed_permissions = compress_data(all_permissions)
        
        data = [
            ['数据类型', '压缩数据', '记录数', '更新时间'],
            ['permissions', compressed_permissions, len(df), current_time]
        ]
        
        worksheet.update('A1', data)
        logger.info(f"权限数据保存成功: {len(df)} 条记录")
        
        # 清除缓存
        if 'cache_permissions_load' in st.session_state:
            del st.session_state['cache_permissions_load']
        
        return True
    except Exception as e:
        logger.error(f"保存权限数据失败: {str(e)}")
        st.error(f"❌ 保存权限数据失败: {str(e)}")
        return False

def load_permissions_from_sheets(gc) -> Optional[pd.DataFrame]:
    """加载权限数据"""
    # 检查缓存
    if 'cache_permissions_load' in st.session_state:
        cache_data = st.session_state['cache_permissions_load']
        if time.time() - cache_data['timestamp'] < CACHE_DURATION:
            logger.info("从缓存加载权限数据")
            return cache_data['data']
    
    try:
        spreadsheet = get_or_create_spreadsheet(gc)
        worksheet = spreadsheet.worksheet(PERMISSIONS_SHEET_NAME)
        data = worksheet.get_all_values()
        
        if len(data) <= 1:
            logger.info("权限表为空")
            return None
        
        if len(data) >= 2 and len(data[1]) >= 2:
            compressed_data = data[1][1]
            decompressed_data = decompress_data(compressed_data)
            
            permissions = []
            for item in decompressed_data.split(';'):
                if '|' in item:
                    store_name, user_id = item.split('|', 1)
                    permissions.append({
                        '门店名称': store_name.strip(),
                        '人员编号': user_id.strip()
                    })
            
            if permissions:
                result_df = pd.DataFrame(permissions)
                result_df = result_df[
                    (result_df['门店名称'] != '') & 
                    (result_df['人员编号'] != '')
                ]
                
                logger.info(f"权限数据加载成功: {len(result_df)} 条记录")
                
                # 设置缓存
                st.session_state['cache_permissions_load'] = {
                    'data': result_df,
                    'timestamp': time.time()
                }
                return result_df
        
        return None
        
    except gspread.WorksheetNotFound:
        logger.info("权限表不存在")
        return None
    except Exception as e:
        logger.error(f"加载权限数据失败: {str(e)}")
        st.error(f"❌ 加载权限数据失败: {str(e)}")
        return None

def save_reports_to_sheets(reports_dict: Dict[str, pd.DataFrame], gc) -> bool:
    """保存报表数据"""
    try:
        spreadsheet = get_or_create_spreadsheet(gc)
        worksheet = get_or_create_worksheet(spreadsheet, REPORTS_SHEET_NAME, rows=300, cols=8)
        
        with st.spinner("清理旧数据..."):
            worksheet.clear()
            time.sleep(1)
        
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        all_data = [['门店名称', '压缩数据', '行数', '列数', '更新时间', '分片序号', '总分片数', '数据哈希']]
        
        with st.spinner("压缩并保存数据..."):
            total_stores = len(reports_dict)
            progress_bar = st.progress(0)
            
            for idx, (store_name, df) in enumerate(reports_dict.items()):
                try:
                    compressed_data = clean_and_compress_dataframe(df)
                    data_hash = hashlib.md5(compressed_data.encode('utf-8')).hexdigest()[:16]
                    
                    if len(compressed_data) <= MAX_CHUNK_SIZE:
                        all_data.append([
                            store_name, compressed_data, len(df), len(df.columns), 
                            current_time, "1", "1", data_hash
                        ])
                    else:
                        chunks = []
                        for i in range(0, len(compressed_data), MAX_CHUNK_SIZE):
                            chunks.append(compressed_data[i:i + MAX_CHUNK_SIZE])
                        
                        total_chunks = len(chunks)
                        for chunk_idx, chunk in enumerate(chunks):
                            chunk_name = f"{store_name}_分片{chunk_idx+1}"
                            all_data.append([
                                chunk_name, chunk, len(df), len(df.columns),
                                current_time, str(chunk_idx+1), str(total_chunks), data_hash
                            ])
                    
                    progress_bar.progress((idx + 1) / total_stores)
                    logger.info(f"处理完成: {store_name}")
                    
                except Exception as e:
                    logger.error(f"处理 {store_name} 时出错: {str(e)}")
                    continue
            
            progress_bar.empty()
        
        # 分批保存数据
        batch_size = 15
        if len(all_data) > 1:
            for i in range(1, len(all_data), batch_size):
                batch_data = all_data[i:i+batch_size]
                
                if i == 1:
                    worksheet.update('A1', [all_data[0]] + batch_data)
                else:
                    row_num = i + 1
                    worksheet.update(f'A{row_num}', batch_data)
                
                time.sleep(0.8)
        
        logger.info(f"报表数据保存完成: {len(all_data) - 1} 条记录")
        
        # 清除缓存
        if 'cache_reports_load' in st.session_state:
            del st.session_state['cache_reports_load']
        
        return True
    except Exception as e:
        logger.error(f"保存报表数据失败: {str(e)}")
        st.error(f"❌ 保存报表数据失败: {str(e)}")
        return False

def load_reports_from_sheets(gc) -> Dict[str, pd.DataFrame]:
    """加载报表数据"""
    # 检查缓存
    if 'cache_reports_load' in st.session_state:
        cache_data = st.session_state['cache_reports_load']
        if time.time() - cache_data['timestamp'] < CACHE_DURATION:
            logger.info("从缓存加载报表数据")
            return cache_data['data']
    
    try:
        spreadsheet = get_or_create_spreadsheet(gc)
        worksheet = spreadsheet.worksheet(REPORTS_SHEET_NAME)
        data = worksheet.get_all_values()
        
        if len(data) <= 1:
            logger.info("报表数据为空")
            return {}
        
        reports_dict = {}
        fragments_dict = {}
        
        for row in data[1:]:
            if len(row) >= 7:
                store_name = row[0]
                compressed_data = row[1]
                chunk_num = row[5]
                total_chunks = row[6]
                data_hash = row[7] if len(row) > 7 else ''
                
                if '_分片' in store_name:
                    base_name = store_name.split('_分片')[0]
                    if base_name not in fragments_dict:
                        fragments_dict[base_name] = []
                    
                    fragments_dict[base_name].append({
                        'data': compressed_data,
                        'chunk_num': chunk_num,
                        'total_chunks': total_chunks,
                        'data_hash': data_hash
                    })
                else:
                    fragments_dict[store_name] = [{
                        'data': compressed_data,
                        'chunk_num': '1',
                        'total_chunks': '1',
                        'data_hash': data_hash
                    }]
        
        # 重构所有数据
        for store_name, fragments in fragments_dict.items():
            try:
                if len(fragments) == 1:
                    compressed_data = fragments[0]['data']
                else:
                    fragments.sort(key=lambda x: int(x['chunk_num']))
                    compressed_data = ''.join([frag['data'] for frag in fragments])
                
                json_data = decompress_data(compressed_data)
                df = pd.read_json(json_data, orient='records')
                
                if not df.empty:
                    # 数据后处理（简化版本）
                    if len(df) > 0:
                        first_row = df.iloc[0]
                        non_empty_count = sum(1 for val in first_row if pd.notna(val) and str(val).strip() != '')
                        if non_empty_count <= 2 and len(df) > 1:
                            df = df.iloc[1:].reset_index(drop=True)
                    
                    # 处理表头
                    if len(df) > 1:
                        header_row = df.iloc[0].fillna('').astype(str).tolist()
                        data_rows = df.iloc[1:].copy()
                        
                        cols = []
                        for i, col in enumerate(header_row):
                            col = str(col).strip()
                            if col == '' or col == 'nan' or col == '0':
                                col = f'列{i+1}' if i > 0 else '项目名称'
                            
                            original_col = col
                            counter = 1
                            while col in cols:
                                col = f"{original_col}_{counter}"
                                counter += 1
                            cols.append(col)
                        
                        min_cols = min(len(data_rows.columns), len(cols))
                        cols = cols[:min_cols]
                        data_rows = data_rows.iloc[:, :min_cols]
                        data_rows.columns = cols
                        df = data_rows.reset_index(drop=True).fillna('')
                    
                    reports_dict[store_name] = df
                    logger.info(f"{store_name} 数据加载成功: {len(df)} 行")
            
            except Exception as e:
                logger.error(f"解析 {store_name} 数据失败: {str(e)}")
                continue
        
        logger.info(f"报表数据加载完成: {len(reports_dict)} 个门店")
        
        # 设置缓存
        st.session_state['cache_reports_load'] = {
            'data': reports_dict,
            'timestamp': time.time()
        }
        return reports_dict
        
    except gspread.WorksheetNotFound:
        logger.info("报表数据表不存在")
        return {}
    except Exception as e:
        logger.error(f"加载报表数据失败: {str(e)}")
        st.error(f"❌ 加载报表数据失败: {str(e)}")
        return {}

def analyze_receivable_data(df: pd.DataFrame) -> Dict[str, Any]:
    """分析应收未收额数据"""
    result = {}
    
    if len(df.columns) == 0 or len(df) == 0:
        return result
    
    # 检查第一行是否是门店名称
    first_row = df.iloc[0] if len(df) > 0 else None
    if first_row is not None:
        non_empty_count = sum(1 for val in first_row if pd.notna(val) and str(val).strip() != '')
        if non_empty_count <= 2:
            df = df.iloc[1:].reset_index(drop=True)
            result['skipped_store_name_row'] = True
    
    # 查找第69行
    target_row_index = 68
    
    if len(df) > target_row_index:
        row = df.iloc[target_row_index]
        first_col_value = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
        
        keywords = ['应收-未收额', '应收未收额', '应收-未收', '应收未收']
        
        for keyword in keywords:
            if keyword in first_col_value:
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
    
    # 备用查找
    if '应收-未收额' not in result:
        keywords = ['应收-未收额', '应收未收额', '应收-未收', '应收未收']
        
        for idx, row in df.iterrows():
            try:
                row_name = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
                if not row_name.strip():
                    continue
                
                for keyword in keywords:
                    if keyword in row_name:
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
                                            'row_name': row_name,
                                            'row_index': idx,
                                            'actual_row_number': idx + 1,
                                            'note': f'在第{idx+1}行找到（非第69行）'
                                        }
                                        return result
                                except ValueError:
                                    continue
                        break
            except Exception:
                continue
    
    result['debug_info'] = {
        'total_rows': len(df),
        'checked_row_69': len(df) > target_row_index,
        'row_69_content': str(df.iloc[target_row_index].iloc[0]) if len(df) > target_row_index else 'N/A'
    }
    
    return result

def verify_user_permission(store_name: str, user_id: str, permissions_data: Optional[pd.DataFrame]) -> bool:
    """验证用户权限"""
    if permissions_data is None or len(permissions_data.columns) < 2:
        return False
    
    store_col = permissions_data.columns[0]
    id_col = permissions_data.columns[1]
    
    for _, row in permissions_data.iterrows():
        stored_store = str(row[store_col]).strip()
        stored_id = str(row[id_col]).strip()
        
        if (store_name in stored_store or stored_store in store_name) and stored_id == str(user_id):
            return True
    
    return False

def find_matching_reports(store_name: str, reports_data: Dict[str, pd.DataFrame]) -> List[str]:
    """查找匹配的报表"""
    matching = []
    for sheet_name in reports_data.keys():
        if store_name in sheet_name or sheet_name in store_name:
            matching.append(sheet_name)
    return matching

# 初始化会话状态
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
if 'store_name' not in st.session_state:
    st.session_state.store_name = ""
if 'user_id' not in st.session_state:
    st.session_state.user_id = ""
if 'is_admin' not in st.session_state:
    st.session_state.is_admin = False

# 主标题
st.markdown('<h1 class="main-header">📊 门店报表查询系统</h1>', unsafe_allow_html=True)

# 认证选择和连接
gc = show_authentication_selector()

if not gc:
    st.info("👆 请先完成Google账号认证")
    st.stop()

# 显示当前认证状态
auth_method = st.session_state.get('auth_method', 'unknown')
auth_time = st.session_state.get('auth_timestamp', 0)
time_ago = int(time.time() - auth_time) if auth_time > 0 else 0

if auth_method == 'oauth':
    st.markdown(f"""
    <div class="status-success">
        ✅ <strong>使用OAuth个人账号</strong><br>
        🗄️ 享受15GB个人存储空间<br>
        ⏰ 认证时间: {time_ago//60}分钟前<br>
        🔗 权限范围: Google Sheets
    </div>
    """, unsafe_allow_html=True)
elif auth_method == 'service':
    st.markdown(f"""
    <div class="status-success">
        ✅ <strong>使用服务账号</strong><br>
        🏢 使用Google Cloud项目配额<br>
        ⏰ 连接时间: {time_ago//60}分钟前<br>
        🔗 权限范围: Google Sheets
    </div>
    """, unsafe_allow_html=True)

# 侧边栏
with st.sidebar:
    st.title("⚙️ 系统功能")
    
    # 系统状态
    st.subheader("📡 系统状态")
    if gc:
        st.success("🟢 已连接Google Sheets")
        st.info(f"🔐 认证方式: {auth_method}")
        if time_ago > 0:
            st.info(f"⏰ 连接时长: {time_ago//60}分钟")
    else:
        st.error("🔴 未连接")
    
    # 认证管理
    st.subheader("🔐 认证管理")
    if st.button("🔄 切换认证方式"):
        # 清除认证信息
        auth_keys = ['google_credentials', 'auth_method', 'auth_timestamp']
        for key in auth_keys:
            if key in st.session_state:
                del st.session_state[key]
        st.rerun()
    
    if st.button("🗑️ 清理系统缓存"):
        cache_keys = [key for key in st.session_state.keys() if key.startswith('cache_')]
        for key in cache_keys:
            del st.session_state[key]
        st.success("✅ 缓存已清除")
        time.sleep(1)
        st.rerun()
    
    user_type = st.radio("选择用户类型", ["普通用户", "管理员"])
    
    if user_type == "管理员":
        st.subheader("🔐 管理员登录")
        admin_password = st.text_input("管理员密码", type="password")
        
        if st.button("验证管理员身份"):
            if admin_password == ADMIN_PASSWORD:
                st.session_state.is_admin = True
                st.success("✅ 验证成功！")
                st.rerun()
            else:
                st.error("❌ 密码错误！")
        
        if st.session_state.is_admin:
            st.subheader("📁 文件管理")
            
            # 上传权限表
            permissions_file = st.file_uploader("上传门店权限表", type=['xlsx', 'xls'])
            if permissions_file:
                try:
                    with st.spinner("处理权限表文件..."):
                        df = pd.read_excel(permissions_file)
                        if len(df.columns) >= 2:
                            with st.spinner("保存到云端..."):
                                if save_permissions_to_sheets(df, gc):
                                    st.success(f"✅ 权限表已上传：{len(df)} 个用户")
                                    st.balloons()
                                else:
                                    st.error("❌ 保存失败")
                        else:
                            st.error("❌ 格式错误：需要至少两列（门店名称、人员编号）")
                except Exception as e:
                    st.error(f"❌ 处理失败：{str(e)}")
            
            # 上传财务报表
            reports_file = st.file_uploader("上传财务报表", type=['xlsx', 'xls'])
            if reports_file:
                try:
                    with st.spinner("处理报表文件..."):
                        excel_file = pd.ExcelFile(reports_file)
                        reports_dict = {}
                        
                        for sheet in excel_file.sheet_names:
                            try:
                                df = pd.read_excel(reports_file, sheet_name=sheet)
                                if not df.empty:
                                    reports_dict[sheet] = df
                                    logger.info(f"读取工作表 '{sheet}': {len(df)} 行")
                            except Exception as e:
                                logger.warning(f"跳过工作表 '{sheet}': {str(e)}")
                                continue
                        
                        if reports_dict:
                            with st.spinner("保存到云端..."):
                                if save_reports_to_sheets(reports_dict, gc):
                                    st.success(f"✅ 报表已上传：{len(reports_dict)} 个门店")
                                    st.balloons()
                                else:
                                    st.error("❌ 保存失败")
                        else:
                            st.error("❌ 文件中没有有效的工作表")
                            
                except Exception as e:
                    st.error(f"❌ 处理失败：{str(e)}")
    
    else:
        if st.session_state.logged_in:
            st.subheader("👤 当前登录")
            st.info(f"门店：{st.session_state.store_name}")
            st.info(f"编号：{st.session_state.user_id}")
            
            if st.button("🚪 退出登录"):
                st.session_state.logged_in = False
                st.session_state.store_name = ""
                st.session_state.user_id = ""
                st.rerun()

# 主界面
if user_type == "管理员" and st.session_state.is_admin:
    st.markdown(f'''
    <div class="admin-panel">
        <h3>👨‍💼 管理员控制面板</h3>
        <p>当前认证: {auth_method} | 支持数据压缩和智能缓存 | 连接时长: {time_ago//60}分钟</p>
    </div>
    ''', unsafe_allow_html=True)
    
    try:
        with st.spinner("加载数据统计..."):
            permissions_data = load_permissions_from_sheets(gc)
            reports_data = load_reports_from_sheets(gc)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            perms_count = len(permissions_data) if permissions_data is not None else 0
            st.metric("权限表用户数", perms_count)
        with col2:
            reports_count = len(reports_data)
            st.metric("报表门店数", reports_count)
        with col3:
            cache_count = len([key for key in st.session_state.keys() if key.startswith('cache_')])
            st.metric("缓存项目数", cache_count)
            
        # 数据预览
        if permissions_data is not None and len(permissions_data) > 0:
            st.subheader("👥 权限数据预览")
            st.dataframe(permissions_data.head(10), use_container_width=True)
        
        if reports_data:
            st.subheader("📊 报表数据预览")
            report_names = list(reports_data.keys())[:3]
            for name in report_names:
                with st.expander(f"📋 {name}"):
                    df = reports_data[name]
                    st.write(f"数据规模: {len(df)} 行 × {len(df.columns)} 列")
                    st.dataframe(df.head(3), use_container_width=True)
                    
    except Exception as e:
        st.error(f"❌ 数据加载失败：{str(e)}")

elif user_type == "管理员" and not st.session_state.is_admin:
    st.info("👈 请在左侧边栏输入管理员密码")

else:
    if not st.session_state.logged_in:
        st.subheader("🔐 用户登录")
        
        try:
            with st.spinner("加载权限数据..."):
                permissions_data = load_permissions_from_sheets(gc)
            
            if permissions_data is None:
                st.warning("⚠️ 系统维护中，请联系管理员")
            else:
                stores = sorted(permissions_data[permissions_data.columns[0]].unique().tolist())
                
                with st.form("login_form"):
                    selected_store = st.selectbox("选择门店", stores)
                    user_id = st.text_input("人员编号")
                    submit = st.form_submit_button("🚀 登录")
                    
                    if submit and selected_store and user_id:
                        if verify_user_permission(selected_store, user_id, permissions_data):
                            st.session_state.logged_in = True
                            st.session_state.store_name = selected_store
                            st.session_state.user_id = user_id
                            st.success("✅ 登录成功！")
                            st.balloons()
                            st.rerun()
                        else:
                            st.error("❌ 门店或编号错误！")
                            
        except Exception as e:
            st.error(f"❌ 权限验证失败：{str(e)}")
    
    else:
        # 已登录 - 显示报表
        st.markdown(f'''
        <div class="store-info">
            <h3>🏪 {st.session_state.store_name}</h3>
            <p>操作员：{st.session_state.user_id} | 认证方式：{auth_method} | 连接时长：{time_ago//60}分钟</p>
        </div>
        ''', unsafe_allow_html=True)
        
        try:
            with st.spinner("加载报表数据..."):
                reports_data = load_reports_from_sheets(gc)
                matching_sheets = find_matching_reports(st.session_state.store_name, reports_data)
            
            if matching_sheets:
                if len(matching_sheets) > 1:
                    selected_sheet = st.selectbox("选择报表", matching_sheets)
                else:
                    selected_sheet = matching_sheets[0]
                
                df = reports_data[selected_sheet]
                
                # 应收-未收额看板
                st.subheader("💰 应收-未收额")
                
                try:
                    analysis_results = analyze_receivable_data(df)
                    
                    if '应收-未收额' in analysis_results:
                        data = analysis_results['应收-未收额']
                        amount = data['amount']
                        
                        col1, col2, col3 = st.columns([1, 2, 1])
                        with col2:
                            if amount > 0:
                                st.markdown(f'''
                                    <div class="receivable-positive">
                                        <h1 style="margin: 0; font-size: 3rem;">💳 ¥{amount:,.2f}</h1>
                                        <h3 style="margin: 0.5rem 0;">门店应付款</h3>
                                        <p style="margin: 0; font-size: 0.9rem;">数据来源: {data['row_name']} (第{data['actual_row_number']}行)</p>
                                    </div>
                                ''', unsafe_allow_html=True)
                            
                            elif amount < 0:
                                st.markdown(f'''
                                    <div class="receivable-negative">
                                        <h1 style="margin: 0; font-size: 3rem;">💚 ¥{abs(amount):,.2f}</h1>
                                        <h3 style="margin: 0.5rem 0;">总部应退款</h3>
                                        <p style="margin: 0; font-size: 0.9rem;">数据来源: {data['row_name']} (第{data['actual_row_number']}行)</p>
                                    </div>
                                ''', unsafe_allow_html=True)
                            
                            else:
                                st.markdown('''
                                    <div style="background: #e8f5e8; color: #2e7d32; padding: 2rem; border-radius: 15px; text-align: center;">
                                        <h1 style="margin: 0; font-size: 3rem;">⚖️ ¥0.00</h1>
                                        <h3 style="margin: 0.5rem 0;">收支平衡</h3>
                                        <p style="margin: 0;">应收未收额为零，账目平衡</p>
                                    </div>
                                ''', unsafe_allow_html=True)
                    
                    else:
                        st.warning("⚠️ 未找到应收-未收额数据")
                
                except Exception as e:
                    st.error(f"❌ 分析数据时出错：{str(e)}")
                
                st.divider()
                
                # 完整报表数据
                st.subheader("📋 完整报表数据")
                
                search_term = st.text_input("🔍 搜索报表内容")
                
                try:
                    if search_term:
                        search_df = df.copy()
                        for col in search_df.columns:
                            search_df[col] = search_df[col].astype(str).fillna('')
                        
                        mask = search_df.apply(
                            lambda x: x.str.contains(search_term, case=False, na=False, regex=False)
                        ).any(axis=1)
                        filtered_df = df[mask]
                        st.info(f"找到 {len(filtered_df)} 条包含 '{search_term}' 的记录")
                    else:
                        filtered_df = df
                    
                    st.info(f"📊 数据统计：共 {len(filtered_df)} 条记录，{len(df.columns)} 列")
                    
                    if len(filtered_df) > 0:
                        display_df = filtered_df.copy()
                        
                        # 确保列名唯一
                        unique_columns = []
                        for i, col in enumerate(display_df.columns):
                            col_name = str(col)
                            if col_name in unique_columns:
                                col_name = f"{col_name}_{i}"
                            unique_columns.append(col_name)
                        display_df.columns = unique_columns
                        
                        # 清理数据内容
                        for col in display_df.columns:
                            display_df[col] = display_df[col].astype(str).fillna('')
                        
                        st.dataframe(display_df, use_container_width=True, height=400)
                    
                    else:
                        st.warning("没有找到符合条件的数据")
                        
                except Exception as e:
                    st.error(f"❌ 数据处理时出错：{str(e)}")
                
                # 下载功能
                st.subheader("📥 数据下载")
                
                col1, col2 = st.columns(2)
                with col1:
                    try:
                        buffer = io.BytesIO()
                        download_df = df.copy()
                        
                        # 确保列名唯一
                        unique_cols = []
                        for i, col in enumerate(download_df.columns):
                            col_name = str(col)
                            if col_name in unique_cols:
                                col_name = f"{col_name}_{i}"
                            unique_cols.append(col_name)
                        download_df.columns = unique_cols
                        
                        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                            download_df.to_excel(writer, index=False)
                        
                        st.download_button(
                            "📥 下载完整报表 (Excel)",
                            buffer.getvalue(),
                            f"{st.session_state.store_name}_报表_{datetime.now().strftime('%Y%m%d')}.xlsx",
                            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                    except Exception as e:
                        st.error(f"Excel下载准备失败：{str(e)}")
                
                with col2:
                    try:
                        csv_df = df.copy()
                        unique_cols = []
                        for i, col in enumerate(csv_df.columns):
                            col_name = str(col)
                            if col_name in unique_cols:
                                col_name = f"{col_name}_{i}"
                            unique_cols.append(col_name)
                        csv_df.columns = unique_cols
                        
                        csv = csv_df.to_csv(index=False, encoding='utf-8-sig')
                        st.download_button(
                            "📥 下载CSV格式",
                            csv,
                            f"{st.session_state.store_name}_报表_{datetime.now().strftime('%Y%m%d')}.csv",
                            "text/csv"
                        )
                    except Exception as e:
                        st.error(f"CSV下载准备失败：{str(e)}")
            
            else:
                st.error(f"❌ 未找到门店 '{st.session_state.store_name}' 的报表")
                
        except Exception as e:
            st.error(f"❌ 报表加载失败：{str(e)}")

# 页面底部状态信息
st.divider()
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.caption(f"🕒 当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
with col2:
    cache_count = len([key for key in st.session_state.keys() if key.startswith('cache_')])
    st.caption(f"💾 缓存项目: {cache_count}")
with col3:
    st.caption(f"⏰ 连接时长: {time_ago//60}分钟" if time_ago > 0 else "⏰ 未连接")
with col4:
    st.caption(f"🔧 版本: v8.0 (稳定OAuth) | 认证: {auth_method}")
