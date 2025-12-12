# Streamlit Cloud 部署指南

## 🚀 快速部署

### 1. 必需文件
确保您的仓库包含以下文件：
- `streamlit_app.py` (主入口文件)
- `requirements_cloud.txt` (依赖包列表)
- `.streamlit/config.toml` (Streamlit配置)

### 2. 部署步骤

#### 在Streamlit Cloud：
1. 访问 [share.streamlit.io](https://share.streamlit.io)
2. 连接您的GitHub仓库
3. 选择分支和主文件：`streamlit_app.py`
4. 配置Secrets（见下方）

### 3. Secrets配置

在Streamlit Cloud App设置的Secrets部分添加：

```toml
[mongodb]
uri = "your_mongodb_connection_string"
database_name = "store_reports"

[security]
admin_password = "admin123"

[app]
secret_key = "your_secret_key"
debug = false
session_timeout = 14400
```

### 4. MongoDB Atlas设置

如果使用MongoDB Atlas：
1. 创建MongoDB Atlas账户
2. 创建集群
3. 设置数据库用户和密码
4. 配置网络访问（允许所有IP: 0.0.0.0/0）
5. 获取连接字符串

### 5. 功能说明

#### 📍 当前版本功能：
- ✅ 门店查询系统（基础版本）
- ✅ 管理员登录验证
- ✅ 数据库连接状态检查
- ✅ 基础报表展示
- ⚠️ 上传功能（简化版本）
- ⚠️ 权限管理（基础版本）

#### 🔧 完整功能版本：
如需完整功能，请确保所有模块文件都在仓库中：
- `mongodb_store_system_fixed.py`
- `bulk_uploader_fixed.py`
- `permission_manager_fixed.py`
- `database_manager.py`
- `data_models.py`
- `config.py`

### 6. 常见问题

#### Q: 应用无法启动
A: 检查requirements.txt和主文件名是否正确

#### Q: 数据库连接失败
A: 验证MongoDB连接字符串和网络设置

#### Q: 模块导入错误
A: 确保所有依赖文件都在仓库中

#### Q: Secrets配置无效
A: 在Streamlit Cloud App设置中重新保存Secrets

### 7. 本地测试

本地运行测试：
```bash
pip install -r requirements_cloud.txt
streamlit run streamlit_app.py
```

### 8. 更新部署

更新应用：
1. 推送代码到GitHub
2. Streamlit Cloud会自动重新部署

---

🎯 **重要提示**：当前`streamlit_app.py`是简化版本，包含基础功能。如需完整功能，请使用完整的模块化版本。