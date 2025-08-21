# 配置指南

## Streamlit Secrets 配置

### 1. 创建配置文件

在项目根目录创建 `.streamlit/secrets.toml` 文件：

```bash
mkdir -p .streamlit
cp .streamlit/secrets.example.toml .streamlit/secrets.toml
```

### 2. 配置MongoDB连接

编辑 `.streamlit/secrets.toml` 文件：

```toml
[mongodb]
# MongoDB Atlas云数据库（推荐）
uri = "mongodb+srv://username:password@cluster.mongodb.net/?retryWrites=true&w=majority"
database_name = "store_reports"

[app]
secret_key = "your_secure_secret_key"
debug = false

[security]
admin_password = "your_admin_password"
session_timeout = 14400
```

### 3. MongoDB Atlas 设置步骤

#### 3.1 注册MongoDB Atlas账号
1. 访问 https://www.mongodb.com/atlas
2. 创建免费账号
3. 创建新的Cluster

#### 3.2 创建数据库用户
1. 在Atlas Dashboard点击 "Database Access"
2. 添加新用户，设置用户名和密码
3. 给用户读写权限

#### 3.3 设置网络访问
1. 点击 "Network Access"
2. 添加IP地址（0.0.0.0/0 允许所有IP，或添加特定IP）

#### 3.4 获取连接字符串
1. 点击 "Connect" 按钮
2. 选择 "Connect your application"
3. 复制连接字符串，格式类似：
   ```
   mongodb+srv://username:password@cluster.mongodb.net/?retryWrites=true&w=majority
   ```

### 4. 本地开发配置

如果使用本地MongoDB：

```toml
[mongodb]
uri = "mongodb://localhost:27017/"
database_name = "store_reports_dev"
```

### 5. 环境变量配置（可选）

如果不想使用Streamlit secrets，也可以使用环境变量：

```bash
export MONGODB_URI="mongodb+srv://username:password@cluster.mongodb.net/"
export DATABASE_NAME="store_reports"
export SECRET_KEY="your_secret_key"
```

### 6. Streamlit Cloud 部署配置

部署到Streamlit Cloud时：

1. 在Streamlit Cloud Dashboard中
2. 点击 "Advanced settings"
3. 在 "Secrets" 区域粘贴配置内容：

```toml
[mongodb]
uri = "mongodb+srv://username:password@cluster.mongodb.net/"
database_name = "store_reports"

[app]
secret_key = "production_secret_key"
debug = false
```

### 7. 安全注意事项

- ✅ **切勿**将 `secrets.toml` 提交到版本控制
- ✅ 使用强密码
- ✅ 定期更换密钥
- ✅ 生产环境使用专用数据库
- ✅ 限制数据库访问IP范围

### 8. 配置验证

启动应用时，系统会自动验证配置：
- ✅ 绿色：配置正确
- ⚠️ 黄色：使用默认配置
- ❌ 红色：配置错误

### 9. 故障排除

#### 连接失败
```
pymongo.errors.ServerSelectionTimeoutError
```
- 检查MongoDB URI是否正确
- 确认网络访问设置
- 验证用户名密码

#### 认证失败
```
pymongo.errors.OperationFailure: Authentication failed
```
- 检查用户名密码
- 确认数据库用户权限

#### 网络问题
```
pymongo.errors.NetworkTimeout
```
- 检查网络连接
- 确认IP白名单设置

### 10. 示例配置模板

```toml
# 生产环境配置
[mongodb]
uri = "mongodb+srv://prod_user:secure_password@prod-cluster.mongodb.net/?retryWrites=true&w=majority"
database_name = "store_reports_prod"

[app]
secret_key = "production_secret_key_very_secure"
debug = false

[security]
admin_password = "super_secure_admin_password"
session_timeout = 7200  # 2小时

# 开发环境配置
# [mongodb]
# uri = "mongodb://localhost:27017/"
# database_name = "store_reports_dev"
# 
# [app]
# secret_key = "dev_secret_key"
# debug = true
```