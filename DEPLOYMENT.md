# 🚀 Streamlit Cloud 部署指南

## 准备工作

### 1. GitHub 仓库准备

将项目推送到GitHub仓库：

```bash
# 初始化Git仓库
git init

# 添加文件（.gitignore会自动排除敏感文件）
git add .

# 提交代码
git commit -m "初始化门店报表查询系统"

# 添加远程仓库
git remote add origin https://github.com/your-username/store-report-system.git

# 推送到GitHub
git push -u origin main
```

### 2. 重要文件清单

确保以下文件已包含在仓库中：

- ✅ `main.py` - 主入口文件
- ✅ `enhanced_app.py` - 门店查询应用
- ✅ `bulk_uploader.py` - 批量上传应用
- ✅ `config_manager.py` - 配置管理器
- ✅ `requirements.txt` - 依赖包列表
- ✅ `.streamlit/config.toml` - Streamlit配置
- ✅ `.gitignore` - Git忽略文件

## Streamlit Cloud 部署步骤

### 第一步：访问 Streamlit Cloud

1. 访问 https://share.streamlit.io/
2. 使用GitHub账号登录

### 第二步：部署应用

1. 点击 **"New app"**
2. 选择你的GitHub仓库
3. 配置部署信息：
   - **Repository**: `your-username/store-report-system`
   - **Branch**: `main`
   - **Main file path**: `main.py`
   - **App URL**: 选择一个唯一的URL

### 第三步：配置Secrets

在Streamlit Cloud的应用设置中，添加以下secrets：

```toml
[mongodb]
uri = "mongodb+srv://username:password@cluster.mongodb.net/?retryWrites=true&w=majority"
database_name = "store_reports"

[app]
secret_key = "your_production_secret_key"
debug = false

[security]
admin_password = "your_admin_password"
session_timeout = 14400
```

### 第四步：部署应用

1. 点击 **"Deploy!"**
2. 等待应用构建完成
3. 访问生成的URL

## 🔧 配置MongoDB Atlas（如果还没有）

### 1. 创建MongoDB Atlas账号

1. 访问 https://www.mongodb.com/atlas
2. 注册免费账号
3. 创建新的Cluster

### 2. 配置数据库访问

1. **Database Access**: 创建数据库用户
2. **Network Access**: 添加IP白名单（0.0.0.0/0 允许所有IP）
3. **Connect**: 获取连接字符串

### 3. 获取连接URI

连接字符串格式：
```
mongodb+srv://username:password@cluster.mongodb.net/?retryWrites=true&w=majority
```

## 📱 应用功能

部署后的应用包含两个主要功能：

### 🏪 门店查询系统
- 门店用户登录查询
- 应收未收金额看板
- 财务报表分析
- 多月份数据对比

### 📤 批量上传系统
- 管理员批量上传Excel
- 自动门店识别
- 上传进度显示
- 结果统计分析

## 🔒 安全注意事项

### 生产环境配置
- ✅ 使用强密码
- ✅ 设置复杂的secret_key
- ✅ 限制MongoDB访问IP（可选）
- ✅ 定期更换密码

### 数据保护
- ✅ secrets.toml不会被提交到Git
- ✅ 敏感信息通过Streamlit Cloud Secrets管理
- ✅ 数据库连接加密传输

## 🐛 故障排除

### 常见部署问题

#### 1. 依赖包安装失败
```
ERROR: Could not install packages due to an EnvironmentError
```
**解决方案**: 检查requirements.txt中的包版本是否兼容

#### 2. MongoDB连接失败
```
pymongo.errors.ServerSelectionTimeoutError
```
**解决方案**: 
- 检查Secrets中的MongoDB URI
- 确认网络访问白名单设置

#### 3. 应用启动错误
```
ModuleNotFoundError: No module named 'config_manager'
```
**解决方案**: 确保所有Python文件都已推送到GitHub

### 调试技巧

1. **查看日志**: 在Streamlit Cloud应用页面查看详细日志
2. **本地测试**: 部署前在本地测试所有功能
3. **分步部署**: 先部署基础功能，再逐步添加复杂功能

## 📊 性能优化

### Streamlit Cloud限制
- **内存**: 1GB RAM
- **文件上传**: 最大200MB
- **并发用户**: 适中规模

### 优化建议
- ✅ 使用 `@st.cache_resource` 缓存数据库连接
- ✅ 使用 `@st.cache_data` 缓存查询结果
- ✅ 分批处理大文件上传
- ✅ 优化数据库查询索引

## 🔄 更新部署

更新应用只需：

1. 修改代码并推送到GitHub
2. Streamlit Cloud会自动重新部署
3. 无需重新配置Secrets

```bash
# 更新代码
git add .
git commit -m "更新功能"
git push origin main
```

## 📞 支持

如遇到部署问题：

1. 📖 查看本文档的故障排除部分
2. 📝 检查Streamlit Cloud应用日志
3. 🔍 确认MongoDB连接配置
4. 💬 参考Streamlit Community论坛

---

🎉 **部署完成后，你将拥有一个完全云端化的门店报表查询系统！**