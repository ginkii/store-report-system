# 门店报表查询系统

一个基于Streamlit的门店报表查询系统，支持汇总Excel文件上传到腾讯云COS，用户可通过门店选择和编码查询获取对应的报表数据。

## 功能特点

- **管理员功能**：
  - 密码登录验证
  - 汇总Excel文件上传到腾讯云COS
  - 自动解析Excel文件中的门店sheet
  - 报表管理和统计
  - 系统监控和设置

- **用户功能**：
  - 两步查询：先选择门店，再输入查询编码
  - 支持精确和模糊匹配
  - 数据预览和导出
  - 查询历史记录

- **系统特性**：
  - 基于JSON的数据存储（支持并发访问）
  - 腾讯云COS文件存储
  - 多用户并发支持
  - 响应式Web界面

## 项目结构

```
├── app.py              # 主应用文件
├── config.py           # 配置文件
├── json_handler.py     # JSON数据操作
├── cos_handler.py      # 腾讯云COS操作
├── excel_parser.py     # Excel文件解析
├── query_handler.py    # 查询处理逻辑
├── requirements.txt    # 项目依赖
├── data.json          # 数据文件（自动生成）
└── README.md          # 项目说明
```

## 环境要求

- Python 3.8+
- Streamlit
- 腾讯云COS账号和配置

## 安装部署

### 1. 克隆项目

```bash
git clone <project-repo>
cd store-report-query
```

### 2. 安装依赖

```bash
pip install -r requirements.txt
```

### 3. 配置环境变量

在系统环境变量中设置以下配置：

```bash
# 管理员密码
export ADMIN_PASSWORD="your_admin_password"

# 腾讯云COS配置
export COS_REGION="ap-beijing"
export COS_SECRET_ID="your_secret_id"
export COS_SECRET_KEY="your_secret_key"
export COS_BUCKET="your-bucket-name"
export COS_DOMAIN="your-custom-domain"  # 可选
```

或者直接在 `config.py` 文件中修改配置：

```python
# 管理员配置
ADMIN_PASSWORD = 'your_admin_password'

# 腾讯云COS配置
COS_CONFIG = {
    'region': 'ap-beijing',
    'secret_id': 'your_secret_id',
    'secret_key': 'your_secret_key',
    'bucket': 'your-bucket-name',
    'domain': 'your-custom-domain',  # 可选
}
```

### 4. 运行应用

```bash
streamlit run app.py
```

系统将在默认端口8501启动，访问 `http://localhost:8501`

## 使用说明

### 管理员操作

1. **登录**：在"管理员登录"标签页输入密码
2. **上传报表**：
   - 选择Excel汇总文件
   - 系统自动解析所有sheet（门店）
   - 填写报表描述并确认上传
3. **管理报表**：查看当前报表、门店列表和历史记录
4. **系统监控**：查看查询统计和系统状态

### 用户查询

1. **选择门店**：从下拉列表选择要查询的门店
2. **输入编码**：输入查询编码（支持数字、字母混合）
3. **查询设置**：选择精确匹配或模糊匹配
4. **查看结果**：查看匹配结果和详细数据
5. **导出数据**：将查询结果导出为Excel文件

## Excel文件要求

- 支持格式：`.xlsx`, `.xls`
- 文件结构：按sheet分门店，每个sheet名称即为门店名称
- 数据格式：每个sheet包含该门店的完整数据
- 文件大小：建议不超过50MB

## 性能优化

- 使用JSON文件存储，支持并发读写
- 文件直接从COS流式下载，无本地缓存
- 分页显示大量数据
- 异步处理文件上传和解析

## 故障排除

### 1. COS连接失败
- 检查COS配置是否正确
- 验证网络连接
- 确认COS权限设置

### 2. 文件上传失败
- 检查文件格式和大小
- 验证COS存储空间
- 确认文件权限

### 3. 查询无结果
- 确认门店是否存在
- 检查编码格式
- 尝试模糊匹配

### 4. 系统错误
- 查看控制台错误日志
- 检查data.json文件权限
- 重启应用服务

## 技术支持

如需技术支持，请联系系统管理员或查看以下资源：

- [Streamlit官方文档](https://docs.streamlit.io/)
- [腾讯云COS文档](https://cloud.tencent.com/document/product/436)
- [Pandas文档](https://pandas.pydata.org/docs/)

## 更新日志

### v1.0.0
- 基础功能实现
- 管理员报表上传
- 用户两步查询
- 数据导出功能

## 许可证

本项目采用MIT许可证。
