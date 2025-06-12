#!/bin/bash

# 门店报表查询系统 - 自动部署脚本

echo "========================================="
echo "门店报表查询系统 - 自动部署脚本"
echo "========================================="

# 检查Python是否安装
if ! command -v python3 &> /dev/null; then
    echo "❌ Python3 未安装。请先安装Python 3.8或更高版本。"
    exit 1
fi

echo "✅ Python3 已安装"

# 创建虚拟环境
echo "📦 创建虚拟环境..."
python3 -m venv venv

# 激活虚拟环境
echo "🔧 激活虚拟环境..."
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
    # Windows
    source venv/Scripts/activate
else
    # macOS/Linux
    source venv/bin/activate
fi

# 升级pip
echo "📦 升级pip..."
pip install --upgrade pip

# 安装依赖
echo "📦 安装依赖包..."
pip install -r requirements.txt

# 创建必要的目录
echo "📁 创建目录结构..."
mkdir -p data
mkdir -p logs
mkdir -p .streamlit

# 创建Streamlit配置文件
echo "⚙️ 创建Streamlit配置..."
cat > .streamlit/config.toml << EOF
[theme]
primaryColor = "#1f77b4"
backgroundColor = "#FFFFFF"
secondaryBackgroundColor = "#F0F2F6"
textColor = "#262730"
font = "sans serif"

[server]
maxUploadSize = 200
enableCORS = false
enableXsrfProtection = true

[browser]
gatherUsageStats = false
EOF

# 生成示例数据
echo "📊 生成示例数据..."
python generate_sample_data.py

# 提示用户
echo ""
echo "========================================="
echo "✅ 部署准备完成！"
echo "========================================="
echo ""
echo "下一步操作："
echo "1. 运行应用：streamlit run app.py"
echo "2. 访问地址：http://localhost:8501"
echo ""
echo "部署到云端："
echo "1. 推送到GitHub"
echo "2. 在 share.streamlit.io 部署"
echo ""
echo "提示："
echo "- 默认管理员密码：admin123（请修改）"
echo "- 使用生成的示例文件测试系统"
echo "========================================="
