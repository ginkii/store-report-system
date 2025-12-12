# setup.py - 简化的配置脚本
#!/usr/bin/env python3
"""
门店报表系统 - 快速配置脚本
"""

import os
import secrets
import string
from pathlib import Path

def generate_secret_key(length=32):
    """生成安全密钥"""
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))

def create_streamlit_config():
    """创建Streamlit配置"""
    print("🔧 门店报表系统 - 配置向导")
    print("=" * 50)
    
    # 创建.streamlit目录
    streamlit_dir = Path(".streamlit")
    streamlit_dir.mkdir(exist_ok=True)
    
    # MongoDB配置
    print("\n📋 MongoDB配置")
    mongo_uri = input("MongoDB连接URI (留空使用本地数据库): ").strip()
    if not mongo_uri:
        mongo_uri = "mongodb://localhost:27017/"
    
    db_name = input("数据库名称 [store_reports]: ").strip()
    if not db_name:
        db_name = "store_reports"
    
    # 自动生成密钥
    secret_key = generate_secret_key()
    
    # 创建secrets.toml
    config_content = f"""# 门店报表系统配置文件

[mongodb]
uri = "{mongo_uri}"
database_name = "{db_name}"

[app]
secret_key = "{secret_key}"
debug = false
session_timeout = 14400

[security]
admin_password = "admin123"

# 注意: 请妥善保管此文件，不要提交到版本控制
"""
    
    secrets_file = streamlit_dir / "secrets.toml"
    with open(secrets_file, 'w', encoding='utf-8') as f:
        f.write(config_content)
    
    # 设置文件权限
    try:
        os.chmod(secrets_file, 0o600)
    except OSError:
        pass  # Windows系统可能不支持
    
    print(f"\n✅ 配置文件已创建: {secrets_file}")
    print("🔑 管理员密码: admin123")
    
    return True

def create_requirements():
    """创建requirements.txt"""
    requirements = """streamlit>=1.28.0
pandas>=1.5.0
pymongo>=4.5.0
openpyxl>=3.1.0
plotly>=5.15.0
numpy>=1.24.0
"""
    
    with open("requirements.txt", 'w', encoding='utf-8') as f:
        f.write(requirements)
    
    print("📦 requirements.txt 已创建")

def test_setup():
    """测试配置"""
    try:
        print("\n🔍 测试配置...")
        
        # 测试配置文件
        secrets_file = Path(".streamlit/secrets.toml")
        if not secrets_file.exists():
            print("❌ 配置文件不存在")
            return False
        
        # 测试数据库连接
        try:
            import sys
            sys.path.append('.')
            from config import ConfigManager
            from pymongo import MongoClient
            
            config = ConfigManager.get_mongodb_config()
            client = MongoClient(config['uri'], serverSelectionTimeoutMS=5000)
            db = client[config['database_name']]
            db.command('ping')
            
            print("✅ 数据库连接成功")
            client.close()
            return True
            
        except Exception as e:
            print(f"⚠️ 数据库连接测试失败: {e}")
            print("💡 这是正常的，如果你还没有设置MongoDB")
            return True
            
    except Exception as e:
        print(f"❌ 配置测试失败: {e}")
        return False

def main():
    """主函数"""
    print("门店报表系统配置工具\n")
    print("选择操作:")
    print("1. 创建配置文件")
    print("2. 创建依赖文件")
    print("3. 测试配置")
    print("4. 完整安装")
    print("5. 退出")
    
    while True:
        choice = input("\n请选择 (1-5): ").strip()
        
        if choice == "1":
            create_streamlit_config()
        elif choice == "2":
            create_requirements()
        elif choice == "3":
            test_setup()
        elif choice == "4":
            print("\n🚀 开始完整安装...")
            create_requirements()
            create_streamlit_config()
            test_setup()
            print("\n✅ 安装完成！")
            print("\n🎯 下一步:")
            print("1. 安装依赖: pip install -r requirements.txt")
            print("2. 启动应用: streamlit run main_app.py")
            break
        elif choice == "5":
            print("👋 再见！")
            break
        else:
            print("❌ 无效选择")

if __name__ == "__main__":
    main()
