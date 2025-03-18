# Automation Studio Alarm Data Handler

## 核心功能

- **持续监控**：程序运行后，自动监控指定目录中的新 Automation Studios Alarm文件
- **解码**：支持包括 UTF-8 和中文字符集
- **数据库存储**：自动将Alarm数据存储到 MySQL 数据库，并确保不会重复导入相同的记录
- **错误处理**：错误处理机制和自动重连
- **详细日志**：Log信息输出到控制台和文件(alarm_monitor.log，默认保存在项目同一目录)
- **数据查看**：通过命令行工具查看和分析数据库中存储的Alarm数据
- **表管理**：可以运行clear_table.py直接清空数据表

## 系统要求

- Python 3.6 或更高版本
- MySQL 服务器 5.7 或更高版本
- 必需的 Python 包:
  - `mysql-connector-python>=8.0.26`
  - `configparser>=5.0.0`

## 安装步骤

### 1. 安装 MySQL 数据库

#### Windows 系统安装 MySQL

1. 访问 MySQL 官方下载页面: https://dev.mysql.com/downloads/mysql/
2. 下载适合 Windows 版本的 MySQL 安装包（推荐使用 MySQL Installer）
3. 运行下载的安装程序，按照向导进行安装:
   - 选择"Developer Default"或"Server only"安装类型
   - 设置 MySQL root 账户的密码，务必记住此密码
   - 保留默认配置选项
   - 完成安装并确保 MySQL 服务已启动

#### 配置 MySQL 以允许远程连接

**重要说明**: 如MySQL 数据库将部署在虚拟机上并需要来自远程的访问，则需进行以下配置：

1. 编辑 MySQL 配置文件：
   - 找到 `bind-address` 设置，修改为 `bind-address = 0.0.0.0` 或将该行注释掉
   - 重启 MySQL 服务以应用更改

2. 为远程访问创建用户账户：
   ```sql
   CREATE USER 'alarm_user'@'%' IDENTIFIED BY 'your_password';
   GRANT ALL PRIVILEGES ON alarm_db.* TO 'alarm_user'@'%';
   FLUSH PRIVILEGES;
   ```
   注意: `'alarm_user'@'%'` 中的 `%` 表示允许从任何IP地址连接，可以替换为特定IP地址以提高安全性

3. 查看并记录虚拟机的 IP 地址（其他系统将使用此IP连接到MySQL）：
   ```
   ipconfig
   ```

4. 如果虚拟机上有防火墙，确保开放 MySQL 端口（默认 3306）：
   - 在 Windows 防火墙中添加入站规则
   - 或使用以下命令：
     ```
     netsh advfirewall firewall add rule name="MySQL" dir=in action=allow protocol=TCP localport=3306
     ```

### 2. 配置 MySQL 数据库
安装完成后，需要创建数据库和用户:

```bash
# 登录 MySQL（Windows 使用 MySQL Command Line Client，下载时默认安装好）
mysql -u root -p

# 创建用于存储报警数据的数据库
# 便于支持中文和特殊字符
# 对数据库性能影响很小
CREATE DATABASE alarm_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

# 创建专用用户并设置密码（将 'password' 改为强密码）
CREATE USER 'alarm_user'@'localhost' IDENTIFIED BY 'password';

# 授予用户对数据库的所有权限
GRANT ALL PRIVILEGES ON alarm_db.* TO 'alarm_user'@'localhost';

# 刷新权限
FLUSH PRIVILEGES;

# 退出 MySQL
EXIT;
```

### 3. 安装 Python 项目

1. 克隆或下载此仓库
2. 安装必需的 Python 包:
   ```
   pip install -r requirements.txt
   ```

3. 从模板创建并配置 `config.ini` 文件:

   Windows 命令行:
   ```
   copy config.ini.template config.ini
   ```

   修改 `config.ini` 文件中的数据库连接参数:
   - `db_host`: MySQL 服务器地址
     - 本地连接使用 `localhost`
     - **对于远程部署**: 使用虚拟机的实际 IP 地址（如 `192.168.1.100`）
     - **重要**: **如果程序和 MySQL 都部署在同一虚拟机上，但需要允许远程读写访问，仍需配置为 MySQL 服务器可从外部访问，但程序连接可使用 `localhost`**
   - `db_port`: MySQL 端口（默认 3306）
   - `db_user`: 所创建的 MySQL 用户名 (如 `alarm_user`)
   - `db_password`: 该用户的对应密码
   - `db_name`: 所创建的数据库名称 (如 `alarm_db`)




## 配置说明

应用程序使用 `config.ini` 文件进行设置。如果该文件不存在，程序会在首次启动时创建一个默认配置文件，也可以从模板创建配置文件：

```
# Windows 命令行
#该行会将template中的内容复制一份并生成一个新的文件config.ini
copy config.ini.template config.ini
```

### 主要配置选项

下述为设置的默认值，需要修改后使用：

```ini
[DEFAULT]
# 监控新报警文件的目录
monitoring_dir = ./alarm_files

# 报警文件匹配模式
file_pattern = Alarms_*.csv

# 检查新文件的时间间隔（秒）
polling_interval = 10

# 数据库连接参数
db_host = localhost
db_port = 3306
db_user = alarm_user
db_password = password
db_name = alarm_db
table_name = alarms

[LOGGING]
# 日志级别: DEBUG, INFO, WARNING, ERROR, CRITICAL
log_level = INFO
log_file = alarm_monitor.log
```

### Windows 路径注意事项

在 Windows 系统中配置路径时，可以使用以下格式：

1. 相对路径（相对于应用程序所在目录）：
   ```
   monitoring_dir = ./alarm_files
   ```

2. 绝对路径（使用正斜杠或双反斜杠）：
   ```
   monitoring_dir = C:/AlarmData/alarm_files
   ```
   或
   ```
   monitoring_dir = C:\\AlarmData\\alarm_files
   ```



## 使用方法

### 启动alarm_monitor.py

直接运行应用程序:

```
python alarm_monitor.py
```

应用程序将开始监控配置的目录，并在新的报警文件出现时处理它们，直到监控停止。



### 停止监控

在终端中按 `Ctrl+C` 停止监控进程，应用程序将进行正确的断开数据库连接及关闭操作。



### 查看报警数据

使用 `view_alarms.py` 脚本查看存储在数据库中的报警数据:

```
python view_alarms.py
```

默认情况下，这将显示数据库中最近的 300 条报警记录。

#### 查看选项

该脚本支持几个命令行选项，用于过滤和查看报警数据:

- `--limit N`: 显示最多 N 条记录 (默认: 300)
- `--stats`: 显示所有存储Alarm的统计信息
- `--code CODE`: 按指定代码过滤Alarm信息
- `--severity LEVEL`: 按严重性级别过滤Alarm
- `--start-time "YYYY-MM-DD HH:MM:SS"`: 过滤的开始时间
- `--end-time "YYYY-MM-DD HH:MM:SS"`: 过滤的结束时间

示例:

```bash
# 显示最新的 50 条报警
python view_alarms.py --limit 50

# 显示所有报警的统计信息
python view_alarms.py --stats

# 显示代码为 11 的报警
python view_alarms.py --code 11

# 显示严重性级别为 2 的报警
python view_alarms.py --severity 2

# 显示特定时间范围内的报警
python view_alarms.py --start-time "2025-01-01 00:00:00" --end-time "2025-01-31 23:59:59"
```

### 远程查看报警数据

如果需要从远程系统查看虚拟机上数据库中的报警数据：

1. 确保虚拟机上的 MySQL 已配置为允许远程连接
2. 在远程系统上安装必要的 Python 包
3. 复制 `view_alarms.py` 和 `config.ini.template` 到远程系统
4. 配置 `config.ini` 文件，将 `db_host` 设置为虚拟机的 IP 地址
5. 运行查看命令:
   ```
   python view_alarms.py
   ```

### 清空数据表[!]

使用 `clear_table.py` 脚本清空报警数据表:

```
python clear_table.py
```

此操作将删除数据表中的所有数据，但保留表结构。执行前会要求确认，防止意外数据丢失。



## 文件结构

- `alarm_monitor.py` - 主应用程序，实现监控和数据处理功能
- `view_alarms.py` - 查看和分析存储的报警数据
- `clear_table.py` - 清空报警数据表
- `config.ini` - 配置文件 (首次运行时会自动创建)
- `config.ini.template` - 配置模板
- `requirements.txt` - Python 包依赖



## 中文字符路径处理

应用程序内置了对非 ASCII 字符路径的支持，包括中文:

1. 确保 `config.ini` 文件以 UTF-8 编码保存
2. 如果监控目录包含中文字符，应用程序将自动处理编码
3. 应用程序同时支持 UTF-8、GBK 和 GB18030 等常见中文编码



## 技术实现细节

### Automation Studios Alarm文件处理流程

1. 监控所指定目录中的新 CSV 文件
2. 检测文件编码（支持 UTF-8、GBK、GB18030 等）
3. 会根据CSV文件的文件名进行解析（如Alarms_2025_01_06_18_04_01.csv，会在文件夹中根据时间戳匹配导入)
4. 解析 CSV 数据，提取Alarm信息
5. 检查数据库中是否存在相同时间戳的记录，防止重复导入（后续数据量增大后，可能会对性能造成影响，暂定）
6. 将新的Alarm数据批量插入到数据库中（如设置的默认扫描时间为10s一次，则会插入这10s中产生的所有新的csv格式文件）

### 数据库表结构

```sql
CREATE TABLE alarms (
    id INT AUTO_INCREMENT PRIMARY KEY,              /* 每条记录的唯一标识符 */
    Time DATETIME(3) NOT NULL,                      /* 报警时间戳，精确到毫秒 */
    Instance INT NOT NULL,                          /* 实例 ID */
    Name VARCHAR(255) NOT NULL,                     /* 报警名称 */
    Code INT NOT NULL,                              /* 报警代码 */
    Severity INT NOT NULL,                          /* 严重性级别 */
    AdditionalInformation1 VARCHAR(255),            /* 附加信息字段 1 */
    AdditionalInformation2 VARCHAR(255),            /* 附加信息字段 2 */
    `Change` TEXT NOT NULL,                         /* 报警状态变化（例如，"Inactive -> Active"） */
    Message TEXT NOT NULL,                          /* 报警消息文本 */
    file_source VARCHAR(255) NOT NULL,              /* 源文件名，用于跟踪 */
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, /* 记录创建时间戳 */
    INDEX idx_time (Time)                           /* 时间索引，加速查询 */
)
```

## 故障排除

### 可能出现的问题

1. **MySQL 连接问题**:
   - 确保 MySQL 服务器正在运行
     - Windows: 在Windows Services中检查 MySQL 服务是否启动
       - 按 Win+R，输入 services.msc，查找并确认 MySQL 服务状态
     - 远程连接问题: 使用 telnet 测试连接
       - `telnet [server_ip] 3306`
   - 验证数据库连接参数
     - 特别检查 `db_host` 设置是否正确:
       - 本地连接: `localhost`
       - 远程连接: MySQL服务器的IP地址
   - 检查指定的数据库是否存在
     - 可以使用命令 `SHOW DATABASES;` 在 MySQL 命令行中验证
2. **文件未找到问题**:
   - 验证监控目录路径是否正确
     - 确保使用正确的路径分隔符（在 Windows 中使用 "/" 或 "\\"）
   - 确保应用程序对目录有读取权限
   - 检查文件模式是否与实际文件匹配
3. **编码问题**:
   - 应用程序尝试使用多种编码读取文件，但也可以检查日志文件中的编码问题
   - 确保.ini配置文件以 UTF-8 编码保存
     - 可以使用 Notepad++ 等编辑器明确保存为 UTF-8 格式
   
### 远程部署特定问题

1. **服务权限问题**:
   - 如果作为 Windows 服务运行，确保服务账户有足够权限

     

2. **远程数据库访问问题**:
   - 确认 MySQL 配置允许远程连接（`bind-address = 0.0.0.0`）
   - 确认 MySQL 用户有远程连接权限（`'user'@'%'`）
   - 检查防火墙设置允许 3306 端口的连接
   - 尝试从远程系统使用 MySQL 客户端工具连接测试:
     ```
     mysql -h [虚拟机IP] -u [用户名] -p
     ```


### 连接自动重试机制

应用程序具有自动重连机制，在数据库连接失败时会自动尝试重新连接，最多尝试 5 次，每次间隔 5 秒。



### 查看日志

查看日志文件（默认：`alarm_monitor.log`）以获取有关操作期间遇到的问题的详细信息。

如果应用作为 Windows 服务运行，日志文件将位于服务工作目录中。

## TODO
后续可能的更新：
使用 NSSM 将 Python 程序设置为 Windows 服务
优点：
1. 开机自动启动
2. 服务崩溃时可自动重启
3. 无需用户登录即可运行
4. 可通过 Windows 服务管理界面控制