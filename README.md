# Automation Studio Data Handler

## 核心功能

- **持续监控与处理**: 自动监控指定目录中的 Automation Studio 报警CSV文件，并进行解析和存入数据库。
- **智能解码**: 可以识别并处理包括 UTF-8 和 GBK 在内文件编码。
- **数据存储**: 将Alarm数据存入 MySQL 数据库，并通过优化的去重策略确保数据库中的数据唯一性。
- **系统恢复与维护**:
  - **崩溃恢复**: 能自动恢复因程序意外中断而未完成处理的文件。
  - **自动维护**: 定期清理过期数据和因意外关闭产生的临时资源（从config.ini.template进行配置具体参数）。
- **数据库自动管理**:
  - **自动初始化**: 首次运行时，能自动创建所需的数据库表和索引（如果所需数据库表已经存在，沿用原来的数据库表；如果不存在，自动创建新的数据库表）。
  - **结构验证**: 启动时验证关键数据库结构，确保系统正常运行。
- **错误处理**: 包含错误处理机制和数据库自动重连功能。
- **详细日志**: 将详细的操作和错误信息输出到控制台和日志文件 (`alarm_monitor.log`)。

## 系统要求

- Python 3.6 或更高版本
- MySQL 服务器 5.7 或更高版本
- 必需的 Python 包 (详见 `requirements.txt`):
  - `mysql-connector-python>=8.0.26`
  - configparser>=5.0.0 

## 安装步骤

### 1. 安装 MySQL 数据库

#### Windows 系统安装 MySQL

1. 访问 MySQL 官方下载页面: https://dev.mysql.com/downloads/mysql/
2. 下载适合 Windows 版本的 MySQL 安装包（推荐使用 MySQL Installer）。
3. 运行安装程序，按照向导完成安装，并务必记住 `root` 账户的密码。
4. 确保 MySQL 服务已启动。

#### 配置 MySQL 以允许远程连接 (可选)

如果程序和数据库部署在不同机器上，需要进行此项配置：

1. 编辑 MySQL 配置文件 (my.ini 或 my.cnf)，将 `bind-address` 设置为 `0.0.0.0` 或注释掉该行。
2. 重启 MySQL 服务。
3. 创建一个可远程访问的用户账户：
   ```sql
   -- 创建一个允许任何主机连接的用户（'%'代表通配符）
   CREATE USER 'alarm_user'@'%' IDENTIFIED BY 'your_strong_password';
   -- 授予该用户对 alarm_db 数据库的所有权限
   GRANT ALL PRIVILEGES ON alarm_db.* TO 'alarm_user'@'%';
   -- 刷新权限使其生效
   FLUSH PRIVILEGES;
   ```
4. 确保服务器防火墙已开放 MySQL 端口（默认为 3306）。

### 2. 配置本地 MySQL 数据库

安装完成后，需要为应用创建专用的数据库和用户：

```bash
# 登录 MySQL (Windows 可使用 MySQL Command Line Client)
mysql -u root -p

# 创建用于存储报警数据的数据库，并指定字符集以支持中文
CREATE DATABASE alarm_db CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;

# 创建专用用户并设置密码（将 'password' 替换为强密码）
CREATE USER 'alarm_user'@'localhost' IDENTIFIED BY 'password';

# 授予用户对数据库的所有权限
GRANT ALL PRIVILEGES ON alarm_db.* TO 'alarm_user'@'localhost';

# 刷新权限
FLUSH PRIVILEGES;

# 退出 MySQL
EXIT;
```

### 3. 安装 Python 项目

1. 克隆或下载此代码仓库到所需要的文件夹

2. 配置python运行环境

3. 安装必需的 Python 包:
   ```
   pip install -r requirements.txt
   ```

4. 从模板创建并手动配置 `config.ini` 文件，在 Windows 命令行中:
   ```
   copy config.ini.template config.ini
   ```

5. 修改 `config.ini` 文件中的数据库连接参数 (`db_host`, `db_port`, `db_user`, `db_password`, `db_name`)，使其与所需的MySQL 配置匹配。


## 配置说明

应用程序通过 `config.ini` 文件进行配置。如果该文件不存在，程序会在首次启动时从 `config.ini.template` 模板自动创建。

### 主要配置选项

```ini
[DEFAULT]
# 监控来自Automation Studios的Alarm文件的目录
monitoring_dir = ./alarm_files
# 报警文件匹配模式
file_pattern = Alarms_*.csv
# 检查新文件的时间间隔（秒）
polling_interval = 20
# 数据库连接参数
db_host = localhost
db_port = 3306
db_user = alarm_user
db_password = password
db_name = alarm_db
table_name = alarms

[PERFORMANCE]
# 备用插入策略的批处理大小（需要对数据库整体进行查询和比对，在数据表较大时性能较低，仅作为临时表方案失败时的备选，确保数据不会遗漏）
batch_size = 2000
# 是否启用基于临时表的高性能插入
enable_temp_table_optimization = true
# 遗留临时表的清理检查周期（秒），用于清理超过1小时未处理的表
# 正常情况下，临时表仅会存在几秒就会因插入完成而自动删除
# 该值清理的是因插入失败或出故障而未能成功删除的临时表
temp_table_cleanup_interval = 3600

[DATA_MANAGEMENT]
# 数据在数据库保留月数，超过此时长的数据将被自动清理（由alarms数据表的最后一列“created at”字段判定时长），程序自动执行
data_retention_months = 12
# 是否启用自动数据清理（true/false)
auto_cleanup_enabled = true
# 自动清理任务的检查周期（小时）
cleanup_check_interval = 24
# processing_state 表中保留的最大记录数
# 该表用于故障恢复，告知程序哪些文件已经被处理过，哪些处理未成功，从而可以从正确位置进行恢复
max_processing_state_records = 100

[LOGGING]
# 日志级别: DEBUG, INFO, WARNING, ERROR, CRITICAL
log_level = INFO
# 日志文件路径及名称
# 如开启日志文件自动轮转，则仅需指定存储的目录，程序会对日志文件自动进行命名
log_file_path = ./logs/alarm_monitor.log
# 是否启用日志文件自动轮转（基于文件大小）
log_rotation_enabled = true
# 单个日志文件的最大容量（MB）
log_rotation_max_size_mb = 30
# 日志轮转时保留的备份文件数量
log_rotation_backup_count = 10
```

### Windows 路径注意事项

在 Windows 系统中配置 `monitoring_dir` 时，建议使用正斜杠 `/` 或双反斜杠 `\\`，例如：
`monitoring_dir = C:/AlarmData/files` 或 `monitoring_dir = C:\\AlarmData\\files`


## 使用方法

### 1. 启动监控

直接运行主程序:
```
python alarm_monitor.py
```
程序将开始监控指定目录，自动处理新文件，直到被手动停止。

### 2. 停止监控

在程序运行的终端中按 `Ctrl+C`。程序将安全地关闭数据库连接并退出。


## 文件结构

- `alarm_monitor.py` - 主应用程序，负责核心的监控和文件处理调度。
- `system_recovery.py` - 系统恢复与维护模块，负责数据库管理、崩溃恢复和性能优化，确保数据不会因为数据库意外中断或程序退出而丢失；在重新连接时，可以自动检查并找到正确的要导入的文件，继续导入
- `config.ini.template` - 配置文件模板。
- `config.ini` - 实际使用的配置文件（自动生成或手动创建）。
- `requirements.txt` - Python 包依赖列表。
- `README.md` - 本说明文件。


## 技术实现细节

### 核心架构

系统采用模块化设计，主要分为两大组件：
- **`AlarmMonitor` (`alarm_monitor.py`)**: 作为应用的核心控制器，负责文件系统的监控、文件处理任务的调度以及与用户的交互。
- **`SystemRecoveryManager` (`system_recovery.py`)**: 作为系统的"健康管家"，封装了所有与数据库维护、系统恢复和数据完整性相关的功能。



### 文件处理与崩溃恢复流程

为了确保数据处理的可靠性，引入了 `processing_state` 状态跟踪表：

1.  **标记开始**: 在处理一个文件前，在 `processing_state` 表中记录该文件的状态为 `started`。
2.  **数据处理**: 执行文件解析和数据插入操作。
3.  **标记完成/失败**:
    - 如果成功，更新状态为 `completed`。
    - 如果失败，更新状态为 `failed` 并记录错误信息。
4.  **崩溃恢复**: 如果程序在处理过程中意外崩溃，下次启动时 `SystemRecoveryManager` 会检测到仍处于 `started` 状态的文件。它会将这些文件标记为 `failed (interrupted)`，并使其能够被重新处理，从而避免数据丢失。

### 数据插入策略

为高效处理大量报警数据并避免重复，系统采用一种双重策略：

1.  **主策略 (Optimized)**:
    - 创建一个与主表结构相同的临时表 (`temp_alarms_*`)。
    - 将从 CSV 文件解析出的所有数据一次性批量插入到该临时表。
    - 使用 `LEFT JOIN` 查询，将临时表中不存在于主表的新记录一次性插入主表。
    - 操作完成后立即删除临时表。
    - **优势**: 性能高，将去重逻辑完全交由数据库处理，避免了应用层的逐条比对。

    
    
2.  **备用策略 (Batch Fallback)**:
    - 如果主策略因任何原因失败（如数据库权限限制），程序会自动切换到此备用策略。
    - 分批次从主表中查询已存在的记录键。
    - 在应用内存中进行比对，过滤掉重复数据。
    - 将剩余的新数据批量插入主表。
    - **优势**: 兼容性好，功能可靠，保证程序健壮性。
    - **劣势**：数据量大时，速度较慢且相对消耗更多内存。
    
    

### 日志系统与自动轮转

程序集成日志功能，并将日志输出到控制台和文件。为了防止日志文件无限增长，内置自动轮转机制：
- **基于大小轮转**: 当日志文件达到指定大小（如 `30MB`）时，会自动将其重命名为备份文件（如 `alarm_monitor.log.1`）。
- **备份管理**: 系统会保留指定数量的备份文件（如 `10`个），当新的备份产生时，最旧的备份文件会被自动删除。
- **可配置**: 日志级别、轮转开关、文件大小和备份数量均可在 `config.ini` 中进行配置。



### 数据库表结构

系统会自动创建和维护以下两个核心数据表：

#### `alarms` (主数据表)

存储所有解析后的报警信息。

```sql
CREATE TABLE alarms (
    id INT AUTO_INCREMENT PRIMARY KEY,              -- 记录的唯一ID，自增
    Time DATETIME(3) NOT NULL,                      -- 报警时间戳 (精确到毫秒)
    Instance INT NOT NULL,                          -- 实例 ID
    Name VARCHAR(255) NOT NULL,                     -- 报警名称
    Code INT NOT NULL,                              -- 报警代码
    Severity INT NOT NULL,                          -- 严重性级别
    AdditionalInformation1 VARCHAR(255),            -- 附加信息 1
    AdditionalInformation2 VARCHAR(255),            -- 附加信息 2
    `Change` TEXT NOT NULL,                         -- 报警状态变化
    Message TEXT NOT NULL,                          -- 报警消息文本
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- 记录入库时间，程序根据此确定是否需要进行清理
    -- 索引以加速查询和去重
    INDEX idx_time (Time),
    INDEX idx_duplicate_check (Time, Instance, Code, Name),
    INDEX idx_cleanup (created_at)
);
```

#### `processing_state` (文件处理状态表)

用于跟踪每个文件的处理状态，是实现崩溃恢复的关键，该表的最大存储条目数（max_processing_state_records）在config.ini中进行配置，默认100条。

```sql
CREATE TABLE processing_state (
    id INT AUTO_INCREMENT PRIMARY KEY,
    file_name VARCHAR(255) UNIQUE NOT NULL,         -- 文件名 (唯一)
    file_path VARCHAR(500) NOT NULL,                -- 文件完整路径
    processing_status ENUM('started', 'completed', 'failed') NOT NULL, -- 处理状态
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- 开始处理时间
    completed_at TIMESTAMP NULL,                    -- 完成处理时间
    error_message TEXT NULL,                        -- 错误信息
    record_count INT DEFAULT 0,                     -- 成功插入的记录数
    -- 索引
    INDEX idx_status (processing_status),
    INDEX idx_started (started_at)
);
```

## 故障排除

### 常见问题

1.  **MySQL 连接失败**:
    - 确保 MySQL 服务正在运行。
    - 验证 `config.ini` 中的数据库连接参数（地址、端口、用户名、密码）是否正确。
    - 如果是远程连接，检查网络和防火墙设置。可使用 `telnet <db_host> 3306` 测试端口连通性。
2.  **文件未被处理**:
    - 验证 `monitoring_dir` 路径是否正确，并确保程序有该目录的读取权限。
    - 检查 `file_pattern` 是否能匹配到您的报警文件名。
3.  **编码错误**:
    - 确保 `config.ini` 文件本身以 UTF-8 编码保存。

### 查看日志

排查问题的首选方法是查看日志文件（默认为 `alarm_monitor.log`）。日志中会详细记录程序的启动、文件处理过程以及遇到的任何错误。