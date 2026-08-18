# Embedded Linux RS485 + Modbus RTU + MQTT Edge Collector

适用于嵌入式 Linux 网关的模块化边缘采集程序。两路 RS485 并发采集，本地规则保护，MQTT 上报，断网缓存补传，并支持远程配置下发与 systemd watchdog。

## 架构

```text
main.py
 └── edge_collector/
      ├── service.py        主循环、热加载、生命周期
      ├── collector.py      两路 RS485 并发 + Modbus 批量读
      ├── modbus_port.py    串口连接、读写、重试
      ├── decode.py         数据类型与字节序
      ├── rules.py          阈值 / 通信故障 / 双重关断
      ├── gpio_control.py   libgpiod 关断输出
      ├── mqtt_manager.py   TLS、上报、配置订阅、离线补传
      ├── store.py          SQLite outbox
      ├── config.py         配置校验与原子写入
      └── watchdog.py       systemd READY / WATCHDOG
```

## 功能

### 配置与启动

- 从 `config.json` 加载并校验运行参数
- 命令行：`python3 main.py`
- `Ctrl+C` 优雅退出
- systemd：`Type=notify`、开机自启、异常重启、watchdog
- `install.sh` 安装依赖并注册服务
- MQTT 热更新采集参数：完整替换或局部 patch，写入本地后热加载，无需 SSH
- 主循环按步骤隔离：RS485 / MQTT / SQLite 任一异常只恢复对应模块，并继续喂狗

### 多串口 / Modbus RTU 采集

- 两路（或多路）RS485 **并发采集**，同一总线内仍按从站串行
- 每路多从站、多参数
- 相邻寄存器 **批量读取**（`batch_max_gap` / `batch_max_count`）
- 读 Holding / Input Register；写 Holding Register
- 读失败按 `collect.retry` 重试；`request_timeout` 作为串口超时
- 连接失败自动重连；兼容 pymodbus `slave` / `device_id`

### 数据解析与换算

- `uint16` / `int16` / `uint32` / `int32` / `float32`
- 16 位字节序：`AB`、`BA`
- 32 位字节序：`ABCD`、`CDAB`、`BADC`、`DCBA`
- `scale` / `offset`：`value * scale + offset`
- 测点名：`端口名:从站号:参数名`

### 规则引擎与双重关断

- 阈值规则：`>` `>=` `<` `<=` `==` `!=`，连续 N 次触发
- 通信故障规则 `comm_fail`：端口 `RS485_1`、设备 `RS485_1:1` 或测点 `RS485_1:1:temperature` 连续 N 次读取失败
- 触发后发 MQTT 告警（`alarm_topic`），并可执行 `alarm` / `modbus_write` / `gpio` / `dual`
- 条件恢复后才允许再次触发

### MQTT

- 用户名密码、断线重连、QoS 1
- 可选 TLS（CA / 客户端证书，`insecure` 可关校验）
- 采集周期与上报周期分离
- 上报 JSON 含测点值和 `comm` 通信状态
- 订阅 `config_topic` 接收配置，向 `config_ack_topic` 回执
- 规则触发时向 `alarm_topic` 上报告警

### 离线缓存与补传

- SQLite WAL outbox
- MQTT 不可用时入库，恢复后 FIFO 批量补传
- 超过 `max_retry` 丢弃毒消息，避免堵死队列
- 超过 `max_records` 删除最旧记录

### Watchdog 与模块自恢复

- 启动 `READY=1`，每轮结束 `WATCHDOG=1`，退出 `STOPPING=1`
- 采集、规则、上报、健康检查分步隔离，单模块异常不退出进程
- RS485 连续断开则重建串口客户端；MQTT 连续失联则重启客户端（保留待处理配置）；SQLite 出错则重开数据库
- 单路采集超时只标记该路失败并恢复该口，另一路继续
- systemd `WatchdogSec=30`：进程完全卡死时由 systemd 拉起

## 1. 安装依赖

```bash
pip3 install -r requirements.txt
```

## 2. 修改配置

编辑 `config.json`：

- 串口路径、波特率、从站、寄存器、数据类型、`byte_order`、scale
- MQTT 地址、账号、TLS 证书、数据 topic / 配置 topic
- 批量读窗口、采集/上报周期、读重试
- GPIO chip/line、阈值规则、通信故障规则

### 远程配置热更新

向 `mqtt.config_topic` 发布，无需 SSH 登录网关。结果回执在 `mqtt.config_ack_topic`。

局部修改采集参数：

```json
{"op": "patch", "collect": {"interval": 5, "upload_interval": 30}}
```

也可以直接发：

```json
{"collect": {"interval": 5}}
```

替换整份配置：

```json
{"op": "replace", "config": { }}
```

旧文件备份为 `config.json.bak`。

### 通信异常告警

`type=comm_fail` 的 `source` 可以是端口、从站或测点。连续失败后发到 `alarm_topic`：

```json
{"device_id":"EDGE_001","name":"rs485_1_comm_fail","type":"comm_fail","source":"RS485_1:1","consecutive":5,"severity":"critical","code":"COMM_FAIL"}
```

`action.type=alarm` 只告警；`dual` 则告警并执行 GPIO/Modbus 关断。

### 字节序

| 配置 | 含义 |
| --- | --- |
| `AB` | 16 位大端 |
| `BA` | 16 位字节交换 |
| `ABCD` | 32 位大端 |
| `CDAB` | word swap |
| `BADC` | byte swap |
| `DCBA` | 32 位小端 |

## 3. 运行

```bash
python3 main.py
```

## 4. systemd

将项目放到 `/opt/edge_collector` 后：

```bash
cp edge-collector.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable edge-collector
systemctl start edge-collector
```

查看：

```bash
systemctl status edge-collector
journalctl -u edge-collector -f
```

## 注意

1. 寄存器地址是 Modbus PDU 地址。手册若写 40001/30001，需确认是否要减 1。
2. 关断规则会写真实 GPIO 和 Modbus 寄存器，必须按设备协议核对 line、地址和值。
3. MQTT 不可用时数据进入 SQLite outbox，恢复后 FIFO 补传。
4. `gpiod` 仅在 Linux 目标板上生效；Windows 开发机上 GPIO 动作会记录失败日志。
5. 启用 MQTT TLS 时，把 `mqtt.tls.enable` 设为 `true`，并配置 CA/证书路径。
6. `Type=notify` 依赖 systemd `NOTIFY_SOCKET`；直接 `python3 main.py` 时 watchdog 通知会被忽略。
7. 模块自恢复不能替代 systemd：只有进程彻底卡死/退出时才由 `WatchdogSec` 拉起整个服务。
