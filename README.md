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
- MQTT 远程下发完整配置，校验通过后原子写入并热加载；结果发布到 ack topic

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
- 通信故障规则 `comm_fail`：设备或测点连续 N 次失败
- 动作类型：`modbus_write`、`gpio`、`dual`（GPIO + Modbus 都执行）
- 触发后锁存，条件恢复后才允许再次触发

### MQTT

- 用户名密码、断线重连、QoS 1
- 可选 TLS（CA / 客户端证书，`insecure` 可关校验）
- 采集周期与上报周期分离
- 上报 JSON 含测点值和 `comm` 通信状态
- 订阅 `config_topic` 接收配置，向 `config_ack_topic` 回执

### 离线缓存与补传

- SQLite WAL outbox
- MQTT 不可用时入库，恢复后 FIFO 批量补传
- 超过 `max_retry` 丢弃毒消息，避免堵死队列
- 超过 `max_records` 删除最旧记录

### systemd watchdog

- 启动发送 `READY=1`
- 每个采集周期发送 `WATCHDOG=1`
- 停止发送 `STOPPING=1`
- 服务文件 `WatchdogSec=30`，超时由 systemd 拉起进程

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

### 远程配置下发

向 `mqtt.config_topic` 发布完整配置 JSON，或 `{"config": {...}}`。

成功/失败会发布到 `mqtt.config_ack_topic`：

```json
{"ok": true, "message": "config applied", "device_id": "EDGE_001"}
```

旧配置备份为 `config.json.bak`。

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
