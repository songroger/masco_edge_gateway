# Embedded Linux RS485 + Modbus RTU + MQTT Edge Collector

适用于嵌入式 Linux 网关的基础边缘采集程序。

## 功能

- 两路独立 RS485
- Modbus RTU
- 多从站、多参数
- holding/input register
- uint16/int16/uint32/int32/float32
- scale/offset 数据换算
- MQTT 每分钟上传
- MQTT QoS 1
- MQTT 断线自动重连
- SQLite 离线缓存
- 网络恢复后自动补传
- 本地阈值规则
- 连续 N 次超阈值后执行 Modbus 写寄存器
- systemd 开机自启动

## 1. 安装依赖

```bash
pip3 install -r requirements.txt
```

## 2. 修改配置

编辑 `config.json`：

- 修改 `/dev/ttyS3`、`/dev/ttyS4`
- 修改波特率、校验位等
- 修改 Modbus slave_id
- 修改寄存器地址
- 修改数据类型和 scale
- 修改 MQTT 服务端地址、账号和密码
- 修改安全规则

## 3. 运行

```bash
python3 main.py
```

## 4. systemd

将项目放到：

```text
/opt/edge_collector
```

执行：

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

1. `config.json` 中的寄存器地址是 Modbus PDU 地址，很多设备手册使用 40001/30001 等显示地址，需要按照设备手册确认是否需要减 1。
2. `float32`、`uint32` 当前使用大端寄存器顺序。如果设备采用 word swap / byte swap，需要扩展 decoder。
3. 关断规则涉及真实设备控制时，必须根据实际设备协议确认写寄存器地址和值，避免误动作。
4. MQTT 服务器不可用时，采集数据会进入 SQLite outbox；恢复后按先进先出方式补传。
