### API gateway mqtt

1. 数据同步「目前频率1分钟」

    topic: light/gateway/data
    json结构见示例

2. 控制接收

    topic: light/gateway/command/<device-sn>

    配置项：`mqtt.topics.command` 写前缀（如 `light/gateway/command`），加载时自动拼接 `/{gateway.sn}` 得到完整订阅路径。
    例：`gateway.sn=GW-003` → 实际订阅 `light/gateway/command/GW-003`。

    json:
    {
        "sn": "GW-003",
        "slave_id": 1,
        "point": "ch1_status",
        "value": 1
    }

    字段说明：
    - `sn`：必须与本网关 `gateway.sn`（缺省 `gateway.id`）一致，否则丢弃
    - `slave_id`：目标从站
    - `point`：可写点位名（见下表）
    - `value`：保持寄存器点为 **Modbus 原始寄存器值**（不做 scale 换算）。例如通道调光 50% 下发 `50`（协议 0x0032）

    可写点位 = `config.json` 中已配置的点位 ∩ `docs/light-control.yaml` 中 `access: RW|W` 的单寄存器保持寄存器。
    白名单实现：`internal/mqtt/command.go` → `ControllablePoints`；写地址仍以 config 为准。
    只读点（电流/功率/电能等）、以及 `longitude`/`latitude`（float32 双寄存器）不接受本接口控制。

    | point | 地址 | 说明 |
    |-------|------|------|
    | `rtc_year_month` | 0x0000 | 当前时间（年/月） |
    | `rtc_day_hour` | 0x0001 | 当前时间（日/时） |
    | `rtc_min_sec` | 0x0002 | 当前时间（分/秒） |
    | `rtc_weekday_reserved` | 0x0003 | 星期/预留 |
    | `do_status` | 0x0005 | 开关量输出 DO1/DO2 |
    | `ch1_8_switch_status` | 0x0007 | 通道1-8开关状态位 |
    | `ch1_8_close_bits` | 0x0009 | 写入通道1-8合（W） |
    | `ch1_8_open_bits` | 0x000B | 写入通道1-8分（W） |
    | `ch1_status` … `ch8_status` | 0x000C–0x0013 | 单通道：0=分，1=合，10–100=调光 |
    | `slave_address` | 0x0101 | 从机地址 1–247 |
    | `baud_rate` | 0x0103 | 2400/4800/9600/19200/38400 |
    | `di1_link_func_mode` / `di1_link_channels` / `di1_link_action` | 0x0104 / 0x0106 / 0x0108 | DI1 联动 |
    | `di2_link_func_mode` / `di2_link_channels` / `di2_link_action` | 0x0109 / 0x010B / 0x010D | DI2 联动 |
    | `timer_taskN_channels` / `_weekday_hour` / `_min_action` | 0x1007+ | 常规定时任务 N=1..5（以 config 为准） |
    | `sched_taskN_channels` / `_year_month` / `_day_hour` / `_min_action` | 0x1101+ | 预约定时任务 N=1..5（以 config 为准） |

    写入方式：按 config 地址用 **FC16**（写多个寄存器，单点 quantity=1）。

    示例：通道1合闸

    ```json
    {"sn":"GW-003","slave_id":1,"point":"ch1_status","value":1}
    ```

    示例：通道1调光 80%

    ```json
    {"sn":"GW-003","slave_id":1,"point":"ch1_status","value":80}
    ```

    示例：bit0+bit1 合闸（写合状态位）

    ```json
    {"sn":"GW-003","slave_id":1,"point":"ch1_8_close_bits","value":3}
    ```

    示例：设置波特率 9600

    ```json
    {"sn":"GW-003","slave_id":1,"point":"baud_rate","value":9600}
    ```

3. 实时获取最新

    topic: light/gateway/refresh/<device-sn>

    配置项：`mqtt.topics.refresh` 写前缀（如 `light/gateway/refresh`），加载时自动拼接 `/{gateway.sn}` 得到完整订阅路径。
    例：`gateway.sn=GW-003` → 实际订阅 `light/gateway/refresh/GW-003`。

    方向：服务端/web端 → 网关（网关连接成功后自动订阅上述完整 topic）

    收到消息后：校验 `sn` → 全量读取所有通道寄存器 → 按 telemetry topic（`light/gateway/data`）立即上报。

    json:
    {
        "sn": "GW-003"
    }
