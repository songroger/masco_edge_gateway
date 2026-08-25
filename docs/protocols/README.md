# 硬件 Modbus 协议整理

本目录由 `docs/mptt-modbus.pdf` 与 `docs/IPower-Plus.pdf` 自动整理生成。

## 协议列表

| 设备 | 版本 | 默认ID | 波特率 | 文件 |
|------|------|--------|--------|------|
| 第三代太阳能控制器 | V1.00 | 1 | 115200 | [solar-controller-g3.md](solar-controller-g3.md) / [solar-controller-g3.yaml](solar-controller-g3.yaml) |
| 逆变器通信协议 | V1.0 | 3 | 115200 | [inverter-ipower-plus.md](inverter-ipower-plus.md) / [inverter-ipower-plus.yaml](inverter-ipower-plus.yaml) |

## 地址说明

- 文档中的地址为 **16 进制 Modbus PDU 地址**（基址 0x00）。
- 程序内部直接使用十进制 PDU 地址，例如 `0x3108` = `12552`。
- 32 位量由 **L（低字）+ H（高字）** 两个连续寄存器组成，实际值 = `(H<<16)|L` / scale。
- 有符号数（int16）：最高位为符号位，负数按文档补码规则解析。

## 功能码对照

| 数据区 | 读 | 写(单) | 写(多) |
|--------|-----|--------|--------|
| 线圈 | 0x01 | 0x05 | 0x0F |
| 离散输入 | 0x02 | — | — |
| 输入寄存器 | 0x04 | — | — |
| 保持寄存器 | 0x03 | 0x06 | 0x10 |
