#!/bin/sh
set -e

APP_DIR="/opt/edge_collector"

mkdir -p "$APP_DIR"

echo "Install dependencies..."
python3 -m pip install -r "$APP_DIR/requirements.txt"

echo "Install systemd service..."
cp "$APP_DIR/edge-collector.service" /etc/systemd/system/edge-collector.service

systemctl daemon-reload
systemctl enable edge-collector

echo "Installation complete."
echo "Start with:"
echo "  systemctl start edge-collector"
echo "Check with:"
echo "  journalctl -u edge-collector -f"
