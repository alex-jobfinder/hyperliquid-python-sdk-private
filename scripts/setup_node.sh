#!/bin/bash

set -e  # Exit on any error

echo "Setting up HyperLiquid Mainnet Node..."

# wsl.exe --install -d Ubuntu-24.04

# Check if running on Ubuntu 24.04
if ! grep -q "Ubuntu 24.04" /etc/os-release; then
    echo "Error: This script requires Ubuntu 24.04"
    exit 1
fi

# Check system requirements
CPU_CORES=$(nproc)
TOTAL_MEM=$(free -g | awk '/^Mem:/{print $2}')
DISK_SPACE=$(df -BG --output=avail $HOME | tail -n 1 | tr -d 'G')

if [ $CPU_CORES -lt 4 ] || [ $TOTAL_MEM -lt 32 ] || [ $DISK_SPACE -lt 200 ]; then
    echo "Warning: System does not meet minimum requirements:"
    echo "Required: 4 CPU cores, 32GB RAM, 200GB disk"
    echo "Found: $CPU_CORES cores, ${TOTAL_MEM}GB RAM, ${DISK_SPACE}GB disk"
    read -p "Continue anyway? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Create necessary directories
mkdir -p ~/hl/data

# Configure chain for Mainnet
echo '{"chain": "Mainnet"}' > ~/visor.json

# Download and verify the public key
echo "Downloading and importing public key..."
curl -s https://raw.githubusercontent.com/hyperliquid-dex/node/main/pub_key.asc > ~/pub_key.asc
gpg --import ~/pub_key.asc

# Download Mainnet visor binary
echo "Downloading Mainnet visor binary..."
curl -s https://binaries.hyperliquid.xyz/Mainnet/hl-visor > ~/hl-visor
chmod a+x ~/hl-visor

# Download and verify signature
echo "Verifying binary signature..."
curl -s https://binaries.hyperliquid.xyz/Mainnet/hl-visor.asc > ~/hl-visor.asc
if ! gpg --verify ~/hl-visor.asc ~/hl-visor; then
    echo "Error: Binary verification failed!"
    exit 1
fi

# Check if ports 4001 and 4002 are open
echo "Checking required ports..."
for PORT in 4001 4002; do
    if ! nc -z localhost $PORT; then
        echo "Warning: Port $PORT appears to be closed"
        echo "Please ensure ports 4001 and 4002 are open for optimal performance"
    fi
done

# Create a systemd service file for the node
echo "Creating systemd service..."
sudo tee /etc/systemd/system/hyperliquid-node.service << EOF
[Unit]
Description=HyperLiquid Node
After=network.target

[Service]
Type=simple
User=$USER
ExecStart=$HOME/hl-visor run-non-validator --write-misc-events
Restart=always
RestartSec=10
StandardOutput=append:/var/log/hyperliquid-node.log
StandardError=append:/var/log/hyperliquid-node.error.log

[Install]
WantedBy=multi-user.target
EOF

# Reload systemd and enable/start the service
sudo systemctl daemon-reload
sudo systemctl enable hyperliquid-node
sudo systemctl start hyperliquid-node

echo "HyperLiquid node setup complete!"
echo "The node is running as a systemd service 'hyperliquid-node'"
echo "To check status: sudo systemctl status hyperliquid-node"
echo "To view logs: tail -f /var/log/hyperliquid-node.log"
echo "To view errors: tail -f /var/log/hyperliquid-node.error.log"
echo
echo "Node data will be written to: ~/hl/data/"
echo "Warning: This will generate approximately 100GB of logs per day" 