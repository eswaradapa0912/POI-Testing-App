#!/bin/bash

echo "========================================="
echo "POI Validation System Launcher"
echo "========================================="

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is not installed"
    exit 1
fi

# Check and install required packages
echo "Checking required Python packages..."

packages=("flask" "flask-cors" "pandas" "openpyxl")
for package in "${packages[@]}"; do
    if ! python3 -c "import $package" 2>/dev/null; then
        echo "Installing $package..."
        pip3 install $package
    else
        echo "✓ $package is already installed"
    fi
done

# Set the working directory
cd /mnt/data/POI_Testing_Automation/version=5_0_2/test_automate_code/poi_validation_system

echo ""
echo "========================================="
echo "Starting POI Validation Server..."
echo "========================================="
echo ""
echo "The application will be available at:"
echo "→ http://localhost:5002"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""

# Make the Python script executable
chmod +x poi_validation_server.py

# Run the server
python3 poi_validation_server.py