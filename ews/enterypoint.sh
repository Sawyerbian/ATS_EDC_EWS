#!/bin/bash
set -e

set -a
[ -f ../.env ] && source ../.env
set +a

echo "Mounting SMB share..."

MOUNT_POINT="./xlsx_files"

mkdir -p "$MOUNT_POINT"

# Mount using env vars
mount -t cifs "$WINDOWS_SMB" "$MOUNT_POINT" \
  -o username="$NT_USER",password="$NT_PASSWORD",vers=3.0,dir_mode=0777,file_mode=0777,uid=$(id -u),gid=$(id -g)

echo "SMB mounted at $MOUNT_POINT"
echo "Starting Python app..."

# exec python ./main.py
