#!/bin/bash

set -e
set -a
[ -f ../.env ] && source ../.env
set +a

LOCAL_DIR="./xlsx_files"
mkdir -p "$LOCAL_DIR"

echo "Fetching files via smbclient..."
cd "$LOCAL_DIR"
smbclient "$WINDOWS_SMB" "$NT_PASSWORD" -U "$NT_USER" -c "prompt OFF; recurse ON; lcd $LOCAL_DIR; mget *"

echo "SMB sync completed."
