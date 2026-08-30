#!/bin/bash

echo "🚀 Starting TypeScript Raft Node 3..."

export NODE_ID="ts-node-3"
export PORT="3003"
export PEERS="localhost:3001,localhost:3003"

npm run dev