#!/bin/bash

echo "🚀 Starting TypeScript Raft Node 1..."

export NODE_ID="ts-node-1"
export PORT="3001"
export PEERS="localhost:3002,localhost:3001"

npm run dev