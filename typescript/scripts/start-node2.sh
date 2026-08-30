#!/bin/bash

echo "🚀 Starting TypeScript Raft Node 2..."

export NODE_ID="ts-node-2"
export PORT="3002" 
export PEERS="http://192.168.137.145:7001,http://192.168.138.252:7001,http://192.168.137.131:7001,http://183.82.105.79:7001,localhost:3001,localhost:3003"
npm run dev