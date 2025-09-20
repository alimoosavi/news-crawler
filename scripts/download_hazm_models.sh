#!/usr/bin/env bash
set -e

RESOURCES_DIR="resources"
mkdir -p "$RESOURCES_DIR"

# Hazm 0.10.0 compatible models
BASE_URL="https://github.com/sobhe/hazm/releases/download/v0.10"

MODELS=(
  "postagger.model"
  "ner.model"
)

echo "Downloading Hazm models for version 0.10.0..."
for MODEL in "${MODELS[@]}"; do
  if [ ! -f "$RESOURCES_DIR/$MODEL" ]; then
    echo "Fetching $MODEL..."
    curl -L -o "$RESOURCES_DIR/$MODEL" "$BASE_URL/$MODEL"
  else
    echo "$MODEL already exists, skipping."
  fi
done

echo "✅ Hazm models are ready in $RESOURCES_DIR"
