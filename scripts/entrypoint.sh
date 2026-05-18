#!/bin/sh
# Seed model artifacts from the bundled image copy into the data volume.
# The named volume at /app/data shadows baked-in image files, so models
# must be seeded on first container start. Existing volume files are not
# overwritten (allows container-retrained models to persist).
SEED_DIR="/app/bundled_models"
MODEL_DIR="/app/data/models"
if [ -d "$SEED_DIR" ]; then
    mkdir -p "$MODEL_DIR"
    for f in "$SEED_DIR"/*.joblib; do
        [ -f "$f" ] || continue
        fname=$(basename "$f")
        if [ ! -f "$MODEL_DIR/$fname" ]; then
            cp "$f" "$MODEL_DIR/$fname"
            echo "Seeded model: $fname"
        fi
    done
fi
exec "$@"
