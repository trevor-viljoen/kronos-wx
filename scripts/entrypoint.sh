#!/bin/sh
# Seed model artifacts from the bundled image copy into the data volume.
# The named volume at /app/data shadows baked-in image files, so models
# must be seeded on every container start. Bundled models (from the image)
# are always authoritative — they are overwritten to fix stale volume files.
SEED_DIR="/app/bundled_models"
MODEL_DIR="/app/data/models"
if [ -d "$SEED_DIR" ]; then
    mkdir -p "$MODEL_DIR"
    for f in "$SEED_DIR"/*.joblib; do
        [ -f "$f" ] || continue
        fname=$(basename "$f")
        cp "$f" "$MODEL_DIR/$fname"
        echo "Seeded model: $fname"
    done
fi
exec "$@"
