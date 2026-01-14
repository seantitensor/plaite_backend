# Plaite Backend

Backend CLI tools for Plaite recipe management. Get stats from Firebase, upload recipes, and process images.

## Setup

```bash
# Install uv if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone and install
cd ~/Documents/plaite_backend
uv sync --all-extras
```

## Configuration

### Environment Variables

Create a `.env` file in the project root:

```bash
# Path to recipe data (supports .pkl or .parquet)
RECIPES_PATH=/path/to/all_enriched_recipes.pkl
```

### Firebase Credentials

Edit `configs/firebase.yaml` with your Firebase credential paths:

```yaml
prod:
  credentials_path: "/path/to/plaite-production-firebase-adminsdk.json"
  storage_bucket: "plaite-production.firebasestorage.app"
  collection: "recipes"

dev:
  credentials_path: "/path/to/plaite-dev-firebase-adminsdk.json"
  storage_bucket: "plaite-ff1e7.firebasestorage.app"
  collection: "recipes"
```

### Upload Settings

Edit `configs/upload.yaml` for batch upload and image processing settings:

```yaml
batch_size: 50

images:
  output_dir: "./processed_images"
  overlay_suffix: "_overlayed"
  max_width: 1000
  max_height: 1600
  quality: 80
```

---

## Data Module

The `plaite.data` module provides efficient recipe data loading using Polars:

```python
import plaite.data as data

# Get available columns
print(data.get_recipes_columns())

# Load all recipes
df = data.load_recipes()

# Load specific columns
df = data.load_recipes(columns=["id", "name", "ingredients"])

# Filter with Django-style operators
df = data.filter_recipes({
    "category": "dessert",
    "calories__lt": 500,
    "rating__gte": 4.0
})

# Advanced queries
import polars as pl
df = (
    data.recipes_table.scan()
    .filter(pl.col("calories") < 500)
    .group_by("category")
    .agg(pl.col("id").count())
    .collect()
)
```

**See:** [QUICK_START.md](QUICK_START.md) and [src/plaite/data/README.md](src/plaite/data/README.md)

---

## CLI Commands

### `plaite stats`

Get statistics from the Firebase recipe database.

```bash
# Basic usage (defaults to dev environment)
uv run plaite stats

# Use production environment
uv run plaite stats --env prod

# Save output to JSON file
uv run plaite stats --env prod --output stats.json

# Limit number of recipes to analyze (for testing)
uv run plaite stats --env dev --limit 100

# Custom config file
uv run plaite stats --config /path/to/firebase.yaml --env prod
```

**Stats included:**
- Total recipe count
- Tag distribution (count per tag)
- Ingredient distribution (top 100)
- Nutrient coverage (% with nutrients, which fields)
- Missing fields analysis

---

### `plaite upload`

Upload a batch of recipes to Firebase.

```bash
# Dry run - validate without uploading
uv run plaite upload recipes/batch4.json --dry-run

# Upload to dev
uv run plaite upload recipes/batch4.json --env dev

# Upload to production
uv run plaite upload recipes/batch4.json --env prod

# Upload with images directory
uv run plaite upload recipes/batch4.json --env prod --images-dir ./batch4_images

# Custom config files
uv run plaite upload recipes/batch4.json \
  --config configs/firebase.yaml \
  --upload-config configs/upload.yaml \
  --env prod
```

**Options:**
| Option | Description |
|--------|-------------|
| `--env` | Environment: `prod` or `dev` (default: dev) |
| `--config`, `-c` | Firebase config path |
| `--upload-config` | Upload config path |
| `--images-dir` | Directory containing images to upload |
| `--dry-run` | Validate only, don't upload |

---

### `plaite process-images`

Download and process images for a batch of recipes.

```bash
# Basic usage - download and process images
uv run plaite process-images recipes/batch4.json

# Specify output directory
uv run plaite process-images recipes/batch4.json --output ./batch4_images

# Skip downloading, just process existing images
uv run plaite process-images recipes/batch4.json --no-download

# Custom config
uv run plaite process-images recipes/batch4.json --config configs/upload.yaml
```

**Options:**
| Option | Description |
|--------|-------------|
| `--output`, `-o` | Output directory (default: ./processed_images) |
| `--config` | Upload config path |
| `--no-download` | Skip downloading, process existing files only |

**Output:**
- Downloads images from recipe URLs
- Resizes to configured max dimensions
- Saves with overlay suffix
- Creates `image_mapping.json` in output directory

---

### `plaite version`

Show version information.

```bash
uv run plaite version
```

---

## Typical Workflow

### 1. Upload New Recipes

```bash
# Step 1: Process images
uv run plaite process-images recipes/batch4.json --output ./batch4_images

# Step 2: Dry run to validate
uv run plaite upload recipes/batch4.json --env dev --dry-run

# Step 3: Upload to dev for testing
uv run plaite upload recipes/batch4.json --env dev --images-dir ./batch4_images_overlayed

# Step 4: Verify with stats
uv run plaite stats --env dev

# Step 5: Upload to production
uv run plaite upload recipes/batch4.json --env prod --images-dir ./batch4_images_overlayed
```

### 2. Get Database Stats

```bash
# Quick check
uv run plaite stats --env prod

# Full export for analysis
uv run plaite stats --env prod --output stats_$(date +%Y%m%d).json
```

---

## Development

```bash
# Run linter
uv run ruff check src/

# Auto-fix lint issues
uv run ruff check src/ --fix

# Format code
uv run ruff format src/

# Run tests
uv run pytest
```

---

## Project Structure

```
plaite_backend/
├── configs/
│   ├── firebase.yaml       # Firebase credentials (prod/dev)
│   └── upload.yaml         # Upload and image settings
├── src/plaite/
│   ├── __init__.py
│   ├── cli.py              # CLI entrypoint (typer)
│   ├── config.py           # Config loading & validation
│   ├── data/               # NEW: Data loading module
│   │   ├── __init__.py     # Public API
│   │   ├── _tables.py      # Table wrapper (pkl/parquet)
│   │   ├── _queries.py     # Pre-built query templates
│   │   ├── loader.py       # Loading functions
│   │   └── README.md       # Full documentation
│   ├── firebase/
│   │   ├── client.py       # Firebase initialization
│   │   ├── stats.py        # Stats collection
│   │   └── upload.py       # Recipe upload pipeline
│   └── images/
│       └── process.py      # Image download & processing
├── examples/
│   ├── test_data_module.py # Test data module
│   └── advanced_queries.py # Advanced query examples
├── archive/                # Old code for reference
├── tests/
├── .env                    # Environment variables (not committed)
├── pyproject.toml          # Project config (uv, ruff, pytest)
├── QUICK_START.md          # Data module quick start
└── README.md
```
