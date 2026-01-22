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
# Path to recipe data (parquet format)
RECIPES_PATH=/path/to/all_enriched_recipes.parquet
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

### `plaite sync`

Upload recipes from local data with interactive filtering.

```bash
Basic Usage

# Interactive upload with filters (default: excludes already-uploaded)
plaite sync 50

# You'll be prompted for filters:
# - Title (contains)
# - Ingredient (contains) 
# - Health grade (A/B/C/D/F)
# - Max cook time
# - Min rating
Command Options

# Dry run - validate only, don't upload
plaite sync 50 --dry-run

# Include recipes already in Firebase (default excludes them)
plaite sync 50 --include-uploaded

# Skip confirmation prompt
plaite sync 50 --yes

# Use production environment
plaite sync 50 --env prod
Upload Flow (as implemented)
1. Interactive filter selection

Prompts for filters (title, ingredient, health grade, cook time, rating)
Shows total matching recipes
2. Create batch

Selects N random recipes from local data matching filters
Shows preview table
3. Check Firebase

Fetches existing recipe IDs from Firebase
Filters out already-uploaded recipes (unless --include-uploaded)
Shows skip count
4. Validate & Transform

Maps local schema to Firebase schema
Validates required fields (tags, instructions, ingredients)
Type checks (must be lists)
Transforms nutrients (dict → array)
Normalizes numServings ("4 servings" → 4.0)
Validates image URL exists
5. Upload

Uploads valid recipes to Firebase in batches
Shows detailed results
Example Session

$ plaite sync 50

Plaite Sync - Upload to Firebase (dev)

Target: recipes-dev

Recipe Selection
Filter by title (contains): chicken
Filter by ingredient (contains): 
Filter by health grade (A/B/C/D/F): 
Max cook time (minutes): 
Min rating (0-5): 

Found 234 matching recipes.
Will select 50 random recipes.

Continue with upload? [Y/n]: y

Selecting recipes from local data...
Selected 50 recipes

# Preview table shown here

Checking Firebase for existing recipes...
Found 1500 recipes in Firebase
Skipping 10 already-uploaded recipes

Validating and transforming recipes...
Validating: 100%|████████| 40/40
Valid: 38, Invalid: 2

Uploading to Firebase...
Uploading: 100%|████████| 38/38

Done!
  Selected: 50
  Valid: 38
  Uploaded: 38
  Skipped: 10 (already uploaded)
  Failed: 2
    recipe_123: Missing required fields (tags, instructions, or ingredients)
    recipe_456: tags is not a list
```

---

### `plaite upload`

Upload recipes from a JSON file to Firebase.

```bash
# Upload from JSON file (default: excludes already-uploaded)
uv run plaite upload recipes/batch4.json --env dev

# Dry run - validate without uploading
uv run plaite upload recipes/batch4.json --dry-run

# Upload to production
uv run plaite upload recipes/batch4.json --env prod

# Include recipes already in Firebase
uv run plaite upload recipes/batch4.json --include-uploaded

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
| `--dry-run` | Validate only, don't upload |
| `--include-uploaded` | Include recipes already in Firebase |

**Flow:**
1. Load recipes from JSON file
2. Check Firebase for existing recipes
3. Validate and transform (field renaming, nutrients transformation, type checking)
4. Upload to Firebase

---

### `plaite scrape`

Scrape a recipe from a URL and upload to Firebase.

```bash
# Scrape and upload a recipe (default: excludes already-uploaded)
uv run plaite scrape "https://www.allrecipes.com/recipe/123/chocolate-cake/"

# Dry run to test scraping and validation
uv run plaite scrape "https://example.com/recipe" --dry-run

# Upload to production
uv run plaite scrape "https://example.com/recipe" --env prod

# Include even if already uploaded
uv run plaite scrape "https://example.com/recipe" --include-uploaded
```

**Options:**
| Option | Description |
|--------|-------------|
| `--env` | Environment: `prod` or `dev` (default: dev) |
| `--config`, `-c` | Firebase config path |
| `--upload-config` | Upload config path |
| `--dry-run` | Validate only, don't upload |
| `--include-uploaded` | Include recipes already in Firebase |

**Flow:**
1. Scrape recipe data from URL (using recipe-scrapers library)
2. Preview scraped title, host, author, tags
3. Check Firebase for existing recipes (by URL hash)
4. Validate and transform (field renaming, nutrients transformation, type checking)
5. Upload to Firebase if valid

**Note:** All three commands (`sync`, `upload`, `scrape`) use identical validation and produce consistent output.

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
│   ├── data/               # Data loading module
│   │   ├── __init__.py     # Public API
│   │   ├── _tables.py      # Table wrapper (parquet)
│   │   ├── _queries.py     # Pre-built query templates
│   │   ├── loader.py       # Loading functions
│   │   ├── columns.py      # Column definitions
│   │   ├── query.py        # Col query builder
│   │   └── README.md       # Full documentation
│   ├── pipeline/           # Upload pipelines
│   │   ├── upload.py       # Upload from local/file/URL
│   │   └── validation.py   # Comprehensive validation
│   ├── scraper/            # Recipe scraping
│   │   ├── __init__.py
│   │   └── scraper.py      # URL scraping with recipe-scrapers
│   ├── firebase/
│   │   ├── client.py       # Firebase initialization
│   │   ├── stats.py        # Stats collection
│   │   └── upload.py       # Legacy batch upload
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
