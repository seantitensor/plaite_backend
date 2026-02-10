# Image Generation with Google Imagen 4

Generate recipe images using Google's Imagen 4 API - a lightweight, simple solution for creating high-quality food photography from text descriptions.

## Setup

### 1. Install Dependencies

```bash
cd plaite_backend
uv sync  # This will install google-genai>=0.3.0
```

### 2. Get API Key

Get a Google API key from [Google AI Studio](https://aistudio.google.com/):
1. Go to https://aistudio.google.com/
2. Click "Get API Key"
3. Create a new API key or use an existing one

### 3. Set Environment Variable

```bash
export GOOGLE_API_KEY="your-api-key-here"
```

Or add to your `.env` file:
```bash
GOOGLE_API_KEY=your-api-key-here
```

## CLI Usage

### Basic Generation

```bash
# Generate a single image
uv run plaite generate-image "Delicious chocolate cake with strawberries"

# Specify output path
uv run plaite generate-image "Homemade pizza" --output pizza.png

# Use ultra quality model
uv run plaite generate-image "Gourmet burger" --model ultra

# Change aspect ratio
uv run plaite generate-image "Pasta carbonara" --aspect-ratio 16:9

# Generate multiple variations
uv run plaite generate-image "Fresh salad" --num-images 4
```

### Available Options

| Option | Values | Default | Description |
|--------|--------|---------|-------------|
| `--output`, `-o` | Path | `generated_image.png` | Output file path |
| `--model` | `standard`, `ultra`, `fast` | `standard` | Model variant |
| `--aspect-ratio` | `1:1`, `3:4`, `4:3`, `9:16`, `16:9` | `4:3` | Image aspect ratio |
| `--num-images` | 1-4 | 1 | Number of images to generate |

### Model Variants

- **standard** (`imagen-4.0-generate-001`): Balanced quality and speed - best for most use cases
- **ultra** (`imagen-4.0-ultra-generate-001`): Highest quality - for professional assets
- **fast** (`imagen-4.0-fast-generate-001`): Fastest generation - for quick iterations

### Aspect Ratios

- **4:3** - Standard food photography (recommended for recipes)
- **1:1** - Square format (Instagram posts)
- **16:9** - Wide format (website banners, headers)
- **9:16** - Vertical format (Instagram stories, mobile)
- **3:4** - Portrait format

## Python API Usage

### Basic Example

```python
from plaite.generation import ImageGenerator

# Initialize generator
generator = ImageGenerator()

# Generate image
images = generator.generate("Chocolate cake with strawberries")

# Save image
images[0].save("cake.png")
```

### Generate Multiple Images

```python
# Generate 4 variations at once
images = generator.generate(
    "Homemade pizza with basil",
    num_images=4,
    aspect_ratio="16:9",
    model="fast"
)

# Save all variations
for i, image in enumerate(images, start=1):
    image.save(f"pizza_{i}.png")
```

### High Quality Generation

```python
# Use ultra model with 2K resolution
images = generator.generate(
    "Professional food photography of a gourmet burger",
    model="ultra",
    image_size="2K",
    aspect_ratio="4:3"
)
```

### Convenience Function

```python
from plaite.generation import generate_recipe_image

# One-liner for single recipe images
path = generate_recipe_image(
    "Fluffy pancakes with maple syrup",
    "pancakes.png",
    model="standard"
)
```

### Batch Generation

```python
generator = ImageGenerator()

recipes = [
    "Classic spaghetti carbonara",
    "Fresh Greek salad with feta",
    "Chocolate chip cookies",
]

for i, prompt in enumerate(recipes, start=1):
    images = generator.generate(prompt, aspect_ratio="4:3")
    images[0].save(f"recipe_{i}.png")
```

## Examples

Run the comprehensive examples:

```bash
# Make sure GOOGLE_API_KEY is set
export GOOGLE_API_KEY="your-api-key"

# Run examples
uv run python examples/generate_images.py
```

The example script demonstrates:
- Basic single image generation
- Multiple image variations
- High quality generation
- Batch processing
- Different aspect ratios
- Convenience functions

## API Reference

### `ImageGenerator`

Main class for image generation.

**Methods:**

#### `__init__(api_key: str | None = None)`

Initialize the generator. Uses `GOOGLE_API_KEY` env var if api_key not provided.

#### `generate(prompt, *, model="standard", num_images=1, aspect_ratio="1:1", image_size="1K", person_generation="dont_allow")`

Generate images from a text prompt.

**Parameters:**
- `prompt` (str): Text description (English only, max 480 tokens)
- `model` (str): Model variant - `"standard"`, `"ultra"`, or `"fast"`
- `num_images` (int): Number of images (1-4)
- `aspect_ratio` (str): Aspect ratio - `"1:1"`, `"3:4"`, `"4:3"`, `"9:16"`, `"16:9"`
- `image_size` (str): Resolution - `"1K"` or `"2K"`
- `person_generation` (str): `"dont_allow"`, `"allow_adult"`, `"allow_all"`

**Returns:** List of PIL Image objects

#### `generate_and_save(prompt, output_dir, *, filename_prefix="generated", **kwargs)`

Generate images and save to disk.

**Parameters:**
- `prompt` (str): Text description
- `output_dir` (str | Path): Directory to save images
- `filename_prefix` (str): Prefix for output filenames
- `**kwargs`: Additional arguments passed to `generate()`

**Returns:** List of Path objects for saved images

### `generate_recipe_image()`

Convenience function for single recipe images with optimized defaults.

```python
generate_recipe_image(
    prompt: str,
    output_path: str | Path,
    *,
    api_key: str | None = None,
    model: str = "standard",
    aspect_ratio: str = "4:3"
) -> Path
```

## Pricing

**Imagen 4 Pricing (as of February 2026):**

| Resolution | Price per Image |
|-----------|----------------|
| 1K (1024x1024) | $0.02 |
| 2K (2048x2048) | $0.04 |

**Note:** 1K resolution is sufficient for most recipe images. 2K recommended only for high-resolution professional assets.

## Best Practices

### Writing Effective Prompts

✅ **Good prompts:**
- "Professional food photography of a chocolate cake with fresh strawberries"
- "Homemade margherita pizza with fresh basil leaves and melted mozzarella"
- "Close-up of fluffy pancakes drizzled with maple syrup"

❌ **Avoid:**
- Vague prompts: "food", "dinner"
- Non-English prompts (English only)
- Overly complex prompts (max 480 tokens)

### Choosing the Right Model

- **Start with `standard`** - Best quality/cost balance for recipes
- **Use `fast`** - For quick iterations and testing
- **Use `ultra`** - Only for final professional assets

### Aspect Ratios for Recipes

- **4:3** - Default for recipe cards (recommended)
- **1:1** - Social media squares
- **16:9** - Website headers and banners

## Error Handling

```python
from plaite.generation import ImageGenerator

generator = ImageGenerator()

try:
    images = generator.generate("Chocolate cake")
    images[0].save("cake.png")
except ValueError as e:
    print(f"Invalid parameters: {e}")
except RuntimeError as e:
    print(f"Generation failed: {e}")
```

## Troubleshooting

### "Google API key required"

Make sure `GOOGLE_API_KEY` environment variable is set:
```bash
export GOOGLE_API_KEY="your-api-key"
```

### "Image generation failed"

- Check your API key is valid
- Ensure you have billing enabled in Google Cloud Console
- Verify your prompt is in English and under 480 tokens
- Check you're not rate-limited

### Import errors

Make sure dependencies are installed:
```bash
uv sync
```

## Resources

- [Official Imagen Documentation](https://ai.google.dev/gemini-api/docs/imagen)
- [Google Gen AI SDK Documentation](https://googleapis.github.io/python-genai/)
- [Image Generation API Reference](https://docs.cloud.google.com/vertex-ai/generative-ai/docs/model-reference/imagen-api)

---

*Generated with Google Imagen 4 | Simple & Lightweight Recipe Image Generation*
