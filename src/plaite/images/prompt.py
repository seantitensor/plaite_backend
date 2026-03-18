"""Food photography prompt generation for Google Imagen 4."""

_NEGATIVE_PROMPT = (
    "blur, noise, overexposed, underexposed, text, watermark, logo, cartoon, illustration, "
    "painting, drawing, distortion, hands, people, plastic-looking, artificial, low quality, "
    "grainy, out of focus background clutter"
)

_DEFAULT_STYLE = {
    "angle": "45-degree angle",
    "lighting": "soft warm studio lighting",
    "surface": "white ceramic plate on a dark slate surface",
    "mood": "appetizing and professional",
}

_DISH_TYPE_STYLES = [
    (
        {"breakfast", "brunch"},
        {
            "angle": "overhead flat lay",
            "lighting": "soft natural window light from the right side",
            "surface": "white ceramic plate on a light linen tablecloth",
            "mood": "bright and airy",
        },
    ),
    (
        {"dessert"},
        {
            "angle": "45-degree angle close-up",
            "lighting": "soft warm side lighting",
            "surface": "dark marble surface",
            "mood": "rich and indulgent",
        },
    ),
    (
        {"salad"},
        {
            "angle": "overhead flat lay",
            "lighting": "soft diffused natural light",
            "surface": "white plate on a light wooden table",
            "mood": "fresh and vibrant",
        },
    ),
    (
        {"soup", "stew"},
        {
            "angle": "45-degree angle",
            "lighting": "warm side lighting",
            "surface": "rustic ceramic bowl on a dark wooden table",
            "mood": "hearty and comforting",
        },
    ),
    (
        {"beverage", "drink"},
        {
            "angle": "eye-level",
            "lighting": "backlit with warm rim lighting",
            "surface": "dark oak bar surface",
            "mood": "elegant and inviting",
        },
    ),
    (
        {"sandwich"},
        {
            "angle": "eye-level close-up",
            "lighting": "dramatic warm studio lighting",
            "surface": "dark wooden board",
            "mood": "bold and appetizing",
        },
    ),
]

_COOKING_METHOD_TEXTURES = [
    ({"grill / bbq", "grilled"}, "with visible grill marks and caramelized char"),
    ({"baking", "baked"}, "with a golden-brown crust and flaky layers"),
    ({"air fryer"}, "with a crispy golden exterior"),
    ({"slow cooker", "slow cooked"}, "tender and falling apart, with a rich glossy sauce"),
]

_DIET_OVERRIDES = [
    (
        {"vegan", "vegetarian"},
        {
            "surface": "white plate on a bright marble surface",
            "extra": "fresh herbs as garnish, vibrant natural colors",
        },
    ),
    (
        {"keto", "low carb"},
        {"extra": "rich sauces and proteins as the focal point"},
    ),
    (
        {"healthy", "high protein"},
        {"mood": "fresh and energizing", "lighting": "bright natural light"},
    ),
]

_GARNISH_KEYWORDS = {"garnish", "top with", "sprinkle", "finish with", "drizzle", "serve with"}


def _normalise_tags(tags: list) -> set[str]:
    return {str(t).lower().strip() for t in tags}


def _pick_dish_style(tag_set: set[str]) -> dict:
    for keywords, style in _DISH_TYPE_STYLES:
        if tag_set & keywords:
            return dict(style)
    return dict(_DEFAULT_STYLE)


def _pick_texture(tag_set: set[str], cooking_method: str | None) -> str:
    method_lower = (cooking_method or "").lower().strip()
    combined = tag_set | ({method_lower} if method_lower else set())
    for keywords, texture in _COOKING_METHOD_TEXTURES:
        if combined & keywords:
            return texture
    return ""


def _apply_diet_overrides(style: dict, tag_set: set[str]) -> tuple[dict, str]:
    extra_parts: list[str] = []
    for keywords, overrides in _DIET_OVERRIDES:
        if tag_set & keywords:
            for key, value in overrides.items():
                if key == "extra":
                    extra_parts.append(value)
                else:
                    style[key] = value
    return style, ", ".join(extra_parts)


def _pick_ingredient_highlights(ingredient_strings: list) -> str:
    candidates = [s.strip() for s in ingredient_strings[:8] if s and s.strip()]
    # Prefer shorter, single-word or two-word items that read visually (skip long prep strings)
    visual = [s for s in candidates if len(s.split()) <= 4][:3]
    if not visual:
        visual = candidates[:2]
    if not visual:
        return ""
    if len(visual) == 1:
        return f"featuring {visual[0]}"
    return f"featuring {', '.join(visual[:-1])} and {visual[-1]}"


def _extract_garnish(instructions: list) -> str:
    """Pull the garnish step out of the instructions if one exists."""
    for step in instructions:
        step_lower = str(step).lower()
        if any(kw in step_lower for kw in _GARNISH_KEYWORDS):
            # Truncate to a reasonable length for the prompt
            return str(step).strip()[:120]
    return ""


def build_food_prompt(recipe: dict) -> tuple[str, str]:
    title = recipe.get("title", "dish")
    tags: list = recipe.get("tags") or []
    ingredient_strings: list = recipe.get("ingredientStrings") or []
    cooking_method: str | None = recipe.get("cookingMethod")
    instructions: list = recipe.get("instructions") or []

    tag_set = _normalise_tags(tags)

    style = _pick_dish_style(tag_set)
    texture = _pick_texture(tag_set, cooking_method)
    style, diet_extra = _apply_diet_overrides(style, tag_set)

    ingredient_highlights = _pick_ingredient_highlights(ingredient_strings)
    garnish = _extract_garnish(instructions)

    texture_clause = f", {texture}" if texture else ""
    diet_clause = f" {diet_extra}." if diet_extra else ""

    parts = [
        f"{style['angle']} photograph of {title}{texture_clause}.",
        f"Served on {style['surface']}.",
        f"{style['lighting']}, casting soft shadows to highlight the dish's texture and depth.",
        f"{style['mood']} composition.",
    ]

    if ingredient_highlights:
        parts.append(f"{ingredient_highlights}.{diet_clause}")
    elif diet_extra:
        parts.append(diet_extra + ".")

    if garnish:
        parts.append(f"{garnish}.")

    parts.append("Professional food photography, sharp focus on the dish, shallow depth of field, 4K.")

    prompt = " ".join(parts)
    return prompt, _NEGATIVE_PROMPT
