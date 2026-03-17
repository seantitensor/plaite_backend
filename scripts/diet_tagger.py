#!/usr/bin/env python3
"""
Diet Tagger - Classify recipes with diet tags using a hybrid rule + LLM approach.

Strategy:
  1. Rule pass: check ingredient strings against curated blocklists per diet.
     - Whole-word match on blocklist  → confidence 0.92, applies=False
     - Match is a known safe compound  → confidence 0.45, uncertain (→ LLM)
     - No matches found               → confidence 0.85, applies=True
  2. Nutrient pass: for nutrient-based diets (keto, low-carb, high-protein)
     parse the nutrients list and check thresholds.
  3. LLM pass (Gemini): uncertain cases batched and sent for accurate classification.
     Requires GOOGLE_API_KEY env var. If missing, uncertain cases are skipped.
  4. Final: tags with applies=True and confidence >= APPLY_THRESHOLD are merged
     into existing recipe tags.

Targets:
  - Parquet file (RECIPES_PATH env var): tags column updated in-place, backup created.
  - Firebase (optional): existing docs patched using ArrayUnion to preserve other tags.

Usage:
    uv run python scripts/diet_tagger.py --dry-run
    uv run python scripts/diet_tagger.py --env dev
    uv run python scripts/diet_tagger.py --env prod --limit 500
    uv run python scripts/diet_tagger.py --env dev --skip-tagged
    uv run python scripts/diet_tagger.py --parquet-only
    uv run python scripts/diet_tagger.py --firebase-only --env dev
"""

from __future__ import annotations

import json
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

import polars as pl
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table as RichTable

load_dotenv(override=True)

console = Console()

# ─── Tunables ─────────────────────────────────────────────────────────────────

# Below this confidence the rule classifier escalates to the LLM
UNCERTAINTY_THRESHOLD = 0.90

# A classification must reach this confidence before the tag is applied
APPLY_THRESHOLD = 0.75

# Max recipes per LLM batch request (keep < 20 to avoid token limits)
LLM_BATCH_SIZE = 20


# ─── Diet Definitions ─────────────────────────────────────────────────────────
# Edit this list to add, remove, or tweak diets.
# Each diet has:
#   blocklist      - ingredients that DISQUALIFY the diet
#   safe_compounds - phrases that contain a blocklist word but are still OK
#                    e.g. "almond milk" contains "milk" but is dairy-free
#   nutrient_rule  - optional nutrient-threshold check
#                    {"field": "<name>", "op": "lt"|"gt", "value": <float>}
#                    Field names are matched case-insensitively against the
#                    nutrient "name" field (substring match).


@dataclass
class DietDef:
    name: str
    description: str
    blocklist: list[str] = field(default_factory=list)
    safe_compounds: list[str] = field(default_factory=list)
    nutrient_rule: dict | None = None


DIETS: list[DietDef] = [
    # ── Animal-product diets ──────────────────────────────────────────────────
    DietDef(
        name="vegan",
        description="No animal products (meat, fish, dairy, eggs, honey, gelatin)",
        blocklist=[
            "chicken", "beef", "pork", "lamb", "turkey", "duck", "veal", "rabbit",
            "venison", "bison", "bacon", "ham", "sausage", "salami", "pepperoni",
            "chorizo", "prosciutto", "lard", "suet", "tallow",
            "fish", "salmon", "tuna", "cod", "tilapia", "halibut", "trout",
            "shrimp", "prawn", "crab", "lobster", "scallop", "clam", "oyster",
            "squid", "anchovy", "sardine",
            "milk", "cream", "butter", "cheese", "yogurt", "parmesan",
            "mozzarella", "cheddar", "brie", "feta", "ricotta", "ghee",
            "whey", "lactose", "casein", "sour cream", "creme fraiche",
            "egg", "mayonnaise",
            "honey", "gelatin",
        ],
        safe_compounds=[
            "almond milk", "coconut milk", "oat milk", "rice milk", "soy milk",
            "cashew milk", "almond butter", "peanut butter", "cocoa butter",
            "cream of tartar", "coconut cream", "coconut butter",
            "dairy-free butter", "vegan butter", "plant-based butter",
            "eggplant",
        ],
    ),
    DietDef(
        name="vegetarian",
        description="No meat or fish; dairy and eggs allowed",
        blocklist=[
            "chicken", "beef", "pork", "lamb", "turkey", "duck", "veal", "rabbit",
            "venison", "bison", "bacon", "ham", "sausage", "salami", "pepperoni",
            "chorizo", "prosciutto", "lard", "suet", "tallow",
            "fish", "salmon", "tuna", "cod", "tilapia", "halibut", "trout",
            "shrimp", "prawn", "crab", "lobster", "scallop", "clam", "oyster",
            "squid", "anchovy", "sardine",
            "gelatin",
        ],
        safe_compounds=[],
    ),
    # ── Allergen-free diets ───────────────────────────────────────────────────
    DietDef(
        name="gluten-free",
        description="No gluten (wheat, barley, rye, and their derivatives)",
        blocklist=[
            "flour", "wheat", "barley", "rye", "bread", "pasta", "noodle",
            "breadcrumb", "panko", "cracker", "crouton", "soy sauce",
            "beer", "malt", "semolina", "bulgur", "farro", "spelt",
            "couscous", "tortilla", "pita", "croissant", "baguette",
        ],
        safe_compounds=[
            "rice flour", "almond flour", "coconut flour", "buckwheat flour",
            "corn flour", "chickpea flour", "gluten-free flour",
            "rice noodle", "glass noodle", "rice pasta", "zucchini noodle",
            "gluten-free soy sauce", "tamari", "coconut aminos",
            "corn tortilla",
        ],
    ),
    DietDef(
        name="dairy-free",
        description="No dairy products",
        blocklist=[
            "milk", "cream", "butter", "cheese", "yogurt", "parmesan",
            "mozzarella", "cheddar", "brie", "feta", "ricotta", "ghee",
            "whey", "lactose", "casein", "sour cream", "creme fraiche",
            "half-and-half",
        ],
        safe_compounds=[
            "almond milk", "coconut milk", "oat milk", "rice milk", "soy milk",
            "cashew milk", "almond butter", "peanut butter", "cocoa butter",
            "cream of tartar", "coconut cream", "coconut butter",
            "dairy-free butter", "vegan butter", "plant-based butter",
        ],
    ),
    DietDef(
        name="nut-free",
        description="No tree nuts or peanuts",
        blocklist=[
            "almond", "cashew", "walnut", "pecan", "pistachio", "hazelnut",
            "macadamia", "brazil nut", "pine nut", "peanut",
            "nut butter", "almond flour", "almond milk", "almond extract",
            "marzipan", "praline",
        ],
        safe_compounds=["nutmeg", "donut", "doughnut", "coconut"],
    ),
    # ── Macro-based diets (ingredient + nutrient signals) ─────────────────────
    DietDef(
        name="keto",
        description="Very low carb, high fat — net carbs < 20g per serving",
        blocklist=[
            "sugar", "brown sugar", "honey", "syrup", "maple syrup", "agave",
            "bread", "pasta", "rice", "potato", "flour", "oat", "oats",
            "corn", "quinoa", "lentil", "chickpea", "bean", "beans",
            "fruit juice", "soda",
        ],
        safe_compounds=[
            "cauliflower rice", "zucchini noodle", "almond flour",
            "coconut flour", "monk fruit",
        ],
        nutrient_rule={"field": "carbohydrate", "op": "lt", "value": 20.0},
    ),
    DietDef(
        name="low-carb",
        description="Reduced carbohydrates — net carbs < 50g per serving",
        blocklist=[],
        safe_compounds=[],
        nutrient_rule={"field": "carbohydrate", "op": "lt", "value": 50.0},
    ),
    DietDef(
        name="high-protein",
        description="High protein content — protein > 25g per serving",
        blocklist=[],
        safe_compounds=[],
        nutrient_rule={"field": "protein", "op": "gt", "value": 25.0},
    ),
    # ── Add new diets below ───────────────────────────────────────────────────

    # ── Allergen-free diets (continued) ───────────────────────────────────────
    DietDef(
        name="egg-free",
        description="No eggs or egg-derived ingredients",
        blocklist=[
            "egg", "eggs", "mayonnaise", "meringue", "aioli",
            "hollandaise", "custard", "quiche", "frittata",
            "albumin", "lysozyme", "ovalbumin",
        ],
        safe_compounds=["eggplant"],
    ),
    DietDef(
        name="soy-free",
        description="No soy or soy-derived ingredients",
        blocklist=[
            "soy", "soybean", "tofu", "tempeh", "edamame", "miso",
            "soy sauce", "soy milk", "soy protein", "soy lecithin",
            "tamari", "natto", "textured vegetable protein",
        ],
        safe_compounds=[],
    ),
    DietDef(
        name="shellfish-free",
        description="No shellfish (crustaceans and mollusks)",
        blocklist=[
            "shrimp", "prawn", "crab", "lobster", "crayfish", "crawfish",
            "scallop", "clam", "mussel", "oyster", "squid", "calamari",
            "octopus", "abalone", "snail", "escargot",
        ],
        safe_compounds=[],
    ),
    DietDef(
        name="sesame-free",
        description="No sesame seeds or sesame-derived ingredients",
        blocklist=[
            "sesame", "sesame seed", "sesame oil", "tahini", "halva",
            "hummus", "gomashio", "sesame paste",
        ],
        safe_compounds=[],
    ),

    # ── Religious / cultural diets ────────────────────────────────────────────
    DietDef(
        name="halal",
        description="No pork, alcohol, or non-halal slaughtered meat",
        blocklist=[
            "pork", "bacon", "ham", "prosciutto", "pancetta", "lard",
            "salami", "pepperoni", "chorizo", "gelatin",
            "wine", "beer", "liquor", "rum", "bourbon", "whiskey",
            "vodka", "brandy", "champagne", "sake", "mirin",
            "wine vinegar", "marsala",
        ],
        safe_compounds=[
            "halal gelatin", "halal marshmallow",
        ],
    ),
    DietDef(
        name="kosher",
        description="No pork, no shellfish, no mixing of meat and dairy",
        blocklist=[
            "pork", "bacon", "ham", "prosciutto", "pancetta", "lard",
            "salami", "pepperoni", "chorizo",
            "shrimp", "prawn", "crab", "lobster", "crayfish", "crawfish",
            "scallop", "clam", "mussel", "oyster", "squid", "calamari",
            "octopus", "abalone", "snail", "escargot",
            "gelatin",
        ],
        safe_compounds=["kosher gelatin", "kosher salt"],
    ),
    DietDef(
        name="hindu-vegetarian",
        description="No meat, fish, or eggs; dairy allowed. No alcohol",
        blocklist=[
            "chicken", "beef", "pork", "lamb", "turkey", "duck", "veal",
            "rabbit", "venison", "bison", "bacon", "ham", "sausage",
            "salami", "pepperoni", "chorizo", "prosciutto", "lard",
            "suet", "tallow",
            "fish", "salmon", "tuna", "cod", "tilapia", "halibut", "trout",
            "shrimp", "prawn", "crab", "lobster", "scallop", "clam",
            "oyster", "squid", "anchovy", "sardine",
            "egg", "eggs", "mayonnaise",
            "gelatin",
            "wine", "beer", "liquor", "rum", "bourbon", "whiskey",
            "vodka", "brandy", "champagne", "sake", "mirin",
        ],
        safe_compounds=["eggplant"],
    ),

    # ── Lifestyle diets ───────────────────────────────────────────────────────
    DietDef(
        name="pescatarian",
        description="No meat; fish, seafood, dairy, and eggs allowed",
        blocklist=[
            "chicken", "beef", "pork", "lamb", "turkey", "duck", "veal",
            "rabbit", "venison", "bison", "bacon", "ham", "sausage",
            "salami", "pepperoni", "chorizo", "prosciutto", "lard",
            "suet", "tallow",
        ],
        safe_compounds=[],
    ),
    DietDef(
        name="paleo",
        description="No grains, legumes, dairy, refined sugar, or processed foods",
        blocklist=[
            "wheat", "flour", "bread", "pasta", "rice", "oat", "oats",
            "corn", "barley", "rye", "quinoa", "bulgur", "farro",
            "spelt", "couscous", "semolina", "noodle", "cracker",
            "tortilla", "cereal",
            "bean", "beans", "lentil", "chickpea", "peanut", "peanut butter",
            "soybean", "tofu", "tempeh", "edamame", "miso",
            "milk", "cream", "butter", "cheese", "yogurt", "parmesan",
            "mozzarella", "cheddar", "brie", "feta", "ricotta", "ghee",
            "whey", "sour cream", "creme fraiche",
            "sugar", "brown sugar", "corn syrup", "high fructose corn syrup",
            "agave", "artificial sweetener", "canola oil", "vegetable oil",
            "soybean oil", "margarine",
        ],
        safe_compounds=[
            "almond flour", "coconut flour", "almond milk", "coconut milk",
            "almond butter", "coconut cream", "coconut butter",
            "coconut aminos", "ghee",  # ghee is sometimes paleo-accepted
        ],
    ),
    DietDef(
        name="whole30",
        description="No grains, legumes, dairy, added sugar, alcohol, soy, or carrageenan",
        blocklist=[
            "wheat", "flour", "bread", "pasta", "rice", "oat", "oats",
            "corn", "barley", "rye", "quinoa", "bulgur", "farro",
            "spelt", "couscous", "semolina", "noodle", "cracker",
            "tortilla", "cereal",
            "bean", "beans", "lentil", "chickpea", "peanut", "peanut butter",
            "soybean", "tofu", "tempeh", "edamame", "miso", "soy sauce",
            "soy milk", "soy lecithin",
            "milk", "cream", "butter", "cheese", "yogurt", "parmesan",
            "mozzarella", "cheddar", "brie", "feta", "ricotta",
            "whey", "sour cream", "creme fraiche",
            "sugar", "brown sugar", "honey", "syrup", "maple syrup",
            "agave", "stevia", "monk fruit", "xylitol", "aspartame",
            "sucralose", "corn syrup",
            "wine", "beer", "liquor", "rum", "bourbon", "whiskey",
            "vodka", "brandy", "champagne", "sake", "mirin",
            "carrageenan", "sulfite", "MSG",
        ],
        safe_compounds=[
            "almond flour", "coconut flour", "almond milk", "coconut milk",
            "coconut cream", "coconut aminos", "ghee",
            "coconut butter", "almond butter",
        ],
    ),
    DietDef(
        name="mediterranean",
        description="Limits red meat, processed food, refined sugar, and processed grains",
        blocklist=[
            "beef", "pork", "lamb", "veal", "bison", "venison",
            "bacon", "ham", "sausage", "salami", "pepperoni",
            "chorizo", "prosciutto", "hot dog",
            "sugar", "brown sugar", "corn syrup", "high fructose corn syrup",
            "artificial sweetener",
            "white bread", "white rice", "margarine",
            "canola oil", "vegetable oil", "soybean oil",
            "soda", "candy",
        ],
        safe_compounds=[],
    ),
    DietDef(
        name="low-fodmap",
        description="Restricts fermentable carbs (garlic, onion, wheat, certain fruits/legumes)",
        blocklist=[
            "garlic", "onion", "shallot", "leek", "scallion",
            "wheat", "rye", "barley",
            "apple", "pear", "watermelon", "mango", "cherry", "peach",
            "plum", "nectarine", "apricot", "blackberry",
            "bean", "beans", "lentil", "chickpea", "kidney bean",
            "black bean", "baked beans",
            "milk", "yogurt", "ice cream", "soft cheese", "ricotta",
            "cottage cheese", "cream cheese",
            "honey", "agave", "high fructose corn syrup",
            "cauliflower", "mushroom", "asparagus", "artichoke",
            "sugar alcohol", "sorbitol", "mannitol", "xylitol",
            "inulin", "chicory root",
        ],
        safe_compounds=[
            "garlic-infused oil", "green part of scallion",
            "lactose-free milk", "lactose-free yogurt",
            "hard cheese", "parmesan", "cheddar", "brie",
            "sourdough spelt bread",
        ],
    ),

    # ── Additional macro / nutrient-based diets ───────────────────────────────
    DietDef(
        name="low-fat",
        description="Low fat content — total fat < 10g per serving",
        blocklist=[],
        safe_compounds=[],
        nutrient_rule={"field": "fat", "op": "lt", "value": 10.0},
    ),
    DietDef(
        name="low-sugar",
        description="Low added sugar — sugar < 6g per serving",
        blocklist=[
            "sugar", "brown sugar", "corn syrup", "high fructose corn syrup",
            "agave", "candy", "soda", "sweetened condensed milk",
        ],
        safe_compounds=["sugar snap pea"],
        nutrient_rule={"field": "sugar", "op": "lt", "value": 6.0},
    ),
    DietDef(
        name="low-sodium",
        description="Low sodium — sodium < 600mg per serving",
        blocklist=[],
        safe_compounds=[],
        nutrient_rule={"field": "sodium", "op": "lt", "value": 600.0},
    ),

    # ── Anti-inflammatory diet ────────────────────────────────────────────────
    
    DietDef(
        name="anti-inflammatory",
        description="Avoids processed foods, refined sugar, refined grains, and common inflammatory triggers",
        blocklist=[
            "sugar", "brown sugar", "corn syrup", "high fructose corn syrup",
            "agave", "artificial sweetener", "aspartame", "sucralose",
            "white bread", "white rice", "white flour", "refined flour",
            "pasta", "cracker", "breadcrumb", "croissant", "baguette",
            "margarine", "shortening", "canola oil", "vegetable oil",
            "soybean oil", "corn oil", "sunflower oil", "safflower oil",
            "trans fat", "hydrogenated oil",
            "soda", "candy", "hot dog", "sausage", "salami", "pepperoni",
            "bacon", "deli meat", "processed cheese",
            "MSG", "carrageenan",
            "beer", "liquor", "wine",  # alcohol is inflammatory
        ],
        safe_compounds=[
            "extra virgin olive oil", "avocado oil",
            "whole grain bread", "brown rice", "wild rice",
            "whole wheat pasta",
        ],
    )
]

DIET_MAP: dict[str, DietDef] = {d.name: d for d in DIETS}


# ─── Data classes ─────────────────────────────────────────────────────────────


@dataclass
class DietResult:
    diet: str
    applies: bool | None  # None means uncertain
    confidence: float
    method: str  # "rules" | "nutrients" | "llm" | "uncertain"
    reason: str


@dataclass
class RecipeClassification:
    recipe_id: str
    title: str
    results: list[DietResult]

    @property
    def new_tags(self) -> list[str]:
        """Return diet names that apply with sufficient confidence."""
        return [
            r.diet
            for r in self.results
            if r.applies is True and r.confidence >= APPLY_THRESHOLD
        ]


# ─── Nutrient helpers ─────────────────────────────────────────────────────────


def _parse_quantity(quantity_str: str | None) -> float | None:
    """Extract first numeric value from a quantity string like '45.2g' or '300 kcal'."""
    if not quantity_str:
        return None
    match = re.search(r"[\d.]+", str(quantity_str))
    return float(match.group()) if match else None


def _extract_nutrient(nutrients: list[dict], field_substr: str) -> float | None:
    """
    Find a nutrient whose name contains `field_substr` (case-insensitive)
    and return its numeric quantity.
    """
    field_lower = field_substr.lower()
    for n in nutrients or []:
        name = (n.get("name") or "").lower()
        if field_lower in name:
            return _parse_quantity(n.get("quantity"))
    return None


# ─── Rule classifier ──────────────────────────────────────────────────────────


def _classify_by_rules(
    ingredient_strings: list[str], nutrients: list[dict], diet: DietDef
) -> DietResult:
    """
    Rule-based diet classification for a single recipe + diet pair.

    Returns a DietResult with method "rules", "nutrients", or "uncertain".
    """
    combined = " | ".join(ingredient_strings).lower()

    # ── Ingredient blocklist check ─────────────────────────────────────────
    if diet.blocklist:
        for term in diet.blocklist:
            pattern = r"\b" + re.escape(term) + r"\b"
            if re.search(pattern, combined):
                # Check if it's a known safe compound before flagging
                is_safe = any(
                    safe.lower() in combined for safe in diet.safe_compounds
                    if term.lower() in safe.lower()
                )
                if is_safe:
                    return DietResult(
                        diet=diet.name,
                        applies=None,
                        confidence=0.45,
                        method="uncertain",
                        reason=f"'{term}' found but may be a safe compound — needs LLM review",
                    )
                return DietResult(
                    diet=diet.name,
                    applies=False,
                    confidence=0.92,
                    method="rules",
                    reason=f"Contains '{term}'",
                )

        # No blocklist hit → likely applies
        ingredient_result = DietResult(
            diet=diet.name,
            applies=True,
            confidence=0.85,
            method="rules",
            reason="No disqualifying ingredients found",
        )
    else:
        ingredient_result = None

    # ── Nutrient rule check ────────────────────────────────────────────────
    if diet.nutrient_rule:
        rule = diet.nutrient_rule
        value = _extract_nutrient(nutrients, rule["field"])

        if value is not None:
            op = rule["op"]
            threshold = rule["value"]
            passes = (value < threshold) if op == "lt" else (value > threshold)
            direction = "below" if op == "lt" else "above"
            return DietResult(
                diet=diet.name,
                applies=passes,
                confidence=0.90,
                method="nutrients",
                reason=f"{rule['field']} = {value:.1f}g "
                       f"({'✓' if passes else '✗'} {direction} {threshold}g threshold)",
            )
        # Nutrient data missing → fall back to ingredient result or uncertain
        if ingredient_result is not None:
            return ingredient_result
        return DietResult(
            diet=diet.name,
            applies=None,
            confidence=0.40,
            method="uncertain",
            reason="Nutrient data unavailable for threshold check",
        )

    return ingredient_result or DietResult(
        diet=diet.name,
        applies=None,
        confidence=0.40,
        method="uncertain",
        reason="No rules configured for this diet",
    )


def classify_recipe_by_rules(
    ingredient_strings: list[str], nutrients: list[dict]
) -> list[DietResult]:
    return [_classify_by_rules(ingredient_strings, nutrients, d) for d in DIETS]


# ─── LLM classifier ───────────────────────────────────────────────────────────


def _build_llm_prompt(recipes_payload: list[dict], diet_names: list[str]) -> str:
    diet_descriptions = {d.name: d.description for d in DIETS if d.name in diet_names}
    return f"""You are a diet classification expert. Given each recipe's title and
ingredient list, determine which diets apply.

Diets to classify:
{json.dumps(diet_descriptions, indent=2)}

Recipes:
{json.dumps(recipes_payload, indent=2)}

Respond with ONLY valid JSON, no markdown, no extra text:
{{
  "results": [
    {{
      "id": 0,
      "classifications": {{
        "<diet_name>": {{
          "applies": true,
          "confidence": 0.95,
          "reason": "short reason"
        }}
      }}
    }}
  ]
}}

Rules:
- confidence: 0.0–1.0
- Only classify the diets listed above
- Base judgment SOLELY on the ingredient list provided
- Be conservative: if genuinely uncertain, use lower confidence (< 0.6)
"""


def llm_classify_batch(
    recipes: list[RecipeClassification],
    uncertain_diet_map: dict[str, list[str]],  # recipe_id → list[diet_name]
) -> dict[str, dict[str, DietResult]]:
    """
    Send uncertain (recipe, diet) pairs to Gemini for classification.

    Returns: {recipe_id: {diet_name: DietResult}}
    """
    try:
        from google import genai
        from google.genai import types
    except ImportError:
        console.print("[yellow]google-genai not available — skipping LLM pass[/yellow]")
        return {}

    api_key = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
    if not api_key:
        console.print(
            "[yellow]GOOGLE_API_KEY / GEMINI_API_KEY not set — skipping LLM pass[/yellow]"
        )
        return {}

    # Gemini 2.5 Flash Lite pricing (per 1M tokens, as of 2025)
    MODEL = "gemini-2.5-flash-lite"
    COST_PER_1M_INPUT = 0.10   # USD
    COST_PER_1M_OUTPUT = 0.40  # USD

    client = genai.Client(api_key=api_key)
    recipe_map = {r.recipe_id: r for r in recipes}
    llm_results: dict[str, dict[str, DietResult]] = {}
    total_input_tokens = 0
    total_output_tokens = 0

    # Build batches
    items = list(uncertain_diet_map.items())
    batches = [items[i : i + LLM_BATCH_SIZE] for i in range(0, len(items), LLM_BATCH_SIZE)]

    for batch_num, batch in enumerate(batches, 1):
        all_diets_in_batch: set[str] = set()
        # Use a sequential index instead of UUID so the LLM can't hallucinate IDs.
        # idx_map: int → real recipe_id
        idx_map: dict[int, str] = {}
        payload: list[dict] = []
        for idx, (recipe_id, diet_names) in enumerate(batch):
            rec = recipe_map[recipe_id]
            idx_map[idx] = recipe_id
            payload.append({
                "id": idx,
                "title": rec.title,
                "ingredients": rec._ingredient_strings,  # type: ignore[attr-defined]
            })
            all_diets_in_batch.update(diet_names)

        prompt = _build_llm_prompt(payload, list(all_diets_in_batch))

        try:
            response = client.models.generate_content(
                model=MODEL,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                ),
            )
            data = json.loads(response.text)

            # Track token usage
            usage = response.usage_metadata
            if usage:
                in_tok = usage.prompt_token_count or 0
                out_tok = usage.candidates_token_count or 0
                total_input_tokens += in_tok
                total_output_tokens += out_tok
                batch_cost = (in_tok / 1_000_000 * COST_PER_1M_INPUT +
                              out_tok / 1_000_000 * COST_PER_1M_OUTPUT)
                console.print(
                    f"  [dim]Batch {batch_num}/{len(batches)}: "
                    f"{in_tok} in / {out_tok} out tokens  "
                    f"≈ ${batch_cost:.5f}[/dim]"
                )
        except Exception as e:
            console.print(f"[red]LLM batch failed: {e}[/red]")
            continue

        for item in data.get("results", []):
            # Map index back to real recipe_id
            idx = item.get("id")
            recipe_id = idx_map.get(idx)
            if recipe_id is None:
                console.print(f"[yellow]LLM returned unknown id={idx!r}, skipping[/yellow]")
                continue
            llm_results.setdefault(recipe_id, {})
            for diet_name, cls in item.get("classifications", {}).items():
                llm_results[recipe_id][diet_name] = DietResult(
                    diet=diet_name,
                    applies=bool(cls.get("applies")),
                    confidence=float(cls.get("confidence", 0.7)),
                    method="llm",
                    reason=cls.get("reason", "LLM classification"),
                )

    # ── Cost summary ──────────────────────────────────────────────────────────
    if total_input_tokens or total_output_tokens:
        total_cost = (total_input_tokens / 1_000_000 * COST_PER_1M_INPUT +
                      total_output_tokens / 1_000_000 * COST_PER_1M_OUTPUT)
        console.print(
            f"\n[bold]LLM cost summary[/bold]  ({MODEL})\n"
            f"  Input tokens : {total_input_tokens:,}\n"
            f"  Output tokens: {total_output_tokens:,}\n"
            f"  [bold green]Estimated cost: ${total_cost:.4f}[/bold green]"
        )

    return llm_results


# ─── Hybrid classifier ────────────────────────────────────────────────────────


def classify_all(df: pl.DataFrame, use_llm: bool = True) -> list[RecipeClassification]:
    """
    Run hybrid classification over all rows in the DataFrame.

    Expected columns: recipe_id, title, ingredients, nutrients
    """
    classifications: list[RecipeClassification] = []
    uncertain_diet_map: dict[str, list[str]] = {}

    for row in df.iter_rows(named=True):
        recipe_id = row.get("recipe_id") or ""
        title = row.get("title") or ""
        ingredient_strings: list[str] = row.get("ingredients") or []
        nutrients_raw = row.get("nutrients") or []

        # Normalise nutrients: polars gives list of dicts (structs)
        nutrients: list[dict] = []
        for n in nutrients_raw:
            if isinstance(n, dict):
                nutrients.append(n)

        rule_results = classify_recipe_by_rules(ingredient_strings, nutrients)

        rc = RecipeClassification(
            recipe_id=recipe_id,
            title=title,
            results=rule_results,
        )
        # Attach raw data for LLM pass
        rc._ingredient_strings = ingredient_strings  # type: ignore[attr-defined]

        classifications.append(rc)

        # Collect uncertain results
        for r in rule_results:
            if r.applies is None and r.confidence < UNCERTAINTY_THRESHOLD:
                uncertain_diet_map.setdefault(recipe_id, []).append(r.diet)

    # ── LLM pass ──────────────────────────────────────────────────────────────
    if use_llm and uncertain_diet_map:
        console.print(
            f"\n[cyan]LLM pass:[/cyan] {len(uncertain_diet_map)} recipes × "
            f"{sum(len(v) for v in uncertain_diet_map.values())} uncertain (diet, recipe) pairs"
        )
        llm_results = llm_classify_batch(classifications, uncertain_diet_map)

        # Merge LLM results back
        rc_map = {rc.recipe_id: rc for rc in classifications}
        for recipe_id, diet_results in llm_results.items():
            rc = rc_map[recipe_id]
            for i, r in enumerate(rc.results):
                if r.diet in diet_results:
                    rc.results[i] = diet_results[r.diet]

    return classifications


# ─── Parquet updater ──────────────────────────────────────────────────────────


def update_parquet(
    classifications: list[RecipeClassification],
    parquet_path: str,
    dry_run: bool,
) -> int:
    """
    Merge new diet tags into the tags column and write back to the parquet file.
    Returns the number of recipes whose tags were updated.
    """
    path = Path(parquet_path)
    df = pl.read_parquet(path)

    # Build a mapping: recipe_id → new diet tags to add
    new_tags_map: dict[str, list[str]] = {
        rc.recipe_id: rc.new_tags
        for rc in classifications
        if rc.new_tags
    }

    if not new_tags_map:
        console.print("[yellow]No new diet tags to add to parquet.[/yellow]")
        return 0

    # Merge tags row-by-row
    updated_count = 0
    tag_series = df["tags"].to_list()
    ids = df["recipe_id"].to_list()

    for i, recipe_id in enumerate(ids):
        new = new_tags_map.get(recipe_id)
        if not new:
            continue
        existing: list[str] = tag_series[i] or []
        merged = list(dict.fromkeys(existing + new))  # dedup, preserve order
        if merged != existing:
            tag_series[i] = merged
            updated_count += 1

    if dry_run:
        console.print(
            f"[yellow]DRY RUN:[/yellow] Would update tags for {updated_count} recipes in parquet."
        )
        return updated_count

    # Backup original
    backup_path = path.with_suffix(".parquet.bak")
    import shutil
    shutil.copy2(path, backup_path)
    console.print(f"[dim]Backup created: {backup_path}[/dim]")

    # Write updated DataFrame
    updated_df = df.with_columns(pl.Series("tags", tag_series))
    updated_df.write_parquet(path)
    console.print(f"[green]Parquet updated:[/green] {updated_count} recipes tagged.")
    return updated_count


# ─── Firebase patcher ─────────────────────────────────────────────────────────


def patch_firebase(
    classifications: list[RecipeClassification],
    env: str,
    dry_run: bool,
) -> int:
    """
    For each recipe with new diet tags, patch the Firestore document using
    ArrayUnion so existing non-diet tags are preserved.
    Returns the number of documents patched.
    """
    try:
        from firebase_admin import firestore as fb_firestore

        from plaite.config import load_firebase_config
        from plaite.firebase.client import get_client, get_collection
    except ImportError as e:
        console.print(f"[red]Firebase import failed: {e}[/red]")
        return 0

    config_path = Path(__file__).parent.parent / "configs" / "firebase.yaml"
    config = load_firebase_config(config_path, env)  # type: ignore[arg-type]

    recipes_with_tags = [rc for rc in classifications if rc.new_tags]
    if not recipes_with_tags:
        console.print("[yellow]No new diet tags to patch in Firebase.[/yellow]")
        return 0

    console.print(f"\nFetching existing Firebase recipe IDs ({env})…")
    collection = get_collection(config)
    existing_ids = {doc.id for doc in collection.select([]).stream()}
    console.print(f"  Found {len(existing_ids)} documents in Firestore.")

    db = get_client(config)
    patched = 0
    skipped_missing = 0
    batch = db.batch()
    batch_count = 0

    for rc in recipes_with_tags:
        if rc.recipe_id not in existing_ids:
            skipped_missing += 1
            continue

        if dry_run:
            patched += 1
            continue

        doc_ref = collection.document(rc.recipe_id)
        batch.update(doc_ref, {"tags": fb_firestore.ArrayUnion(rc.new_tags)})
        batch_count += 1
        patched += 1

        if batch_count >= 400:  # Firestore batch limit is 500
            batch.commit()
            console.print(f"  Committed batch of {batch_count}")
            batch = db.batch()
            batch_count = 0

    if not dry_run and batch_count > 0:
        batch.commit()
        console.print(f"  Committed final batch of {batch_count}")

    if dry_run:
        console.print(
            f"[yellow]DRY RUN:[/yellow] Would patch {patched} Firebase docs "
            f"(skipped {skipped_missing} not in Firebase)."
        )
    else:
        console.print(
            f"[green]Firebase patched:[/green] {patched} docs updated "
            f"(skipped {skipped_missing} not in Firebase)."
        )

    return patched


# ─── Display helpers ──────────────────────────────────────────────────────────

CONFIDENCE_COLOR = {
    "high": "green",
    "med": "yellow",
    "low": "red",
}


def _conf_color(c: float) -> str:
    if c >= 0.80:
        return "green"
    if c >= 0.55:
        return "yellow"
    return "red"


def display_sample(classifications: list[RecipeClassification], n: int = 5) -> None:
    """Show a rich table preview of the first n classified recipes."""
    console.print(f"\n[bold]Sample classifications (first {n} recipes):[/bold]")

    for rc in classifications[:n]:
        t = RichTable(
            title=f"[bold]{rc.title}[/bold]  [dim]({rc.recipe_id})[/dim]",
            show_header=True,
            header_style="bold cyan",
        )
        t.add_column("Diet", style="bold", width=16)
        t.add_column("Applies", width=8)
        t.add_column("Confidence", width=12)
        t.add_column("Method", width=10)
        t.add_column("Reason")

        for r in rc.results:
            applies_str = (
                "[green]Yes[/green]" if r.applies is True
                else "[red]No[/red]" if r.applies is False
                else "[yellow]?[/yellow]"
            )
            color = _conf_color(r.confidence)
            t.add_row(
                r.diet,
                applies_str,
                f"[{color}]{r.confidence:.2f}[/{color}]",
                r.method,
                r.reason,
            )

        console.print(t)
        if rc.new_tags:
            console.print(f"  [bold green]→ New tags:[/bold green] {rc.new_tags}\n")
        else:
            console.print("  [dim]→ No new tags above threshold[/dim]\n")


def display_summary(classifications: list[RecipeClassification]) -> None:
    """Show aggregate stats: how many recipes got each diet tag."""
    tag_counts: dict[str, int] = {}
    uncertain_counts: dict[str, int] = {}
    total_tagged = 0

    for rc in classifications:
        if rc.new_tags:
            total_tagged += 1
        for r in rc.results:
            if r.applies is True and r.confidence >= APPLY_THRESHOLD:
                tag_counts[r.diet] = tag_counts.get(r.diet, 0) + 1
            elif r.applies is None:
                uncertain_counts[r.diet] = uncertain_counts.get(r.diet, 0) + 1

    t = RichTable(title="Diet Tag Summary", header_style="bold cyan")
    t.add_column("Diet", style="bold")
    t.add_column("Recipes tagged", justify="right")
    t.add_column("Still uncertain", justify="right")
    t.add_column("% of total", justify="right")

    total = len(classifications)
    for diet in DIETS:
        count = tag_counts.get(diet.name, 0)
        unc = uncertain_counts.get(diet.name, 0)
        pct = f"{count / total * 100:.1f}%" if total else "–"
        t.add_row(diet.name, str(count), str(unc), pct)

    console.print(t)
    console.print(
        f"\nTotal: [bold]{total_tagged}[/bold] / {total} recipes will receive at least one diet tag."
    )


# ─── Verify ───────────────────────────────────────────────────────────────────


def verify(parquet_path: str, env: str | None = None, samples: int = 3) -> None:
    """
    Check that diet tags were correctly written to parquet and optionally Firebase.

    Shows:
    - Tag distribution across all recipes in parquet
    - Sample recipes per diet tag so you can spot-check accuracy
    - If env is given: compares parquet tags vs live Firestore tags for a sample
    """
    diet_names = [d.name for d in DIETS]

    # ── Parquet ───────────────────────────────────────────────────────────────
    console.print(f"\n[bold]Verifying parquet:[/bold] [dim]{parquet_path}[/dim]")
    df = pl.read_parquet(parquet_path, columns=["recipe_id", "title", "tags"])
    total = len(df)

    # Count recipes with at least one diet tag
    any_diet = df.filter(
        pl.col("tags").list.eval(pl.element().is_in(diet_names)).list.any()
    )
    console.print(f"  Recipes with ≥1 diet tag: [bold]{len(any_diet)}[/bold] / {total}")

    # Per-diet counts + samples
    t = RichTable(title="Diet tag distribution (parquet)", header_style="bold cyan")
    t.add_column("Diet", style="bold")
    t.add_column("Count", justify="right")
    t.add_column("% of total", justify="right")
    t.add_column(f"Sample titles (up to {samples})")

    for diet in DIETS:
        tagged = df.filter(
            pl.col("tags").list.eval(pl.element() == diet.name).list.any()
        )
        count = len(tagged)
        pct = f"{count / total * 100:.1f}%" if total else "–"
        sample_titles = tagged["title"].head(samples).to_list()
        t.add_row(diet.name, str(count), pct, " | ".join(sample_titles) or "–")

    console.print(t)

    # ── Firebase comparison (optional) ────────────────────────────────────────
    if env is None:
        return

    console.print(f"\n[bold]Comparing with Firebase ({env})…[/bold]")
    try:
        from plaite.config import load_firebase_config
        from plaite.firebase.client import get_collection
    except ImportError as e:
        console.print(f"[red]Firebase import failed: {e}[/red]")
        return

    config_path = Path(__file__).parent.parent / "configs" / "firebase.yaml"
    config = load_firebase_config(config_path, env)  # type: ignore[arg-type]
    collection = get_collection(config)

    # Sample recipes that have diet tags in parquet and check Firebase agrees
    sample_df = any_diet.head(10)
    mismatches = 0

    check_table = RichTable(
        title=f"Parquet vs Firebase spot-check (sample of {len(sample_df)})",
        header_style="bold cyan",
    )
    check_table.add_column("Title")
    check_table.add_column("Parquet tags")
    check_table.add_column("Firebase tags")
    check_table.add_column("Match?", justify="center")

    for row in sample_df.iter_rows(named=True):
        recipe_id = row["recipe_id"]
        parquet_diet_tags = sorted(
            t for t in (row["tags"] or []) if t in diet_names
        )

        doc = collection.document(recipe_id).get()
        if not doc.exists:
            check_table.add_row(
                row["title"], str(parquet_diet_tags), "[dim]not in Firebase[/dim]", "–"
            )
            continue

        fb_tags = doc.to_dict().get("tags") or []
        fb_diet_tags = sorted(t for t in fb_tags if t in diet_names)

        match = parquet_diet_tags == fb_diet_tags
        if not match:
            mismatches += 1
        match_str = "[green]✓[/green]" if match else "[red]✗[/red]"
        check_table.add_row(
            row["title"],
            ", ".join(parquet_diet_tags) or "–",
            ", ".join(fb_diet_tags) or "–",
            match_str,
        )

    console.print(check_table)
    if mismatches:
        console.print(f"[red]{mismatches} mismatches found — Firebase may not be fully synced.[/red]")
    else:
        console.print("[green]All sampled recipes match between parquet and Firebase.[/green]")


# ─── Main ─────────────────────────────────────────────────────────────────────


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Classify recipes by diet and update parquet + Firebase tags."
    )
    parser.add_argument(
        "--env", choices=["dev", "prod"], default="dev",
        help="Firebase environment (default: dev)"
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Only process the first N recipes (useful for testing)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview changes without writing anything"
    )
    parser.add_argument(
        "--skip-tagged", action="store_true",
        help="Skip recipes that already have at least one diet tag"
    )
    parser.add_argument(
        "--parquet-only", action="store_true",
        help="Update parquet only, skip Firebase"
    )
    parser.add_argument(
        "--firebase-only", action="store_true",
        help="Patch Firebase only, skip parquet update"
    )
    parser.add_argument(
        "--no-llm", action="store_true",
        help="Disable LLM pass (rules + nutrients only)"
    )
    parser.add_argument(
        "--verify", action="store_true",
        help="Check diet tag distribution in parquet (and Firebase if --env is set)"
    )
    args = parser.parse_args()

    # ── Load data ─────────────────────────────────────────────────────────────
    recipes_path = os.getenv("RECIPES_PATH")
    if not recipes_path:
        console.print("[red]Error: RECIPES_PATH env var not set.[/red]")
        sys.exit(1)

    # ── Verify mode ───────────────────────────────────────────────────────────
    if args.verify:
        firebase_env = None if args.parquet_only else args.env
        verify(recipes_path, env=firebase_env)
        return

    console.print(f"[bold]Diet Tagger[/bold]  |  env=[cyan]{args.env}[/cyan]"
                  f"  dry_run=[cyan]{args.dry_run}[/cyan]")

    if args.firebase_only:
        console.print(f"Loading recipes from Firebase ([cyan]{args.env}[/cyan])…")
        from pathlib import Path as _Path
        from plaite.config import load_firebase_config
        from plaite.firebase.client import get_collection
        _config_path = _Path(__file__).parent.parent / "configs" / "firebase.yaml"
        _config = load_firebase_config(_config_path, args.env)
        _collection = get_collection(_config)
        _docs = _collection.select(["title", "ingredientStrings", "nutrients", "tags"]).stream()
        _rows = []
        for _doc in _docs:
            _d = _doc.to_dict()
            # Prefer ingredientStrings; fall back to ingredients[].displayString
            _ing_strings = _d.get("ingredientStrings") or []
            if not _ing_strings:
                _ing_strings = [
                    i.get("displayString") for i in (_d.get("ingredients") or [])
                    if isinstance(i, dict) and i.get("displayString")
                ]
            _rows.append({
                "recipe_id": _doc.id,
                "title": _d.get("title") or "",
                "ingredients": [i for i in _ing_strings if i],
                "nutrients": _d.get("nutrients") or [],
                "tags": _d.get("tags") or [],
            })
        df = pl.DataFrame(_rows, schema={
            "recipe_id": pl.String,
            "title": pl.String,
            "ingredients": pl.List(pl.String),
            "nutrients": pl.List(pl.Struct({"name": pl.String, "amount": pl.Float64, "unit": pl.String})),
            "tags": pl.List(pl.String),
        })
    else:
        console.print(f"Loading recipes from: [dim]{recipes_path}[/dim]")
        df = pl.read_parquet(recipes_path, columns=[
            "recipe_id", "title", "ingredients", "nutrients", "tags",
        ])

    console.print(f"Loaded [bold]{len(df)}[/bold] recipes.")

    # ── Optional filters ──────────────────────────────────────────────────────
    if args.skip_tagged:
        known_diet_names = [d.name for d in DIETS]
        # Keep only recipes whose tags list has no current diet tags
        df = df.filter(
            ~pl.col("tags").list.eval(
                pl.element().is_in(known_diet_names)
            ).list.any()
        )
        console.print(f"After --skip-tagged: [bold]{len(df)}[/bold] recipes remaining.")

    if args.limit:
        df = df.head(args.limit)
        console.print(f"Limiting to first [bold]{args.limit}[/bold] recipes.")

    # ── Classify ──────────────────────────────────────────────────────────────
    console.print("\n[bold]Running hybrid classification…[/bold]")
    use_llm = not args.no_llm
    classifications = classify_all(df, use_llm=use_llm)

    # ── Preview ───────────────────────────────────────────────────────────────
    display_sample(classifications, n=3)
    display_summary(classifications)

    if args.dry_run:
        console.print("\n[yellow]── DRY RUN — no files or databases modified ──[/yellow]")
        return

    # ── Write ─────────────────────────────────────────────────────────────────
    if not args.firebase_only:
        update_parquet(classifications, recipes_path, dry_run=False)

    if not args.parquet_only:
        patch_firebase(classifications, env=args.env, dry_run=False)

    console.print("\n[bold green]Done.[/bold green]")


if __name__ == "__main__":
    main()
