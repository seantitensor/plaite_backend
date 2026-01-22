"""Comprehensive recipe validation and transformation."""

import re
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ValidationError:
    """Detailed validation error with context."""

    recipe_id: str
    field: str | None
    error: str
    value: Any | None = None


@dataclass
class ValidationResult:
    """Result of recipe validation."""

    is_valid: bool
    recipe: dict[str, Any] | None
    errors: list[ValidationError] = field(default_factory=list)


def validate_and_transform_recipe(recipe: dict[str, Any]) -> ValidationResult:
    """
    Comprehensive validation and transformation matching user's validation logic.

    Steps:
    1. Field renaming (ingredients → ingredientStrings, procesedIngredients → ingredients)
    2. Required field presence checks (tags, instructions, ingredients)
    3. Type validation for list fields
    4. Nutrients transformation (dict → array)
    5. numServings normalization
    6. Image URL validation

    Args:
        recipe: Recipe dictionary with local schema fields

    Returns:
        ValidationResult with is_valid flag, transformed recipe, and any errors
    """
    errors = []
    recipe_id = recipe.get("id", "unknown")

    # Make a copy to avoid mutating the original
    recipe_data = recipe.copy()

    # 1. Field renaming - ingredients → ingredientStrings
    if "ingredients" in recipe_data:
        recipe_data["ingredientStrings"] = recipe_data.pop("ingredients")

    # Field renaming - procesedIngredients → ingredients
    if "procesedIngredients" in recipe_data:
        recipe_data["ingredients"] = recipe_data.pop("procesedIngredients")

    # 2. Required field checks
    if (
        "tags" not in recipe_data
        or "instructions" not in recipe_data
        or "ingredients" not in recipe_data
    ):
        errors.append(
            ValidationError(
                recipe_id=recipe_id,
                field=None,
                error="Missing required fields (tags, instructions, or ingredients)",
            )
        )
        return ValidationResult(is_valid=False, recipe=None, errors=errors)

    # 3. Type validation for required list fields
    if not isinstance(recipe_data["tags"], list):
        errors.append(
            ValidationError(
                recipe_id=recipe_id,
                field="tags",
                error="tags is not a list",
                value=type(recipe_data["tags"]).__name__,
            )
        )
        return ValidationResult(is_valid=False, recipe=None, errors=errors)

    if not isinstance(recipe_data["instructions"], list):
        errors.append(
            ValidationError(
                recipe_id=recipe_id,
                field="instructions",
                error="instructions is not a list",
                value=type(recipe_data["instructions"]).__name__,
            )
        )
        return ValidationResult(is_valid=False, recipe=None, errors=errors)

    if not isinstance(recipe_data["ingredients"], list):
        errors.append(
            ValidationError(
                recipe_id=recipe_id,
                field="ingredients",
                error="ingredients is not a list",
                value=type(recipe_data["ingredients"]).__name__,
            )
        )
        return ValidationResult(is_valid=False, recipe=None, errors=errors)

    # 4. Nutrients transformation: dict → array of {name, quantity}
    if "nutrients" in recipe_data:
        nutrients = recipe_data["nutrients"]
        if isinstance(nutrients, dict):
            # Convert dictionary to array of Nutrient objects
            nutrients_array = []
            for name, quantity in nutrients.items():
                nutrient_obj = {"name": name, "quantity": str(quantity)}
                nutrients_array.append(nutrient_obj)
            recipe_data["nutrients"] = nutrients_array
        elif not isinstance(nutrients, list):
            # If it's neither dict nor list, set to empty array
            recipe_data["nutrients"] = []

    # 5. numServings normalization
    if "numServings" in recipe_data:
        servings = recipe_data["numServings"]
        if isinstance(servings, str):
            # Extract number from strings like "4 servings", "4-6 servings"
            numbers = re.findall(r"\d+\.?\d*", servings)
            if numbers:
                recipe_data["numServings"] = float(numbers[0])
            else:
                recipe_data["numServings"] = None
        elif not isinstance(servings, (int, float)):
            recipe_data["numServings"] = None

    # 6. Image URL validation
    if "url" not in recipe_data:
        errors.append(
            ValidationError(
                recipe_id=recipe_id, field="url", error="Missing image URL"
            )
        )
        return ValidationResult(is_valid=False, recipe=None, errors=errors)

    # All validations passed
    return ValidationResult(is_valid=True, recipe=recipe_data, errors=[])
