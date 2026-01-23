from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator


class Nutrient(BaseModel):
    name: str
    quantity: str


class FoodCodes(BaseModel):
    ingredientID: str | None = None


class Ingredient(BaseModel):
    quantity: float | None = None
    unit: str | None = None
    displayString: str | None = None
    foodCodes: FoodCodes | None = None


class IngredientGroup(BaseModel):
    ingredients: list[str] = Field(default_factory=list)
    purpose: str | None = None


class Recipe(BaseModel):
    # Core identifiers
    id: str
    uuid: str | None = None

    # Basic info
    title: str
    description: str | None = None
    url: str | None = None
    host: str | None = None
    image: str | None = None
    author: str | None = None

    # Content
    instructions: list[str] = Field(default_factory=list)
    ingredientGroups: list[IngredientGroup] = Field(default_factory=list)
    ingredientStrings: list[str] = Field(default_factory=list)
    ingredients: list[Ingredient] = Field(default_factory=list)

    # Metadata
    tags: list[str] = Field(default_factory=list)
    cookingMethod: str | None = None

    # Nutrition
    nutrients: list[Nutrient] = Field(default_factory=list)
    healthScore: float | None = None
    healthGrade: str | None = None

    # Timing/servings
    numServings: float | None = None
    cookTime: int | None = None
    prepTime: int | None = None
    totalTime: int | None = None

    # Ratings
    ratings: float | None = None
    ratingsCount: int | None = None

    # ML
    embedding: list[float] = Field(default_factory=list)
    cluster_id: int | None = None

    # App meta
    channel: str = "discover"

    @field_validator("nutrients", mode="before")
    @classmethod
    def _normalize_nutrients(cls, v: Any) -> list[dict[str, str]]:
        if v is None:
            return []
        if isinstance(v, dict):
            return [{"name": k, "quantity": str(val)} for k, val in v.items()]
        if isinstance(v, list):
            return v
        return []

    @field_validator("numServings", mode="before")
    @classmethod
    def _normalize_servings(cls, v: Any) -> float | None:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v)
        if isinstance(v, str):
            # "4 servings", "4-6 servings"
            import re
            nums = re.findall(r"\d+\.?\d*", v)
            return float(nums[0]) if nums else None
        return None

    @model_validator(mode="after")
    def _ensure_ingredient_strings(self) -> Recipe:
        if not self.ingredientStrings and self.ingredients:
            self.ingredientStrings = [
                ing.displayString for ing in self.ingredients if ing.displayString
            ]
        return self

# ...existing code...
    @classmethod
    def from_raw(cls, data: dict[str, Any]) -> Recipe:
        """
        Map raw/local recipe data into unified Firebase-ready schema.
        Keep all mapping in one place.
        """
        raw_ingredients = data.get("ingredients")
        processed = data.get("procesedIngredients")  # local typo

        ingredient_strings = data.get("ingredientStrings") or []
        if not ingredient_strings and isinstance(raw_ingredients, list):
            if raw_ingredients and isinstance(raw_ingredients[0], str):
                ingredient_strings = raw_ingredients

        structured_ingredients: list[dict[str, Any]] = []
        if isinstance(processed, list):
            structured_ingredients = processed
        elif isinstance(raw_ingredients, list) and raw_ingredients and isinstance(raw_ingredients[0], dict):
            structured_ingredients = raw_ingredients

        return cls(
            id=data.get("id") or data.get("recipe_id"),
            uuid=data.get("uuid"),
            title=data.get("title") or "Unknown",
            description=data.get("description"),
            url=data.get("url"),
            host=data.get("host"),
            image=data.get("image"),
            author=data.get("author"),
            instructions=data.get("instructions") or [],
            ingredientGroups=data.get("ingredientGroups") or [],
            ingredientStrings=ingredient_strings,
            ingredients=structured_ingredients,
            tags=data.get("tags") or [],
            cookingMethod=data.get("cookingMethod"),
            nutrients=data.get("nutrients"),
            healthScore=data.get("healthScore"),
            healthGrade=data.get("healthGrade"),
            numServings=data.get("numServings"),
            cookTime=data.get("cookTime"),
            prepTime=data.get("prepTime"),
            totalTime=data.get("totalTime"),
            ratings=data.get("ratings"),
            ratingsCount=data.get("ratingsCount"),
            embedding=data.get("embedding") or [],
            cluster_id=data.get("cluster_id"),
            channel=data.get("channel") or "discover",
        )

    def validate(self) -> None:
        """
        Validate required fields for Firebase-ready recipe.

        Raises:
            ValueError: if any required field is missing/invalid.
        """
        errors: list[str] = []

        if not self.id:
            errors.append("id is required")
        if not self.uuid:
            errors.append("uuid is required")
        if not self.title:
            errors.append("title is required")

        if not bool(self.ingredients):
            errors.append("ingredients are required structured")
        if not bool(self.ingredientStrings):
            errors.append("ingredient strings are required")

        if not self.nutrients:
            errors.append("nutrients are required")

        if errors:
            raise ValueError("; ".join(errors))
