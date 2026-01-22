"""Recipe scraping from URLs using recipe-scrapers library."""

import hashlib
from typing import Any

import requests
from recipe_scrapers import scrape_html


def get_tags(scraper) -> list[str]:
    """Extract all available tags from the scraper."""
    tags = []

    # Category
    try:
        category = scraper.category()
        if category:
            tags.append(category)
    except Exception:
        pass

    # Cuisine
    try:
        cuisine = scraper.cuisine()
        if cuisine:
            tags.append(cuisine)
    except Exception:
        pass

    # Dietary restrictions
    try:
        diet_tags = scraper.dietary_restrictions()
        if diet_tags:
            tags.extend(diet_tags)
    except Exception:
        pass

    # Equipment
    try:
        equipment = scraper.equipment()
        if equipment:
            tags.append(equipment)
    except Exception:
        pass

    # Keywords
    try:
        keywords = scraper.keywords()
        if keywords:
            tags.extend(keywords)
    except Exception:
        pass

    return list(set(tags))


def scrape_recipe(url: str, timeout: int = 10) -> dict[str, Any] | None:
    """
    Scrape recipe information from a URL.

    Args:
        url: Recipe URL to scrape
        timeout: Request timeout in seconds

    Returns:
        Recipe dictionary with scraped data, or None if scraping fails
    """
    # Fetch HTML
    try:
        headers = {"User-Agent": "Bot/1.0"}
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        html = response.content
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL {url}: {e}")
        return None

    # Initialize scraper
    try:
        scraper = scrape_html(html, org_url=url)
    except Exception as e:
        print(f"Error initializing scraper for {url}: {e}")
        return None

    # Extract title (required field)
    try:
        title = scraper.title()
        if not title:
            print(f"Title is None for {url}, skipping.")
            return None
    except Exception as e:
        print(f"Error getting title: {e}")
        return None

    # Extract all fields with individual error handling
    recipe_data = {"title": title, "url": url}

    # Description
    try:
        recipe_data["description"] = scraper.description()
    except Exception:
        recipe_data["description"] = None

    # Nutrients
    try:
        recipe_data["nutrients"] = scraper.nutrients()
    except Exception:
        recipe_data["nutrients"] = None

    # Host
    try:
        recipe_data["host"] = scraper.host()
    except Exception:
        recipe_data["host"] = None

    # Image
    try:
        recipe_data["image"] = scraper.image()
    except Exception:
        recipe_data["image"] = None

    # Instructions
    try:
        instructions = scraper.instructions()
        recipe_data["instructions"] = (
            instructions.splitlines() if instructions else []
        )
    except Exception:
        recipe_data["instructions"] = []

    # Ingredient groups
    try:
        ingredient_groups = scraper.ingredient_groups()
        recipe_data["ingredientGroups"] = [
            group.__dict__ for group in ingredient_groups
        ]
    except Exception:
        recipe_data["ingredientGroups"] = []

    # Ingredients
    try:
        recipe_data["ingredients"] = scraper.ingredients()
    except Exception:
        recipe_data["ingredients"] = []

    # Author
    try:
        recipe_data["author"] = scraper.author()
    except Exception:
        recipe_data["author"] = None

    # Tags
    try:
        recipe_data["tags"] = get_tags(scraper)
    except Exception:
        recipe_data["tags"] = []

    # Servings
    try:
        recipe_data["numServings"] = scraper.yields()
    except Exception:
        recipe_data["numServings"] = None

    # Cook time
    try:
        recipe_data["cookTime"] = scraper.cook_time()
    except Exception:
        recipe_data["cookTime"] = None

    # Prep time
    try:
        recipe_data["prepTime"] = scraper.prep_time()
    except Exception:
        recipe_data["prepTime"] = None

    # Total time
    try:
        recipe_data["totalTime"] = scraper.total_time()
    except Exception:
        recipe_data["totalTime"] = None

    # Cooking method
    try:
        recipe_data["cookingMethod"] = scraper.cooking_method()
    except Exception:
        recipe_data["cookingMethod"] = None

    # Ratings
    try:
        recipe_data["ratings"] = scraper.ratings()
    except Exception:
        recipe_data["ratings"] = None

    # Ratings count
    try:
        recipe_data["ratingsCount"] = scraper.ratings_count()
    except Exception:
        recipe_data["ratingsCount"] = None

    # Generate unique ID from URL
    recipe_data["id"] = hashlib.sha256(url.encode("utf-8")).hexdigest()

    return recipe_data
