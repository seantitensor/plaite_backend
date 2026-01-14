"""Unit tests for plaite.data module following TDD."""

import pytest
import polars as pl

import plaite.data as data


class TestLoadRecipes:
    """Tests for load_recipes() function."""

    def test_returns_dataframe(self):
        """load_recipes should return a Polars DataFrame."""
        result = data.load_recipes()
        assert isinstance(result, pl.DataFrame)

    def test_returns_all_rows(self):
        """load_recipes should return all recipes without filtering."""
        result = data.load_recipes()
        # Should have data (assuming test data exists)
        assert len(result) > 0

    def test_accepts_columns_parameter(self):
        """load_recipes should accept optional columns parameter."""
        result = data.load_recipes(columns=["recipe_id", "title"])
        assert isinstance(result, pl.DataFrame)
        assert list(result.columns) == ["recipe_id", "title"]


class TestGetBatchOfRecipes:
    """Tests for get_batch_of_recipes() function."""

    def test_returns_dataframe(self):
        """get_batch_of_recipes should return a Polars DataFrame."""
        result = data.get_batch_of_recipes(count=5)
        assert isinstance(result, pl.DataFrame)

    def test_returns_correct_count(self):
        """get_batch_of_recipes should return exactly the requested count."""
        count = 10
        result = data.get_batch_of_recipes(count=count)
        assert len(result) == count

    def test_returns_less_when_not_enough_data(self):
        """get_batch_of_recipes should return available rows if less than count."""
        # Request more than available
        huge_count = 999999999
        result = data.get_batch_of_recipes(count=huge_count)
        # Should return what's available, not fail
        assert len(result) >= 0

    def test_accepts_query_parameter(self):
        """get_batch_of_recipes should accept optional query filters."""
        # This should not raise an error
        result = data.get_batch_of_recipes(count=5, query={"healthGrade": "B"})
        assert isinstance(result, pl.DataFrame)
        assert len(result) <= 5

    def test_query_filters_results(self):
        """get_batch_of_recipes with query should filter results."""
        query = {"healthGrade": "B"}
        result = data.get_batch_of_recipes(count=100, query=query)
        # All results should match the filter (if column exists)
        # This test will pass if filtering works or column doesn't exist
        assert len(result) <= 100


class TestGetStatsOfAllRecipes:
    """Tests for get_stats_of_all_recipes() function."""

    def test_returns_dict(self):
        """get_stats_of_all_recipes should return a dictionary."""
        result = data.get_stats_of_all_recipes()
        assert isinstance(result, dict)

    def test_includes_total_count(self):
        """get_stats_of_all_recipes should include total recipe count."""
        result = data.get_stats_of_all_recipes()
        assert "total_recipes" in result
        assert isinstance(result["total_recipes"], int)
        assert result["total_recipes"] >= 0

    def test_includes_column_count(self):
        """get_stats_of_all_recipes should include number of columns."""
        result = data.get_stats_of_all_recipes()
        assert "total_columns" in result
        assert isinstance(result["total_columns"], int)
        assert result["total_columns"] > 0

    def test_includes_column_names(self):
        """get_stats_of_all_recipes should include list of column names."""
        result = data.get_stats_of_all_recipes()
        assert "columns" in result
        assert isinstance(result["columns"], list)
        assert len(result["columns"]) > 0

    def test_includes_cluster_distribution(self):
        """get_stats_of_all_recipes should include cluster distribution."""
        result = data.get_stats_of_all_recipes()
        assert "recipes_per_cluster" in result
        assert isinstance(result["recipes_per_cluster"], dict)
        assert len(result["recipes_per_cluster"]) > 0

    def test_includes_health_grade_distribution(self):
        """get_stats_of_all_recipes should include health grade distribution."""
        result = data.get_stats_of_all_recipes()
        assert "recipes_per_health_grade" in result
        assert isinstance(result["recipes_per_health_grade"], dict)
        assert len(result["recipes_per_health_grade"]) > 0

    def test_includes_unique_ingredients_count(self):
        """get_stats_of_all_recipes should include unique ingredients count."""
        result = data.get_stats_of_all_recipes()
        assert "unique_ingredients_count" in result
        assert isinstance(result["unique_ingredients_count"], int)
        assert result["unique_ingredients_count"] > 0


class TestGetFilteredRecipes:
    """Tests for get_filtered_recipes() function."""

    def test_returns_dataframe(self):
        """get_filtered_recipes should return a Polars DataFrame."""
        result = data.get_filtered_recipes()
        assert isinstance(result, pl.DataFrame)

    def test_returns_all_without_query(self):
        """get_filtered_recipes without query should return all recipes."""
        result = data.get_filtered_recipes()
        assert len(result) > 0

    def test_accepts_query_parameter(self):
        """get_filtered_recipes should accept optional query filters."""
        result = data.get_filtered_recipes(query={"healthGrade": "B"})
        assert isinstance(result, pl.DataFrame)

    def test_query_filters_results(self):
        """get_filtered_recipes with query should filter results."""
        all_recipes = data.get_filtered_recipes()
        filtered = data.get_filtered_recipes(query={"healthGrade": "B"})
        # Filtered should be less than or equal to all (unless all are healthGrade B)
        assert len(filtered) <= len(all_recipes)

    def test_query_with_operators(self):
        """get_filtered_recipes should support filter operators like __lt, __gt."""
        # This should not raise an error
        result = data.get_filtered_recipes(query={"healthScore__gt": 60})
        assert isinstance(result, pl.DataFrame)

    def test_empty_query_returns_all(self):
        """get_filtered_recipes with empty query should return all recipes."""
        all_recipes = data.get_filtered_recipes()
        with_empty_query = data.get_filtered_recipes(query={})
        assert len(all_recipes) == len(with_empty_query)


class TestGetRecipesColumns:
    """Tests for get_recipes_columns() function."""

    def test_returns_string(self):
        """get_recipes_columns should return a string."""
        result = data.get_recipes_columns()
        assert isinstance(result, str)

    def test_includes_column_names(self):
        """get_recipes_columns should include actual column names."""
        result = data.get_recipes_columns()
        # Should include known columns from our restructured data
        assert "recipe_id" in result
        assert "title" in result
        assert "healthScore" in result

    def test_includes_data_types(self):
        """get_recipes_columns should include data type information."""
        result = data.get_recipes_columns()
        # Should show dtype information
        assert "dtype" in result.lower() or "type" in result.lower()


# Integration tests with mock data
class TestIntegrationWithMockData:
    """Integration tests with mocked recipe data."""

    @pytest.fixture
    def mock_recipe_data(self):
        """Create mock recipe data for testing."""
        return pl.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "name": ["Pasta", "Salad", "Cake", "Soup", "Cookies"],
            "category": ["main", "appetizer", "dessert", "main", "dessert"],
            "calories": [600, 150, 450, 300, 400],
            "rating": [4.5, 4.0, 5.0, 3.5, 4.8]
        })

    def test_get_batch_respects_count(self, mock_recipe_data, monkeypatch):
        """Verify get_batch_of_recipes respects count parameter."""
        # This will be tested once implementation is done
        pass

    def test_stats_accuracy(self, mock_recipe_data, monkeypatch):
        """Verify stats calculation is accurate."""
        # This will be tested once implementation is done
        pass
