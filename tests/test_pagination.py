"""Unit tests for apps/core/pagination.py."""

from apps.core.pagination import calculate_skip


class TestCalculateSkip:
    """Tests for calculate_skip function."""

    def test_first_page(self):
        """First page should have skip=0."""
        result = calculate_skip(page=1, limit=10)
        assert result == 0

    def test_second_page(self):
        """Second page with limit 10 should skip 10."""
        result = calculate_skip(page=2, limit=10)
        assert result == 10

    def test_third_page(self):
        """Third page with limit 10 should skip 20."""
        result = calculate_skip(page=3, limit=10)
        assert result == 20

    def test_different_limit(self):
        """Should work with different limit values."""
        result = calculate_skip(page=2, limit=25)
        assert result == 25

    def test_with_offset(self):
        """Should add offset to skip value."""
        result = calculate_skip(page=2, limit=10, offset=5)
        assert result == 15  # (2-1)*10 + 5 = 15

    def test_large_page(self):
        """Should work with large page numbers."""
        result = calculate_skip(page=100, limit=10)
        assert result == 990

    def test_limit_one(self):
        """Should work with limit=1."""
        result = calculate_skip(page=5, limit=1)
        assert result == 4


class TestPaginationEdgeCases:
    """Edge case tests for pagination."""

    def test_zero_offset(self):
        """Zero offset should not affect result."""
        result = calculate_skip(page=3, limit=10, offset=0)
        assert result == 20

    def test_large_offset(self):
        """Large offset should be added correctly."""
        result = calculate_skip(page=1, limit=10, offset=100)
        assert result == 100

    def test_formula_verification(self):
        """Verify the formula (page-1)*limit + offset."""
        for page in range(1, 11):
            for limit in [5, 10, 25, 50]:
                for offset in [0, 5, 10]:
                    expected = (page - 1) * limit + offset
                    result = calculate_skip(page, limit, offset)
                    assert result == expected
