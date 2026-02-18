"""Unit tests for apps/modules/utils.py."""

from apps.modules.utils import get_fields_from_sigma


class TestGetFieldsFromSigma:
    """Tests for get_fields_from_sigma function."""

    def test_extract_fields_from_detection(self, sample_sigma_rule):
        """Should extract field names from detection section."""
        fields = get_fields_from_sigma(sample_sigma_rule)
        assert "EventID" in fields
        assert "CommandLine" in fields
        assert "User" in fields

    def test_strip_modifiers(self, sigma_rule_with_modifiers):
        """Should strip field modifiers (|contains, |endswith, etc)."""
        fields = get_fields_from_sigma(sigma_rule_with_modifiers)
        assert "CommandLine" in fields
        assert "TargetFilename" in fields
        assert "User" in fields
        # Modifiers should be stripped
        assert "CommandLine|contains|all" not in fields
        assert "TargetFilename|endswith" not in fields

    def test_no_detection_returns_empty(self, sigma_rule_no_detection):
        """Should return empty set when no detection section."""
        fields = get_fields_from_sigma(sigma_rule_no_detection)
        assert fields == set()

    def test_invalid_yaml_returns_empty(self, invalid_yaml):
        """Should return empty set for invalid YAML."""
        fields = get_fields_from_sigma(invalid_yaml)
        assert fields == set()

    def test_empty_string_returns_empty(self):
        """Should return empty set for empty string."""
        fields = get_fields_from_sigma("")
        assert fields == set()

    def test_non_dict_yaml_returns_empty(self):
        """Should return empty set when YAML is not a dict."""
        fields = get_fields_from_sigma("- item1\n- item2")
        assert fields == set()

    def test_detection_not_dict_returns_empty(self):
        """Should return empty set when detection is not a dict."""
        yaml_str = "detection: not_a_dict"
        fields = get_fields_from_sigma(yaml_str)
        assert fields == set()

    def test_multiple_selections(self):
        """Should extract fields from multiple selections."""
        yaml_str = """
detection:
  selection1:
    FieldA: value
  selection2:
    FieldB: value
  filter:
    FieldC: value
  condition: selection1 or selection2 and not filter
"""
        fields = get_fields_from_sigma(yaml_str)
        assert "FieldA" in fields
        assert "FieldB" in fields
        assert "FieldC" in fields

    def test_list_of_conditions(self):
        """Should handle list of conditions in selection."""
        yaml_str = """
detection:
  selection:
    - CommandLine: test1
      EventID: 1
    - CommandLine: test2
      EventID: 2
  condition: selection
"""
        fields = get_fields_from_sigma(yaml_str)
        assert "CommandLine" in fields
        assert "EventID" in fields

    def test_nested_modifier_extraction(self):
        """Should extract base field name with multiple modifiers."""
        yaml_str = """
detection:
  selection:
    Image|endswith|all:
      - "\\\\cmd.exe"
      - "\\\\powershell.exe"
  condition: selection
"""
        fields = get_fields_from_sigma(yaml_str)
        assert "Image" in fields
        assert len(fields) == 1

    def test_condition_field_excluded(self):
        """Condition is not a field, should handle properly."""
        yaml_str = """
detection:
  selection:
    EventID: 4688
  condition: selection
"""
        fields = get_fields_from_sigma(yaml_str)
        assert "EventID" in fields
        # condition value should not be treated as a field
        assert "selection" not in fields
