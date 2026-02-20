import tempfile
import unittest

from app.token_matcher import SemanticTokenMatcher


class TokenMatcherTests(unittest.TestCase):
    def test_match_can_detect_blocked_sensitive_field_from_raw_query_text(self):
        matcher = SemanticTokenMatcher("app/semantics/smartbi_demo_macau_banking_semantic.yaml")
        features = {
            "tokens": [],
            "metrics": [],
            "dimensions": [],
            "filters": [],
            "time_start": "",
            "time_end": "",
            "query_text": "查詢何俊傑的身份證號",
        }

        token_hits = matcher.match(features)

        blocked_names = {item.get("canonical_name") for item in token_hits.get("blocked_matches", [])}
        self.assertIn("customer.id_no", blocked_names)

    def test_match_treats_string_false_sensitive_allowed_as_blocked(self):
        semantic_yaml = """
version: 1
semantic_layer:
  entities:
    customer:
      table: core_customer
      sensitive_fields:
        - name: id_no
          expr: core_customer.id_no
          allowed: "false"
          synonyms: [身份證號]
  datasets: {}
"""
        with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=True) as f:
            f.write(semantic_yaml)
            f.flush()
            matcher = SemanticTokenMatcher(f.name)
            features = {
                "tokens": [],
                "metrics": [],
                "dimensions": [],
                "filters": [],
                "time_start": "",
                "time_end": "",
                "query_text": "查詢身份證號",
            }
            token_hits = matcher.match(features)

        blocked_names = {item.get("canonical_name") for item in token_hits.get("blocked_matches", [])}
        match_names = {item.get("canonical_name") for item in token_hits.get("matches", [])}
        self.assertIn("customer.id_no", blocked_names)
        self.assertNotIn("customer.id_no", match_names)



if __name__ == "__main__":
    unittest.main()
