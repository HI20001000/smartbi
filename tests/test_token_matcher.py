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


if __name__ == "__main__":
    unittest.main()
