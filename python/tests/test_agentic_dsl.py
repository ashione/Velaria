import unittest

from velaria.agentic_dsl import compile_rule_spec, compile_template_rule, parse_rule_spec


class AgenticDslTest(unittest.TestCase):
    def test_compile_template_rule_window_count(self):
        spec = compile_template_rule(
            template_id="window_count",
            template_params={"group_by": ["source_key", "event_type"], "count_threshold": 5},
            source={"kind": "external_event", "binding": "ticks"},
            execution_mode="stream",
            name="window-count",
        )
        parsed = parse_rule_spec(spec)
        self.assertEqual(parsed["execution"]["mode"], "stream")
        compiled = compile_rule_spec(spec)
        self.assertEqual(compiled["execution_mode"], "stream")
        self.assertIn("COUNT(*) AS cnt", compiled["compiled_rules"][0]["sql"])

    def test_parse_rule_spec_requires_grounded_shape(self):
        with self.assertRaises(ValueError):
            parse_rule_spec({"version": "v1", "name": "bad"})
