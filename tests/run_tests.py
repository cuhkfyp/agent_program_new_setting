from __future__ import annotations

import ast
import pathlib
import py_compile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
AGENT = ROOT / "agent" / "agent_program.py"
API = ROOT / "server" / "api_agent_sync.py"
SETUP = ROOT / "server" / "agent_sync_setup.py"


class StaticContracts(unittest.TestCase):
    def test_python_sources_compile(self) -> None:
        for path in (AGENT, API, SETUP):
            py_compile.compile(str(path), doraise=True)

    def test_source_identity_is_not_suffix_trimmed(self) -> None:
        source = AGENT.read_text(encoding="utf-8")
        self.assertNotIn("def trim_suffix", source)
        self.assertIn("doc_data.get('ccd_stable_source_key')", source)

    def test_zero_delta_precedes_capacity(self) -> None:
        source = AGENT.read_text(encoding="utf-8")
        delta_position = source.index("if to_insert or to_delete or to_update:")
        acquire_position = source.index(
            "acquire_master_mutation_slot()", delta_position
        )
        zero_position = source.index("zero delta", delta_position)
        self.assertLess(acquire_position, zero_position)

    def test_fast_api_is_opt_in_and_namespaced(self) -> None:
        source = API.read_text(encoding="utf-8")
        self.assertIn('FAST_MODE = "Fast Bulk Insert"', source)
        self.assertIn('"agent_sync_run_id"', source)
        self.assertIn("_require_lease", source)

    def test_no_hardcoded_erp_endpoint_or_password(self) -> None:
        tree = ast.parse(AGENT.read_text(encoding="utf-8"))
        text_values = {
            node.value
            for node in ast.walk(tree)
            if isinstance(node, ast.Constant) and isinstance(node.value, str)
        }
        self.assertFalse(
            any(value.startswith("https://") for value in text_values),
            "agent source must not embed a deployment URL",
        )
        unsafe_test_credential = "password=" + '"root"'
        self.assertNotIn(unsafe_test_credential, AGENT.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
