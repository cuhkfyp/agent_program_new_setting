from __future__ import annotations

import ast
import hashlib
import pathlib
import py_compile
import re
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
AGENT = ROOT / "agent" / "agent_program.py"
API = ROOT / "server" / "api_agent_sync.py"
SETUP = ROOT / "server" / "agent_sync_setup.py"
DEPLOYMENT = ROOT / "deployment" / "install_runtime.sh"
WINDOWS_SETUP = ROOT / "agent" / "setup_windows.bat"
WINDOWS_SETUP_NO_POWERSHELL = (
    ROOT / "agent" / "setup_windows_no_powershell.bat"
)
CONFIGURE_AGENT = ROOT / "agent" / "configure_agent.py"
REQUIREMENTS = ROOT / "agent" / "requirements.txt"


class StaticContracts(unittest.TestCase):
    def _generated_daemon_source(self) -> str:
        tree = ast.parse(AGENT.read_text(encoding="utf-8"))
        builder = next(
            node
            for node in tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == "build_job_daemon_script"
        )
        namespace: dict[str, object] = {}
        exec(
            compile(
                ast.Module(body=[builder], type_ignores=[]),
                str(AGENT),
                "exec",
            ),
            namespace,
        )
        return namespace["build_job_daemon_script"](
            job_name="JOB01",
            task_name="contract-test",
            actions=["SYNC_TO_CCD_MASTER_BULK"],
            interval_seconds=60,
            log_file="agent-test.log",
            registration_id="REG-2",
            source_id="HOST-DB",
            physical_hostname="HOST",
        )

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

    def test_generated_daemon_source_compiles(self) -> None:
        script = self._generated_daemon_source()
        compile(script, "<generated-daemon>", "exec")

    def test_blank_temporal_values_are_database_nulls(self) -> None:
        script = self._generated_daemon_source()
        tree = ast.parse(script)
        normalizer = next(
            node
            for node in tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == "normalize_mapped_value"
        )
        namespace: dict[str, object] = {}
        exec(
            compile(
                ast.Module(body=[normalizer], type_ignores=[]),
                "<generated-normalizer>",
                "exec",
            ),
            namespace,
        )
        normalize = namespace["normalize_mapped_value"]

        for fieldtype in ("Date", "Datetime", " date ", "DATETIME"):
            for value in (None, "", "   ", "\t"):
                self.assertIsNone(normalize(fieldtype, value))

        self.assertEqual(normalize("Date", "2026-09-17"), "2026-09-17")
        self.assertEqual(
            normalize("Datetime", "2026-09-17 12:34:56"),
            "2026-09-17 12:34:56",
        )
        self.assertEqual(normalize("Data", None), "")
        self.assertEqual(normalize("Data", "   "), "   ")
        self.assertEqual(
            script.count("return normalize_mapped_value("),
            4,
            "all registration and Master mapping paths must share normalization",
        )

    def test_master_sync_never_invokes_automatic_clear_or_delete(self) -> None:
        source = AGENT.read_text(encoding="utf-8")
        start = source.index("# ---- SYNC_TO_CCD_MASTER_BULK macro ----")
        end = source.index("# ---- SQL statements ----", start)
        master_sync = source[start:end]
        self.assertNotIn('"action": "clear"', master_sync)
        self.assertNotIn('"action": "delete_by_source_keys"', master_sync)
        self.assertNotIn('sess, "DELETE"', master_sync)
        self.assertIn("inspect_master_source_state", master_sync)
        self.assertIn("Reconciliation Required", master_sync)

    def test_retirement_service_is_not_coupled_to_sync_api(self) -> None:
        source = API.read_text(encoding="utf-8")
        self.assertNotIn("api_identity_retirement", source)
        self.assertIn("An active submitted CCD Registration is required", source)
        self.assertIn("inspect_master_source_state", source)
        self.assertIn("report_reconciliation_required", source)

    def test_master_cache_is_scoped_to_registration_revision(self) -> None:
        source = AGENT.read_text(encoding="utf-8")
        self.assertIn('"version": 2', source)
        self.assertIn('"registration_id": registration_id', source)
        self.assertIn('"sync_generation": _hashlib.sha256', source)
        self.assertIn("_cache_scope_mismatch", source)

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

    def test_both_windows_installers_target_the_same_agent_contract(self) -> None:
        normal = WINDOWS_SETUP.read_text(encoding="utf-8")
        legacy = WINDOWS_SETUP_NO_POWERSHELL.read_text(encoding="utf-8")
        required_fragments = (
            "agent_program.py",
            "configure_agent.py",
            "requirements.txt",
            "Run_Agent.bat",
            "ccd_agent",
            "erpnext_url",
            "erpnext_user",
            "erpnext_pass",
            "CCD_AGENT_ARTIFACT_BASE_URL",
        )
        for fragment in required_fragments:
            self.assertIn(fragment, normal)
            self.assertIn(fragment, legacy)
        self.assertIn("powershell -nologo", normal.lower())
        self.assertNotIn("powershell", legacy.lower())
        self.assertNotIn("hksrfam", normal.lower())
        self.assertNotIn("hksrfam", legacy.lower())

    def test_windows_installer_checksums_match_packaged_assets(self) -> None:
        expected_assets = {
            "AGENT_SHA256": AGENT,
            "CONFIGURE_SHA256": CONFIGURE_AGENT,
            "REQUIREMENTS_SHA256": REQUIREMENTS,
        }
        for installer in (WINDOWS_SETUP, WINDOWS_SETUP_NO_POWERSHELL):
            source = installer.read_text(encoding="utf-8")
            for variable, asset in expected_assets.items():
                match = re.search(
                    rf'^set "{variable}=([0-9a-f]{{64}})"$',
                    source,
                    flags=re.MULTILINE,
                )
                self.assertIsNotNone(match, f"{variable} missing in {installer.name}")
                actual = hashlib.sha256(asset.read_bytes()).hexdigest()
                self.assertEqual(match.group(1), actual)

    def test_windows_installers_preserve_sync_state(self) -> None:
        for installer in (WINDOWS_SETUP, WINDOWS_SETUP_NO_POWERSHELL):
            source = installer.read_text(encoding="utf-8").lower()
            self.assertNotIn('rmdir /s /q "daemon_logs"', source)
            self.assertNotIn("del *delta_cache", source)
            self.assertIn("existing logs and delta caches", source)

    def test_deployment_installs_namespaced_agent_templates(self) -> None:
        deployment = DEPLOYMENT.read_text(encoding="utf-8")
        setup = SETUP.read_text(encoding="utf-8")
        for filename in ("setup_windows.bat", "setup_windows_no_powershell.bat"):
            self.assertIn(filename, deployment)
            self.assertIn(filename, setup)
        self.assertIn('"Windows - Central Sync"', setup)
        self.assertIn('"Windows - Central Sync (No PowerShell)"', setup)
        self.assertNotIn('"Windows 11":', setup)


if __name__ == "__main__":
    unittest.main()
