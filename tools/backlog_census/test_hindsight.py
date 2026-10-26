"""Resumption and source-review boundaries for candidate Hindsight recalls."""

import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import hindsight
from prismabuild.digest_primitives import sorted_json


CENSUS = {"candidates": [{"id": "abc123", "kind": "duplicate_cluster", "tickets": ["RobTand/prismabuild#1"]}]}
SNAPSHOT = {"issues": [{"repo": "RobTand/prismabuild", "number": 1, "title": "A ticket"}]}


class ByteIdentityTests(unittest.TestCase):
    def test_pretty_json_retains_ascii_sorting_nonfinite_values_and_one_lf(self):
        value = {"é": [float("nan"), float("inf"), -float("inf")], "a": 1}
        expected = b'{\n  "a": 1,\n  "\\u00e9": [\n    NaN,\n    Infinity,\n    -Infinity\n  ]\n}\n'
        self.assertEqual(hindsight.json_bytes(value), expected)

    def test_default_sorted_profile_and_raw_digest_keep_existing_bytes(self):
        self.assertEqual(sorted_json({"é": 2, "a": 1}), '{"a": 1, "\\u00e9": 2}')
        self.assertEqual(hindsight.raw_sha256(b"abc"),
                         "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")

    def test_pretty_json_sorts_nested_objects_and_preserves_json_values(self):
        value = {"z": [{"z": [], "a": {"z": "é\n", "a": -0.0}}],
                 "a": [None, True, False, 1.25, 1e-12, {}, []]}
        expected = (b'{\n  "a": [\n    null,\n    true,\n    false,\n'
                    b'    1.25,\n    1e-12,\n    {},\n    []\n  ],\n'
                    b'  "z": [\n    {\n      "a": {\n        "a": -0.0,\n'
                    b'        "z": "\\u00e9\\n"\n      },\n      "z": []\n'
                    b'    }\n  ]\n}\n')
        self.assertEqual(hindsight.json_bytes(value), expected)


class RecallTests(unittest.TestCase):
    def test_parent_write_rows_get_their_own_recall(self):
        census = copy.deepcopy(CENSUS)
        census["write_list"] = [{"id": "row123", "action": "native_sub_issue", "issue": "RobTand/prismabuild#1"}]
        request = Mock(return_value={"results": []})
        with tempfile.TemporaryDirectory() as directory:
            index = hindsight.collect(census, SNAPSHOT, directory, request)
        self.assertIn("row123", index)
        self.assertEqual(request.call_count, 2)

    def test_saved_recall_has_identity_query_digest_and_no_source_review(self):
        request = Mock(return_value={"results": [{"id": "memory-1", "text": "a lead"}]})
        with tempfile.TemporaryDirectory() as directory:
            result = hindsight.collect(CENSUS, SNAPSHOT, directory, request)
            receipt = Path(directory, "abc123.json")
            self.assertEqual(result["abc123"]["sha256"], hindsight.raw_sha256(receipt.read_bytes()))
            self.assertEqual(result["abc123"]["input_hash"], result["_input_hash"])
            self.assertFalse(result["abc123"]["sources_reviewed"])
            self.assertIn("RobTand/prismabuild#1: A ticket", request.call_args.args[0])
            self.assertEqual(json.loads(receipt.read_text()), request.return_value)

    def test_resume_uses_saved_recall_without_repeating_the_call(self):
        request = Mock(return_value={"results": []})
        with tempfile.TemporaryDirectory() as directory:
            first = hindsight.collect(CENSUS, SNAPSHOT, directory, request)
            second = hindsight.collect(CENSUS, SNAPSHOT, directory, request)
            self.assertEqual(first, second)
            request.assert_called_once()
            self.assertFalse(second["abc123"]["sources_reviewed"])

    def test_changed_snapshot_or_census_refuses_resume(self):
        with tempfile.TemporaryDirectory() as directory:
            hindsight.collect(CENSUS, SNAPSHOT, directory, Mock(return_value={"results": []}))
            snapshot = copy.deepcopy(SNAPSHOT)
            snapshot["issues"][0]["title"] = "Changed"
            request = Mock()
            with self.assertRaisesRegex(ValueError, "inputs differ"):
                hindsight.collect(CENSUS, snapshot, directory, request)
            request.assert_not_called()

    def test_error_payload_is_not_saved_as_a_completed_recall(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "omitted results"):
                hindsight.collect(CENSUS, SNAPSHOT, directory, Mock(return_value={"error": "unavailable"}))
            self.assertFalse(Path(directory, "abc123.json").exists())
            saved = json.loads(Path(directory, "index.json").read_text())
            self.assertIsNone(saved["abc123"]["sha256"])

    def test_partial_failure_keeps_completed_recalls_for_resume(self):
        census = copy.deepcopy(CENSUS)
        census["candidates"].append({"id": "def456", "kind": "claim_without_evidence", "tickets": ["RobTand/prismabuild#1"]})
        request = Mock(side_effect=[{"results": []}, TimeoutError("slow")])
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(TimeoutError):
                hindsight.collect(census, SNAPSHOT, directory, request)
            saved = json.loads(Path(directory, "index.json").read_text())
            self.assertIsNotNone(saved["abc123"]["sha256"])
            self.assertIsNone(saved["def456"]["sha256"])
            resumed = Mock(return_value={"results": []})
            hindsight.collect(census, SNAPSHOT, directory, resumed)
            resumed.assert_called_once()

    def test_receipt_saved_before_index_update_is_adopted_without_a_new_recall(self):
        with tempfile.TemporaryDirectory() as directory:
            hindsight.collect(CENSUS, SNAPSHOT, directory, Mock(return_value={"results": []}))
            index_path = Path(directory, "index.json")
            index = json.loads(index_path.read_text())
            index["abc123"]["sha256"] = None
            index_path.write_text(json.dumps(index))
            request = Mock()
            result = hindsight.collect(CENSUS, SNAPSHOT, directory, request)
            request.assert_not_called()
            self.assertIsNotNone(result["abc123"]["sha256"])

    def test_tampered_or_missing_receipts_refuse_resume(self):
        for remove in (False, True):
            with self.subTest(remove=remove), tempfile.TemporaryDirectory() as directory:
                hindsight.collect(CENSUS, SNAPSHOT, directory, Mock(return_value={"results": []}))
                receipt = Path(directory, "abc123.json")
                receipt.unlink()
                if not remove:
                    receipt.write_text('{"results": ["changed"]}')
                with self.assertRaises(ValueError):
                    hindsight.collect(CENSUS, SNAPSHOT, directory, Mock())

    def test_unsafe_and_duplicate_candidate_ids_are_rejected(self):
        for candidates in ([CENSUS["candidates"][0]] * 2,
                           [{"id": "../escape", "kind": "duplicate_cluster", "tickets": []}]):
            with tempfile.TemporaryDirectory() as directory:
                with self.assertRaises(ValueError):
                    hindsight.collect({"candidates": candidates}, SNAPSHOT, directory, Mock())
                self.assertEqual(list(Path(directory).iterdir()), [])

    def test_network_call_uses_fixed_bank_and_bounded_recall_without_token_in_payload(self):
        response = Mock()
        response.read.return_value = b'{"results": []}'
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=None)
        with patch.object(Path, "read_text", return_value="private-token\n"), patch.object(hindsight, "urlopen", return_value=response) as open_url:
            hindsight.recall("query")
        request = open_url.call_args.args[0]
        self.assertEqual(request.full_url, hindsight.ENDPOINT)
        self.assertEqual(open_url.call_args.kwargs["timeout"], 60)
        self.assertEqual(json.loads(request.data), {"query": "query", "types": ["world", "experience"], "budget": "mid", "max_tokens": 1500})
        self.assertNotIn(b"private-token", request.data)


if __name__ == "__main__":
    unittest.main()
