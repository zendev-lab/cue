"""Observable release safeguards with isolated filesystem and registry responses."""

import base64
import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import release


class ReleaseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.cwd = Path.cwd()
        os.chdir(self.root)
        self.addCleanup(os.chdir, self.cwd)
        Path("Cargo.toml").write_text(
            '[workspace]\nmembers=[]\n[workspace.package]\nversion="0.2.0"\n'
        )
        Path("package.json").write_text(
            '{"name":"@zendev-lab/cue","version":"0.1.2"}\n'
        )
        self.output = self.root / "output"
        self.env = patch.dict(
            os.environ,
            {
                "GITHUB_OUTPUT": str(self.output),
                "GITHUB_REPOSITORY": "zendev-lab/cue",
                "GITHUB_SHA": "release-sha",
            },
        )
        self.env.start()
        self.addCleanup(self.env.stop)

    def test_only_exact_merged_release_pr_can_release(self):
        pr = {
            "merged_at": "today",
            "merge_commit_sha": "release-sha",
            "base": {"ref": "main"},
            "head": {
                "repo": {"full_name": "zendev-lab/cue"},
                "ref": "release-plz-next",
            },
        }
        for change, allowed in [
            ({}, True),
            ({"merged_at": None}, False),
            ({"merge_commit_sha": "other"}, False),
            (
                {
                    "head": {
                        "repo": {"full_name": "fork/cue"},
                        "ref": "release-plz-next",
                    }
                },
                False,
            ),
            (
                {"head": {"repo": {"full_name": "zendev-lab/cue"}, "ref": "feature"}},
                False,
            ),
        ]:
            with self.subTest(change=change):
                self.output.write_text("")
                with patch.object(
                    release, "run", return_value=json.dumps([pr | change])
                ):
                    self.assertEqual(release.release_context(), allowed)

    def test_pypi_retry_keeps_unpublished_and_removes_only_identical_files(self):
        dist = Path("dist")
        dist.mkdir()
        (dist / "existing.whl").write_bytes(b"old")
        (dist / "new.whl").write_bytes(b"new")
        metadata = {
            "urls": [
                {
                    "filename": "existing.whl",
                    "digests": {"sha256": hashlib.sha256(b"old").hexdigest()},
                }
            ]
        }
        with patch.object(release, "get_json", return_value=metadata):
            release.pending_pypi(dist)
        self.assertEqual([p.name for p in dist.iterdir()], ["new.whl"])
        self.assertEqual(self.output.read_text(), "pending=true\n")

    def test_pypi_conflict_does_not_delete_staged_artifacts(self):
        dist = Path("dist")
        dist.mkdir()
        (dist / "conflict.whl").write_bytes(b"new")
        metadata = {
            "urls": [{"filename": "conflict.whl", "digests": {"sha256": "other"}}]
        }
        with (
            patch.object(release, "get_json", return_value=metadata),
            self.assertRaises(ValueError),
        ):
            release.pending_pypi(dist)
        self.assertTrue((dist / "conflict.whl").exists())
        self.assertFalse(self.output.exists())

    def test_product_tag_rejects_partial_release_and_wrong_crate_commit(self):
        with (
            patch.object(release, "release_context", return_value=True),
            patch.object(release, "workspace", return_value=("0.2.0", ["cue-core"])),
            patch.object(release, "run", return_value="release-sha") as git,
            patch.object(release, "get_json", return_value=None),
        ):
            with self.assertRaises(ValueError):
                release.tag_product()
            self.assertFalse(any("push" in call.args for call in git.call_args_list))
        with (
            patch.object(release, "release_context", return_value=True),
            patch.object(release, "workspace", return_value=("0.2.0", ["cue-core"])),
            patch.object(
                release, "run", side_effect=["release-sha", "other-sha"]
            ) as git,
            patch.object(
                release, "get_json", return_value={"version": {"yanked": False}}
            ),
        ):
            with self.assertRaises(ValueError):
                release.tag_product()
            self.assertFalse(any("push" in call.args for call in git.call_args_list))

    def test_npm_existing_version_must_match_tarball(self):
        dist = Path("dist")
        dist.mkdir()
        (dist / "cue.tgz").write_bytes(b"tarball")
        digest = (
            "sha512-" + base64.b64encode(hashlib.sha512(b"tarball").digest()).decode()
        )
        for published, expected in [
            (None, "pending=true\n"),
            ({"dist": {"integrity": digest}}, "pending=false\n"),
        ]:
            self.output.write_text("")
            with patch.object(release, "get_json", return_value=published):
                release.pending_npm(dist)
            self.assertEqual(self.output.read_text(), expected)
        with (
            patch.object(
                release, "get_json", return_value={"dist": {"integrity": "other"}}
            ),
            self.assertRaises(ValueError),
        ):
            release.pending_npm(dist)

    def test_existing_product_tag_is_idempotent_but_never_moved(self):
        for existing, valid in [("release-sha", True), ("other-sha", False)]:
            with (
                self.subTest(existing=existing),
                patch.object(release, "release_context", return_value=True),
                patch.object(release, "workspace", return_value=("0.2.0", [])),
                patch.object(
                    release,
                    "run",
                    side_effect=["release-sha", f"{existing}\trefs/tags/v0.2.0"],
                ) as git,
            ):
                if valid:
                    release.tag_product()
                else:
                    with self.assertRaises(ValueError):
                        release.tag_product()
                self.assertFalse(
                    any("push" in call.args for call in git.call_args_list)
                )


if __name__ == "__main__":
    unittest.main()
