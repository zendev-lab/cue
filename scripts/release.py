#!/usr/bin/env python3
"""Cue-specific glue around release-plz and the product release tag."""

import argparse
import base64
import hashlib
import json
import os
import subprocess
import tomllib
import urllib.error
import urllib.request
from pathlib import Path


def run(*args):
    return subprocess.check_output(args, text=True).strip()


def workspace(root=Path(".")):
    manifest = tomllib.loads((root / "Cargo.toml").read_text())
    version = manifest["workspace"]["package"]["version"]
    names = [
        tomllib.loads((root / member / "Cargo.toml").read_text())["package"]["name"]
        for member in manifest["workspace"]["members"]
    ]
    return version, names


def get_json(url):
    request = urllib.request.Request(
        url, headers={"User-Agent": "cue-release (zendev-lab/cue)"}
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return None
        raise


def release_context():
    repo, sha = os.environ["GITHUB_REPOSITORY"], os.environ["GITHUB_SHA"]
    prs = json.loads(run("gh", "api", f"repos/{repo}/commits/{sha}/pulls"))
    eligible = any(
        pr["merged_at"]
        and pr["merge_commit_sha"] == sha
        and pr["base"]["ref"] == "main"
        and pr["head"]["repo"]
        and pr["head"]["repo"]["full_name"] == repo
        and pr["head"]["ref"].startswith("release-plz-")
        for pr in prs
    )
    return eligible


def tag_product():
    if not release_context():
        return
    version, names = workspace()
    sha = os.environ["GITHUB_SHA"]
    if run("git", "rev-parse", "HEAD") != sha:
        raise ValueError("checkout differs from the release commit")
    for name in names:
        published = get_json(f"https://crates.io/api/v1/crates/{name}/{version}")
        if not published or published["version"]["yanked"]:
            raise ValueError(f"{name}@{version} is not available on crates.io")
        crate_sha = run("git", "rev-parse", f"refs/tags/{name}-v{version}^{{commit}}")
        if crate_sha != sha:
            raise ValueError(f"{name} tag does not identify the release commit")
    tag = f"v{version}"
    remote = run(
        "git", "ls-remote", "origin", f"refs/tags/{tag}", f"refs/tags/{tag}^{{}}"
    )
    if remote:
        refs = dict(line.split()[::-1] for line in remote.splitlines())
        existing = refs.get(f"refs/tags/{tag}^{{}}", refs.get(f"refs/tags/{tag}"))
        if existing != sha:
            raise ValueError(f"{tag} already exists on a different commit")
        print(f"{tag} already points to {sha}")
        return
    run("git", "tag", tag, sha)
    run("git", "push", "origin", f"refs/tags/{tag}")


def pending_pypi(directory):
    """Remove only byte-identical uploaded files from this run's staging dir."""
    version, _ = workspace()
    published = get_json(f"https://pypi.org/pypi/cue-run/{version}/json")
    files = {item["filename"]: item for item in published["urls"]} if published else {}
    paths = list(directory.glob("*.whl")) + list(directory.glob("*.tar.gz"))
    if not paths:
        raise ValueError("no Python artifacts to publish")
    identical = []
    for path in paths:
        if path.name in files:
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            if digest != files[path.name]["digests"]["sha256"]:
                raise ValueError(f"PyPI already has different bytes for {path.name}")
            identical.append(path)
    for path in identical:
        path.unlink()
    with open(os.environ["GITHUB_OUTPUT"], "a") as output:
        output.write(f"pending={str(len(paths) > len(identical)).lower()}\n")


def pending_npm(directory):
    version, _ = workspace()
    paths = list(directory.glob("*.tgz"))
    if len(paths) != 1:
        raise ValueError("expected exactly one npm tarball")
    published = get_json(f"https://registry.npmjs.org/@zendev-lab%2fcue/{version}")
    if published:
        integrity = (
            "sha512-"
            + base64.b64encode(hashlib.sha512(paths[0].read_bytes()).digest()).decode()
        )
        if published["dist"].get("integrity") != integrity:
            raise ValueError("npm already has different bytes for this version")
    with open(os.environ["GITHUB_OUTPUT"], "a") as output:
        output.write(f"pending={str(published is None).lower()}\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("tag-product")
    pending = commands.add_parser("pending-pypi")
    pending.add_argument("directory", type=Path)
    npm = commands.add_parser("pending-npm")
    npm.add_argument("directory", type=Path)
    args = parser.parse_args()
    if args.command == "tag-product":
        tag_product()
    elif args.command == "pending-npm":
        pending_npm(args.directory)
    elif args.command == "pending-pypi":
        pending_pypi(args.directory)


if __name__ == "__main__":
    main()
