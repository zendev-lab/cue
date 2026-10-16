#!/usr/bin/env python3
"""Render and verify the source-backed atlas, using only Python's standard library."""
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import tomllib

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'docs/codemap'


def digest(path):
    return hashlib.sha256((ROOT / path).read_bytes()).hexdigest()



def discovered_tests(paths):
    result = []
    for path in sorted(p for p in paths if p.endswith('.rs')):
        text = (ROOT / path).read_text()
        pattern = r'#\[(?:tokio::)?test(?:\([^\]]*\))?\]\s*(?:#\[[^\]]*\]\s*)*(?:async\s+)?fn\s+(\w+)'
        for match in re.finditer(pattern, text):
            result.append({'path': path, 'symbol': match[1],
                           'line': text[:match.start()].count('\n') + 1})
    return result


def validate_inventory(data):
    """Detect new/deleted crate files and tests, not just changes to locked files."""
    workspace = tomllib.loads((ROOT / 'Cargo.toml').read_text())['workspace']
    members = set(workspace['members'])
    nodes = {node['id']: node for node in data['nodes'] if node['id'].startswith('cue-')}
    assert members == {'crates/' + name for name in nodes}, 'Workspace ownership changed'
    discovered = {str(p.parent.relative_to(ROOT)) for p in (ROOT / 'crates').glob('*/Cargo.toml')}
    assert discovered == members, 'Unmapped crate manifest'
    owned = set()
    expected_edges = set()
    external = {'rusqlite': 'sqlite', 'libc': 'posix', 'tokio': 'tokio',
                'ratatui': 'terminal', 'crossterm': 'terminal', 'serde': 'codec',
                'serde_json': 'codec', 'blake3': 'codec', 'uuid': 'codec'}
    for name, node in nodes.items():
        files = {str(p.relative_to(ROOT)) for p in (ROOT / 'crates' / name).rglob('*') if p.is_file()}
        assert files == set(node['files']), 'Stale file inventory: ' + name
        assert node['tests'] == discovered_tests(files), 'Stale test inventory: ' + name
        assert not owned.intersection(files), 'Overlapping crate ownership'
        owned.update(files)
        manifest = 'crates/' + name + '/Cargo.toml'
        for dependency in tomllib.loads((ROOT / manifest).read_text()).get('dependencies', {}):
            target = dependency if dependency in nodes else external.get(dependency)
            if target:
                expected_edges.add((name, target, 'depends on ' + dependency, manifest))
    actual = {str(p.relative_to(ROOT)) for p in (ROOT / 'crates').rglob('*') if p.is_file()}
    assert actual == owned, 'Unmapped file under crates/'
    edges = [e for e in data['edges'] if e['kind'] == 'dependency']
    actual_edges = {(e['source'], e['target'], e['label'], e['evidence'][0]) for e in edges}
    assert len(edges) == len(actual_edges) and actual_edges == expected_edges, 'Stale production dependency graph'

def load():
    data = json.loads((OUT / 'codemap.json').read_text())
    ids = [node['id'] for node in data['nodes']]
    assert len(ids) == len(set(ids)) and 0 < len(ids) <= 20
    assert 3 <= len(data['flows']) <= 5
    validate_inventory(data)
    refs = set()
    for node in data['nodes']:
        assert node['role'] and node['impact'] and node['evidence']
        refs.update(node['files'] + node['evidence'])
        for test in node['tests']:
            refs.add(test['path'])
            lines = (ROOT / test['path']).read_text().splitlines()
            excerpt = '\n'.join(lines[test['line'] - 1:test['line'] + 5])
            assert re.search(r'#\[(?:tokio::)?test', excerpt), test
            assert re.search(r'fn\s+' + re.escape(test['symbol']) + r'\b', excerpt), test
    edge_ids = [edge['id'] for edge in data['edges']]
    assert len(edge_ids) == len(set(edge_ids))
    for edge in data['edges']:
        assert edge['source'] in ids and edge['target'] in ids and edge['evidence']
        assert edge['kind'] in ('dependency', 'dataflow')
        refs.update(edge['evidence'])
    assert len({flow['id'] for flow in data['flows']}) == len(data['flows'])
    for flow in data['flows']:
        assert flow['steps'] and flow['command']
        for step in flow['steps']:
            assert step['node'] in ids and step['description'] and step['evidence']
            refs.update(step['evidence'])
    refs.update(['Cargo.lock', 'Cargo.toml', 'pyproject.toml', 'package.json'])
    for path in refs:
        full = (ROOT / path).resolve()
        assert full.is_relative_to(ROOT) and full.is_file(), path
    subprocess.run(['git', 'cat-file', '-e', data['baseline'] + '^{commit}'], cwd=ROOT, check=True)
    return data, refs


def html(data):
    payload = json.dumps(data, ensure_ascii=False).replace('<', '\\u003c')
    return (OUT / 'template.html').read_text().replace('__MAP_DATA__', payload)


def main():
    mode = sys.argv[1] if len(sys.argv) == 2 else ''
    if mode not in ('build', 'check'):
        raise SystemExit('Usage: python3 scripts/codemap.py build|check')
    data, refs = load()
    required = refs | {'docs/codemap/codemap.json', 'docs/codemap/codemap.html', 'docs/codemap/template.html', 'docs/codemap/README.md', 'scripts/codemap.py', 'README.md', 'ARCHITECTURE.md', 'docs/design/README.md', 'docs/testing.md'}
    if mode == 'build':
        (OUT / 'codemap.html').write_text(html(data))
        locked = required
        lock = {'schemaVersion': 1, 'baseline': data['baseline'], 'algorithm': 'sha256', 'scope': 'Exact source/test evidence and generated atlas, not runtime acceptance.', 'files': {p: digest(p) for p in sorted(locked)}}
        (OUT / 'codemap.lock').write_text(json.dumps(lock, indent=2) + '\n')
    lock = json.loads((OUT / 'codemap.lock').read_text())
    assert lock['baseline'] == data['baseline']
    assert required == set(lock['files']), 'Evidence manifest differs'
    for path, sha in lock['files'].items():
        assert digest(path) == sha, 'Stale evidence: ' + path
    assert (OUT / 'codemap.html').read_text() == html(data), 'HTML differs from data/template'
    print(f"Verified {len(data['nodes'])} nodes, {len(data['edges'])} edges, {len(data['flows'])} workflows, {len(lock['files'])} SHA-256 files; all paths and test declarations resolve.")


if __name__ == '__main__':
    main()
