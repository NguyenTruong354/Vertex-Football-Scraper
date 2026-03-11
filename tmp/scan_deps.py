import os, re, sys
from pathlib import Path

# Common stdlib modules to exclude
stdlib = {
    'os', 'sys', 'time', 'datetime', 'json', 're', 'logging', 'argparse', 'subprocess',
    'pathlib', 'typing', 'asyncio', 'signal', 'urllib', 'http', 'abc', 'collections',
    'enum', 'functools', 'hashlib', 'inspect', 'itertools', 'math', 'random', 'shutil',
    'tempfile', 'threading', 'traceback', 'uuid', 'warnings', 'base64', 'csv', 'copy',
    'glob', 'socket', 'contextlib', 'argparse', 'bisect', 'calendar', 'codecs', 'copy'
}

# Project packages to exclude
project_packages = {'services', 'db', 'fbref', 'understat', 'sofascore', 'transfermarkt', 'news_radar'}

found_imports = set()
root = Path(r'd:\Vertex_Football_Scraper2')

for py_file in root.rglob('*.py'):
    if '.venv' in str(py_file): continue
    try:
        content = py_file.read_text(encoding='utf-8')
        # simple match for 'import foo' or 'from foo import bar'
        matches = re.findall(r'^(?:from|import)\s+([a-zA-Z0-9_\-]+)\b', content, re.MULTILINE)
        for m in matches:
            if m not in stdlib and m not in project_packages:
                found_imports.add(m)
    except Exception:
        continue

# Check specifically for nodriver (often in strings/inline scripts)
if "nodriver" in " ".join([f.read_text(errors='ignore') for f in root.glob('*.py')]):
    found_imports.add("nodriver")

# Mapping
mapping = {
    'bs4': 'beautifulsoup4',
    'dotenv': 'python-dotenv',
    'google': 'google-genai',
    'psycopg2': 'psycopg2-binary'
}

final_list = sorted({mapping.get(i, i) for i in found_imports if i})
print("\n[DETECTED PACKAGES]")
for pkg in final_list:
    if pkg and pkg.lower() != 'none':
        print(pkg)
