import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
PLUGIN_DIR = HERE.parent / "a2a_hub_origin"


@pytest.fixture
def hermes_src():
    src = os.environ.get("HERMES_SRC")
    if not src or not (Path(src) / "hermes_cli" / "plugins.py").is_file():
        pytest.skip("HERMES_SRC must name a Hermes Agent source tree")
    return Path(src)


@pytest.fixture
def hermes_home(tmp_path):
    home = tmp_path / "hermes-home"
    (home / "plugins").mkdir(parents=True)
    (tmp_path / "home").mkdir()
    (tmp_path / "managed").mkdir()
    shutil.copytree(PLUGIN_DIR, home / "plugins" / "a2a_hub_origin",
                    ignore=shutil.ignore_patterns("__pycache__"))
    return home


@pytest.fixture
def run_hermes(hermes_src, hermes_home, tmp_path):
    """Run a script of this directory in a fresh Python process on the Hermes source tree."""

    def run(script, *args, env=None):
        child_env = {
            "PATH": os.environ.get("PATH", ""),
            "HOME": str(tmp_path / "home"),
            "HERMES_HOME": str(hermes_home),
            "HERMES_MANAGED_DIR": str(tmp_path / "managed"),
            "PYTHONPATH": str(hermes_src),
            "PYTHONDONTWRITEBYTECODE": "1",
            "TMPDIR": str(tmp_path),
            **(env or {}),
        }
        proc = subprocess.run([sys.executable, str(HERE / script), *args], env=child_env,
                              capture_output=True, text=True, timeout=180, cwd=tmp_path)
        assert proc.returncode == 0, proc.stderr[-4000:]
        return json.loads(proc.stdout.strip().splitlines()[-1])

    return run
