import hashlib
import pathlib
import re
import subprocess
import tomllib

import lyrelog
from lyrelog._transport import SDK_VERSION

ROOT = pathlib.Path(__file__).resolve().parent.parent
PROJECT = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]


def test_the_package_version_is_the_sdk_version():
    assert PROJECT["version"] == SDK_VERSION == lyrelog.__version__


def test_the_metadata_is_complete():
    assert PROJECT["description"] and "Add your description" not in PROJECT["description"]
    assert PROJECT["readme"] == "README.md"
    assert PROJECT["license"] == "MIT" and PROJECT["license-files"] == ["LICENSE"]
    classifiers = PROJECT["classifiers"]
    assert "Typing :: Typed" in classifiers
    for minor in range(11, 15):
        assert f"Programming Language :: Python :: 3.{minor}" in classifiers
    urls = PROJECT["urls"]
    assert urls["Repository"] == "https://github.com/lyrelog/lyrelog-python"
    assert urls["Issues"] == "https://github.com/lyrelog/lyrelog-python/issues"
    assert urls["Homepage"] == "https://lyrelog.com"
    assert urls["Documentation"] == "https://docs.lyrelog.com"
    assert PROJECT["authors"] == [{"name": "Lyrelog", "email": "hello@lyrelog.com"}]
    assert "Development Status :: 5 - Production/Stable" in classifiers
    assert PROJECT["keywords"]
    assert (ROOT / "src" / "lyrelog" / "py.typed").exists()


def test_the_readme_installs_this_version():
    readme = (ROOT / "README.md").read_text()
    assert f"lyrelog-python/{SDK_VERSION}" in readme
    assert f"lyrelog=={SDK_VERSION}" in readme


# SHA-256 of words that must never be in a public file (a real person, his apps, his account),
# so this test doesn't spell them either.
PRIVATE_WORDS = {
    "4859d1729b697a9ccc9bf17b6a3c676bc935834e5640d203a7cfa489bd45d619",
    "976c617a20d12e4261e40bd8e4ce88493578ec463d082af3b2784b642b82ef0d",
    "b9e5b115929357704655d70d95b79daf630c3cea6e084adec79b4bcb7e0cd7b1",
    "34eb8131c759d8ea0bdf432f943e6e78d8bd2e1d9ad50f2bf166fad074edcea8",
    "7e421181fc7b52ab66f2c22bd396acce92c3cdd9a29122dde0f1b7ddc0eb4435",
}


def _public_files() -> list[pathlib.Path]:
    try:
        tracked = subprocess.run(["git", "ls-files"], cwd=ROOT, capture_output=True, text=True,
                                 check=True).stdout.split()
        files = [ROOT / name for name in tracked]
    except (OSError, subprocess.CalledProcessError):  # an sdist without git
        files = [ROOT / "README.md", ROOT / "LICENSE", ROOT / "pyproject.toml"]
        files += sorted((ROOT / "src").rglob("*.py")) + sorted((ROOT / "tests").rglob("*.py"))
    return [f for f in files if f.suffix in {"", ".md", ".toml", ".py", ".txt", ".cfg"} and f.is_file()
            and f.name != "uv.lock"]


def test_no_public_file_names_a_real_person():
    found = []
    for path in _public_files():
        text = path.read_text(errors="replace").lower()
        # Whole tokens (an account name with digits) and their letter runs.
        words = set(re.findall(r"[a-z0-9]+", text)) | set(re.findall(r"[a-z]+", text))
        for word in words:
            if hashlib.sha256(word.encode()).hexdigest() in PRIVATE_WORDS:
                found.append(path.relative_to(ROOT).as_posix())
    assert found == [], found
