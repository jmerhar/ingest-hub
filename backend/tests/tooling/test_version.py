"""bin/version.py and bin/release.sh: one version in every manifest, released from the changelog."""

from __future__ import annotations

import importlib.util
import shutil
import subprocess
from datetime import date
from pathlib import Path

import pytest

BIN = Path(__file__).parents[3] / "bin"
_spec = importlib.util.spec_from_file_location("version", BIN / "version.py")
assert _spec is not None and _spec.loader is not None
version = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(version)

CHANGELOG = """# Changelog

Intro.

## Unreleased

### Added

- Something new.

## 1.2.3 — 2026-10-01

### Fixed

- Something old.
"""


COMPOSE = """services:
  ingest-hub:
    image: ghcr.io/jmerhar/ingest-hub:{major}
    container_name: ingest-hub
"""


@pytest.fixture
def root(tmp_path: Path) -> Path:
    """A checkout's manifests and changelog, all at 1.2.3."""
    for path in version.PYPROJECTS:
        (tmp_path / path).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / path).write_text('[project]\nname = "x"\nversion = "1.2.3"\n')
    (tmp_path / "CHANGELOG.md").write_text(CHANGELOG)
    (tmp_path / "docker-compose.yml").write_text(COMPOSE.format(major=1))
    return tmp_path


def run(root: Path, *args: str) -> tuple[int, str, str]:
    out = subprocess.run(
        [str(BIN / "version.py"), "--root", str(root), *args],
        capture_output=True, text=True, check=False, timeout=30,
    )  # fmt: skip
    return out.returncode, out.stdout, out.stderr


def test_agreeing_manifests_pass_and_a_tag_must_name_their_released_version(root: Path) -> None:
    assert run(root, "check") == (0, "every manifest names 1.2.3\n", "")
    assert run(root, "check", "v1.2.3")[0] == 0
    code, _, err = run(root, "check", "v1.2.4")
    assert (code, err) == (1, "the tag is v1.2.4, but the manifests name 1.2.3\n")


def test_a_version_out_of_step_with_the_compose_pin_is_named(root: Path) -> None:
    pyproject = root / "backend/pyproject.toml"
    pyproject.write_text(pyproject.read_text().replace("1.2.3", "2.0.0"))

    code, _, err = run(root, "check")

    assert code == 1
    assert err == "docker-compose.yml pins ghcr.io/jmerhar/ingest-hub:1, not 2 as 2.0.0 is\n"


def test_the_compose_file_must_follow_the_major_version(root: Path) -> None:
    compose = root / "docker-compose.yml"
    compose.write_text(COMPOSE.format(major="latest"))
    assert run(root, "check")[2] == (
        "docker-compose.yml pins ghcr.io/jmerhar/ingest-hub:latest, not 1 as 1.2.3 is\n"
    )
    compose.write_text("services: {}\n")
    assert run(root, "check")[2] == "docker-compose.yml names no ingest-hub image\n"
    # A line naming the image that cannot be read for its tag is refused, never skipped.
    compose.write_text(COMPOSE.format(major=1).replace("ingest-hub:1", "ingest-hub@sha256:ab"))
    assert run(root, "check")[2] == "docker-compose.yml line 3 names an image without a plain tag\n"


@pytest.mark.parametrize(
    ("release", "followed"), [("0.1.0", "0.1"), ("0.12.3", "0.12"), ("1.0.0", "1"), ("2.5.1", "2")]
)
def test_before_1_0_the_compose_file_follows_the_minor_version(release: str, followed: str) -> None:
    assert version.line(release) == followed


def test_a_pin_with_a_comment_after_it_is_read_and_moved(root: Path) -> None:
    compose = root / "docker-compose.yml"
    compose.write_text(COMPOSE.format(major=1).replace("ingest-hub:1", 'ingest-hub:1"  # pinned'))
    (root / "CHANGELOG.md").write_text(CHANGELOG.replace("- Something new.", "- Breaking."))

    version.set_version(root, "2.0.0", date(2026, 10, 14))

    assert compose.read_text() == COMPOSE.format(major=2).replace(
        "ingest-hub:2", 'ingest-hub:2"  # pinned'
    )


def test_a_major_release_moves_the_compose_file_and_a_minor_one_keeps_it(root: Path) -> None:
    version.set_version(root, "1.3.0", date(2026, 10, 14))
    assert (root / "docker-compose.yml").read_text() == COMPOSE.format(major=1)
    (root / "CHANGELOG.md").write_text(
        (root / "CHANGELOG.md")
        .read_text()
        .replace("## 1.3.0", "## Unreleased\n\n- Breaking.\n\n## 1.3.0")
    )
    version.set_version(root, "2.0.0", date(2026, 10, 15))
    assert (root / "docker-compose.yml").read_text() == COMPOSE.format(major=2)
    assert run(root, "check", "v2.0.0")[0] == 0


def test_a_manifest_without_a_version_is_refused(root: Path) -> None:
    (root / "backend/pyproject.toml").write_text('[project]\nname = "x"\n')

    assert run(root, "check")[2] == (
        "backend/pyproject.toml names no version where one is expected\n"
    )


def test_setting_a_version_writes_every_manifest_and_dates_the_changelog(root: Path) -> None:
    version.set_version(root, "1.3.0", date(2026, 10, 14))

    assert set(version.versions(root).values()) == {"1.3.0"}
    text = (root / "CHANGELOG.md").read_text()
    assert "## Unreleased" not in text
    assert "## 1.3.0 — 2026-10-14\n\n### Added\n\n- Something new.\n\n## 1.2.3" in text
    assert run(root, "notes", "1.3.0") == (0, "### Added\n\n- Something new.\n", "")
    assert run(root, "check", "v1.3.0")[0] == 0


@pytest.mark.parametrize(
    ("change", "wanted", "problem"),
    [
        (None, "1.3", "1.3 is not a version like 1.2.3"),
        (("## Unreleased\n", "## Later\n"), "1.3.0", "has no '## Unreleased' section"),
        (("- Something new.\n", ""), "1.3.0", "'## Unreleased' section is empty"),
        (
            ("## Unreleased\n", "## 1.3.0\n\n## Unreleased\n"),
            "1.3.0",
            "already has a section for 1.3.0",
        ),
        (None, "1.2.3", "1.2.3 is not after 1.2.3, the current version"),
        (None, "0.9.0", "0.9.0 is not after 1.2.3, the current version"),
        (
            ("## Unreleased\n", "## Unreleased\n\n- More.\n\n## Unreleased\n"),
            "1.3.0",
            "has more than one '## Unreleased' section",
        ),
    ],
)
def test_a_release_that_cannot_be_cut_changes_nothing(
    root: Path, change: tuple[str, str] | None, wanted: str, problem: str
) -> None:
    changelog = root / "CHANGELOG.md"
    if change:
        changelog.write_text(CHANGELOG.replace(*change))
    before = changelog.read_text()

    code, _, err = run(root, "set", wanted)

    assert code == 1 and problem in err
    assert changelog.read_text() == before
    assert set(version.versions(root).values()) == {"1.2.3"}


def test_a_mention_of_the_heading_in_prose_is_left_alone(root: Path) -> None:
    changelog = root / "CHANGELOG.md"
    changelog.write_text(
        CHANGELOG.replace("Intro.", "Write notes under `## Unreleased` until a release.")
    )

    version.set_version(root, "1.3.0", date(2026, 10, 14))

    text = changelog.read_text()
    assert "Write notes under `## Unreleased` until a release." in text
    assert "## 1.3.0 — 2026-10-14\n\n### Added" in text


@pytest.mark.parametrize("broken", ["missing", "versionless"])
def test_a_manifest_that_cannot_be_read_stops_the_release_before_anything_is_written(
    root: Path, broken: str
) -> None:
    pyproject = root / "backend/pyproject.toml"
    if broken == "missing":
        pyproject.unlink()
    else:
        pyproject.write_text('[project]\nname = "x"\n')
    before = {p: p.read_text() for p in root.rglob("*") if p.is_file()}

    code, _, err = run(root, "set", "1.3.0")

    assert code == 1 and "Traceback" not in err
    assert err in (
        "backend/pyproject.toml is missing\n",
        "backend/pyproject.toml names no version where one is expected\n",
    )
    assert {p: p.read_text() for p in root.rglob("*") if p.is_file()} == before


def test_notes_need_a_dated_section(root: Path) -> None:
    assert run(root, "notes", "9.9.9")[2] == "CHANGELOG.md has no dated section for 9.9.9\n"
    code, _, err = run(root, "check", "v1.2.3")
    assert code == 0, err
    (root / "CHANGELOG.md").write_text(CHANGELOG.replace("## 1.2.3 — 2026-10-01", "## 1.2.3"))
    assert run(root, "check", "v1.2.3")[0] == 1


def git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args], capture_output=True, text=True, check=True, timeout=30
    ).stdout


@pytest.fixture
def repo(root: Path) -> Path:
    """`root` as a git repository on main, with the release scripts, signing off."""
    (root / "bin").mkdir()
    for script in ("release.sh", "version.py"):
        shutil.copy2(BIN / script, root / "bin" / script)
    git(root, "init", "--quiet", "--initial-branch=main")
    for key, value in (("user.name", "T"), ("user.email", "t@example.test"),
                       ("commit.gpgsign", "false"), ("tag.gpgsign", "false"),
                       ("user.signingkey", "")):  # fmt: skip
        git(root, "config", key, value)
    git(root, "add", ".")
    git(root, "commit", "--quiet", "--message", "start")
    return root


def release(repo: Path, wanted: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(repo / "bin/release.sh"), wanted], capture_output=True, text=True, check=False,
        timeout=60,
    )  # fmt: skip


def test_a_release_is_committed_and_tagged_with_its_notes(repo: Path) -> None:
    result = release(repo, "1.3.0")

    assert result.returncode == 0, result.stderr
    assert "git push origin main v1.3.0" in result.stdout
    assert git(repo, "log", "-1", "--format=%s") == "chore(release): 1.3.0\n"
    # The headings survive: git strips lines starting with # from a message unless told not to.
    assert git(repo, "tag", "-l", "--format=%(contents)", "v1.3.0").rstrip() == (
        "ingest-hub 1.3.0\n\n### Added\n\n- Something new."
    )
    assert git(repo, "status", "--porcelain") == ""


def test_a_release_is_signed_when_git_has_a_signing_key(
    repo: Path, tmp_path_factory: pytest.TempPathFactory
) -> None:
    key = tmp_path_factory.mktemp("key") / "key"
    made = subprocess.run(
        ["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(key)],
        capture_output=True, check=False, timeout=30,
    )  # fmt: skip
    if made.returncode:
        pytest.skip("ssh-keygen is not available")
    git(repo, "config", "gpg.format", "ssh")
    git(repo, "config", "user.signingkey", f"{key}.pub")

    assert release(repo, "1.3.0").returncode == 0
    assert "-----BEGIN SSH SIGNATURE-----" in git(repo, "cat-file", "tag", "v1.3.0")


def test_a_release_that_cannot_be_cut_leaves_no_commit_or_tag(repo: Path) -> None:
    (repo / "CHANGELOG.md").write_text(CHANGELOG.replace("- Something new.\n", ""))
    git(repo, "commit", "--quiet", "--all", "--message", "empty notes")

    result = release(repo, "1.3.0")

    assert result.returncode == 1 and "section is empty" in result.stderr
    assert git(repo, "log", "-1", "--format=%s") == "empty notes\n"
    assert (git(repo, "tag"), git(repo, "status", "--porcelain")) == ("", "")


def test_a_release_needs_a_clean_main(repo: Path) -> None:
    (repo / "stray.txt").write_text("x")
    assert "working tree has changes" in release(repo, "1.3.0").stderr
    (repo / "stray.txt").unlink()
    git(repo, "switch", "--quiet", "--create", "other")
    assert "cut from main" in release(repo, "1.3.0").stderr
    assert git(repo, "tag") == ""


@pytest.fixture
def gh(repo: Path, tmp_path_factory: pytest.TempPathFactory) -> Path:
    """A stand-in gh on PATH: it records its calls, and has a release for the tags in `exists`."""
    shutil.copy2(BIN / "github-release.sh", repo / "bin" / "github-release.sh")
    git(repo, "add", ".")
    git(repo, "commit", "--quiet", "--message", "the release script")
    fake = tmp_path_factory.mktemp("fake-bin")
    (fake / "gh").write_text(
        "#!/usr/bin/env bash\n"
        'echo "$*" >> "$GH_LOG"\n'
        'if [ "$1 $2" = "release view" ]; then grep -qx "$3" "$GH_EXISTS" 2>/dev/null; exit; fi\n'
        'if [ "$1 $2" = "release create" ]; then cat >> "$GH_LOG"; fi\n'
    )
    (fake / "gh").chmod(0o755)
    return fake


def github_release(repo: Path, fake: Path, tag: str, exists: str = "") -> tuple[int, str]:
    log, existing = fake / "log", fake / "exists"
    existing.write_text(exists)
    env = {**__import__("os").environ, "PATH": f"{fake}:/usr/bin:/bin", "GH_LOG": str(log),
           "GH_EXISTS": str(existing)}  # fmt: skip
    code = subprocess.run(
        [str(repo / "bin/github-release.sh"), tag], env=env, capture_output=True, text=True,
        check=False, timeout=60,
    ).returncode  # fmt: skip
    return code, log.read_text() if log.exists() else ""


def test_the_github_release_carries_the_notes_and_is_made_once(repo: Path, gh: Path) -> None:
    assert release(repo, "1.3.0").returncode == 0

    code, calls = github_release(repo, gh, "v1.3.0")
    assert code == 0
    assert "release create v1.3.0 --verify-tag --title ingest-hub 1.3.0 --notes-file -" in calls
    assert "### Added\n\n- Something new." in calls
    # A re-run of the workflow finds the release and leaves it.
    (gh / "log").unlink()
    code, calls = github_release(repo, gh, "v1.3.0", exists="v1.3.0\n")
    assert code == 0 and "release create" not in calls


def test_no_github_release_for_a_tag_the_manifests_do_not_name(repo: Path, gh: Path) -> None:
    code, calls = github_release(repo, gh, "v9.9.9")

    assert code == 1 and calls == ""
