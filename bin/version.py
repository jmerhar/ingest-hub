#!/usr/bin/env python3
"""The release version, which every project's manifest and CHANGELOG.md carry.

    version.py check [TAG]   every manifest names the same version, and docker-compose.yml pins
                             its image to that version's line (see `line`); with a tag (v1.2.3),
                             that the manifests name that version and CHANGELOG.md has its section
    version.py set X.Y.Z     write X.Y.Z, which must follow the current version, into every
                             manifest and its line into docker-compose.yml, and date
                             CHANGELOG.md's Unreleased section as X.Y.Z's; a release that cannot be
                             cut changes nothing
    version.py notes X.Y.Z   print X.Y.Z's section of CHANGELOG.md, without its heading

`--root DIR` reads and writes a checkout other than this one, as the tests do.
"""

from __future__ import annotations

import argparse
import re
import sys
from datetime import date
from pathlib import Path

PYPROJECTS = ("backend/pyproject.toml",)
CHANGELOG = "CHANGELOG.md"
#: The shipped compose file follows a release line, so `docker compose pull` brings every release
#: that keeps the image's settings and stored data working, and no other.
COMPOSE = "docker-compose.yml"
#: An image line naming one of the project's images, its tag, and anything after the tag.
COMPOSE_IMAGE = re.compile(
    r"""^(\s*image:\s*["']?ghcr\.io/jmerhar/ingest-hub:)([0-9A-Za-z._-]+)(.*)$"""
)
OUR_IMAGE = re.compile(r"^\s*image:.*ghcr\.io/jmerhar/ingest-hub")
SEMVER = re.compile(r"\d+\.\d+\.\d+")
UNRELEASED = "## Unreleased"
UNRELEASED_HEADING = re.compile(r"(?m)^## Unreleased$")

#: Each manifest's own version: a pyproject's `version`.
PATTERNS = {p: [re.compile(r'(?m)^(version = ")([^"]*)(")$')] for p in PYPROJECTS}


class VersionError(Exception):
    """What stops a check or a release, said to the person running it."""


def _read(root: Path, path: str) -> str:
    try:
        return (root / path).read_text(encoding="utf-8")
    except FileNotFoundError:
        raise VersionError(f"{path} is missing") from None


def versions(root: Path) -> dict[str, str]:
    """Every manifest's version, by where it was found."""
    found = {}
    for path, patterns in PATTERNS.items():
        text = _read(root, path)
        for n, pattern in enumerate(patterns):
            match = pattern.search(text)
            if match is None:
                raise VersionError(f"{path} names no version where one is expected")
            found[f"{path}#{n}" if n else path] = match.group(2)
    return found


def _pins(text: str) -> list[tuple[int, re.Match[str]]]:
    """Each line of a compose file naming one of the project's images, read for its tag."""
    pins = []
    for n, line in enumerate(text.splitlines()):
        if OUR_IMAGE.match(line):
            match = COMPOSE_IMAGE.match(line)
            if match is None:
                raise VersionError(f"{COMPOSE} line {n + 1} names an image without a plain tag")
            pins.append((n, match))
    if not pins:
        raise VersionError(f"{COMPOSE} names no ingest-hub image")
    return pins


def line(version: str) -> str:
    """The image tag the compose file follows for `version`: its major, or before 1.0 its minor.

    A 0.x release may change anything, and the image builder publishes no bare major tag for one,
    so until 1.0 the compose file follows major.minor instead.
    """
    major, minor, _ = version.split(".")
    return major if major != "0" else f"{major}.{minor}"


def check(root: Path, tag: str | None) -> str:
    """The version every manifest agrees on; with `tag`, also that it is the tag's, released."""
    found = versions(root)
    if len(set(found.values())) != 1:
        listed = "\n".join(f"  {where}: {v}" for where, v in found.items())
        raise VersionError(f"the manifests name different versions:\n{listed}")
    version = next(iter(found.values()))
    expected = line(version)
    for _, pin in _pins(_read(root, COMPOSE)):
        if pin.group(2) != expected:
            image = pin.group(1).split("image:")[1].strip().strip("\"'")
            raise VersionError(
                f"{COMPOSE} pins {image}{pin.group(2)}, not {expected} as {version} is"
            )
    if tag is not None:
        if tag != f"v{version}":
            raise VersionError(f"the tag is {tag}, but the manifests name {version}")
        notes(root, version)
    return version


def set_version(root: Path, version: str, today: date) -> None:
    """Write `version` into every manifest and its line into the compose file, and date the
    Unreleased changelog section as it. Every file is worked out before any is written, so a
    release that cannot be cut changes nothing."""
    if not SEMVER.fullmatch(version):
        raise VersionError(f"{version} is not a version like 1.2.3")
    current = check(root, None)
    if tuple(map(int, version.split("."))) <= tuple(map(int, current.split("."))):
        raise VersionError(f"{version} is not after {current}, the current version")
    text = _read(root, CHANGELOG)
    headings = UNRELEASED_HEADING.findall(text)
    if len(headings) != 1:
        found = "no" if not headings else "more than one"
        raise VersionError(f"{CHANGELOG} has {found} '{UNRELEASED}' section to release")
    if not any(
        line.strip() and not line.startswith("#")
        for line in _section(text, UNRELEASED).splitlines()
    ):
        raise VersionError(f"{CHANGELOG}'s '{UNRELEASED}' section is empty")
    if re.search(rf"(?m)^## {re.escape(version)}(\s|$)", text):
        raise VersionError(f"{CHANGELOG} already has a section for {version}")
    planned = {CHANGELOG: UNRELEASED_HEADING.sub(f"## {version} — {today.isoformat()}", text, 1)}
    # check() above has found every pattern in its file, so each substitution has its line.
    for path, patterns in PATTERNS.items():
        content = _read(root, path)
        for pattern in patterns:
            content = pattern.sub(rf"\g<1>{version}\g<3>", content, count=1)
        planned[path] = content
    compose = _read(root, COMPOSE)
    lines = compose.split("\n")
    for n, pin in _pins(compose):
        lines[n] = f"{pin.group(1)}{line(version)}{pin.group(3)}"
    planned[COMPOSE] = "\n".join(lines)
    for path, content in planned.items():
        (root / path).write_text(content, encoding="utf-8")
    check(root, f"v{version}")


def notes(root: Path, version: str) -> str:
    """`version`'s changelog section, without its heading."""
    text = (root / CHANGELOG).read_text(encoding="utf-8")
    match = re.search(rf"(?m)^## {re.escape(version)} — \d{{4}}-\d{{2}}-\d{{2}}$", text)
    if match is None:
        raise VersionError(f"{CHANGELOG} has no dated section for {version}")
    return _section(text, match.group(0)).strip() + "\n"


def _section(text: str, heading: str) -> str:
    """The text under `heading`, up to the next section's."""
    after = text.split(heading + "\n", 1)[1] if heading + "\n" in text else ""
    return re.split(r"(?m)^## ", after, maxsplit=1)[0]


def main(argv: list[str] | None = None) -> int:
    """Run one command; a VersionError is reported and exits 1."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("check").add_argument("tag", nargs="?")
    commands.add_parser("set").add_argument("version")
    commands.add_parser("notes").add_argument("version")
    args = parser.parse_args(argv)
    try:
        if args.command == "check":
            print(f"every manifest names {check(args.root, args.tag)}")
        elif args.command == "set":
            set_version(args.root, args.version, date.today())
        else:
            sys.stdout.write(notes(args.root, args.version))
    except VersionError as exc:
        print(exc, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
