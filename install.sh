#!/bin/sh
# Install the standalone `ambers` command (and its alias `am`) on macOS or Linux.
#
#   curl -LsSf https://raw.githubusercontent.com/albertxli/ambers/main/install.sh | sh
#
# It downloads the archive for this machine from the latest GitHub Release and
# puts the two programs in ~/.local/bin. Nothing else is touched, and no
# administrator rights are needed. Run it again to update.
#
# Settings (environment variables):
#   AMBERS_VERSION          install this version (e.g. 0.5.0) instead of the latest
#   AMBERS_INSTALL_DIR      install here instead of ~/.local/bin
#   AMBERS_INSTALL_ARCHIVE  use this local .tar.gz instead of downloading (for testing)

set -eu

repo="albertxli/ambers"

say() { printf '%s\n' "$*"; }
fail() { printf 'ambers install: %s\n' "$*" >&2; exit 1; }

# Fetch a URL to stdout with whichever of curl / wget is there.
fetch() {
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL "$1"
    elif command -v wget >/dev/null 2>&1; then
        wget -qO- "$1"
    else
        fail "curl or wget is needed to download"
    fi
}

other_ways="Other ways to get it: 'uv tool install ambers' (needs Python), or build from source with 'cargo install --git https://github.com/$repo ambers-cli'."

os=$(uname -s)
arch=$(uname -m)
case "$os/$arch" in
    Linux/x86_64 | Linux/amd64) target="x86_64-unknown-linux-musl" ;;
    Darwin/arm64 | Darwin/aarch64) target="aarch64-apple-darwin" ;;
    Linux/*) fail "there is no ready-made program for Linux on $arch yet. $other_ways" ;;
    Darwin/*) fail "there is no ready-made program for Intel Macs yet. $other_ways" ;;
    *) fail "this script is for macOS and Linux (found $os). On Windows use install.ps1." ;;
esac

install_dir="${AMBERS_INSTALL_DIR:-$HOME/.local/bin}"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

if [ -n "${AMBERS_INSTALL_ARCHIVE:-}" ]; then
    [ -f "$AMBERS_INSTALL_ARCHIVE" ] || fail "no such file: $AMBERS_INSTALL_ARCHIVE"
    cp "$AMBERS_INSTALL_ARCHIVE" "$work/ambers.tar.gz"
else
    version="${AMBERS_VERSION:-}"
    if [ -z "$version" ]; then
        # The latest release's tag, e.g. "tag_name": "v0.5.0".
        version=$(fetch "https://api.github.com/repos/$repo/releases/latest" |
            sed -n 's/.*"tag_name": *"v\{0,1\}\([^"]*\)".*/\1/p' | head -n 1) || true
        [ -n "$version" ] || fail "could not find the latest release. Set AMBERS_VERSION, e.g. AMBERS_VERSION=0.5.0"
    fi
    version="${version#v}"
    archive="ambers-$version-$target.tar.gz"
    url="https://github.com/$repo/releases/download/v$version/$archive"
    say "Downloading $archive"
    fetch "$url" >"$work/ambers.tar.gz" ||
        fail "could not download $url (version $version may have no program for this machine)"
fi

tar -xzf "$work/ambers.tar.gz" -C "$work" || fail "the downloaded archive could not be unpacked"
# The archive holds one folder with the two programs inside.
found=$(find "$work" -type f -name ambers | head -n 1)
[ -n "$found" ] || fail "the archive does not contain the ambers program"
source_dir=$(dirname "$found")

mkdir -p "$install_dir"
for program in ambers am; do
    [ -f "$source_dir/$program" ] || fail "the archive does not contain $program"
    # Copy beside the old one, then swap: a running copy is not disturbed.
    cp "$source_dir/$program" "$install_dir/$program.new"
    chmod 755 "$install_dir/$program.new"
    mv -f "$install_dir/$program.new" "$install_dir/$program"
done

installed=$("$install_dir/ambers" --version 2>/dev/null) || fail "installed, but $install_dir/ambers does not run"
say "Installed $installed to $install_dir (commands: ambers, am)"

case ":$PATH:" in
    *":$install_dir:"*) say "Try it: am info yourfile.sav" ;;
    *)
        say ""
        say "$install_dir is not on your PATH. Add this line to your shell's start-up file"
        say "(~/.zshrc, ~/.bashrc or ~/.profile), then open a new terminal:"
        say ""
        say "    export PATH=\"$install_dir:\$PATH\""
        ;;
esac
