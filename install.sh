#!/bin/sh
# Install the standalone `ambers` command (and its alias `am`) on macOS or Linux.
#
#   curl -LsSf https://raw.githubusercontent.com/albertxli/ambers/main/install.sh | sh
#
# It downloads the archive for this machine from the latest GitHub Release and
# puts the two programs in ~/.local/bin. If that folder is not on your PATH,
# one line that adds it is appended to your shell's start-up file (~/.zshrc
# for zsh, the Mac's default). No administrator rights are needed. Run it
# again to update.
#
# Settings (environment variables):
#   AMBERS_VERSION          install this version (e.g. 0.5.0) instead of the latest
#   AMBERS_INSTALL_DIR      install here instead of ~/.local/bin
#   AMBERS_NO_MODIFY_PATH   set to 1 to leave the start-up files alone
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
    Linux/aarch64 | Linux/arm64) target="aarch64-unknown-linux-musl" ;;
    Darwin/arm64 | Darwin/aarch64) target="aarch64-apple-darwin" ;;
    Darwin/x86_64) target="x86_64-apple-darwin" ;;
    Linux/*) fail "there is no ready-made program for Linux on $arch yet. $other_ways" ;;
    Darwin/*) fail "there is no ready-made program for macOS on $arch yet. $other_ways" ;;
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

# The line that puts the folder on PATH, as it is written to a start-up file.
if [ "$install_dir" = "$HOME/.local/bin" ]; then
    # shellcheck disable=SC2016  # $HOME and $PATH are for the user's shell to fill in
    path_line='export PATH="$HOME/.local/bin:$PATH"'
else
    path_line="export PATH=\"$install_dir:\$PATH\""
fi

# A path the way a person writes it: ~/.zshrc.
short() {
    case "$1" in
        "$HOME"/*) say "~${1#"$HOME"}" ;;
        *) say "$1" ;;
    esac
}

by_hand() {
    say "Add this line to your shell's start-up file (~/.zshrc, ~/.bashrc or ~/.profile),"
    say "then open a new terminal window:"
    say ""
    say "    $path_line"
}

# Append the PATH line to the start-up file of the user's shell. The files are
# tried in order; the second is for when the first cannot be written to (a
# ~/.zshrc that belongs to root, left by something installed with sudo).
add_to_path() {
    case "${SHELL##*/}" in
        zsh)
            set -- "${ZDOTDIR:-$HOME}/.zshrc" "${ZDOTDIR:-$HOME}/.zprofile"
            ;;
        bash)
            if [ "$os" = Darwin ]; then
                # Terminal on a Mac starts bash as a login shell, which reads
                # only the first of these that exists: never create one that
                # would hide another.
                set -- "$HOME/.profile"
                for name in .bash_login .bash_profile; do
                    if [ -e "$HOME/$name" ]; then set -- "$HOME/$name"; fi
                done
            else
                set -- "$HOME/.bashrc"
            fi
            ;;
        sh | dash | ksh | ash)
            set -- "$HOME/.profile"
            ;;
        fish)
            say "$install_dir is not on your PATH. To add it, run:"
            say ""
            say "    fish_add_path \"$install_dir\""
            return
            ;;
        *)
            say "$install_dir is not on your PATH."
            by_hand
            return
            ;;
    esac

    for file in "$@"; do
        if [ -f "$file" ] && grep -qF "$path_line" "$file" 2>/dev/null; then
            say "$(short "$file") already puts $install_dir on your PATH."
            say "Open a new terminal window, then try: am info yourfile.sav"
            return
        fi
    done
    for file in "$@"; do
        if { printf '\n# Added by the ambers installer\n%s\n' "$path_line" >>"$file"; } 2>/dev/null; then
            say "Added $install_dir to your PATH (a line in $(short "$file"))."
            say "Open a new terminal window, then try: am info yourfile.sav"
            return
        fi
    done
    say "$install_dir is not on your PATH, and $(short "$1") could not be written to."
    by_hand
}

case ":$PATH:" in
    *":$install_dir:"*) say "Try it: am info yourfile.sav" ;;
    *)
        say ""
        if [ "${AMBERS_NO_MODIFY_PATH:-}" = 1 ]; then
            say "$install_dir is not on your PATH."
            by_hand
        else
            add_to_path
        fi
        ;;
esac
