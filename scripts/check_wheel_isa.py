"""Fail if a built wheel contains CPU instructions above the portable baseline.

Guards against GitHub issue #6: wheels compiled with `target-cpu=native` on a CI runner
that happened to have AVX-512, which then crash with SIGILL / 0xC000001D on import on
ordinary CPUs.

Usage:
    python scripts/check_wheel_isa.py [--x86-baseline x86-64-v2|x86-64-v3] PATH [PATH ...]

PATH may be a wheel, a directory of wheels, or a bare .so/.pyd/.dylib. Needs GNU `objdump`
(x86-64 ELF and PE) and `llvm-objdump` (arm64 Mach-O) on PATH; both are preinstalled on
GitHub's ubuntu runners. Standard library only.

What is checked:
  x86-64, any baseline : no AVX-512 at all (zmm, k1-k7 mask regs, xmm/ymm16-31, EVEX-only
                         mnemonics). No supported baseline includes it.
  x86-64, x86-64-v2    : AVX/AVX2/BMI2 instruction count stays under --max-avx. A few dozen
                         come from dependencies that check the CPU at runtime before using
                         them (safe); a native/v3 build has thousands.
  arm64                : nothing newer than Apple M1 (no SVE/SME, bf16, i8mm).
"""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

BINARY_SUFFIXES = (".so", ".pyd", ".dylib")

# x86-64 -------------------------------------------------------------------
# objdump line: "  1234:\tmnemonic operands"
X86_INSN = re.compile(r"^\s*[0-9a-f]+:\t(\S+)\s*(.*)$")
AVX512_REG = re.compile(r"%zmm\d|%k[1-7]\b|%[xy]mm(?:1[6-9]|2\d|3[01])\b")
AVX512_MNEMONIC = re.compile(
    r"^(?:kmov|kand|kor|kxor|kxnor|knot|kshift|ktest|kortest|kunpck|kadd"
    r"|vpternlog|vpermt2|vpermi2|vpcompress|vpexpand|vcompress|vexpand|vpopcnt"
    r"|vpmov[bwdq]2m|vpmovm2|vprol|vpror|valign[dq]|vpmovu?s?(?:qd|qw|qb|db|dw|wb)\b"
    r"|vpscatter|vscatter|vrndscale|vgetexp|vgetmant|vfixupimm|vrcp14|vrsqrt14"
    r"|vcvtt?s[sd]2usi|vcvtusi2|vcvtu?qq2|vcvtt?p[sd]2u?qq|vpmullq|vpmaxuq|vpminuq|vpmaxsq|vpminsq"
    r"|vpabsq|vpsraq|vpsravq|vbroadcast[if](?:32x[248]|64x[24])|vextract[if](?:32x[48]|64x[24])"
    r"|vinsert[if](?:32x[48]|64x[24])|vshuf[if](?:32x4|64x2)|vmovdq[au](?:8|16|32|64)"
    r"|vp(?:and|andn|or|xor)[dq]\b|vpshld|vpshrd)"
)
# Instructions that need AVX, AVX2, FMA or BMI1/2 (above x86-64-v2).
AVX_OR_BMI_MNEMONIC = re.compile(
    r"^(?:shlx|shrx|sarx|rorx|mulx|pdep|pext|bzhi|andn|bextr|blsr|blsi|blsmsk"
    r"|vfn?m(?:add|sub)|vpbroadcast|vperm2i128|vinserti128|vextracti128|vpgather|vgather"
    r"|vpblendd|vpermq|vpermd|vpsllv|vpsrlv|vpsrav)"
)
YMM_REG = re.compile(r"%ymm\d")

# arm64 --------------------------------------------------------------------
ARM_INSN = re.compile(r"^\s*[0-9a-f]+:\s+(\S+)\s*(.*)$")
ARM_SVE_REG = re.compile(r"\b(?:z\d+\.[bhsdq]|p\d+/[zm]|za\d*[hv]?\.)")
ARM_POST_M1 = re.compile(
    r"^(?:bfdot|bfmmla|bfmlal[bt]|bfcvtn?2?|smmla|ummla|usmmla|usdot|sudot|smstart|smstop)$"
)


def run(cmd: list[str]) -> str:
    try:
        return subprocess.run(cmd, check=True, capture_output=True, text=True).stdout
    except FileNotFoundError:
        sys.exit(f"error: `{cmd[0]}` not found on PATH")
    except subprocess.CalledProcessError as exc:
        sys.exit(f"error: {' '.join(cmd)} failed:\n{exc.stderr}")


def arch_of(name: str) -> str:
    n = name.lower()
    if any(t in n for t in ("arm64", "aarch64")):
        return "arm64"
    if any(t in n for t in ("x86_64", "amd64", "win_amd64")):
        return "x86_64"
    return "unknown"


def check_x86(binary: Path, baseline: str, max_avx: int) -> list[str]:
    asm = run(["objdump", "-d", "--no-show-raw-insn", str(binary)])
    total = avx512 = avx = 0
    examples: dict[str, int] = {}
    for line in asm.splitlines():
        m = X86_INSN.match(line)
        if not m:
            continue
        total += 1
        mnem, ops = m.group(1), m.group(2)
        if AVX512_REG.search(ops) or AVX512_MNEMONIC.match(mnem):
            avx512 += 1
            examples[mnem] = examples.get(mnem, 0) + 1
        elif YMM_REG.search(ops) or AVX_OR_BMI_MNEMONIC.match(mnem):
            avx += 1
    print(f"    {total:,} instructions | AVX-512: {avx512} | AVX/AVX2/BMI: {avx}")
    if total == 0:
        return ["disassembly produced no instructions (objdump format not recognised?)"]
    problems = []
    if avx512:
        top = ", ".join(f"{k}×{v}" for k, v in sorted(examples.items(), key=lambda kv: -kv[1])[:6])
        problems.append(f"{avx512} AVX-512 instructions ({top})")
    if baseline == "x86-64-v2" and avx > max_avx:
        problems.append(
            f"{avx} AVX/AVX2/BMI instructions > --max-avx {max_avx}: built above x86-64-v2 "
            "(target-cpu=native or x86-64-v3?)"
        )
    return problems


def check_arm64(binary: Path) -> list[str]:
    asm = run(["llvm-objdump", "-d", "--mattr=+all", "--no-show-raw-insn", str(binary)])
    total = 0
    found: dict[str, int] = {}
    for line in asm.splitlines():
        m = ARM_INSN.match(line)
        if not m:
            continue
        total += 1
        mnem, ops = m.group(1).lower(), m.group(2)
        if ARM_POST_M1.match(mnem) or ARM_SVE_REG.search(ops):
            found[mnem] = found.get(mnem, 0) + 1
    print(f"    {total:,} instructions | post-M1 (SVE/SME/bf16/i8mm): {sum(found.values())}")
    if total == 0:
        return ["disassembly produced no instructions (llvm-objdump format not recognised?)"]
    if found:
        top = ", ".join(f"{k}×{v}" for k, v in sorted(found.items(), key=lambda kv: -kv[1])[:6])
        return [f"instructions newer than Apple M1: {top}"]
    return []


def check_binary(binary: Path, arch: str, args: argparse.Namespace) -> list[str]:
    if arch == "x86_64":
        return check_x86(binary, args.x86_baseline, args.max_avx)
    if arch == "arm64":
        return check_arm64(binary)
    return [f"cannot tell the architecture of {binary.name}; name it like the wheel tag"]


def iter_targets(paths: list[str]):
    for p in map(Path, paths):
        if p.is_dir():
            yield from sorted(x for x in p.iterdir() if x.suffix == ".whl")
        elif p.exists():
            yield p
        else:
            sys.exit(f"error: {p} does not exist")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("paths", nargs="+")
    ap.add_argument("--x86-baseline", choices=["x86-64-v2", "x86-64-v3"], default="x86-64-v2")
    ap.add_argument("--max-avx", type=int, default=500,
                    help="max AVX/AVX2/BMI instructions allowed for an x86-64-v2 build (default 500)")
    args = ap.parse_args()

    failed = checked = 0
    for target in iter_targets(args.paths):
        print(f"{target.name}")
        with tempfile.TemporaryDirectory() as tmp:
            if target.suffix == ".whl":
                with zipfile.ZipFile(target) as zf:
                    members = [m for m in zf.namelist() if m.endswith(BINARY_SUFFIXES)]
                    zf.extractall(tmp, members)
                binaries = [Path(tmp, m) for m in members]
                arch = arch_of(target.name)
            else:
                binaries, arch = [target], arch_of(target.name)
            if not binaries:
                print("    no native extension found")
                failed += 1
                continue
            for b in binaries:
                problems = check_binary(b, arch, args)
                checked += 1
                for msg in problems:
                    print(f"    FAIL: {msg}")
                failed += bool(problems)
        print()

    if checked == 0:
        print("no wheels or binaries checked")
        return 1
    print("FAILED" if failed else "OK", f"({checked} binaries, {failed} with problems)")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
