"""Check that a Windows `ambers.exe` is standalone.

Lists the DLLs in the executable's import table and fails if any of them
is not part of Windows itself: the binary must run on a machine with no
Python and no Visual C++ runtime installed (it is built with
`-C target-feature=+crt-static`).

Usage:
    python scripts/check_cli_imports.py path/to/ambers.exe

Standard library only; needs no dumpbin or objdump.
"""

import struct
import sys

# Import prefixes that mean the binary is not standalone.
FORBIDDEN = ("python", "vcruntime", "msvcp", "msvcr", "ucrtbase", "api-ms-win-crt")


def imported_dlls(path: str) -> list[str]:
    data = open(path, "rb").read()
    pe = struct.unpack_from("<I", data, 0x3C)[0]
    if data[pe : pe + 4] != b"PE\0\0":
        raise ValueError(f"{path} is not a PE executable")
    n_sections, opt_size = struct.unpack_from("<H", data, pe + 6)[0], struct.unpack_from("<H", data, pe + 20)[0]
    opt = pe + 24
    magic = struct.unpack_from("<H", data, opt)[0]
    # Data directories start at 96 (PE32) or 112 (PE32+); entry 1 is the import table.
    directories = opt + (112 if magic == 0x20B else 96)
    import_rva = struct.unpack_from("<I", data, directories + 8)[0]

    sections = []
    for i in range(n_sections):
        base = opt + opt_size + 40 * i
        virtual_size, virtual_addr, raw_size, raw_ptr = struct.unpack_from("<IIII", data, base + 8)
        sections.append((virtual_addr, max(virtual_size, raw_size), raw_ptr))

    def offset(rva: int) -> int:
        for virtual_addr, size, raw_ptr in sections:
            if virtual_addr <= rva < virtual_addr + size:
                return rva - virtual_addr + raw_ptr
        raise ValueError(f"RVA {rva:#x} is outside every section")

    dlls = []
    if import_rva:
        entry = offset(import_rva)
        while True:
            name_rva = struct.unpack_from("<I", data, entry + 12)[0]
            if name_rva == 0:
                break
            start = offset(name_rva)
            dlls.append(data[start : data.index(b"\0", start)].decode("ascii"))
            entry += 20
    return dlls


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__)
        return 2
    dlls = imported_dlls(sys.argv[1])
    bad = [dll for dll in dlls if dll.lower().startswith(FORBIDDEN)]
    for dll in dlls:
        print(f"  {'FORBIDDEN' if dll in bad else 'ok':<9} {dll}")
    if bad:
        print(f"FAIL: {sys.argv[1]} is not standalone: it imports {', '.join(bad)}")
        return 1
    print(f"OK: {sys.argv[1]} imports only Windows system DLLs")
    return 0


if __name__ == "__main__":
    sys.exit(main())
