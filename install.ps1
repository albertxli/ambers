# Install the standalone `ambers` command (and its alias `am`) on Windows.
#
#   powershell -NoProfile -ExecutionPolicy ByPass -c "irm https://raw.githubusercontent.com/albertxli/ambers/main/install.ps1 | iex"
#
# It downloads the archive for Windows from the latest GitHub Release, puts the
# two programs in %LOCALAPPDATA%\ambers\bin and adds that folder to your user
# PATH. No administrator rights are needed. Run it again to update.
#
# Settings (environment variables):
#   AMBERS_VERSION          install this version (e.g. 0.5.0) instead of the latest
#   AMBERS_INSTALL_DIR      install here instead of %LOCALAPPDATA%\ambers\bin
#   AMBERS_NO_MODIFY_PATH   set to 1 to leave PATH alone
#   AMBERS_INSTALL_ARCHIVE  use this local .zip instead of downloading (for testing)

$ErrorActionPreference = 'Stop'
# Without this, downloads in Windows PowerShell 5.1 are very slow.
$ProgressPreference = 'SilentlyContinue'

$repo = 'albertxli/ambers'
$target = 'x86_64-pc-windows-msvc'

function Fail($message) {
    throw "ambers install: $message"
}

# Windows PowerShell 5.1 may still default to TLS 1.0, which GitHub refuses.
try {
    [Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12
} catch {}

if (-not [Environment]::Is64BitOperatingSystem) {
    Fail "the ready-made program needs 64-bit Windows. Another way to get it: 'uv tool install ambers' (needs Python)."
}

$installDir = $env:AMBERS_INSTALL_DIR
if (-not $installDir) {
    $installDir = Join-Path $env:LOCALAPPDATA 'ambers\bin'
}

$work = Join-Path ([IO.Path]::GetTempPath()) ("ambers-install-" + [Guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Force -Path $work | Out-Null
try {
    $zip = Join-Path $work 'ambers.zip'
    if ($env:AMBERS_INSTALL_ARCHIVE) {
        if (-not (Test-Path -LiteralPath $env:AMBERS_INSTALL_ARCHIVE)) {
            Fail "no such file: $($env:AMBERS_INSTALL_ARCHIVE)"
        }
        Copy-Item -LiteralPath $env:AMBERS_INSTALL_ARCHIVE -Destination $zip
    } else {
        $version = $env:AMBERS_VERSION
        if (-not $version) {
            try {
                $latest = Invoke-RestMethod -UseBasicParsing -Uri "https://api.github.com/repos/$repo/releases/latest"
                $version = $latest.tag_name
            } catch {
                Fail "could not find the latest release. Set AMBERS_VERSION, e.g. `$env:AMBERS_VERSION = '0.5.0'"
            }
        }
        $version = $version -replace '^v', ''
        $archive = "ambers-$version-$target.zip"
        $url = "https://github.com/$repo/releases/download/v$version/$archive"
        Write-Host "Downloading $archive"
        try {
            Invoke-WebRequest -UseBasicParsing -Uri $url -OutFile $zip
        } catch {
            Fail "could not download $url (version $version may have no program for Windows)"
        }
    }

    Expand-Archive -LiteralPath $zip -DestinationPath $work -Force
    # The archive holds one folder with the two programs inside.
    $found = Get-ChildItem -LiteralPath $work -Recurse -Filter 'ambers.exe' | Select-Object -First 1
    if (-not $found) {
        Fail 'the archive does not contain ambers.exe'
    }
    $sourceDir = $found.DirectoryName

    New-Item -ItemType Directory -Force -Path $installDir | Out-Null
    foreach ($program in 'ambers.exe', 'am.exe') {
        $source = Join-Path $sourceDir $program
        if (-not (Test-Path -LiteralPath $source)) {
            Fail "the archive does not contain $program"
        }
        try {
            Copy-Item -LiteralPath $source -Destination (Join-Path $installDir $program) -Force
        } catch {
            Fail "could not replace $program in $installDir. Close any running 'am view' and run this again."
        }
    }

    $installed = & (Join-Path $installDir 'ambers.exe') --version
    if ($LASTEXITCODE -ne 0) {
        Fail "installed, but $installDir\ambers.exe does not run"
    }
    Write-Host "Installed $installed to $installDir (commands: ambers, am)"

    $userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
    $onPath = ($userPath -split ';') | Where-Object { $_.TrimEnd('\') -ieq $installDir.TrimEnd('\') }
    if ($onPath) {
        Write-Host 'Try it: am info yourfile.sav'
    } elseif ($env:AMBERS_NO_MODIFY_PATH -eq '1') {
        Write-Host "$installDir is not on your PATH; add it to run 'am' from anywhere."
    } else {
        $newPath = if ($userPath) { "$($userPath.TrimEnd(';'));$installDir" } else { $installDir }
        [Environment]::SetEnvironmentVariable('Path', $newPath, 'User')
        # This window too, so the command works straight away.
        $env:Path = "$env:Path;$installDir"
        Write-Host "Added $installDir to your user PATH. New terminal windows will find 'am'."
        Write-Host 'Try it: am info yourfile.sav'
    }
} finally {
    Remove-Item -LiteralPath $work -Recurse -Force -ErrorAction SilentlyContinue
}
