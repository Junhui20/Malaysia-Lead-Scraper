"""
Build a portable package of the Lead Scraper app.
Creates a self-contained folder with embedded Python + all dependencies.
End users just unzip and double-click run.bat or LeadScraper.vbs.

Usage:
    python build_package.py
"""

import os
import platform
import shutil
import subprocess
import sys
import urllib.request
import zipfile
from pathlib import Path

# Config
PYTHON_VERSION = "3.11.9"
PACKAGE_NAME = "LeadScraper"
BUILD_DIR = Path(__file__).parent / "build"
DIST_DIR = Path(__file__).parent / "dist"
PACKAGE_DIR = DIST_DIR / PACKAGE_NAME

# Python embeddable URL (Windows only)
PYTHON_EMBED_URL = f"https://www.python.org/ftp/python/{PYTHON_VERSION}/python-{PYTHON_VERSION}-embed-amd64.zip"
GET_PIP_URL = "https://bootstrap.pypa.io/get-pip.py"

# Files to include in the package
APP_FILES = [
    "app.py",
    "database.py",
    "scrapers.py",
    "utils.py",
    "requirements.txt",
]


def download_file(url: str, dest: Path) -> None:
    print(f"  Downloading {url.split('/')[-1]}...")
    urllib.request.urlretrieve(url, dest)


def step_1_clean():
    print("\n[1/7] Cleaning previous build...")
    if BUILD_DIR.exists():
        shutil.rmtree(BUILD_DIR)
    if PACKAGE_DIR.exists():
        shutil.rmtree(PACKAGE_DIR)
    BUILD_DIR.mkdir(parents=True, exist_ok=True)
    DIST_DIR.mkdir(parents=True, exist_ok=True)
    PACKAGE_DIR.mkdir(parents=True, exist_ok=True)


def step_2_download_python():
    print("\n[2/7] Downloading embedded Python...")
    zip_path = BUILD_DIR / "python_embed.zip"
    download_file(PYTHON_EMBED_URL, zip_path)

    python_dir = PACKAGE_DIR / "python"
    python_dir.mkdir(exist_ok=True)

    print("  Extracting...")
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(python_dir)

    # Enable pip: uncomment "import site" in python312._pth
    pth_files = list(python_dir.glob("python*._pth"))
    for pth in pth_files:
        content = pth.read_text()
        content = content.replace("#import site", "import site")
        pth.write_text(content)
        print(f"  Enabled site-packages in {pth.name}")


def step_3_install_pip():
    print("\n[3/7] Installing pip...")
    get_pip = BUILD_DIR / "get-pip.py"
    download_file(GET_PIP_URL, get_pip)

    python_exe = PACKAGE_DIR / "python" / "python.exe"
    subprocess.run([str(python_exe), str(get_pip), "--no-warn-script-location"],
                   check=True, capture_output=True)
    print("  pip installed")


def step_4_install_deps():
    print("\n[4/7] Installing dependencies (this takes a few minutes)...")
    python_exe = PACKAGE_DIR / "python" / "python.exe"
    req_file = Path(__file__).parent / "requirements.txt"

    subprocess.run(
        [str(python_exe), "-m", "pip", "install", "-r", str(req_file),
         "--no-warn-script-location", "--disable-pip-version-check"],
        check=True,
    )
    print("  All packages installed")


def step_5_install_browser():
    print("\n[5/7] Installing Chromium browser (this takes a minute)...")
    python_exe = PACKAGE_DIR / "python" / "python.exe"

    # Set PLAYWRIGHT_BROWSERS_PATH to keep browsers inside the package
    env = os.environ.copy()
    browsers_dir = PACKAGE_DIR / "browsers"
    browsers_dir.mkdir(exist_ok=True)
    env["PLAYWRIGHT_BROWSERS_PATH"] = str(browsers_dir)

    subprocess.run(
        [str(python_exe), "-m", "playwright", "install", "chromium"],
        check=True,
        env=env,
    )
    print("  Chromium installed")


def step_6_precompile():
    """Pre-compile all .py files to .pyc so first startup is fast."""
    print("\n[6/7] Pre-compiling Python bytecodes...")
    python_exe = PACKAGE_DIR / "python" / "python.exe"
    site_packages = PACKAGE_DIR / "python" / "Lib" / "site-packages"

    subprocess.run(
        [str(python_exe), "-m", "compileall", "-q", "-f", str(site_packages)],
        check=True,
        capture_output=True,
    )
    print("  All packages pre-compiled")


def step_7_copy_app():
    print("\n[7/7] Copying app files...")
    src_dir = Path(__file__).parent

    for filename in APP_FILES:
        src = src_dir / filename
        if src.exists():
            shutil.copy2(src, PACKAGE_DIR / filename)
            print(f"  Copied {filename}")

    # Copy .streamlit config
    streamlit_dir = PACKAGE_DIR / ".streamlit"
    streamlit_dir.mkdir(exist_ok=True)
    src_config = src_dir / ".streamlit" / "config.toml"
    if src_config.exists():
        shutil.copy2(src_config, streamlit_dir / "config.toml")
        print("  Copied .streamlit/config.toml")

    # Create credentials.toml to skip first-run prompt
    credentials = streamlit_dir / "credentials.toml"
    credentials.write_text('[general]\nemail = ""\n', encoding="utf-8")
    print("  Created .streamlit/credentials.toml")

    # Create run.bat — auto-opens browser
    run_bat = PACKAGE_DIR / "run.bat"
    run_bat.write_text(
        '@echo off\r\n'
        'title Lead Scraper\r\n'
        'echo ==========================================\r\n'
        'echo   Lead Scraper\r\n'
        'echo ==========================================\r\n'
        'echo.\r\n'
        'echo Starting... browser will open automatically.\r\n'
        'echo To stop: close this window.\r\n'
        'echo.\r\n'
        'set PLAYWRIGHT_BROWSERS_PATH=%~dp0browsers\r\n'
        'start "" cmd /c "timeout /t 5 /nobreak >nul & start http://localhost:8501"\r\n'
        '"%~dp0python\\python.exe" -m streamlit run "%~dp0app.py"'
        ' --server.headless true'
        ' --server.port 8501'
        ' --server.address localhost'
        ' --browser.gatherUsageStats false'
        ' --global.developmentMode false\r\n'
        'pause\r\n',
        encoding="utf-8",
    )
    print("  Created run.bat")

    # Create LeadScraper.vbs — double-click launcher (runs run.bat)
    vbs = PACKAGE_DIR / "LeadScraper.vbs"
    vbs.write_text(
        'Set WshShell = CreateObject("WScript.Shell")\r\n'
        'WshShell.Run Chr(34) & CreateObject("Scripting.FileSystemObject")'
        '.GetParentFolderName(WScript.ScriptFullName) & "\\run.bat" & Chr(34), 1, False\r\n',
        encoding="utf-8",
    )
    print("  Created LeadScraper.vbs")

    # Create zip
    print("\n  Creating zip file...")
    zip_path = DIST_DIR / f"{PACKAGE_NAME}.zip"
    if zip_path.exists():
        zip_path.unlink()

    shutil.make_archive(str(DIST_DIR / PACKAGE_NAME), "zip", DIST_DIR, PACKAGE_NAME)
    zip_size = zip_path.stat().st_size / (1024 * 1024)
    print(f"  Created {zip_path.name} ({zip_size:.0f} MB)")


def main():
    if platform.system() != "Windows":
        print("This build script is for Windows only.")
        sys.exit(1)

    print("=" * 50)
    print("  Building Lead Scraper Package")
    print("=" * 50)

    step_1_clean()
    step_2_download_python()
    step_3_install_pip()
    step_4_install_deps()
    step_5_install_browser()
    step_6_precompile()
    step_7_copy_app()

    print("\n" + "=" * 50)
    print("  Build complete!")
    print(f"  Package: dist/{PACKAGE_NAME}/")
    print(f"  Zip:     dist/{PACKAGE_NAME}.zip")
    print()
    print("  To use:")
    print(f"  1. Go to dist/{PACKAGE_NAME}/")
    print("  2. Double-click LeadScraper.vbs or run.bat")
    print()
    print("  To distribute:")
    print(f"  Send {PACKAGE_NAME}.zip — user unzips and runs")
    print("=" * 50)


if __name__ == "__main__":
    main()
