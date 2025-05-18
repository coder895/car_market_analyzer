#!/usr/bin/env python3
"""
Build script for creating Windows executable and installer
For the resource-efficient car market trend analyzer
"""

import os
import sys
import subprocess
import shutil
import argparse
from pathlib import Path
import platform


def check_requirements():
    """Check if required tools are installed"""
    print("Checking build requirements...")
    
    requirements_met = True
    
    # Check Python version
    if sys.version_info < (3, 8):
        print("ERROR: Python 3.8 or higher is required")
        requirements_met = False
    else:
        print(f"✓ Python version {platform.python_version()} (OK)")
    
    # Check PyInstaller
    try:
        import PyInstaller
        print(f"✓ PyInstaller {PyInstaller.__version__} (OK)")
    except ImportError:
        print("ERROR: PyInstaller is not installed. Run: pip install pyinstaller")
        requirements_met = False
    
    # Check NSIS (Windows only)
    if platform.system() == "Windows":
        try:
            result = subprocess.run(["makensis", "/VERSION"], 
                                  capture_output=True, text=True, check=False)
            if result.returncode == 0:
                print(f"✓ NSIS {result.stdout.strip()} (OK)")
            else:
                print("ERROR: NSIS is not properly installed or not in PATH")
                requirements_met = False
        except FileNotFoundError:
            print("ERROR: NSIS not found. Please install NSIS from https://nsis.sourceforge.io/")
            requirements_met = False
    
    return requirements_met


def create_executable(output_dir, one_file=False, debug=False):
    """Build executable using PyInstaller"""
    print("Building executable...")
    
    # Create spec file arguments
    args = [
        "pyinstaller",
        "--name=CarMarketAnalyzer",
        "--clean",
        "--noconfirm",
    ]
    
    # Add icon if available
    icon_path = Path("src/assets/icon.ico")
    if icon_path.exists():
        args.append(f"--icon={icon_path}")
    
    # One file or directory
    if one_file:
        args.append("--onefile")
    else:
        args.append("--onedir")
    
    # Debug mode
    if debug:
        args.append("--debug=all")
    else:
        args.append("--windowed")  # No console in production
    
    # Output directory
    dist_dir = Path(output_dir) / "dist"
    args.append(f"--distpath={dist_dir}")
    
    # Build directory
    build_dir = Path(output_dir) / "build"
    args.append(f"--workpath={build_dir}")
    
    # Add main script
    args.append("src/main.py")
    
    # Add data files
    args.extend([
        "--add-data=src/assets;assets",
    ])
    
    # Run PyInstaller
    result = subprocess.run(args, check=False)
    
    if result.returncode != 0:
        print("ERROR: PyInstaller failed to build executable")
        return False
    
    print(f"Executable built successfully in {dist_dir}")
    return True


def create_installer(output_dir):
    """Create Windows installer using NSIS"""
    if platform.system() != "Windows":
        print("Skipping installer creation (not on Windows)")
        return False
    
    print("Creating Windows installer...")
    
    # Create NSIS script file
    nsis_script = Path(output_dir) / "installer.nsi"
    
    # Get application version from setup.py
    version = "0.1.0"  # Default version
    try:
        with open("setup.py", "r") as f:
            for line in f:
                if "version=" in line:
                    version = line.split('"')[1]
                    break
    except (FileNotFoundError, IndexError):
        pass
    
    # Write NSIS script
    with open(nsis_script, "w") as f:
        f.write(f"""
; Car Market Analyzer Installer Script
!include "MUI2.nsh"

; Application information
Name "Car Market Analyzer"
OutFile "CarMarketAnalyzer-{version}-Setup.exe"
InstallDir "$PROGRAMFILES64\\Car Market Analyzer"
InstallDirRegKey HKCU "Software\\Car Market Analyzer" ""

; Request application privileges
RequestExecutionLevel admin

; Interface Settings
!define MUI_ABORTWARNING
!define MUI_ICON "src\\assets\\icon.ico"
!define MUI_UNICON "src\\assets\\icon.ico"

; Pages
!insertmacro MUI_PAGE_WELCOME
!insertmacro MUI_PAGE_LICENSE "LICENSE"
!insertmacro MUI_PAGE_DIRECTORY
!insertmacro MUI_PAGE_INSTFILES
!insertmacro MUI_PAGE_FINISH

!insertmacro MUI_UNPAGE_CONFIRM
!insertmacro MUI_UNPAGE_INSTFILES

; Languages
!insertmacro MUI_LANGUAGE "English"

; Install Section
Section "Install"
    SetOutPath "$INSTDIR"
    
    ; Add files
    File /r "dist\\CarMarketAnalyzer\\*.*"
    
    ; Create uninstaller
    WriteUninstaller "$INSTDIR\\Uninstall.exe"
    
    ; Create shortcuts
    CreateDirectory "$SMPROGRAMS\\Car Market Analyzer"
    CreateShortcut "$SMPROGRAMS\\Car Market Analyzer\\Car Market Analyzer.lnk" "$INSTDIR\\CarMarketAnalyzer.exe"
    CreateShortcut "$SMPROGRAMS\\Car Market Analyzer\\Uninstall.lnk" "$INSTDIR\\Uninstall.exe"
    CreateShortcut "$DESKTOP\\Car Market Analyzer.lnk" "$INSTDIR\\CarMarketAnalyzer.exe"
    
    ; Registry information for Add/Remove Programs
    WriteRegStr HKLM "Software\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\CarMarketAnalyzer" "DisplayName" "Car Market Analyzer"
    WriteRegStr HKLM "Software\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\CarMarketAnalyzer" "UninstallString" "$\\"$INSTDIR\\Uninstall.exe$\\""
    WriteRegStr HKLM "Software\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\CarMarketAnalyzer" "InstallLocation" "$\\"$INSTDIR$\\""
    WriteRegStr HKLM "Software\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\CarMarketAnalyzer" "DisplayIcon" "$\\"$INSTDIR\\CarMarketAnalyzer.exe$\\""
    WriteRegStr HKLM "Software\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\CarMarketAnalyzer" "Publisher" "Car Market Analyzer Project"
    WriteRegStr HKLM "Software\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\CarMarketAnalyzer" "DisplayVersion" "{version}"
    WriteRegDWORD HKLM "Software\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\CarMarketAnalyzer" "NoModify" 1
    WriteRegDWORD HKLM "Software\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\CarMarketAnalyzer" "NoRepair" 1
SectionEnd

; Uninstall Section
Section "Uninstall"
    ; Remove files and directory
    RMDir /r "$INSTDIR"
    
    ; Remove shortcuts
    Delete "$SMPROGRAMS\\Car Market Analyzer\\Car Market Analyzer.lnk"
    Delete "$SMPROGRAMS\\Car Market Analyzer\\Uninstall.lnk"
    RMDir "$SMPROGRAMS\\Car Market Analyzer"
    Delete "$DESKTOP\\Car Market Analyzer.lnk"
    
    ; Remove registry entries
    DeleteRegKey HKLM "Software\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\CarMarketAnalyzer"
SectionEnd
""")
    
    # Run NSIS compiler
    result = subprocess.run(["makensis", str(nsis_script)], check=False)
    
    if result.returncode != 0:
        print("ERROR: NSIS failed to create installer")
        return False
    
    print(f"Installer created successfully: CarMarketAnalyzer-{version}-Setup.exe")
    return True


def copy_additional_files(output_dir):
    """Copy additional required files to distribution directory"""
    print("Copying additional files...")
    
    dist_dir = Path(output_dir) / "dist" / "CarMarketAnalyzer"
    
    # Create assets directory if not exists
    assets_dir = dist_dir / "assets"
    assets_dir.mkdir(exist_ok=True)
    
    # Copy license file
    try:
        shutil.copy("LICENSE", dist_dir)
        print("✓ Copied LICENSE file")
    except FileNotFoundError:
        print("WARNING: LICENSE file not found")
        # Create a minimal license file
        with open(dist_dir / "LICENSE", "w") as f:
            f.write("MIT License\n\nCopyright (c) 2023 Car Market Analyzer Project\n")
    
    # Copy readme
    try:
        shutil.copy("README.md", dist_dir)
        print("✓ Copied README.md file")
    except FileNotFoundError:
        print("WARNING: README.md file not found")
    
    # Copy additional assets if needed
    src_assets = Path("src/assets")
    if src_assets.exists() and src_assets.is_dir():
        for file in src_assets.glob("*"):
            if file.is_file():
                shutil.copy(file, assets_dir)
        print("✓ Copied assets files")
    
    return True


def main():
    """Main build function"""
    parser = argparse.ArgumentParser(description="Build Car Market Analyzer executable and installer")
    parser.add_argument("--output", "-o", default="build", help="Output directory")
    parser.add_argument("--onefile", action="store_true", help="Create a single executable file")
    parser.add_argument("--debug", action="store_true", help="Build in debug mode")
    parser.add_argument("--no-installer", action="store_true", help="Skip installer creation")
    
    args = parser.parse_args()
    
    # Create output directory
    output_dir = Path(args.output)
    output_dir.mkdir(exist_ok=True)
    
    # Check requirements
    if not check_requirements():
        sys.exit(1)
    
    # Build executable
    if not create_executable(output_dir, args.onefile, args.debug):
        sys.exit(1)
    
    # Copy additional files
    if not args.onefile:  # Only needed for directory mode
        if not copy_additional_files(output_dir):
            sys.exit(1)
    
    # Create installer (Windows only)
    if platform.system() == "Windows" and not args.no_installer:
        if not create_installer(output_dir):
            sys.exit(1)
    
    print("Build completed successfully!")


if __name__ == "__main__":
    main()