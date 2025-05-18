"""
System requirements checker
Verifies if the current system meets the minimum requirements for the application
"""

import os
import platform
import psutil
import sys
from pathlib import Path
import shutil


class SystemChecker:
    """Checks system compatibility and available resources"""
    
    def __init__(self):
        """Initialize with minimum requirements"""
        self.min_requirements = {
            "ram_gb": 2.0,
            "cpu_cores": 2,
            "disk_space_mb": 500,
            "python_version": (3, 8),
            "windows_version": (10, 0) if platform.system() == "Windows" else None
        }
    
    def verify_requirements(self):
        """
        Check if system meets the minimum requirements
        
        Returns:
            tuple: (requirements_met, issues_list)
        """
        issues = []
        
        # Check RAM
        available_ram = psutil.virtual_memory().total / (1024 ** 3)  # GB
        if available_ram < self.min_requirements["ram_gb"]:
            issues.append(f"RAM: {available_ram:.1f} GB (minimum {self.min_requirements['ram_gb']} GB)")
        
        # Check CPU
        cpu_count = psutil.cpu_count(logical=False) or 1
        if cpu_count < self.min_requirements["cpu_cores"]:
            issues.append(f"CPU: {cpu_count} cores (minimum {self.min_requirements['cpu_cores']} cores)")
        
        # Check disk space
        try:
            disk_usage = shutil.disk_usage(Path.home())
            free_space_mb = disk_usage.free / (1024 ** 2)  # MB
            if free_space_mb < self.min_requirements["disk_space_mb"]:
                issues.append(f"Disk: {free_space_mb:.0f} MB free (minimum {self.min_requirements['disk_space_mb']} MB)")
        except Exception:
            issues.append("Could not determine available disk space")
        
        # Check Python version
        current_python = sys.version_info[:2]
        if current_python < self.min_requirements["python_version"]:
            py_min = ".".join(map(str, self.min_requirements["python_version"]))
            py_current = ".".join(map(str, current_python))
            issues.append(f"Python: {py_current} (minimum {py_min})")
        
        # Check Windows version if on Windows
        if platform.system() == "Windows" and self.min_requirements["windows_version"]:
            try:
                win_ver = sys.getwindowsversion()
                current_windows = (win_ver.major, win_ver.minor)
                if current_windows < self.min_requirements["windows_version"]:
                    win_min = ".".join(map(str, self.min_requirements["windows_version"]))
                    win_current = ".".join(map(str, current_windows))
                    issues.append(f"Windows: {win_current} (minimum {win_min})")
            except Exception:
                issues.append("Could not determine Windows version")
        
        # Check for required libraries
        missing_libs = self._check_required_libraries()
        if missing_libs:
            issues.append(f"Missing libraries: {', '.join(missing_libs)}")
        
        return len(issues) == 0, "\n".join(issues)
    
    def _check_required_libraries(self):
        """Check if required libraries are installed"""
        required_libs = [
            "selenium", "requests", "bs4", "lxml", 
            "sqlite3", "numpy", "pandas", "tkinter"
        ]
        
        # Add Windows-specific libraries
        if platform.system() == "Windows":
            required_libs.append("win32api")
        
        missing = []
        for lib in required_libs:
            try:
                if lib == "bs4":
                    __import__("bs4")
                elif lib == "win32api":
                    # Only check on Windows
                    if platform.system() == "Windows":
                        __import__("win32api")
                else:
                    __import__(lib)
            except ImportError:
                missing.append(lib)
        
        return missing
    
    def get_system_info(self):
        """Get detailed system information for diagnostics"""
        info = {
            "platform": platform.system(),
            "platform_release": platform.release(),
            "platform_version": platform.version(),
            "architecture": platform.machine(),
            "processor": platform.processor(),
            "ram": f"{psutil.virtual_memory().total / (1024 ** 3):.2f} GB",
            "python_version": platform.python_version(),
        }
        
        if platform.system() == "Windows":
            try:
                win_ver = sys.getwindowsversion()
                info["windows_version"] = f"{win_ver.major}.{win_ver.minor}.{win_ver.build}"
            except Exception:
                info["windows_version"] = "Unknown"
        
        return info
    
    def estimate_performance_profile(self):
        """
        Estimate the performance profile of the system
        
        Returns:
            str: 'low', 'medium', or 'high'
        """
        ram_gb = psutil.virtual_memory().total / (1024 ** 3)
        cpu_count = psutil.cpu_count(logical=False) or 1
        
        if ram_gb >= 8 and cpu_count >= 4:
            return "high"
        elif ram_gb >= 4 and cpu_count >= 2:
            return "medium"
        else:
            return "low"
    
    def get_optimal_settings(self):
        """
        Generate optimal application settings based on system capabilities
        
        Returns:
            dict: Recommended settings for the current system
        """
        profile = self.estimate_performance_profile()
        ram_gb = psutil.virtual_memory().total / (1024 ** 3)
        cpu_count = psutil.cpu_count(logical=False) or 1
        
        settings = {
            "system": {
                "memory_limit_mb": int(ram_gb * 256),  # Use 25% of available RAM
                "cpu_usage_limit": min(80, int(100 / (cpu_count + 1) * cpu_count)),
                "low_resource_mode": profile == "low",
            },
            "scraper": {
                "headless": True,
                "disable_images": profile == "low",
                "batch_size": 25 if profile == "low" else 50 if profile == "medium" else 100,
                "use_simplified_parser": profile == "low",
            },
            "ui": {
                "enable_animations": profile != "low",
                "refresh_rate_ms": 2000 if profile == "low" else 1000 if profile == "medium" else 500,
            },
            "analysis": {
                "parallel_processing": profile == "high",
                "max_chart_points": 50 if profile == "low" else 100 if profile == "medium" else 200,
            }
        }
        
        return settings