#!/usr/bin/env python3
"""
Car Market Trend Analyzer - Main Application
A resource-efficient car market analyzer for Windows systems with limited resources
"""

import os
import sys
import tkinter as tk
from tkinter import messagebox
import platform
import psutil

# Local imports
from src.utils.config import Config
from src.utils.system_check import SystemChecker
from src.ui.main_window import MainWindow
from src.database.db_manager import DatabaseManager


def check_environment():
    """Check if running on Windows and verify system meets minimum requirements"""
    if platform.system() != "Windows" and not os.environ.get("CAR_ANALYZER_BYPASS_OS_CHECK"):
        print("Warning: This application is optimized for Windows. Some features may not work correctly.")
        
    system_checker = SystemChecker()
    requirements_met, issues = system_checker.verify_requirements()
    
    if not requirements_met:
        if platform.system() == "Windows":
            messagebox.warning(
                "System Requirements", 
                f"Your system does not meet the minimum requirements:\n\n{issues}\n\n"
                "The application will run in reduced functionality mode."
            )
        else:
            print(f"System does not meet minimum requirements: {issues}")
            print("Running in reduced functionality mode.")
    
    return requirements_met


def load_config():
    """Load configuration or create with optimal defaults based on system"""
    config = Config()
    
    # Auto-tune based on system capabilities
    memory_gb = psutil.virtual_memory().total / (1024 ** 3)
    cpu_count = psutil.cpu_count(logical=False) or 1
    
    # Set resource limits based on available system resources
    if not config.exists():
        # Conservative defaults for low-end systems
        config.set("system", "memory_limit_mb", min(500, int(memory_gb * 256)))
        config.set("system", "cpu_usage_limit", min(50, int(100 / (cpu_count + 1))))
        config.set("system", "low_resource_mode", memory_gb < 4 or cpu_count < 2)
        config.set("scraper", "headless", True)
        config.set("scraper", "disable_images", True)
        config.set("scraper", "batch_size", min(50, int(memory_gb * 15)))
        config.set("database", "compression_enabled", True)
        config.set("database", "max_size_mb", min(500, int(memory_gb * 128)))
        config.set("ui", "enable_animations", memory_gb > 4)
        config.set("ui", "refresh_rate_ms", 1000 if memory_gb > 4 else 2000)
        config.save()
    
    return config


def main():
    """Main application entry point"""
    # Check system environment
    requirements_met = check_environment()
    
    # Load or create optimized configuration
    config = load_config()
    
    # Initialize database
    db_manager = DatabaseManager(config)
    db_manager.initialize_database()
    
    # Start UI
    root = tk.Tk()
    root.withdraw()  # Hide the root window initially
    
    # Set application in low resource mode if requirements not met
    if not requirements_met:
        config.set("system", "low_resource_mode", True)
        config.save()
    
    app = MainWindow(root, config, db_manager)
    app.show()
    
    # Start main event loop
    root.mainloop()
    
    # Clean up resources
    db_manager.close()


if __name__ == "__main__":
    main()