"""
Configuration manager for the car market analyzer
Handles loading, saving, and providing access to application settings
"""

import os
import json
import platform
from pathlib import Path


class Config:
    """Manages application configuration with auto-tuning capabilities"""
    
    def __init__(self, config_file=None):
        """Initialize configuration with default settings or from file"""
        if platform.system() == "Windows":
            app_data = os.getenv('APPDATA', '')
            self.config_dir = Path(app_data) / "CarMarketAnalyzer"
        else:
            self.config_dir = Path.home() / ".car_market_analyzer"
            
        self.config_dir.mkdir(parents=True, exist_ok=True)
        
        if config_file:
            self.config_file = Path(config_file)
        else:
            self.config_file = self.config_dir / "config.json"
            
        self.config = self._load_default_config()
        
        if self.exists():
            self._load_config()
    
    def _load_default_config(self):
        """Load default configuration settings"""
        return {
            "system": {
                "memory_limit_mb": 512,
                "cpu_usage_limit": 50,
                "low_resource_mode": False,
                "pause_on_battery": True,
                "auto_tune": True,
            },
            "scraper": {
                "headless": True,
                "disable_images": True,
                "batch_size": 50,
                "scroll_pause_time": 1.5,
                "max_listings": 500,
                "use_simplified_parser": False,
                "retry_attempts": 3,
                "backoff_factor": 2.0,
            },
            "database": {
                "path": str(self.config_dir / "car_data.db"),
                "compression_enabled": True,
                "max_size_mb": 500,
                "vacuum_threshold": 0.2,
                "retention_days": 90,
            },
            "ui": {
                "theme": "system",
                "enable_animations": False,
                "refresh_rate_ms": 1000,
                "use_system_tray": True,
                "minimize_to_tray": True,
                "max_results_per_page": 25,
            },
            "analysis": {
                "precompute_common_metrics": True,
                "cache_results": True,
                "cache_ttl_minutes": 60,
                "parallel_processing": False,
                "max_chart_points": 100,
            },
            "scheduler": {
                "enabled": True,
                "scrape_frequency_hours": 24,
                "scan_when_idle": True,
                "idle_threshold_minutes": 10,
                "run_at_startup": False,
            }
        }
    
    def _load_config(self):
        """Load configuration from file, if it exists"""
        try:
            with open(self.config_file, 'r') as f:
                user_config = json.load(f)
                
            # Merge user configuration with defaults
            for section, options in user_config.items():
                if section in self.config:
                    self.config[section].update(options)
                else:
                    self.config[section] = options
        except (json.JSONDecodeError, FileNotFoundError):
            # If configuration file is corrupt or missing, use defaults
            pass
    
    def exists(self):
        """Check if configuration file exists"""
        return self.config_file.exists()
    
    def get(self, section, option, default=None):
        """Get configuration value with fallback to default"""
        try:
            return self.config[section][option]
        except KeyError:
            return default
    
    def set(self, section, option, value):
        """Set configuration value"""
        if section not in self.config:
            self.config[section] = {}
        self.config[section][option] = value
    
    def save(self):
        """Save current configuration to file"""
        with open(self.config_file, 'w') as f:
            json.dump(self.config, f, indent=2)
    
    def reset_to_defaults(self):
        """Reset configuration to default values"""
        self.config = self._load_default_config()
        self.save()
    
    def get_all(self):
        """Get entire configuration dictionary"""
        return self.config.copy()
    
    def update_section(self, section, options):
        """Update multiple options in a section at once"""
        if section not in self.config:
            self.config[section] = {}
        self.config[section].update(options)