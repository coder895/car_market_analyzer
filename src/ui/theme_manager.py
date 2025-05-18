"""
Theme manager for Windows-styled UI appearance
"""

import tkinter as tk
from tkinter import ttk
import os
import platform
import json
from typing import Dict, List, Optional

# Local imports
from src.utils.config import Config


class ThemeManager:
    """Manages application themes and styling"""
    
    def __init__(self, config: Config):
        """Initialize theme manager"""
        self.config = config
        self.current_theme = "system"
        
        # Available theme names
        self.available_themes = ["system", "light", "dark", "blue", "high_contrast"]
        
        # Theme definitions
        self.themes = {
            "system": self._get_system_theme(),
            "light": {
                "bg": "#f0f0f0",
                "fg": "#000000",
                "accent": "#0078d7",
                "button_bg": "#e1e1e1",
                "entry_bg": "#ffffff",
                "sidebar_bg": "#e6e6e6",
                "sidebar_fg": "#333333",
                "border": "#c0c0c0"
            },
            "dark": {
                "bg": "#2d2d2d",
                "fg": "#ffffff",
                "accent": "#0078d7",
                "button_bg": "#444444",
                "entry_bg": "#333333",
                "sidebar_bg": "#252525",
                "sidebar_fg": "#e0e0e0",
                "border": "#555555"
            },
            "blue": {
                "bg": "#eff5fb",
                "fg": "#333333",
                "accent": "#0069c0",
                "button_bg": "#d4e4f7",
                "entry_bg": "#ffffff",
                "sidebar_bg": "#d8e6f6",
                "sidebar_fg": "#333333",
                "border": "#a0c8f0"
            },
            "high_contrast": {
                "bg": "#000000",
                "fg": "#ffffff",
                "accent": "#ffff00",
                "button_bg": "#000000",
                "entry_bg": "#000000",
                "sidebar_bg": "#000000",
                "sidebar_fg": "#ffffff",
                "border": "#ffffff"
            }
        }
    
    def _get_system_theme(self) -> Dict:
        """
        Detect system theme (light or dark mode)
        
        Returns:
            dict: Theme colors based on system settings
        """
        # Default to light theme
        system_theme = self.themes["light"].copy()
        
        if platform.system() == "Windows":
            try:
                import winreg
                
                # Try to detect Windows dark mode
                registry = winreg.ConnectRegistry(None, winreg.HKEY_CURRENT_USER)
                key = winreg.OpenKey(registry, "Software\\Microsoft\\Windows\\CurrentVersion\\Themes\\Personalize")
                value, _ = winreg.QueryValueEx(key, "AppsUseLightTheme")
                
                # AppsUseLightTheme = 0 means dark mode is enabled
                if value == 0:
                    system_theme = self.themes["dark"].copy()
                    
            except Exception:
                # If detection fails, use light theme
                pass
                
        elif platform.system() == "Darwin":  # macOS
            try:
                # Check if dark mode is enabled on macOS
                import subprocess
                
                result = subprocess.run(
                    ["defaults", "read", "-g", "AppleInterfaceStyle"],
                    capture_output=True, text=True
                )
                
                if "Dark" in result.stdout:
                    system_theme = self.themes["dark"].copy()
                    
            except Exception:
                # If detection fails, use light theme
                pass
        
        return system_theme
    
    def apply_theme(self, theme_name: str = None):
        """
        Apply a theme to the application
        
        Args:
            theme_name (str): Name of theme to apply, or None to use config
        """
        # Get theme from config if not specified
        if theme_name is None:
            theme_name = self.config.get("ui", "theme", "system")
        
        # Make sure theme exists
        if theme_name not in self.themes:
            theme_name = "system"
            
        # Get theme colors
        if theme_name == "system":
            colors = self._get_system_theme()
        else:
            colors = self.themes[theme_name]
            
        # Store current theme
        self.current_theme = theme_name
        
        # Create a ttk style object
        style = ttk.Style()
        
        # Configure ttk styles
        style.configure("TFrame", background=colors["bg"])
        style.configure("TLabel", background=colors["bg"], foreground=colors["fg"])
        style.configure("TButton", background=colors["button_bg"], foreground=colors["fg"])
        style.configure("TEntry", fieldbackground=colors["entry_bg"], foreground=colors["fg"])
        style.configure("TNotebook", background=colors["bg"])
        style.configure("TNotebook.Tab", background=colors["button_bg"], foreground=colors["fg"])
        
        # Configure special styles
        style.configure("Sidebar.TFrame", background=colors["sidebar_bg"])
        style.configure("Sidebar.TLabel", background=colors["sidebar_bg"], foreground=colors["sidebar_fg"])
        style.configure("Sidebar.TButton", background=colors["sidebar_bg"], foreground=colors["sidebar_fg"])
        
        # Configure Treeview
        style.configure("Treeview", 
                      background=colors["entry_bg"],
                      foreground=colors["fg"],
                      fieldbackground=colors["entry_bg"])
        
        # Configure special elements
        style.configure("Title.TLabel", 
                      font=("Arial", 14, "bold"),
                      background=colors["bg"],
                      foreground=colors["fg"])
        
        style.configure("Subtitle.TLabel", 
                      font=("Arial", 12),
                      background=colors["bg"],
                      foreground=colors["fg"])
        
        style.configure("Accent.TButton", 
                      background=colors["accent"],
                      foreground="#ffffff")
        
        # Map styles for different states
        style.map("TButton",
                background=[("active", colors["accent"]), ("pressed", colors["accent"])],
                foreground=[("active", "#ffffff"), ("pressed", "#ffffff")])
        
        style.map("Accent.TButton",
                background=[("active", self._lighten_color(colors["accent"], 0.1)), 
                          ("pressed", self._darken_color(colors["accent"], 0.1))])
        
        style.map("Treeview",
                background=[("selected", colors["accent"])],
                foreground=[("selected", "#ffffff")])
        
        # Save theme to config
        self.config.set("ui", "theme", theme_name)
        self.config.save()
    
    def get_theme_colors(self, theme_name: str = None) -> Dict:
        """
        Get colors for a specific theme
        
        Args:
            theme_name (str): Name of theme, or None for current
            
        Returns:
            dict: Theme color values
        """
        if theme_name is None:
            theme_name = self.current_theme
            
        if theme_name not in self.themes:
            theme_name = "system"
            
        if theme_name == "system":
            return self._get_system_theme()
        else:
            return self.themes[theme_name]
    
    def get_available_themes(self) -> List[str]:
        """
        Get list of available themes
        
        Returns:
            list: Available theme names
        """
        return self.available_themes
    
    def save_custom_theme(self, name: str, colors: Dict):
        """
        Save a custom theme
        
        Args:
            name (str): Theme name
            colors (dict): Theme colors
        """
        # Add to themes dict
        self.themes[name] = colors
        
        # Add to available themes if not already there
        if name not in self.available_themes:
            self.available_themes.append(name)
            
        # Save custom themes to config
        custom_themes = self.config.get("ui", "custom_themes", {})
        custom_themes[name] = colors
        self.config.set("ui", "custom_themes", custom_themes)
        self.config.save()
    
    def load_custom_themes(self):
        """Load custom themes from config"""
        custom_themes = self.config.get("ui", "custom_themes", {})
        
        for name, colors in custom_themes.items():
            self.themes[name] = colors
            if name not in self.available_themes:
                self.available_themes.append(name)
    
    def _lighten_color(self, hex_color: str, factor: float = 0.1) -> str:
        """
        Lighten a hex color by a factor
        
        Args:
            hex_color (str): Hex color code
            factor (float): Lightening factor (0-1)
            
        Returns:
            str: Lightened hex color
        """
        # Convert hex to RGB
        hex_color = hex_color.lstrip('#')
        r, g, b = int(hex_color[0:2], 16), int(hex_color[2:4], 16), int(hex_color[4:6], 16)
        
        # Lighten
        r = min(255, int(r + (255 - r) * factor))
        g = min(255, int(g + (255 - g) * factor))
        b = min(255, int(b + (255 - b) * factor))
        
        # Convert back to hex
        return f"#{r:02x}{g:02x}{b:02x}"
    
    def _darken_color(self, hex_color: str, factor: float = 0.1) -> str:
        """
        Darken a hex color by a factor
        
        Args:
            hex_color (str): Hex color code
            factor (float): Darkening factor (0-1)
            
        Returns:
            str: Darkened hex color
        """
        # Convert hex to RGB
        hex_color = hex_color.lstrip('#')
        r, g, b = int(hex_color[0:2], 16), int(hex_color[2:4], 16), int(hex_color[4:6], 16)
        
        # Darken
        r = int(r * (1 - factor))
        g = int(g * (1 - factor))
        b = int(b * (1 - factor))
        
        # Convert back to hex
        return f"#{r:02x}{g:02x}{b:02x}"