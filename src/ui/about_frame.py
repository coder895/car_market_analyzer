"""
About dialog frame showing application information
"""

import tkinter as tk
from tkinter import ttk
import platform
import os
import sys
import webbrowser
from datetime import datetime


class AboutFrame(ttk.Frame):
    """Frame showing application information and credits"""
    
    def __init__(self, parent):
        """Initialize the about frame"""
        super().__init__(parent)
        self.parent = parent
        
        # Configure grid layout
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(0, weight=0)  # Title
        self.grid_rowconfigure(1, weight=0)  # Logo
        self.grid_rowconfigure(2, weight=0)  # Version
        self.grid_rowconfigure(3, weight=1)  # Description
        self.grid_rowconfigure(4, weight=0)  # System info
        self.grid_rowconfigure(5, weight=0)  # Buttons
        
        # Create UI elements
        self._create_header()
        self._create_content()
        self._create_footer()
    
    def _create_header(self):
        """Create the about header"""
        header_frame = ttk.Frame(self)
        header_frame.grid(row=0, column=0, sticky="ew", padx=10, pady=(10, 5))
        
        # About title
        title_label = ttk.Label(header_frame, text="About Car Market Trend Analyzer", 
                              style="Title.TLabel")
        title_label.pack(anchor="center", padx=5)
    
    def _create_content(self):
        """Create the main content area"""
        # Logo/icon area (placeholder)
        logo_frame = ttk.Frame(self)
        logo_frame.grid(row=1, column=0, sticky="ew", padx=10, pady=10)
        
        # Try to load app icon if it exists
        icon_path = os.path.join(os.path.dirname(__file__), "../assets/icon.png")
        
        if os.path.exists(icon_path):
            try:
                # Load and display icon
                from PIL import Image, ImageTk
                
                img = Image.open(icon_path)
                img = img.resize((128, 128), Image.LANCZOS)
                photo = ImageTk.PhotoImage(img)
                
                logo_label = ttk.Label(logo_frame, image=photo)
                logo_label.image = photo  # Keep a reference to prevent garbage collection
                logo_label.pack(pady=10)
                
            except ImportError:
                # Fallback if PIL is not available
                fallback_label = ttk.Label(logo_frame, text="📊 📈", font=("Arial", 48))
                fallback_label.pack(pady=10)
        else:
            # Fallback emoji icon
            fallback_label = ttk.Label(logo_frame, text="📊 📈", font=("Arial", 48))
            fallback_label.pack(pady=10)
        
        # Version info
        version_frame = ttk.Frame(self)
        version_frame.grid(row=2, column=0, sticky="ew", padx=10, pady=5)
        
        version_label = ttk.Label(version_frame, text="Car Market Trend Analyzer v0.1.0",
                                font=("Arial", 12, "bold"))
        version_label.pack(anchor="center")
        
        build_date = datetime.now().strftime("%Y-%m-%d")
        build_label = ttk.Label(version_frame, text=f"Build Date: {build_date}")
        build_label.pack(anchor="center")
        
        # Description
        desc_frame = ttk.Frame(self)
        desc_frame.grid(row=3, column=0, sticky="nsew", padx=20, pady=10)
        
        desc_text = (
            "A resource-efficient Python application for analyzing car market trends on "
            "Facebook Marketplace, optimized for Windows systems with limited CPU resources.\n\n"
            "Features:\n"
            "• Resource-efficient car listing scraper with minimal-footprint Selenium\n"
            "• Optimized market trend analysis with batched processing\n"
            "• Windows-optimized interface using lightweight Tkinter\n"
            "• Performance features including on-demand data loading\n"
            "• Resource-aware background operations that respect system limits\n\n"
            "Developed with focus on performance efficiency and low resource usage.\n\n"
            "© 2023 Car Market Analyzer Project"
        )
        
        desc_label = ttk.Label(desc_frame, text=desc_text, justify="center", wraplength=400)
        desc_label.pack(fill="both", expand=True)
        
        # System info
        sys_frame = ttk.LabelFrame(self, text="System Information")
        sys_frame.grid(row=4, column=0, sticky="ew", padx=20, pady=10)
        
        # Get system info
        python_version = platform.python_version()
        system_info = f"{platform.system()} {platform.release()}"
        processor = platform.processor()
        
        # Display in grid layout
        sys_frame.grid_columnconfigure(1, weight=1)
        
        ttk.Label(sys_frame, text="Python Version:").grid(row=0, column=0, sticky="w", padx=10, pady=2)
        ttk.Label(sys_frame, text=python_version).grid(row=0, column=1, sticky="w", padx=10, pady=2)
        
        ttk.Label(sys_frame, text="Operating System:").grid(row=1, column=0, sticky="w", padx=10, pady=2)
        ttk.Label(sys_frame, text=system_info).grid(row=1, column=1, sticky="w", padx=10, pady=2)
        
        ttk.Label(sys_frame, text="Processor:").grid(row=2, column=0, sticky="w", padx=10, pady=2)
        ttk.Label(sys_frame, text=processor).grid(row=2, column=1, sticky="w", padx=10, pady=2)
    
    def _create_footer(self):
        """Create the footer with buttons"""
        footer_frame = ttk.Frame(self)
        footer_frame.grid(row=5, column=0, sticky="ew", padx=10, pady=10)
        
        # License button
        license_btn = ttk.Button(footer_frame, text="License", command=self._show_license)
        license_btn.pack(side="left", padx=5)
        
        # Website button
        website_btn = ttk.Button(footer_frame, text="Website", command=self._open_website)
        website_btn.pack(side="left", padx=5)
        
        # Close button
        close_btn = ttk.Button(footer_frame, text="Close", command=self._close)
        close_btn.pack(side="right", padx=5)
    
    def _show_license(self):
        """Show license information"""
        # Create a new toplevel window
        license_dialog = tk.Toplevel(self)
        license_dialog.title("License")
        license_dialog.geometry("600x400")
        license_dialog.transient(self)
        license_dialog.grab_set()
        
        # Make dialog modal
        license_dialog.focus_set()
        
        # Configure dialog layout
        license_dialog.grid_columnconfigure(0, weight=1)
        license_dialog.grid_rowconfigure(0, weight=0)  # Title
        license_dialog.grid_rowconfigure(1, weight=1)  # Content
        license_dialog.grid_rowconfigure(2, weight=0)  # Button
        
        # Create title
        title_label = ttk.Label(license_dialog, text="MIT License", font=("Arial", 12, "bold"))
        title_label.grid(row=0, column=0, sticky="ew", padx=10, pady=5)
        
        # Create text widget with scrollbar
        text_frame = ttk.Frame(license_dialog)
        text_frame.grid(row=1, column=0, sticky="nsew", padx=10, pady=5)
        
        text_frame.grid_columnconfigure(0, weight=1)
        text_frame.grid_rowconfigure(0, weight=1)
        
        text_scroll = ttk.Scrollbar(text_frame)
        text_scroll.grid(row=0, column=1, sticky="ns")
        
        license_text = tk.Text(text_frame, yscrollcommand=text_scroll.set, wrap="word")
        license_text.grid(row=0, column=0, sticky="nsew")
        text_scroll.config(command=license_text.yview)
        
        # Add license text
        mit_license = """
MIT License

Copyright (c) 2023 Car Market Analyzer Project

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
        """
        
        license_text.insert("1.0", mit_license)
        license_text.config(state="disabled")
        
        # Create close button
        close_btn = ttk.Button(license_dialog, text="Close", command=license_dialog.destroy)
        close_btn.grid(row=2, column=0, pady=10)
    
    def _open_website(self):
        """Open the project website"""
        # Replace with actual website when available
        website_url = "https://github.com/yourusername/car_market_analyzer"
        webbrowser.open(website_url)
    
    def _close(self):
        """Close the about frame"""
        # In a tabbed interface, this would switch back to the previous tab
        if hasattr(self.parent, "show_frame"):
            self.parent.show_frame("dashboard")
        else:
            # Just hide this frame
            self.grid_forget()