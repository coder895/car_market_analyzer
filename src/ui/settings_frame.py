"""
Settings configuration frame
"""

import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import os
import platform
from typing import Dict, List, Optional, Callable

# Local imports
from src.utils.config import Config
from src.ui.theme_manager import ThemeManager


class SettingsFrame(ttk.Frame):
    """Frame for configuring application settings"""
    
    def __init__(self, parent, config: Config, theme_manager: ThemeManager,
                apply_theme_callback: Callable):
        """Initialize the settings frame"""
        super().__init__(parent)
        self.parent = parent
        self.config = config
        self.theme_manager = theme_manager
        self.apply_theme_callback = apply_theme_callback
        
        # Configure grid layout
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(0, weight=0)  # Header
        self.grid_rowconfigure(1, weight=1)  # Content
        self.grid_rowconfigure(2, weight=0)  # Buttons
        
        # Create UI elements
        self._create_header()
        self._create_content()
        self._create_footer()
        
        # Load current settings
        self._load_settings()
    
    def _create_header(self):
        """Create the settings header"""
        header_frame = ttk.Frame(self)
        header_frame.grid(row=0, column=0, sticky="ew", padx=10, pady=(10, 5))
        
        # Settings title
        title_label = ttk.Label(header_frame, text="Application Settings", 
                              style="Title.TLabel")
        title_label.pack(side=tk.LEFT, padx=5)
    
    def _create_content(self):
        """Create the settings content area with tabs"""
        content_frame = ttk.Frame(self)
        content_frame.grid(row=1, column=0, sticky="nsew", padx=10, pady=5)
        
        # Configure content frame
        content_frame.grid_columnconfigure(0, weight=1)
        content_frame.grid_rowconfigure(0, weight=1)
        
        # Create notebook for tabs
        self.notebook = ttk.Notebook(content_frame)
        self.notebook.grid(row=0, column=0, sticky="nsew")
        
        # Create tabs
        self.general_tab = ttk.Frame(self.notebook)
        self.appearance_tab = ttk.Frame(self.notebook)
        self.performance_tab = ttk.Frame(self.notebook)
        self.advanced_tab = ttk.Frame(self.notebook)
        
        self.notebook.add(self.general_tab, text="General")
        self.notebook.add(self.appearance_tab, text="Appearance")
        self.notebook.add(self.performance_tab, text="Performance")
        self.notebook.add(self.advanced_tab, text="Advanced")
        
        # Create tab contents
        self._create_general_tab()
        self._create_appearance_tab()
        self._create_performance_tab()
        self._create_advanced_tab()
    
    def _create_general_tab(self):
        """Create general settings tab"""
        # Configure tab layout
        self.general_tab.grid_columnconfigure(0, weight=1)
        
        # Database settings
        db_frame = ttk.LabelFrame(self.general_tab, text="Database Settings")
        db_frame.grid(row=0, column=0, sticky="ew", padx=10, pady=10)
        
        # Database path
        path_frame = ttk.Frame(db_frame)
        path_frame.pack(fill="x", padx=10, pady=5)
        
        ttk.Label(path_frame, text="Database Location:").grid(row=0, column=0, sticky="w", padx=5, pady=5)
        
        self.db_path_var = tk.StringVar()
        path_entry = ttk.Entry(path_frame, textvariable=self.db_path_var, width=40)
        path_entry.grid(row=0, column=1, sticky="ew", padx=5, pady=5)
        path_frame.grid_columnconfigure(1, weight=1)
        
        browse_btn = ttk.Button(path_frame, text="Browse", command=self._browse_db_path)
        browse_btn.grid(row=0, column=2, padx=5, pady=5)
        
        # Data retention period
        retention_frame = ttk.Frame(db_frame)
        retention_frame.pack(fill="x", padx=10, pady=5)
        
        ttk.Label(retention_frame, text="Data Retention:").grid(row=0, column=0, sticky="w", padx=5, pady=5)
        
        self.retention_var = tk.StringVar()
        retention_spinbox = ttk.Spinbox(retention_frame, from_=1, to=365, increment=1, 
                                      textvariable=self.retention_var, width=5)
        retention_spinbox.grid(row=0, column=1, sticky="w", padx=5, pady=5)
        
        ttk.Label(retention_frame, text="days").grid(row=0, column=2, sticky="w", padx=5, pady=5)
        
        # Database compression
        self.compression_var = tk.BooleanVar()
        compress_check = ttk.Checkbutton(db_frame, text="Enable Database Compression", 
                                       variable=self.compression_var)
        compress_check.pack(anchor="w", padx=15, pady=5)
        
        # Startup settings
        startup_frame = ttk.LabelFrame(self.general_tab, text="Startup Settings")
        startup_frame.grid(row=1, column=0, sticky="ew", padx=10, pady=10)
        
        # Minimize to tray
        self.minimize_var = tk.BooleanVar()
        minimize_check = ttk.Checkbutton(startup_frame, text="Minimize to System Tray on Close", 
                                       variable=self.minimize_var)
        minimize_check.pack(anchor="w", padx=15, pady=5)
        
        # Start with system
        self.autostart_var = tk.BooleanVar()
        autostart_check = ttk.Checkbutton(startup_frame, text="Start with Windows", 
                                        variable=self.autostart_var)
        autostart_check.pack(anchor="w", padx=15, pady=5)
        
        # Start minimized
        self.start_min_var = tk.BooleanVar()
        start_min_check = ttk.Checkbutton(startup_frame, text="Start Minimized", 
                                        variable=self.start_min_var)
        start_min_check.pack(anchor="w", padx=15, pady=5)
        
        # Default view
        default_frame = ttk.Frame(startup_frame)
        default_frame.pack(fill="x", padx=10, pady=5)
        
        ttk.Label(default_frame, text="Default View:").grid(row=0, column=0, sticky="w", padx=5, pady=5)
        
        self.default_view_var = tk.StringVar()
        views = ["Dashboard", "Scraper", "Analysis", "Settings"]
        default_combo = ttk.Combobox(default_frame, textvariable=self.default_view_var, 
                                    values=views, width=15, state="readonly")
        default_combo.grid(row=0, column=1, sticky="w", padx=5, pady=5)
        
        # Updates
        updates_frame = ttk.LabelFrame(self.general_tab, text="Updates")
        updates_frame.grid(row=2, column=0, sticky="ew", padx=10, pady=10)
        
        # Check for updates
        self.check_updates_var = tk.BooleanVar()
        updates_check = ttk.Checkbutton(updates_frame, text="Check for Updates on Startup", 
                                      variable=self.check_updates_var)
        updates_check.pack(anchor="w", padx=15, pady=5)
        
        # Auto update
        self.auto_update_var = tk.BooleanVar()
        auto_update_check = ttk.Checkbutton(updates_frame, text="Download Updates Automatically", 
                                          variable=self.auto_update_var)
        auto_update_check.pack(anchor="w", padx=15, pady=5)
    
    def _create_appearance_tab(self):
        """Create appearance settings tab"""
        # Configure tab layout
        self.appearance_tab.grid_columnconfigure(0, weight=1)
        
        # Theme settings
        theme_frame = ttk.LabelFrame(self.appearance_tab, text="Theme Settings")
        theme_frame.grid(row=0, column=0, sticky="ew", padx=10, pady=10)
        
        # Theme selection
        theme_select_frame = ttk.Frame(theme_frame)
        theme_select_frame.pack(fill="x", padx=10, pady=5)
        
        ttk.Label(theme_select_frame, text="Application Theme:").grid(row=0, column=0, sticky="w", padx=5, pady=5)
        
        self.theme_var = tk.StringVar()
        themes = self.theme_manager.get_available_themes()
        theme_combo = ttk.Combobox(theme_select_frame, textvariable=self.theme_var, 
                                 values=themes, width=15, state="readonly")
        theme_combo.grid(row=0, column=1, sticky="w", padx=5, pady=5)
        
        # Bind theme change
        theme_combo.bind("<<ComboboxSelected>>", self._on_theme_changed)
        
        # Font settings
        font_frame = ttk.LabelFrame(self.appearance_tab, text="Font Settings")
        font_frame.grid(row=1, column=0, sticky="ew", padx=10, pady=10)
        
        # Font size
        font_size_frame = ttk.Frame(font_frame)
        font_size_frame.pack(fill="x", padx=10, pady=5)
        
        ttk.Label(font_size_frame, text="UI Font Size:").grid(row=0, column=0, sticky="w", padx=5, pady=5)
        
        self.font_size_var = tk.StringVar()
        size_spinbox = ttk.Spinbox(font_size_frame, from_=8, to=16, increment=1, 
                                  textvariable=self.font_size_var, width=5)
        size_spinbox.grid(row=0, column=1, sticky="w", padx=5, pady=5)
        
        # Font example
        font_example = ttk.LabelFrame(font_frame, text="Example Text")
        font_example.pack(fill="x", padx=10, pady=10)
        
        self.example_label = ttk.Label(font_example, 
                                     text="This is example text with the current font settings.",
                                     padding=10)
        self.example_label.pack(fill="x")
        
        # Bind font size change
        self.font_size_var.trace_add("write", self._on_font_size_changed)
        
        # UI settings
        ui_frame = ttk.LabelFrame(self.appearance_tab, text="UI Settings")
        ui_frame.grid(row=2, column=0, sticky="ew", padx=10, pady=10)
        
        # Enable animations
        self.animations_var = tk.BooleanVar()
        anim_check = ttk.Checkbutton(ui_frame, text="Enable UI Animations", 
                                   variable=self.animations_var)
        anim_check.pack(anchor="w", padx=15, pady=5)
        
        # UI refresh rate
        refresh_frame = ttk.Frame(ui_frame)
        refresh_frame.pack(fill="x", padx=10, pady=5)
        
        ttk.Label(refresh_frame, text="UI Refresh Rate:").grid(row=0, column=0, sticky="w", padx=5, pady=5)
        
        self.refresh_var = tk.StringVar()
        refresh_spinbox = ttk.Spinbox(refresh_frame, from_=500, to=5000, increment=100, 
                                    textvariable=self.refresh_var, width=5)
        refresh_spinbox.grid(row=0, column=1, sticky="w", padx=5, pady=5)
        
        ttk.Label(refresh_frame, text="ms").grid(row=0, column=2, sticky="w", padx=5, pady=5)
    
    def _create_performance_tab(self):
        """Create performance settings tab"""
        # Configure tab layout
        self.performance_tab.grid_columnconfigure(0, weight=1)
        
        # Resource limits
        resource_frame = ttk.LabelFrame(self.performance_tab, text="Resource Limits")
        resource_frame.grid(row=0, column=0, sticky="ew", padx=10, pady=10)
        
        # Memory limit
        memory_frame = ttk.Frame(resource_frame)
        memory_frame.pack(fill="x", padx=10, pady=5)
        
        ttk.Label(memory_frame, text="Memory Limit:").grid(row=0, column=0, sticky="w", padx=5, pady=5)
        
        self.memory_limit_var = tk.StringVar()
        memory_spinbox = ttk.Spinbox(memory_frame, from_=128, to=4096, increment=64, 
                                    textvariable=self.memory_limit_var, width=5)
        memory_spinbox.grid(row=0, column=1, sticky="w", padx=5, pady=5)
        
        ttk.Label(memory_frame, text="MB").grid(row=0, column=2, sticky="w", padx=5, pady=5)
        
        # CPU limit
        cpu_frame = ttk.Frame(resource_frame)
        cpu_frame.pack(fill="x", padx=10, pady=5)
        
        ttk.Label(cpu_frame, text="CPU Usage Limit:").grid(row=0, column=0, sticky="w", padx=5, pady=5)
        
        self.cpu_limit_var = tk.StringVar()
        cpu_spinbox = ttk.Spinbox(cpu_frame, from_=10, to=95, increment=5, 
                                textvariable=self.cpu_limit_var, width=5)
        cpu_spinbox.grid(row=0, column=1, sticky="w", padx=5, pady=5)
        
        ttk.Label(cpu_frame, text="%").grid(row=0, column=2, sticky="w", padx=5, pady=5)
        
        # Low resource mode
        self.low_resource_var = tk.BooleanVar()
        low_resource_check = ttk.Checkbutton(resource_frame, 
                                          text="Enable Low Resource Mode (for older systems)", 
                                          variable=self.low_resource_var)
        low_resource_check.pack(anchor="w", padx=15, pady=5)
        
        # Battery options
        self.battery_var = tk.BooleanVar()
        battery_check = ttk.Checkbutton(resource_frame, 
                                      text="Pause Scraping When on Battery Power", 
                                      variable=self.battery_var)
        battery_check.pack(anchor="w", padx=15, pady=5)
        
        # Scraper performance settings
        scraper_frame = ttk.LabelFrame(self.performance_tab, text="Scraper Performance")
        scraper_frame.grid(row=1, column=0, sticky="ew", padx=10, pady=10)
        
        # Batch size
        batch_frame = ttk.Frame(scraper_frame)
        batch_frame.pack(fill="x", padx=10, pady=5)
        
        ttk.Label(batch_frame, text="Batch Size:").grid(row=0, column=0, sticky="w", padx=5, pady=5)
        
        self.batch_size_var = tk.StringVar()
        batch_spinbox = ttk.Spinbox(batch_frame, from_=10, to=200, increment=10, 
                                  textvariable=self.batch_size_var, width=5)
        batch_spinbox.grid(row=0, column=1, sticky="w", padx=5, pady=5)
        
        # Headless mode
        self.headless_var = tk.BooleanVar()
        headless_check = ttk.Checkbutton(scraper_frame, text="Run Browser in Headless Mode", 
                                       variable=self.headless_var)
        headless_check.pack(anchor="w", padx=15, pady=5)
        
        # Disable images
        self.disable_images_var = tk.BooleanVar()
        images_check = ttk.Checkbutton(scraper_frame, text="Disable Image Loading (saves memory)", 
                                     variable=self.disable_images_var)
        images_check.pack(anchor="w", padx=15, pady=5)
        
        # Simplified parser
        self.simple_parser_var = tk.BooleanVar()
        parser_check = ttk.Checkbutton(scraper_frame, 
                                     text="Use Simplified HTML Parser (less accurate, but faster)", 
                                     variable=self.simple_parser_var)
        parser_check.pack(anchor="w", padx=15, pady=5)
        
        # Analysis performance
        analysis_frame = ttk.LabelFrame(self.performance_tab, text="Analysis Performance")
        analysis_frame.grid(row=2, column=0, sticky="ew", padx=10, pady=10)
        
        # Precompute metrics
        self.precompute_var = tk.BooleanVar()
        precompute_check = ttk.Checkbutton(analysis_frame, 
                                         text="Precompute Common Metrics (faster analysis, more storage)", 
                                         variable=self.precompute_var)
        precompute_check.pack(anchor="w", padx=15, pady=5)
        
        # Cache results
        self.cache_var = tk.BooleanVar()
        cache_check = ttk.Checkbutton(analysis_frame, 
                                    text="Cache Analysis Results (saves CPU, uses more memory)", 
                                    variable=self.cache_var)
        cache_check.pack(anchor="w", padx=15, pady=5)
        
        # Cache TTL
        cache_frame = ttk.Frame(analysis_frame)
        cache_frame.pack(fill="x", padx=10, pady=5)
        
        ttk.Label(cache_frame, text="Cache Duration:").grid(row=0, column=0, sticky="w", padx=5, pady=5)
        
        self.cache_ttl_var = tk.StringVar()
        cache_spinbox = ttk.Spinbox(cache_frame, from_=1, to=1440, increment=15, 
                                  textvariable=self.cache_ttl_var, width=5)
        cache_spinbox.grid(row=0, column=1, sticky="w", padx=5, pady=5)
        
        ttk.Label(cache_frame, text="minutes").grid(row=0, column=2, sticky="w", padx=5, pady=5)
    
    def _create_advanced_tab(self):
        """Create advanced settings tab"""
        # Configure tab layout
        self.advanced_tab.grid_columnconfigure(0, weight=1)
        
        # Warning label
        warning_frame = ttk.Frame(self.advanced_tab)
        warning_frame.grid(row=0, column=0, sticky="ew", padx=10, pady=10)
        
        warning_label = ttk.Label(warning_frame, 
                                text="Warning: These settings are for advanced users only. "
                                "Incorrect values may cause performance issues or data loss.",
                                foreground="red", wraplength=400)
        warning_label.pack(pady=10)
        
        # Database advanced settings
        db_frame = ttk.LabelFrame(self.advanced_tab, text="Advanced Database Settings")
        db_frame.grid(row=1, column=0, sticky="ew", padx=10, pady=10)
        
        # Vacuum threshold
        vacuum_frame = ttk.Frame(db_frame)
        vacuum_frame.pack(fill="x", padx=10, pady=5)
        
        ttk.Label(vacuum_frame, text="Vacuum Threshold:").grid(row=0, column=0, sticky="w", padx=5, pady=5)
        
        self.vacuum_var = tk.StringVar()
        vacuum_spinbox = ttk.Spinbox(vacuum_frame, from_=0.0, to=1.0, increment=0.1, 
                                   textvariable=self.vacuum_var, width=5)
        vacuum_spinbox.grid(row=0, column=1, sticky="w", padx=5, pady=5)
        
        # Database maintenance buttons
        maint_frame = ttk.Frame(db_frame)
        maint_frame.pack(fill="x", padx=10, pady=10)
        
        vacuum_btn = ttk.Button(maint_frame, text="Vacuum Database", 
                              command=self._vacuum_database)
        vacuum_btn.pack(side="left", padx=5)
        
        optimize_btn = ttk.Button(maint_frame, text="Optimize Indices", 
                                command=self._optimize_database)
        optimize_btn.pack(side="left", padx=5)
        
        # Scraper advanced settings
        scraper_frame = ttk.LabelFrame(self.advanced_tab, text="Advanced Scraper Settings")
        scraper_frame.grid(row=2, column=0, sticky="ew", padx=10, pady=10)
        
        # Retry attempts
        retry_frame = ttk.Frame(scraper_frame)
        retry_frame.pack(fill="x", padx=10, pady=5)
        
        ttk.Label(retry_frame, text="Retry Attempts:").grid(row=0, column=0, sticky="w", padx=5, pady=5)
        
        self.retry_var = tk.StringVar()
        retry_spinbox = ttk.Spinbox(retry_frame, from_=1, to=10, increment=1, 
                                  textvariable=self.retry_var, width=5)
        retry_spinbox.grid(row=0, column=1, sticky="w", padx=5, pady=5)
        
        # Backoff factor
        backoff_frame = ttk.Frame(scraper_frame)
        backoff_frame.pack(fill="x", padx=10, pady=5)
        
        ttk.Label(backoff_frame, text="Backoff Factor:").grid(row=0, column=0, sticky="w", padx=5, pady=5)
        
        self.backoff_var = tk.StringVar()
        backoff_spinbox = ttk.Spinbox(backoff_frame, from_=1.0, to=5.0, increment=0.5, 
                                    textvariable=self.backoff_var, width=5)
        backoff_spinbox.grid(row=0, column=1, sticky="w", padx=5, pady=5)
        
        # Save raw HTML
        self.save_html_var = tk.BooleanVar()
        html_check = ttk.Checkbutton(scraper_frame, text="Save Raw HTML (increases storage usage)", 
                                   variable=self.save_html_var)
        html_check.pack(anchor="w", padx=15, pady=5)
        
        # Windows integration
        if platform.system() == "Windows":
            windows_frame = ttk.LabelFrame(self.advanced_tab, text="Windows Integration")
            windows_frame.grid(row=3, column=0, sticky="ew", padx=10, pady=10)
            
            # Register with task scheduler
            task_btn = ttk.Button(windows_frame, text="Register with Windows Task Scheduler", 
                                command=self._register_task_scheduler)
            task_btn.pack(anchor="w", padx=15, pady=5)
            
            # Remove from task scheduler
            remove_task_btn = ttk.Button(windows_frame, text="Remove from Windows Task Scheduler", 
                                      command=self._remove_task_scheduler)
            remove_task_btn.pack(anchor="w", padx=15, pady=5)
            
            # Add to startup folder
            startup_btn = ttk.Button(windows_frame, text="Add to Windows Startup", 
                                   command=self._add_to_startup)
            startup_btn.pack(anchor="w", padx=15, pady=5)
    
    def _create_footer(self):
        """Create the footer with buttons"""
        footer_frame = ttk.Frame(self)
        footer_frame.grid(row=2, column=0, sticky="ew", padx=10, pady=10)
        
        # Apply button
        apply_btn = ttk.Button(footer_frame, text="Apply", command=self._apply_settings)
        apply_btn.pack(side=tk.RIGHT, padx=5)
        
        # Save button
        save_btn = ttk.Button(footer_frame, text="Save", command=self._save_settings)
        save_btn.pack(side=tk.RIGHT, padx=5)
        
        # Reset button
        reset_btn = ttk.Button(footer_frame, text="Reset to Defaults", command=self._reset_defaults)
        reset_btn.pack(side=tk.LEFT, padx=5)
    
    def _load_settings(self):
        """Load current settings from config"""
        # General tab
        self.db_path_var.set(self.config.get("database", "path", ""))
        self.retention_var.set(str(self.config.get("database", "retention_days", 90)))
        self.compression_var.set(self.config.get("database", "compression_enabled", True))
        self.minimize_var.set(self.config.get("ui", "minimize_to_tray", True))
        self.autostart_var.set(self.config.get("scheduler", "run_at_startup", False))
        self.start_min_var.set(self.config.get("ui", "start_minimized", False))
        self.default_view_var.set(self.config.get("ui", "default_view", "Dashboard"))
        self.check_updates_var.set(self.config.get("updates", "check_on_startup", True))
        self.auto_update_var.set(self.config.get("updates", "auto_update", False))
        
        # Appearance tab
        self.theme_var.set(self.config.get("ui", "theme", "system"))
        self.font_size_var.set(str(self.config.get("ui", "font_size", 10)))
        self.animations_var.set(self.config.get("ui", "enable_animations", True))
        self.refresh_var.set(str(self.config.get("ui", "refresh_rate_ms", 1000)))
        
        # Performance tab
        self.memory_limit_var.set(str(self.config.get("system", "memory_limit_mb", 512)))
        self.cpu_limit_var.set(str(self.config.get("system", "cpu_usage_limit", 50)))
        self.low_resource_var.set(self.config.get("system", "low_resource_mode", False))
        self.battery_var.set(self.config.get("system", "pause_on_battery", True))
        self.batch_size_var.set(str(self.config.get("scraper", "batch_size", 50)))
        self.headless_var.set(self.config.get("scraper", "headless", True))
        self.disable_images_var.set(self.config.get("scraper", "disable_images", True))
        self.simple_parser_var.set(self.config.get("scraper", "use_simplified_parser", False))
        self.precompute_var.set(self.config.get("analysis", "precompute_common_metrics", True))
        self.cache_var.set(self.config.get("analysis", "cache_results", True))
        self.cache_ttl_var.set(str(self.config.get("analysis", "cache_ttl_minutes", 60)))
        
        # Advanced tab
        self.vacuum_var.set(str(self.config.get("database", "vacuum_threshold", 0.2)))
        self.retry_var.set(str(self.config.get("scraper", "retry_attempts", 3)))
        self.backoff_var.set(str(self.config.get("scraper", "backoff_factor", 2.0)))
        self.save_html_var.set(self.config.get("scraper", "save_raw_html", False))
        
        # Update example text with current font size
        self._update_example_text()
    
    def _browse_db_path(self):
        """Browse for database path"""
        # Get parent directory
        current_path = self.db_path_var.get()
        
        # Default to home directory if path doesn't exist
        init_dir = os.path.dirname(current_path) if os.path.exists(os.path.dirname(current_path)) else os.path.expanduser("~")
        
        # Ask for file path
        new_path = filedialog.asksaveasfilename(
            initialdir=init_dir,
            title="Select Database Location",
            filetypes=[("SQLite Database", "*.db"), ("All Files", "*.*")],
            defaultextension=".db"
        )
        
        # Update path if selected
        if new_path:
            self.db_path_var.set(new_path)
    
    def _on_theme_changed(self, event):
        """Handle theme change"""
        new_theme = self.theme_var.get()
        
        # Apply theme immediately for preview
        self.theme_manager.apply_theme(new_theme)
        
        # Call apply theme callback
        if callable(self.apply_theme_callback):
            self.apply_theme_callback()
    
    def _on_font_size_changed(self, *args):
        """Handle font size change"""
        self._update_example_text()
    
    def _update_example_text(self):
        """Update example text with current font size"""
        try:
            size = int(self.font_size_var.get())
            self.example_label.configure(font=("Arial", size))
        except ValueError:
            pass
    
    def _vacuum_database(self):
        """Vacuum the database to reclaim space"""
        if messagebox.askyesno("Vacuum Database", 
                              "This will optimize the database and reclaim unused space. "
                              "It may take some time. Continue?"):
            try:
                # Get database connection
                from src.database.db_manager import DatabaseManager
                db_manager = DatabaseManager(self.config)
                conn = db_manager.connect()
                
                # Run vacuum
                conn.execute("VACUUM")
                conn.commit()
                
                messagebox.showinfo("Database Maintenance", 
                                  "Database vacuum completed successfully.")
                
            except Exception as e:
                messagebox.showerror("Error", f"Error vacuuming database: {e}")
    
    def _optimize_database(self):
        """Optimize database indices"""
        if messagebox.askyesno("Optimize Database", 
                              "This will optimize database indices for better performance. "
                              "It may take some time. Continue?"):
            try:
                # Get database connection
                from src.database.db_manager import DatabaseManager
                db_manager = DatabaseManager(self.config)
                conn = db_manager.connect()
                
                # Run optimization
                conn.execute("ANALYZE")
                conn.commit()
                
                messagebox.showinfo("Database Maintenance", 
                                  "Database optimization completed successfully.")
                
            except Exception as e:
                messagebox.showerror("Error", f"Error optimizing database: {e}")
    
    def _register_task_scheduler(self):
        """Register with Windows Task Scheduler"""
        if platform.system() != "Windows":
            messagebox.showinfo("Not Available",
                              "This feature is only available on Windows.")
            return
            
        if messagebox.askyesno("Windows Task Scheduler", 
                              "This will register the application to run automatically according "
                              "to your scheduled scraping settings. Continue?"):
            try:
                # Get scheduler instance
                from src.scraper.scheduler import TaskScheduler
                scheduler = TaskScheduler(self.config, None)
                
                # Register with Windows
                result = scheduler.register_with_windows_scheduler()
                
                if result:
                    messagebox.showinfo("Task Scheduler", 
                                      "Successfully registered with Windows Task Scheduler.")
                else:
                    messagebox.showerror("Error", 
                                       "Failed to register with Windows Task Scheduler. "
                                       "Try running the application as Administrator.")
                
            except Exception as e:
                messagebox.showerror("Error", f"Error registering with Task Scheduler: {e}")
    
    def _remove_task_scheduler(self):
        """Remove from Windows Task Scheduler"""
        if platform.system() != "Windows":
            messagebox.showinfo("Not Available",
                              "This feature is only available on Windows.")
            return
            
        if messagebox.askyesno("Windows Task Scheduler", 
                              "This will remove the application from Windows Task Scheduler. Continue?"):
            try:
                # Get scheduler instance
                from src.scraper.scheduler import TaskScheduler
                scheduler = TaskScheduler(self.config, None)
                
                # Remove from Windows
                result = scheduler.remove_from_windows_scheduler()
                
                if result:
                    messagebox.showinfo("Task Scheduler", 
                                      "Successfully removed from Windows Task Scheduler.")
                else:
                    messagebox.showerror("Error", 
                                       "Failed to remove from Windows Task Scheduler. "
                                       "Try running the application as Administrator.")
                
            except Exception as e:
                messagebox.showerror("Error", f"Error removing from Task Scheduler: {e}")
    
    def _add_to_startup(self):
        """Add application to Windows startup"""
        if platform.system() != "Windows":
            messagebox.showinfo("Not Available",
                              "This feature is only available on Windows.")
            return
            
        if messagebox.askyesno("Windows Startup", 
                              "This will add the application to Windows startup. Continue?"):
            try:
                import winreg
                import sys
                
                # Get executable path
                exe_path = sys.executable
                
                # Add to registry
                key = winreg.OpenKey(
                    winreg.HKEY_CURRENT_USER,
                    r"Software\Microsoft\Windows\CurrentVersion\Run",
                    0, winreg.KEY_SET_VALUE
                )
                
                winreg.SetValueEx(
                    key, "CarMarketAnalyzer", 0, winreg.REG_SZ,
                    f'"{exe_path}" --minimized'
                )
                
                winreg.CloseKey(key)
                
                messagebox.showinfo("Windows Startup", 
                                  "Successfully added to Windows startup.")
                
            except Exception as e:
                messagebox.showerror("Error", f"Error adding to Windows startup: {e}")
    
    def _apply_settings(self):
        """Apply settings without saving"""
        try:
            # Update values in config
            self._update_config_values()
            
            # Apply theme
            self.theme_manager.apply_theme(self.theme_var.get())
            
            # Call apply theme callback
            if callable(self.apply_theme_callback):
                self.apply_theme_callback()
                
            messagebox.showinfo("Settings", "Settings have been applied.")
            
        except Exception as e:
            messagebox.showerror("Error", f"Error applying settings: {e}")
    
    def _save_settings(self):
        """Save settings to config file"""
        try:
            # Update values in config
            self._update_config_values()
            
            # Save to file
            self.config.save()
            
            # Apply theme
            self.theme_manager.apply_theme(self.theme_var.get())
            
            # Call apply theme callback
            if callable(self.apply_theme_callback):
                self.apply_theme_callback()
                
            messagebox.showinfo("Settings", "Settings have been saved.")
            
        except Exception as e:
            messagebox.showerror("Error", f"Error saving settings: {e}")
    
    def _update_config_values(self):
        """Update config with values from UI"""
        # General tab
        self.config.set("database", "path", self.db_path_var.get())
        self.config.set("database", "retention_days", int(self.retention_var.get()))
        self.config.set("database", "compression_enabled", self.compression_var.get())
        self.config.set("ui", "minimize_to_tray", self.minimize_var.get())
        self.config.set("scheduler", "run_at_startup", self.autostart_var.get())
        self.config.set("ui", "start_minimized", self.start_min_var.get())
        self.config.set("ui", "default_view", self.default_view_var.get())
        self.config.set("updates", "check_on_startup", self.check_updates_var.get())
        self.config.set("updates", "auto_update", self.auto_update_var.get())
        
        # Appearance tab
        self.config.set("ui", "theme", self.theme_var.get())
        self.config.set("ui", "font_size", int(self.font_size_var.get()))
        self.config.set("ui", "enable_animations", self.animations_var.get())
        self.config.set("ui", "refresh_rate_ms", int(self.refresh_var.get()))
        
        # Performance tab
        self.config.set("system", "memory_limit_mb", int(self.memory_limit_var.get()))
        self.config.set("system", "cpu_usage_limit", int(self.cpu_limit_var.get()))
        self.config.set("system", "low_resource_mode", self.low_resource_var.get())
        self.config.set("system", "pause_on_battery", self.battery_var.get())
        self.config.set("scraper", "batch_size", int(self.batch_size_var.get()))
        self.config.set("scraper", "headless", self.headless_var.get())
        self.config.set("scraper", "disable_images", self.disable_images_var.get())
        self.config.set("scraper", "use_simplified_parser", self.simple_parser_var.get())
        self.config.set("analysis", "precompute_common_metrics", self.precompute_var.get())
        self.config.set("analysis", "cache_results", self.cache_var.get())
        self.config.set("analysis", "cache_ttl_minutes", int(self.cache_ttl_var.get()))
        
        # Advanced tab
        self.config.set("database", "vacuum_threshold", float(self.vacuum_var.get()))
        self.config.set("scraper", "retry_attempts", int(self.retry_var.get()))
        self.config.set("scraper", "backoff_factor", float(self.backoff_var.get()))
        self.config.set("scraper", "save_raw_html", self.save_html_var.get())
    
    def _reset_defaults(self):
        """Reset settings to defaults"""
        if messagebox.askyesno("Reset to Defaults", 
                              "This will reset all settings to their default values. Continue?"):
            # Reset config
            self.config.reset_to_defaults()
            
            # Reload settings
            self._load_settings()
            
            messagebox.showinfo("Settings", "Settings have been reset to defaults.")
    
    def refresh(self):
        """Refresh settings from config"""
        self._load_settings()
    
    def cleanup(self):
        """Clean up resources"""
        # Nothing to clean up for this frame
        pass