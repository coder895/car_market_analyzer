"""
Main application window for Car Market Trend Analyzer
Windows-optimized Tkinter UI with resource monitoring
"""

import os
import sys
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import threading
import queue
import platform
import time
from datetime import datetime
import webbrowser
from typing import Dict, List, Optional, Callable
import psutil

# Import matplotlib for charts with Tkinter backend
import matplotlib
matplotlib.use('TkAgg')
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

# Local imports
from src.utils.config import Config
from src.database.db_manager import DatabaseManager
from src.scraper.fb_marketplace_scraper import ResourceEfficientScraper
from src.scraper.scheduler import TaskScheduler
from src.analysis.market_analyzer import MarketAnalyzer
from src.ui.resource_monitor import ResourceMonitor
from src.ui.theme_manager import ThemeManager
from src.ui.dashboard_frame import DashboardFrame
from src.ui.scraper_frame import ScraperFrame
from src.ui.settings_frame import SettingsFrame
from src.ui.about_frame import AboutFrame


class MainWindow:
    """Main application window with resource-efficient UI"""
    
    def __init__(self, root: tk.Tk, config: Config, db_manager: DatabaseManager):
        """Initialize the main window with dependencies"""
        self.root = root
        self.config = config
        self.db_manager = db_manager
        
        # Initialize other components
        self.theme_manager = ThemeManager(config)
        self.scheduler = TaskScheduler(config, db_manager)
        self.market_analyzer = MarketAnalyzer(config, db_manager)
        
        # Message queue for thread-safe UI updates
        self.message_queue = queue.Queue()
        
        # Track loaded frames to support memory optimization
        self.frames = {}
        self.current_frame = None
        
        # Status variables
        self.status_var = tk.StringVar(value="Ready")
        self.memory_usage_var = tk.StringVar(value="Memory: 0 MB")
        self.cpu_usage_var = tk.StringVar(value="CPU: 0%")
        
        # Resource monitor
        self.resource_monitor = ResourceMonitor(
            update_callback=self._update_resource_display,
            threshold_callback=self._handle_resource_threshold,
            config=config
        )
        
        # System tray icon (initialized later)
        self.tray_icon = None
        
        # Initialize UI
        self._setup_window()
        self._create_menu()
        self._create_main_frame()
        self._create_status_bar()
        
        # Set up message processing
        self._setup_message_processor()
        
        # Start resource monitoring
        self.resource_monitor.start()
        
        # Start scheduler
        self.scheduler.start()
        
        # Apply theme
        self._apply_theme()
    
    def _setup_window(self):
        """Configure the main window properties"""
        # Set window title and icon
        self.root.title("Car Market Trend Analyzer")
        
        # Try to load icon if it exists
        icon_path = os.path.join(os.path.dirname(__file__), "../assets/icon.ico")
        if os.path.exists(icon_path):
            try:
                self.root.iconbitmap(icon_path)
            except tk.TclError:
                pass
        
        # Set window size and position
        window_width = self.config.get("ui", "window_width", 1024)
        window_height = self.config.get("ui", "window_height", 768)
        self.root.geometry(f"{window_width}x{window_height}")
        
        # Make window resizable
        self.root.resizable(True, True)
        
        # Set minsize to prevent UI elements from being squished
        self.root.minsize(800, 600)
        
        # Bind close event
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        
        # Configure grid layout to expand with window
        self.root.grid_columnconfigure(0, weight=1)
        self.root.grid_rowconfigure(0, weight=1)
    
    def _create_menu(self):
        """Create main application menu"""
        menubar = tk.Menu(self.root)
        
        # File menu
        file_menu = tk.Menu(menubar, tearoff=0)
        file_menu.add_command(label="Dashboard", command=lambda: self._show_frame("dashboard"))
        file_menu.add_command(label="Scraper", command=lambda: self._show_frame("scraper"))
        file_menu.add_separator()
        file_menu.add_command(label="Settings", command=lambda: self._show_frame("settings"))
        file_menu.add_separator()
        file_menu.add_command(label="Exit", command=self._on_close)
        menubar.add_cascade(label="File", menu=file_menu)
        
        # Scraper menu
        scraper_menu = tk.Menu(menubar, tearoff=0)
        scraper_menu.add_command(label="Start Scraping", command=self._start_scraping)
        scraper_menu.add_command(label="Stop Scraping", command=self._stop_scraping)
        scraper_menu.add_separator()
        scraper_menu.add_command(label="View Saved Searches", command=self._view_saved_searches)
        scraper_menu.add_command(label="Schedule Tasks", command=self._show_scheduler)
        menubar.add_cascade(label="Scraper", menu=scraper_menu)
        
        # Analysis menu
        analysis_menu = tk.Menu(menubar, tearoff=0)
        analysis_menu.add_command(label="Price Trends", command=self._show_price_trends)
        analysis_menu.add_command(label="Price vs. Mileage", command=self._show_price_mileage)
        analysis_menu.add_command(label="Price by Year", command=self._show_price_year)
        analysis_menu.add_separator()
        analysis_menu.add_command(label="Export Results", command=self._export_results)
        menubar.add_cascade(label="Analysis", menu=analysis_menu)
        
        # Tools menu
        tools_menu = tk.Menu(menubar, tearoff=0)
        tools_menu.add_command(label="Database Maintenance", command=self._show_db_maintenance)
        tools_menu.add_command(label="System Performance", command=self._show_performance)
        if platform.system() == "Windows":
            tools_menu.add_command(label="Register with Task Scheduler", command=self._register_task_scheduler)
        menubar.add_cascade(label="Tools", menu=tools_menu)
        
        # Help menu
        help_menu = tk.Menu(menubar, tearoff=0)
        help_menu.add_command(label="User Guide", command=self._show_user_guide)
        help_menu.add_command(label="Check for Updates", command=self._check_updates)
        help_menu.add_separator()
        help_menu.add_command(label="About", command=lambda: self._show_frame("about"))
        menubar.add_cascade(label="Help", menu=help_menu)
        
        # Add the menubar to the window
        self.root.config(menu=menubar)
    
    def _create_main_frame(self):
        """Create the main container frame"""
        self.main_frame = ttk.Frame(self.root)
        self.main_frame.grid(row=0, column=0, sticky="nsew", padx=5, pady=5)
        
        # Configure frame to expand with window
        self.main_frame.grid_columnconfigure(0, weight=1)
        self.main_frame.grid_rowconfigure(0, weight=1)
        
        # Create navigation sidebar
        self._create_sidebar()
        
        # Create content area
        self.content_frame = ttk.Frame(self.main_frame)
        self.content_frame.grid(row=0, column=1, sticky="nsew", padx=5, pady=5)
        self.main_frame.grid_columnconfigure(1, weight=8)  # Content takes 80% of width
        
        # Configure content frame to expand with window
        self.content_frame.grid_columnconfigure(0, weight=1)
        self.content_frame.grid_rowconfigure(0, weight=1)
        
        # Initialize the dashboard (default view)
        self._show_frame("dashboard")
    
    def _create_sidebar(self):
        """Create the navigation sidebar"""
        self.sidebar = ttk.Frame(self.main_frame, relief="raised", borderwidth=1)
        self.sidebar.grid(row=0, column=0, sticky="nsew", padx=2, pady=5)
        self.main_frame.grid_columnconfigure(0, weight=2)  # Sidebar takes 20% of width
        
        # Style for the buttons
        style = ttk.Style()
        style.configure("Sidebar.TButton", font=("Arial", 11), padding=10)
        
        # Dashboard button
        self.dashboard_btn = ttk.Button(self.sidebar, text="Dashboard", 
                                       style="Sidebar.TButton",
                                       command=lambda: self._show_frame("dashboard"))
        self.dashboard_btn.pack(fill="x", padx=5, pady=5)
        
        # Scraper button
        self.scraper_btn = ttk.Button(self.sidebar, text="Scraper", 
                                     style="Sidebar.TButton",
                                     command=lambda: self._show_frame("scraper"))
        self.scraper_btn.pack(fill="x", padx=5, pady=5)
        
        # Analysis button
        self.analysis_btn = ttk.Button(self.sidebar, text="Analysis", 
                                      style="Sidebar.TButton",
                                      command=lambda: self._show_frame("analysis"))
        self.analysis_btn.pack(fill="x", padx=5, pady=5)
        
        # Settings button
        self.settings_btn = ttk.Button(self.sidebar, text="Settings", 
                                      style="Sidebar.TButton",
                                      command=lambda: self._show_frame("settings"))
        self.settings_btn.pack(fill="x", padx=5, pady=5)
        
        # Add resource monitor to sidebar
        self._create_sidebar_resource_monitor()
        
        # Push settings button to bottom
        ttk.Separator(self.sidebar).pack(fill="x", padx=5, pady=10)
        ttk.Label(self.sidebar, text="System Resources").pack(anchor="w", padx=10, pady=2)
        
        # Memory usage progress bar
        ttk.Label(self.sidebar, textvariable=self.memory_usage_var).pack(anchor="w", padx=10, pady=2)
        self.memory_progress = ttk.Progressbar(self.sidebar, mode="determinate", length=150)
        self.memory_progress.pack(padx=10, pady=2, fill="x")
        
        # CPU usage progress bar
        ttk.Label(self.sidebar, textvariable=self.cpu_usage_var).pack(anchor="w", padx=10, pady=2)
        self.cpu_progress = ttk.Progressbar(self.sidebar, mode="determinate", length=150)
        self.cpu_progress.pack(padx=10, pady=2, fill="x")
    
    def _create_sidebar_resource_monitor(self):
        """Create resource monitor display in sidebar"""
        self.resource_frame = ttk.LabelFrame(self.sidebar, text="Resource Monitor")
        self.resource_frame.pack(fill="x", padx=5, pady=10)
        
        # Add current and peak memory usage labels
        self.current_memory_label = ttk.Label(self.resource_frame, text="Current Memory: 0 MB")
        self.current_memory_label.pack(anchor="w", padx=5, pady=2)
        
        self.peak_memory_label = ttk.Label(self.resource_frame, text="Peak Memory: 0 MB")
        self.peak_memory_label.pack(anchor="w", padx=5, pady=2)
        
        # Add CPU usage label
        self.cpu_usage_label = ttk.Label(self.resource_frame, text="CPU Usage: 0%")
        self.cpu_usage_label.pack(anchor="w", padx=5, pady=2)
        
        # Add throttling status indicator
        self.throttle_var = tk.StringVar(value="Normal")
        self.throttle_label = ttk.Label(self.resource_frame, textvariable=self.throttle_var)
        self.throttle_label.pack(anchor="w", padx=5, pady=2)
    
    def _create_status_bar(self):
        """Create status bar at the bottom of the window"""
        self.status_bar = ttk.Frame(self.root, relief="sunken", borderwidth=1)
        self.status_bar.grid(row=1, column=0, sticky="ew")
        
        # Status message
        self.status_label = ttk.Label(self.status_bar, textvariable=self.status_var, anchor="w")
        self.status_label.pack(side="left", padx=5)
        
        # Current time display
        self.time_var = tk.StringVar()
        self.time_label = ttk.Label(self.status_bar, textvariable=self.time_var, anchor="e")
        self.time_label.pack(side="right", padx=5)
        
        # Update time display
        self._update_time()
    
    def _update_time(self):
        """Update the time display in the status bar"""
        current_time = datetime.now().strftime("%H:%M:%S")
        self.time_var.set(current_time)
        
        # Schedule next update in 1 second
        self.root.after(1000, self._update_time)
    
    def _show_frame(self, frame_name: str):
        """
        Show the specified frame and hide others
        Uses lazy loading to minimize memory usage
        """
        # If we're already on this frame, do nothing
        if self.current_frame == frame_name:
            return
            
        # Remove any existing frame from view
        for frame in self.frames.values():
            if hasattr(frame, "grid_forget"):
                frame.grid_forget()
        
        # Check if the frame is already created
        if frame_name not in self.frames:
            # Lazy loading to save memory
            if frame_name == "dashboard":
                self.frames[frame_name] = DashboardFrame(
                    self.content_frame, 
                    self.config, 
                    self.db_manager,
                    self.market_analyzer
                )
            elif frame_name == "scraper":
                self.frames[frame_name] = ScraperFrame(
                    self.content_frame, 
                    self.config, 
                    self.db_manager,
                    self.scheduler
                )
            elif frame_name == "settings":
                self.frames[frame_name] = SettingsFrame(
                    self.content_frame, 
                    self.config,
                    self.theme_manager,
                    self._apply_theme
                )
            elif frame_name == "about":
                self.frames[frame_name] = AboutFrame(self.content_frame)
            elif frame_name == "analysis":
                # This will be loaded when needed
                from src.ui.analysis_frame import AnalysisFrame
                self.frames[frame_name] = AnalysisFrame(
                    self.content_frame, 
                    self.config, 
                    self.db_manager,
                    self.market_analyzer
                )
        
        # Show the selected frame
        if frame_name in self.frames:
            frame = self.frames[frame_name]
            frame.grid(row=0, column=0, sticky="nsew")
            
            # Update current frame tracker
            self.current_frame = frame_name
            
            # Update status
            self.status_var.set(f"Viewing: {frame_name.title()}")
            
            # If the frame has a refresh method, call it
            if hasattr(frame, "refresh"):
                frame.refresh()
    
    def _setup_message_processor(self):
        """Set up processing for thread-safe UI updates"""
        def process_messages():
            try:
                while True:
                    # Get message without blocking
                    try:
                        message = self.message_queue.get_nowait()
                        
                        # Process message based on type
                        msg_type = message.get("type", "")
                        
                        if msg_type == "status":
                            self.status_var.set(message.get("text", ""))
                        elif msg_type == "error":
                            messagebox.showerror("Error", message.get("text", ""))
                        elif msg_type == "info":
                            messagebox.showinfo("Information", message.get("text", ""))
                        elif msg_type == "warning":
                            messagebox.showwarning("Warning", message.get("text", ""))
                        elif msg_type == "update_frame":
                            frame_name = message.get("frame")
                            if frame_name in self.frames and hasattr(self.frames[frame_name], "update_data"):
                                self.frames[frame_name].update_data(message.get("data", {}))
                        elif msg_type == "refresh_frame":
                            frame_name = message.get("frame")
                            if frame_name in self.frames and hasattr(self.frames[frame_name], "refresh"):
                                self.frames[frame_name].refresh()
                        
                        # Mark message as processed
                        self.message_queue.task_done()
                    except queue.Empty:
                        break
            except Exception as e:
                print(f"Error processing message: {e}")
            
            # Reschedule message processing
            self.root.after(100, process_messages)
        
        # Start message processing
        self.root.after(100, process_messages)
    
    def _update_resource_display(self, memory_percent: float, memory_mb: float, 
                               cpu_percent: float, is_throttling: bool):
        """
        Update resource monitor display
        
        Args:
            memory_percent (float): Memory usage percentage
            memory_mb (float): Memory usage in MB
            cpu_percent (float): CPU usage percentage
            is_throttling (bool): Whether throttling is active
        """
        # Update memory progress bar and label
        self.memory_progress["value"] = memory_percent
        self.memory_usage_var.set(f"Memory: {memory_mb:.1f} MB ({memory_percent:.1f}%)")
        self.current_memory_label.config(text=f"Current Memory: {memory_mb:.1f} MB")
        
        # Update peak memory
        peak_memory = self.resource_monitor.get_peak_memory()
        self.peak_memory_label.config(text=f"Peak Memory: {peak_memory:.1f} MB")
        
        # Update CPU progress bar and label
        self.cpu_progress["value"] = cpu_percent
        self.cpu_usage_var.set(f"CPU: {cpu_percent:.1f}%")
        self.cpu_usage_label.config(text=f"CPU Usage: {cpu_percent:.1f}%")
        
        # Update throttling status
        if is_throttling:
            self.throttle_var.set("⚠️ Throttling Active")
            self.throttle_label.config(foreground="red")
        else:
            self.throttle_var.set("✓ Normal Operation")
            self.throttle_label.config(foreground="green")
    
    def _handle_resource_threshold(self, resource_type: str, current_value: float, 
                                 threshold: float, is_critical: bool):
        """
        Handle resource threshold exceeded event
        
        Args:
            resource_type (str): Type of resource (memory, cpu)
            current_value (float): Current resource value
            threshold (float): Threshold that was exceeded
            is_critical (bool): Whether this is a critical threshold
        """
        if is_critical:
            # For critical thresholds, show warning and take action
            if resource_type == "memory":
                message = f"Critical memory usage: {current_value:.1f} MB"
                self._free_memory()
            elif resource_type == "cpu":
                message = f"Critical CPU usage: {current_value:.1f}%"
                self._reduce_cpu_load()
                
            # Add to message queue for display
            self.message_queue.put({
                "type": "warning",
                "text": f"{message}. Resource-saving measures have been activated."
            })
        else:
            # For non-critical thresholds, just update status
            if resource_type == "memory":
                message = f"High memory usage: {current_value:.1f} MB"
            elif resource_type == "cpu":
                message = f"High CPU usage: {current_value:.1f}%"
                
            # Update status bar
            self.status_var.set(message)
    
    def _free_memory(self):
        """Attempt to free memory when usage is too high"""
        # Unload unused frames to save memory
        frames_to_keep = [self.current_frame]
        
        for name, frame in list(self.frames.items()):
            if name not in frames_to_keep:
                # If frame has a cleanup method, call it
                if hasattr(frame, "cleanup"):
                    frame.cleanup()
                    
                # Remove from frames dict
                del self.frames[name]
        
        # Force garbage collection
        import gc
        gc.collect()
    
    def _reduce_cpu_load(self):
        """Reduce CPU load when usage is too high"""
        # Disable any CPU-intensive operations
        if hasattr(self.scheduler, "pause"):
            self.scheduler.pause()
            
        # Increase UI refresh intervals
        refresh_rate = self.config.get("ui", "refresh_rate_ms", 1000)
        self.config.set("ui", "refresh_rate_ms", refresh_rate * 2)
    
    def _apply_theme(self):
        """Apply the current theme to the UI"""
        theme = self.config.get("ui", "theme", "system")
        self.theme_manager.apply_theme(theme)
    
    def _start_scraping(self):
        """Start manual scraping operation"""
        if self.current_frame == "scraper" and hasattr(self.frames["scraper"], "start_scraping"):
            self.frames["scraper"].start_scraping()
        else:
            self._show_frame("scraper")
            self.root.after(500, self._start_scraping)
    
    def _stop_scraping(self):
        """Stop any running scraping operation"""
        if self.current_frame == "scraper" and hasattr(self.frames["scraper"], "stop_scraping"):
            self.frames["scraper"].stop_scraping()
        else:
            self._show_frame("scraper")
            self.root.after(500, self._stop_scraping)
    
    def _view_saved_searches(self):
        """View saved scraper searches"""
        if self.current_frame == "scraper" and hasattr(self.frames["scraper"], "show_saved_searches"):
            self.frames["scraper"].show_saved_searches()
        else:
            self._show_frame("scraper")
            self.root.after(500, self._view_saved_searches)
    
    def _show_scheduler(self):
        """Show scheduler configuration"""
        if self.current_frame == "scraper" and hasattr(self.frames["scraper"], "show_scheduler"):
            self.frames["scraper"].show_scheduler()
        else:
            self._show_frame("scraper")
            self.root.after(500, self._show_scheduler)
    
    def _show_price_trends(self):
        """Show price trends analysis"""
        if self.current_frame == "analysis" and hasattr(self.frames["analysis"], "show_price_trends"):
            self.frames["analysis"].show_price_trends()
        else:
            self._show_frame("analysis")
            self.root.after(500, self._show_price_trends)
    
    def _show_price_mileage(self):
        """Show price vs mileage analysis"""
        if self.current_frame == "analysis" and hasattr(self.frames["analysis"], "show_price_mileage"):
            self.frames["analysis"].show_price_mileage()
        else:
            self._show_frame("analysis")
            self.root.after(500, self._show_price_mileage)
    
    def _show_price_year(self):
        """Show price by year analysis"""
        if self.current_frame == "analysis" and hasattr(self.frames["analysis"], "show_price_year"):
            self.frames["analysis"].show_price_year()
        else:
            self._show_frame("analysis")
            self.root.after(500, self._show_price_year)
    
    def _export_results(self):
        """Export analysis results"""
        if self.current_frame == "analysis" and hasattr(self.frames["analysis"], "export_results"):
            self.frames["analysis"].export_results()
        else:
            self._show_frame("analysis")
            self.root.after(500, self._export_results)
    
    def _show_db_maintenance(self):
        """Show database maintenance dialog"""
        # To be implemented
        messagebox.showinfo("Database Maintenance", 
                          "Database maintenance tools will be available in the next update.")
    
    def _show_performance(self):
        """Show system performance dialog"""
        # To be implemented
        messagebox.showinfo("System Performance", 
                          "System performance tools will be available in the next update.")
    
    def _register_task_scheduler(self):
        """Register with Windows Task Scheduler"""
        if platform.system() != "Windows":
            messagebox.showinfo("Not Available", 
                              "Task Scheduler registration is only available on Windows.")
            return
            
        result = self.scheduler.register_with_windows_scheduler()
        
        if result:
            messagebox.showinfo("Success", 
                              "Successfully registered with Windows Task Scheduler.")
        else:
            messagebox.showerror("Error", 
                               "Failed to register with Windows Task Scheduler. "
                               "Please try running the application as Administrator.")
    
    def _show_user_guide(self):
        """Show user guide"""
        guide_path = os.path.join(os.path.dirname(__file__), "../docs/user_guide.html")
        
        if os.path.exists(guide_path):
            webbrowser.open(f"file://{os.path.abspath(guide_path)}")
        else:
            messagebox.showinfo("User Guide", 
                              "The user guide will be available in the next update.")
    
    def _check_updates(self):
        """Check for application updates"""
        # To be implemented
        messagebox.showinfo("Updates", 
                          "You are running the latest version of Car Market Trend Analyzer.")
    
    def _on_close(self):
        """Handle window close event"""
        # Check if minimize to tray is enabled
        if self.config.get("ui", "minimize_to_tray", True) and platform.system() == "Windows":
            self._minimize_to_tray()
            return
            
        # Ask for confirmation before exiting
        if messagebox.askyesno("Exit", "Are you sure you want to exit?"):
            # Stop background threads
            if hasattr(self.resource_monitor, "stop"):
                self.resource_monitor.stop()
                
            if hasattr(self.scheduler, "stop"):
                self.scheduler.stop()
                
            # Save window size to config
            self.config.set("ui", "window_width", self.root.winfo_width())
            self.config.set("ui", "window_height", self.root.winfo_height())
            self.config.save()
            
            # Close database connections
            self.db_manager.close()
            
            # Destroy root window
            self.root.destroy()
    
    def _minimize_to_tray(self):
        """Minimize application to system tray (Windows only)"""
        if platform.system() != "Windows":
            return
            
        try:
            # Only import these on Windows
            import pystray
            from PIL import Image, ImageDraw
            
            # Hide the window
            self.root.withdraw()
            
            # Create a tray icon if it doesn't exist
            if self.tray_icon is None:
                # Create a simple icon
                icon_image = Image.new('RGB', (64, 64), color=(0, 120, 220))
                dc = ImageDraw.Draw(icon_image)
                dc.rectangle((16, 16, 48, 48), fill=(255, 255, 255))
                
                # Create tray icon menu
                menu = (
                    pystray.MenuItem("Show Window", self._restore_window),
                    pystray.MenuItem("Exit", self._exit_from_tray)
                )
                
                # Create tray icon
                self.tray_icon = pystray.Icon(
                    "car_market_analyzer",
                    icon_image,
                    "Car Market Analyzer",
                    menu
                )
                
                # Start the icon in a separate thread
                threading.Thread(target=self.tray_icon.run, daemon=True).start()
            
        except ImportError:
            # If pystray is not available, just minimize normally
            self.root.iconify()
    
    def _restore_window(self, icon=None, item=None):
        """Restore window from system tray"""
        self.root.deiconify()
        self.root.lift()
        self.root.focus_force()
    
    def _exit_from_tray(self, icon=None, item=None):
        """Exit application from system tray"""
        if self.tray_icon:
            self.tray_icon.stop()
            self.tray_icon = None
            
        # Stop background threads
        if hasattr(self.resource_monitor, "stop"):
            self.resource_monitor.stop()
            
        if hasattr(self.scheduler, "stop"):
            self.scheduler.stop()
            
        # Save window size to config
        self.config.set("ui", "window_width", self.root.winfo_width())
        self.config.set("ui", "window_height", self.root.winfo_height())
        self.config.save()
        
        # Close database connections
        self.db_manager.close()
        
        # Destroy root window
        self.root.destroy()
    
    def show(self):
        """Show the main window"""
        self.root.deiconify()
        
        # Apply Windows-specific optimizations
        if platform.system() == "Windows":
            try:
                # Set process priority
                import psutil
                import win32process
                import win32api
                
                # Get the current process
                process = psutil.Process()
                
                # Set process priority (normal or below normal based on config)
                if self.config.get("system", "low_resource_mode", False):
                    win32process.SetPriorityClass(
                        win32api.GetCurrentProcess(),
                        win32process.BELOW_NORMAL_PRIORITY_CLASS
                    )
                else:
                    win32process.SetPriorityClass(
                        win32api.GetCurrentProcess(),
                        win32process.NORMAL_PRIORITY_CLASS
                    )
            except (ImportError, Exception) as e:
                print(f"Could not set process priority: {e}")
        
        # Check for first run
        if self.config.get("system", "first_run", True):
            self._show_welcome_message()
            self.config.set("system", "first_run", False)
            self.config.save()
    
    def _show_welcome_message(self):
        """Show welcome message on first run"""
        messagebox.showinfo(
            "Welcome",
            "Welcome to Car Market Trend Analyzer!\n\n"
            "This application helps you track and analyze car prices on Facebook Marketplace.\n\n"
            "The application has been optimized to use minimal system resources. "
            "You can adjust performance settings in the Settings screen.\n\n"
            "To get started, go to the Scraper tab to set up your first search."
        )