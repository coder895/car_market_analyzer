"""
Scraper frame for controlling and monitoring FB Marketplace scraping
"""

import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
import threading
import time
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional

# Local imports
from src.utils.config import Config
from src.database.db_manager import DatabaseManager
from src.scraper.fb_marketplace_scraper import ResourceEfficientScraper
from src.scraper.scheduler import TaskScheduler


class ScraperFrame(ttk.Frame):
    """Frame for controlling and monitoring Facebook Marketplace scraping"""
    
    def __init__(self, parent, config: Config, db_manager: DatabaseManager,
                scheduler: TaskScheduler):
        """Initialize the scraper frame"""
        super().__init__(parent)
        self.parent = parent
        self.config = config
        self.db_manager = db_manager
        self.scheduler = scheduler
        
        # Configure grid layout
        self.grid_columnconfigure(0, weight=1)
        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(0, weight=0)  # Header
        self.grid_rowconfigure(1, weight=0)  # Controls
        self.grid_rowconfigure(2, weight=1)  # Log/monitor
        self.grid_rowconfigure(3, weight=1)  # Results
        
        # Scraper state
        self.scraper = None
        self.scraper_thread = None
        self.is_scraping = False
        self.log_queue = []
        self.max_log_entries = 100
        
        # Saved searches
        self.saved_searches = []
        
        # Create UI elements
        self._create_header()
        self._create_controls()
        self._create_log_monitor()
        self._create_results_area()
        
        # Load saved searches
        self._load_saved_searches()
    
    def _create_header(self):
        """Create the scraper header"""
        header_frame = ttk.Frame(self)
        header_frame.grid(row=0, column=0, columnspan=2, sticky="ew", padx=10, pady=(10, 5))
        
        # Scraper title
        title_label = ttk.Label(header_frame, text="Facebook Marketplace Scraper", 
                              style="Title.TLabel")
        title_label.pack(side=tk.LEFT, padx=5)
        
        # Status indicator
        self.status_frame = ttk.Frame(header_frame)
        self.status_frame.pack(side=tk.RIGHT, padx=5)
        
        self.status_label = ttk.Label(self.status_frame, text="Status: ")
        self.status_label.pack(side=tk.LEFT)
        
        self.status_value = ttk.Label(self.status_frame, text="Ready")
        self.status_value.pack(side=tk.LEFT)
        
        # Set initial status color
        self._update_status("Ready", "black")
    
    def _create_controls(self):
        """Create scraper control panel"""
        controls_frame = ttk.LabelFrame(self, text="Scraping Controls")
        controls_frame.grid(row=1, column=0, columnspan=2, sticky="ew", padx=10, pady=5)
        
        # URL input area
        url_frame = ttk.Frame(controls_frame)
        url_frame.pack(fill=tk.X, padx=10, pady=5)
        
        ttk.Label(url_frame, text="Facebook Marketplace URL:").pack(side=tk.LEFT)
        
        # Create URL combobox with saved searches
        self.url_var = tk.StringVar()
        self.url_combo = ttk.Combobox(url_frame, textvariable=self.url_var, width=50)
        self.url_combo.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)
        
        # Default search URL
        default_url = "https://www.facebook.com/marketplace/category/vehicles"
        self.url_var.set(default_url)
        
        # Load button
        self.load_btn = ttk.Button(url_frame, text="Load", command=self._load_selected_search)
        self.load_btn.pack(side=tk.LEFT, padx=5)
        
        # Save button
        self.save_btn = ttk.Button(url_frame, text="Save", command=self._save_current_search)
        self.save_btn.pack(side=tk.LEFT, padx=5)
        
        # Options frame
        options_frame = ttk.Frame(controls_frame)
        options_frame.pack(fill=tk.X, padx=10, pady=5)
        
        # Max listings
        ttk.Label(options_frame, text="Max Listings:").grid(row=0, column=0, sticky="w", padx=5, pady=2)
        self.max_listings_var = tk.StringVar(value=str(self.config.get("scraper", "max_listings", 500)))
        max_spinbox = ttk.Spinbox(options_frame, from_=10, to=2000, increment=10, 
                                 textvariable=self.max_listings_var, width=5)
        max_spinbox.grid(row=0, column=1, sticky="w", padx=5, pady=2)
        
        # Batch size
        ttk.Label(options_frame, text="Batch Size:").grid(row=0, column=2, sticky="w", padx=5, pady=2)
        self.batch_size_var = tk.StringVar(value=str(self.config.get("scraper", "batch_size", 50)))
        batch_spinbox = ttk.Spinbox(options_frame, from_=10, to=200, increment=10, 
                                  textvariable=self.batch_size_var, width=5)
        batch_spinbox.grid(row=0, column=3, sticky="w", padx=5, pady=2)
        
        # Headless mode
        self.headless_var = tk.BooleanVar(value=self.config.get("scraper", "headless", True))
        headless_check = ttk.Checkbutton(options_frame, text="Headless Mode", 
                                       variable=self.headless_var)
        headless_check.grid(row=0, column=4, sticky="w", padx=5, pady=2)
        
        # Disable images
        self.disable_images_var = tk.BooleanVar(value=self.config.get("scraper", "disable_images", True))
        images_check = ttk.Checkbutton(options_frame, text="Disable Images", 
                                     variable=self.disable_images_var)
        images_check.grid(row=0, column=5, sticky="w", padx=5, pady=2)
        
        # Low resource mode
        self.low_resource_var = tk.BooleanVar(value=self.config.get("system", "low_resource_mode", False))
        low_resource_check = ttk.Checkbutton(options_frame, text="Low Resource Mode", 
                                           variable=self.low_resource_var)
        low_resource_check.grid(row=0, column=6, sticky="w", padx=5, pady=2)
        
        # Action buttons
        buttons_frame = ttk.Frame(controls_frame)
        buttons_frame.pack(fill=tk.X, padx=10, pady=10)
        
        # Start button
        self.start_btn = ttk.Button(buttons_frame, text="Start Scraping", 
                                  command=self.start_scraping,
                                  style="Accent.TButton")
        self.start_btn.pack(side=tk.LEFT, padx=5)
        
        # Stop button (disabled initially)
        self.stop_btn = ttk.Button(buttons_frame, text="Stop Scraping", 
                                 command=self.stop_scraping, state=tk.DISABLED)
        self.stop_btn.pack(side=tk.LEFT, padx=5)
        
        # Pause/Resume button (disabled initially)
        self.pause_btn = ttk.Button(buttons_frame, text="Pause", 
                                  command=self._toggle_pause, state=tk.DISABLED)
        self.pause_btn.pack(side=tk.LEFT, padx=5)
        
        # Schedule button
        self.schedule_btn = ttk.Button(buttons_frame, text="Schedule", 
                                     command=self.show_scheduler)
        self.schedule_btn.pack(side=tk.LEFT, padx=5)
        
        # Saved searches button
        self.searches_btn = ttk.Button(buttons_frame, text="Saved Searches", 
                                     command=self.show_saved_searches)
        self.searches_btn.pack(side=tk.LEFT, padx=5)
        
        # Apply settings button
        self.apply_btn = ttk.Button(buttons_frame, text="Apply Settings", 
                                  command=self._apply_settings)
        self.apply_btn.pack(side=tk.RIGHT, padx=5)
    
    def _create_log_monitor(self):
        """Create scraper log and monitoring area"""
        log_frame = ttk.LabelFrame(self, text="Scraper Log")
        log_frame.grid(row=2, column=0, sticky="nsew", padx=10, pady=5)
        
        # Configure log frame
        log_frame.grid_columnconfigure(0, weight=1)
        log_frame.grid_rowconfigure(0, weight=1)
        
        # Create log text widget with scrollbar
        log_scroll = ttk.Scrollbar(log_frame)
        log_scroll.grid(row=0, column=1, sticky="ns")
        
        self.log_text = tk.Text(log_frame, height=15, width=60, 
                              font=("Consolas", 9),
                              yscrollcommand=log_scroll.set,
                              state=tk.DISABLED)
        self.log_text.grid(row=0, column=0, sticky="nsew", padx=5, pady=5)
        
        log_scroll.config(command=self.log_text.yview)
        
        # Create monitoring area
        monitor_frame = ttk.LabelFrame(self, text="Resource Monitor")
        monitor_frame.grid(row=2, column=1, sticky="nsew", padx=10, pady=5)
        
        # Configure monitor frame
        monitor_frame.grid_columnconfigure(0, weight=1)
        monitor_frame.grid_rowconfigure(0, weight=0)  # Stats
        monitor_frame.grid_rowconfigure(1, weight=1)  # Progress
        
        # Create stats frame
        stats_frame = ttk.Frame(monitor_frame)
        stats_frame.grid(row=0, column=0, sticky="ew", padx=5, pady=5)
        
        # Configure 2x2 grid for stats
        stats_frame.grid_columnconfigure(0, weight=1)
        stats_frame.grid_columnconfigure(1, weight=1)
        
        # Memory usage
        self.memory_var = tk.StringVar(value="Memory: 0 MB")
        ttk.Label(stats_frame, text="Memory Usage:").grid(row=0, column=0, sticky="w", padx=5, pady=2)
        ttk.Label(stats_frame, textvariable=self.memory_var).grid(row=0, column=1, sticky="w", padx=5, pady=2)
        
        # CPU usage
        self.cpu_var = tk.StringVar(value="CPU: 0%")
        ttk.Label(stats_frame, text="CPU Usage:").grid(row=1, column=0, sticky="w", padx=5, pady=2)
        ttk.Label(stats_frame, textvariable=self.cpu_var).grid(row=1, column=1, sticky="w", padx=5, pady=2)
        
        # Elapsed time
        self.time_var = tk.StringVar(value="Time: 0:00")
        ttk.Label(stats_frame, text="Elapsed Time:").grid(row=2, column=0, sticky="w", padx=5, pady=2)
        ttk.Label(stats_frame, textvariable=self.time_var).grid(row=2, column=1, sticky="w", padx=5, pady=2)
        
        # Throttling status
        self.throttle_var = tk.StringVar(value="No")
        ttk.Label(stats_frame, text="Throttling:").grid(row=3, column=0, sticky="w", padx=5, pady=2)
        self.throttle_label = ttk.Label(stats_frame, textvariable=self.throttle_var)
        self.throttle_label.grid(row=3, column=1, sticky="w", padx=5, pady=2)
        
        # Progress section
        progress_frame = ttk.Frame(monitor_frame)
        progress_frame.grid(row=1, column=0, sticky="nsew", padx=5, pady=5)
        
        # Configure progress frame
        progress_frame.grid_columnconfigure(0, weight=1)
        progress_frame.grid_rowconfigure(0, weight=0)  # Labels
        progress_frame.grid_rowconfigure(1, weight=0)  # Progress bar
        progress_frame.grid_rowconfigure(2, weight=0)  # Stats
        progress_frame.grid_rowconfigure(3, weight=1)  # Spacer
        
        # Progress labels
        progress_labels = ttk.Frame(progress_frame)
        progress_labels.grid(row=0, column=0, sticky="ew", padx=5, pady=2)
        
        ttk.Label(progress_labels, text="Scraping Progress:").pack(side=tk.LEFT)
        self.progress_text = ttk.Label(progress_labels, text="0/0 listings (0%)")
        self.progress_text.pack(side=tk.RIGHT)
        
        # Progress bar
        self.progress_var = tk.DoubleVar(value=0)
        self.progress_bar = ttk.Progressbar(progress_frame, 
                                          variable=self.progress_var,
                                          maximum=100)
        self.progress_bar.grid(row=1, column=0, sticky="ew", padx=5, pady=5)
        
        # Stats counts
        counts_frame = ttk.Frame(progress_frame)
        counts_frame.grid(row=2, column=0, sticky="ew", padx=5, pady=2)
        
        # Use grid for better alignment
        counts_frame.grid_columnconfigure(1, weight=1)
        counts_frame.grid_columnconfigure(3, weight=1)
        counts_frame.grid_columnconfigure(5, weight=1)
        
        ttk.Label(counts_frame, text="Found:").grid(row=0, column=0, sticky="w", padx=5)
        self.found_var = tk.StringVar(value="0")
        ttk.Label(counts_frame, textvariable=self.found_var, width=8).grid(row=0, column=1, sticky="w")
        
        ttk.Label(counts_frame, text="New:").grid(row=0, column=2, sticky="w", padx=5)
        self.new_var = tk.StringVar(value="0")
        ttk.Label(counts_frame, textvariable=self.new_var, width=8).grid(row=0, column=3, sticky="w")
        
        ttk.Label(counts_frame, text="Updated:").grid(row=0, column=4, sticky="w", padx=5)
        self.updated_var = tk.StringVar(value="0")
        ttk.Label(counts_frame, textvariable=self.updated_var, width=8).grid(row=0, column=5, sticky="w")
    
    def _create_results_area(self):
        """Create area showing recently scraped listings"""
        results_frame = ttk.LabelFrame(self, text="Recent Listings")
        results_frame.grid(row=3, column=0, columnspan=2, sticky="nsew", padx=10, pady=5)
        
        # Configure results frame
        results_frame.grid_columnconfigure(0, weight=1)
        results_frame.grid_rowconfigure(0, weight=1)
        
        # Create treeview with scrollbars
        tree_scroll_y = ttk.Scrollbar(results_frame)
        tree_scroll_y.grid(row=0, column=1, sticky="ns")
        
        tree_scroll_x = ttk.Scrollbar(results_frame, orient="horizontal")
        tree_scroll_x.grid(row=1, column=0, sticky="ew")
        
        self.results_tree = ttk.Treeview(results_frame, columns=(
            "title", "price", "year", "make", "model", "mileage", "location", "date"
        ), show="headings", yscrollcommand=tree_scroll_y.set, xscrollcommand=tree_scroll_x.set)
        
        # Configure column headings
        self.results_tree.heading("title", text="Title")
        self.results_tree.heading("price", text="Price")
        self.results_tree.heading("year", text="Year")
        self.results_tree.heading("make", text="Make")
        self.results_tree.heading("model", text="Model")
        self.results_tree.heading("mileage", text="Mileage")
        self.results_tree.heading("location", text="Location")
        self.results_tree.heading("date", text="Date")
        
        # Configure column widths
        self.results_tree.column("title", width=200)
        self.results_tree.column("price", width=80, anchor="e")
        self.results_tree.column("year", width=60, anchor="center")
        self.results_tree.column("make", width=100)
        self.results_tree.column("model", width=100)
        self.results_tree.column("mileage", width=80, anchor="e")
        self.results_tree.column("location", width=120)
        self.results_tree.column("date", width=100, anchor="center")
        
        self.results_tree.grid(row=0, column=0, sticky="nsew")
        
        tree_scroll_y.config(command=self.results_tree.yview)
        tree_scroll_x.config(command=self.results_tree.xview)
        
        # Add right-click menu for listings
        self.tree_menu = tk.Menu(self, tearoff=0)
        self.tree_menu.add_command(label="View Listing", command=self._view_selected_listing)
        self.tree_menu.add_command(label="Copy URL", command=self._copy_listing_url)
        
        self.results_tree.bind("<Button-3>", self._show_tree_menu)
        self.results_tree.bind("<Double-1>", self._view_selected_listing)
        
        # Status bar for results
        status_frame = ttk.Frame(results_frame)
        status_frame.grid(row=2, column=0, columnspan=2, sticky="ew", padx=5, pady=2)
        
        self.results_status = ttk.Label(status_frame, text="0 listings displayed")
        self.results_status.pack(side=tk.LEFT)
        
        # Refresh button
        refresh_btn = ttk.Button(status_frame, text="Refresh", command=self._refresh_results)
        refresh_btn.pack(side=tk.RIGHT)
    
    def _load_saved_searches(self):
        """Load saved searches from database"""
        conn = self.db_manager.connect()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
            SELECT id, name, search_params FROM saved_searches
            ORDER BY name
            """)
            
            self.saved_searches = cursor.fetchall()
            
            # Populate URL combobox
            urls = []
            for _, name, params in self.saved_searches:
                try:
                    search_params = json.loads(params)
                    urls.append(search_params.get("url", ""))
                except (json.JSONDecodeError, KeyError):
                    pass
            
            # Add default URL if not already in list
            default_url = "https://www.facebook.com/marketplace/category/vehicles"
            if default_url not in urls:
                urls.append(default_url)
                
            self.url_combo["values"] = urls
            
        except Exception as e:
            self._log(f"Error loading saved searches: {e}", "error")
    
    def _save_current_search(self):
        """Save current search URL to database"""
        url = self.url_var.get().strip()
        
        if not url:
            messagebox.showerror("Error", "Please enter a URL to save.")
            return
            
        # Ask for a name
        name = simpledialog.askstring("Save Search", "Enter a name for this search:")
        
        if not name:
            return
            
        # Create search parameters
        search_params = {
            "url": url,
            "max_listings": int(self.max_listings_var.get()),
            "batch_size": int(self.batch_size_var.get()),
            "headless": self.headless_var.get(),
            "disable_images": self.disable_images_var.get()
        }
        
        # Save to database
        conn = self.db_manager.connect()
        cursor = conn.cursor()
        
        try:
            # Check if name already exists
            cursor.execute("SELECT id FROM saved_searches WHERE name = ?", (name,))
            existing = cursor.fetchone()
            
            if existing:
                # Update existing
                cursor.execute("""
                UPDATE saved_searches 
                SET search_params = ?, last_run = NULL
                WHERE id = ?
                """, (json.dumps(search_params), existing[0]))
                
                self._log(f"Updated saved search: {name}")
            else:
                # Insert new
                cursor.execute("""
                INSERT INTO saved_searches (name, search_params, created_date)
                VALUES (?, ?, ?)
                """, (name, json.dumps(search_params), datetime.now().isoformat()))
                
                self._log(f"Created new saved search: {name}")
            
            conn.commit()
            
            # Reload saved searches
            self._load_saved_searches()
            
        except Exception as e:
            self._log(f"Error saving search: {e}", "error")
    
    def _load_selected_search(self):
        """Load the selected search from combobox"""
        url = self.url_var.get()
        
        # Find the matching saved search
        for _, name, params in self.saved_searches:
            try:
                search_params = json.loads(params)
                if search_params.get("url") == url:
                    # Load the parameters
                    self.max_listings_var.set(str(search_params.get("max_listings", 500)))
                    self.batch_size_var.set(str(search_params.get("batch_size", 50)))
                    self.headless_var.set(search_params.get("headless", True))
                    self.disable_images_var.set(search_params.get("disable_images", True))
                    
                    self._log(f"Loaded saved search: {name}")
                    return
            except (json.JSONDecodeError, KeyError):
                pass
    
    def show_saved_searches(self):
        """Show dialog with all saved searches"""
        # Create a new toplevel window
        dialog = tk.Toplevel(self)
        dialog.title("Saved Searches")
        dialog.geometry("600x400")
        dialog.transient(self)
        dialog.grab_set()
        
        # Make dialog modal
        dialog.focus_set()
        
        # Configure dialog layout
        dialog.grid_columnconfigure(0, weight=1)
        dialog.grid_rowconfigure(0, weight=0)  # Title
        dialog.grid_rowconfigure(1, weight=1)  # Treeview
        dialog.grid_rowconfigure(2, weight=0)  # Buttons
        
        # Create title
        title_frame = ttk.Frame(dialog)
        title_frame.grid(row=0, column=0, sticky="ew", padx=10, pady=5)
        
        ttk.Label(title_frame, text="Manage Saved Searches", 
                font=("Arial", 12, "bold")).pack(side=tk.LEFT)
        
        # Create treeview with scrollbar
        tree_frame = ttk.Frame(dialog)
        tree_frame.grid(row=1, column=0, sticky="nsew", padx=10, pady=5)
        
        tree_frame.grid_columnconfigure(0, weight=1)
        tree_frame.grid_rowconfigure(0, weight=1)
        
        tree_scroll = ttk.Scrollbar(tree_frame)
        tree_scroll.grid(row=0, column=1, sticky="ns")
        
        searches_tree = ttk.Treeview(tree_frame, columns=(
            "name", "url", "last_run", "auto_run"
        ), show="headings", yscrollcommand=tree_scroll.set)
        
        searches_tree.heading("name", text="Name")
        searches_tree.heading("url", text="URL")
        searches_tree.heading("last_run", text="Last Run")
        searches_tree.heading("auto_run", text="Auto Run")
        
        searches_tree.column("name", width=150)
        searches_tree.column("url", width=250)
        searches_tree.column("last_run", width=100)
        searches_tree.column("auto_run", width=80, anchor="center")
        
        searches_tree.grid(row=0, column=0, sticky="nsew")
        tree_scroll.config(command=searches_tree.yview)
        
        # Populate treeview
        conn = self.db_manager.connect()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
            SELECT id, name, search_params, last_run, auto_run
            FROM saved_searches
            ORDER BY name
            """)
            
            for row in cursor.fetchall():
                search_id, name, params, last_run, auto_run = row
                
                try:
                    search_params = json.loads(params)
                    url = search_params.get("url", "")
                    
                    # Format last run
                    if last_run:
                        try:
                            last_dt = datetime.fromisoformat(last_run)
                            last_run_str = last_dt.strftime("%Y-%m-%d %H:%M")
                        except ValueError:
                            last_run_str = last_run
                    else:
                        last_run_str = "Never"
                        
                    auto_run_str = "Yes" if auto_run else "No"
                    
                    searches_tree.insert("", "end", text=str(search_id), values=(
                        name, url, last_run_str, auto_run_str
                    ))
                    
                except (json.JSONDecodeError, KeyError):
                    pass
                    
        except Exception as e:
            messagebox.showerror("Error", f"Error loading saved searches: {e}")
        
        # Create buttons
        buttons_frame = ttk.Frame(dialog)
        buttons_frame.grid(row=2, column=0, sticky="ew", padx=10, pady=10)
        
        # Function to load selected search
        def load_selected():
            selection = searches_tree.selection()
            if not selection:
                return
                
            item = searches_tree.item(selection[0])
            search_id = item["text"]
            
            # Find matching search
            cursor.execute("SELECT search_params FROM saved_searches WHERE id = ?", (search_id,))
            result = cursor.fetchone()
            
            if result:
                try:
                    search_params = json.loads(result[0])
                    url = search_params.get("url", "")
                    
                    # Set URL and parameters
                    self.url_var.set(url)
                    self.max_listings_var.set(str(search_params.get("max_listings", 500)))
                    self.batch_size_var.set(str(search_params.get("batch_size", 50)))
                    self.headless_var.set(search_params.get("headless", True))
                    self.disable_images_var.set(search_params.get("disable_images", True)))
                    
                    dialog.destroy()
                    
                except (json.JSONDecodeError, KeyError):
                    messagebox.showerror("Error", "Invalid search parameters.")
        
        # Function to delete selected search
        def delete_selected():
            selection = searches_tree.selection()
            if not selection:
                return
                
            item = searches_tree.item(selection[0])
            search_id = item["text"]
            name = item["values"][0]
            
            if messagebox.askyesno("Confirm Delete", f"Delete saved search '{name}'?"):
                try:
                    cursor.execute("DELETE FROM saved_searches WHERE id = ?", (search_id,))
                    conn.commit()
                    
                    # Remove from treeview
                    searches_tree.delete(selection[0])
                    
                    # Reload saved searches
                    self._load_saved_searches()
                    
                except Exception as e:
                    messagebox.showerror("Error", f"Error deleting search: {e}")
        
        # Function to toggle auto run
        def toggle_auto_run():
            selection = searches_tree.selection()
            if not selection:
                return
                
            item = searches_tree.item(selection[0])
            search_id = item["text"]
            auto_run = item["values"][3] == "Yes"
            
            try:
                cursor.execute("""
                UPDATE saved_searches SET auto_run = ? WHERE id = ?
                """, (not auto_run, search_id))
                conn.commit()
                
                # Update treeview
                searches_tree.item(selection[0], values=(
                    item["values"][0],
                    item["values"][1],
                    item["values"][2],
                    "No" if auto_run else "Yes"
                ))
                
                # Reload saved searches
                self._load_saved_searches()
                
            except Exception as e:
                messagebox.showerror("Error", f"Error updating search: {e}")
        
        # Add buttons
        ttk.Button(buttons_frame, text="Load", command=load_selected).pack(side=tk.LEFT, padx=5)
        ttk.Button(buttons_frame, text="Delete", command=delete_selected).pack(side=tk.LEFT, padx=5)
        ttk.Button(buttons_frame, text="Toggle Auto Run", command=toggle_auto_run).pack(side=tk.LEFT, padx=5)
        ttk.Button(buttons_frame, text="Close", command=dialog.destroy).pack(side=tk.RIGHT, padx=5)
    
    def show_scheduler(self):
        """Show scheduler configuration dialog"""
        # Create a new toplevel window
        dialog = tk.Toplevel(self)
        dialog.title("Scheduler Configuration")
        dialog.geometry("500x300")
        dialog.transient(self)
        dialog.grab_set()
        
        # Make dialog modal
        dialog.focus_set()
        
        # Configure dialog layout
        dialog.grid_columnconfigure(0, weight=1)
        dialog.grid_rowconfigure(0, weight=0)  # Title
        dialog.grid_rowconfigure(1, weight=1)  # Content
        dialog.grid_rowconfigure(2, weight=0)  # Buttons
        
        # Create title
        title_frame = ttk.Frame(dialog)
        title_frame.grid(row=0, column=0, sticky="ew", padx=10, pady=5)
        
        ttk.Label(title_frame, text="Configure Scheduled Scraping", 
                font=("Arial", 12, "bold")).pack(side=tk.LEFT)
        
        # Create content
        content_frame = ttk.Frame(dialog)
        content_frame.grid(row=1, column=0, sticky="nsew", padx=10, pady=5)
        
        # Enable scheduler
        enabled_var = tk.BooleanVar(value=self.config.get("scheduler", "enabled", True))
        ttk.Checkbutton(content_frame, text="Enable Scheduled Scraping", 
                      variable=enabled_var).grid(row=0, column=0, columnspan=2, 
                                               sticky="w", padx=5, pady=5)
        
        # Scrape frequency
        ttk.Label(content_frame, text="Scrape Every:").grid(row=1, column=0, 
                                                         sticky="w", padx=5, pady=5)
        
        frequency_frame = ttk.Frame(content_frame)
        frequency_frame.grid(row=1, column=1, sticky="w", padx=5, pady=5)
        
        frequency_var = tk.StringVar(value=str(self.config.get("scheduler", "scrape_frequency_hours", 24)))
        ttk.Spinbox(frequency_frame, from_=1, to=168, increment=1, 
                  textvariable=frequency_var, width=5).pack(side=tk.LEFT)
        ttk.Label(frequency_frame, text="Hours").pack(side=tk.LEFT, padx=5)
        
        # Scan when idle
        idle_var = tk.BooleanVar(value=self.config.get("scheduler", "scan_when_idle", True))
        ttk.Checkbutton(content_frame, text="Scan When System is Idle", 
                      variable=idle_var).grid(row=2, column=0, columnspan=2, 
                                            sticky="w", padx=5, pady=5)
        
        # Idle threshold
        ttk.Label(content_frame, text="Idle Threshold:").grid(row=3, column=0, 
                                                           sticky="w", padx=5, pady=5)
        
        idle_frame = ttk.Frame(content_frame)
        idle_frame.grid(row=3, column=1, sticky="w", padx=5, pady=5)
        
        idle_threshold_var = tk.StringVar(value=str(self.config.get("scheduler", "idle_threshold_minutes", 10)))
        ttk.Spinbox(idle_frame, from_=1, to=60, increment=1, 
                  textvariable=idle_threshold_var, width=5).pack(side=tk.LEFT)
        ttk.Label(idle_frame, text="Minutes").pack(side=tk.LEFT, padx=5)
        
        # Run at startup
        startup_var = tk.BooleanVar(value=self.config.get("scheduler", "run_at_startup", False))
        ttk.Checkbutton(content_frame, text="Run at Application Startup", 
                      variable=startup_var).grid(row=4, column=0, columnspan=2, 
                                               sticky="w", padx=5, pady=5)
        
        # Pause on battery
        battery_var = tk.BooleanVar(value=self.config.get("system", "pause_on_battery", True))
        ttk.Checkbutton(content_frame, text="Pause When on Battery Power (Laptops)", 
                      variable=battery_var).grid(row=5, column=0, columnspan=2, 
                                               sticky="w", padx=5, pady=5)
        
        # Status
        status_frame = ttk.LabelFrame(content_frame, text="Scheduler Status")
        status_frame.grid(row=6, column=0, columnspan=2, sticky="ew", padx=5, pady=10)
        
        # Get scheduler status
        scheduler_status = self.scheduler.get_status()
        
        # Last run
        ttk.Label(status_frame, text="Last Run:").grid(row=0, column=0, sticky="w", padx=5, pady=2)
        last_run = "Never"
        if "last_run" in scheduler_status:
            try:
                last_dt = datetime.fromisoformat(scheduler_status["last_run"])
                last_run = last_dt.strftime("%Y-%m-%d %H:%M")
            except (ValueError, TypeError):
                last_run = str(scheduler_status.get("last_run", "Never"))
                
        ttk.Label(status_frame, text=last_run).grid(row=0, column=1, sticky="w", padx=5, pady=2)
        
        # Next run
        ttk.Label(status_frame, text="Next Run:").grid(row=1, column=0, sticky="w", padx=5, pady=2)
        next_run = "Not scheduled"
        if "next_run" in scheduler_status:
            try:
                next_dt = datetime.fromisoformat(scheduler_status["next_run"])
                next_run = next_dt.strftime("%Y-%m-%d %H:%M")
            except (ValueError, TypeError):
                next_run = str(scheduler_status.get("next_run", "Not scheduled"))
                
        ttk.Label(status_frame, text=next_run).grid(row=1, column=1, sticky="w", padx=5, pady=2)
        
        # Current state
        ttk.Label(status_frame, text="Current State:").grid(row=2, column=0, sticky="w", padx=5, pady=2)
        state = "Unknown"
        if scheduler_status.get("running", False):
            if scheduler_status.get("paused", False):
                state = "Paused"
            else:
                state = "Running"
        else:
            state = "Stopped"
            
        ttk.Label(status_frame, text=state).grid(row=2, column=1, sticky="w", padx=5, pady=2)
        
        # Buttons
        buttons_frame = ttk.Frame(dialog)
        buttons_frame.grid(row=2, column=0, sticky="ew", padx=10, pady=10)
        
        # Function to save settings
        def save_settings():
            # Update config
            self.config.set("scheduler", "enabled", enabled_var.get())
            self.config.set("scheduler", "scrape_frequency_hours", int(frequency_var.get()))
            self.config.set("scheduler", "scan_when_idle", idle_var.get())
            self.config.set("scheduler", "idle_threshold_minutes", int(idle_threshold_var.get()))
            self.config.set("scheduler", "run_at_startup", startup_var.get())
            self.config.set("system", "pause_on_battery", battery_var.get())
            
            self.config.save()
            
            # Restart scheduler if needed
            if self.scheduler.running:
                self.scheduler.stop()
                self.scheduler.start()
                
            dialog.destroy()
            
            messagebox.showinfo("Settings Saved", 
                              "Scheduler settings have been saved and applied.")
        
        # Function to run now
        def run_now():
            url = self.url_var.get().strip()
            
            if not url:
                messagebox.showerror("Error", "Please enter a URL to scrape.")
                return
                
            result = self.scheduler.run_now(url)
            
            if result:
                messagebox.showinfo("Scheduled Task", 
                                  "Scraping task has been scheduled to run immediately.")
                dialog.destroy()
            else:
                messagebox.showerror("Error", 
                                   "Failed to schedule immediate scraping task.")
        
        # Add buttons
        ttk.Button(buttons_frame, text="Save Settings", command=save_settings).pack(side=tk.LEFT, padx=5)
        ttk.Button(buttons_frame, text="Run Now", command=run_now).pack(side=tk.LEFT, padx=5)
        ttk.Button(buttons_frame, text="Cancel", command=dialog.destroy).pack(side=tk.RIGHT, padx=5)
    
    def _apply_settings(self):
        """Apply current scraper settings to config"""
        try:
            # Update config with current UI values
            self.config.set("scraper", "max_listings", int(self.max_listings_var.get()))
            self.config.set("scraper", "batch_size", int(self.batch_size_var.get()))
            self.config.set("scraper", "headless", self.headless_var.get())
            self.config.set("scraper", "disable_images", self.disable_images_var.get())
            self.config.set("system", "low_resource_mode", self.low_resource_var.get())
            
            self.config.save()
            
            self._log("Settings applied and saved.")
            
        except Exception as e:
            self._log(f"Error applying settings: {e}", "error")
    
    def start_scraping(self):
        """Start the scraping process"""
        if self.is_scraping:
            messagebox.showinfo("Already Running", "Scraping is already in progress.")
            return
            
        # Get URL from input
        url = self.url_var.get().strip()
        
        if not url:
            messagebox.showerror("Error", "Please enter a URL to scrape.")
            return
            
        # Apply current settings
        self._apply_settings()
        
        # Update UI
        self._update_status("Starting...", "blue")
        self.start_btn.config(state=tk.DISABLED)
        self.stop_btn.config(state=tk.NORMAL)
        self.pause_btn.config(state=tk.NORMAL)
        
        # Clear log
        self._clear_log()
        self._log(f"Starting scraping for URL: {url}")
        
        # Reset progress and stats
        self.progress_var.set(0)
        self.progress_text.config(text="0/0 listings (0%)")
        self.found_var.set("0")
        self.new_var.set("0")
        self.updated_var.set("0")
        self.memory_var.set("Memory: 0 MB")
        self.cpu_var.set("CPU: 0%")
        self.time_var.set("Time: 0:00")
        self.throttle_var.set("No")
        self.throttle_label.config(foreground="black")
        
        # Record start time
        self.start_time = time.time()
        
        # Start timer update
        self._update_timer()
        
        # Start scraping in a background thread
        self.is_scraping = True
        self.scraper_thread = threading.Thread(target=self._run_scraper, args=(url,), daemon=True)
        self.scraper_thread.start()
    
    def _run_scraper(self, url):
        """Run the scraper in a background thread"""
        try:
            # Create scraper instance with current settings
            self.scraper = ResourceEfficientScraper(self.config, self.db_manager)
            
            # Override settings with UI values
            self.scraper.max_listings = int(self.max_listings_var.get())
            self.scraper.batch_size = int(self.batch_size_var.get())
            self.scraper.headless = self.headless_var.get()
            self.scraper.disable_images = self.disable_images_var.get()
            
            # Start scraping
            self.scraper.scrape_marketplace(url)
            
            # Update UI when done
            self.after(100, self._scraping_completed)
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            self._log(f"Error during scraping: {e}", "error")
            
            # Update UI on error
            self.after(100, lambda: self._update_status(f"Error: {str(e)}", "red"))
            self.after(100, self._reset_ui)
    
    def _scraping_completed(self):
        """Handle completion of scraping"""
        # Update log
        stats = getattr(self.scraper, "stats", {})
        self._log(f"Scraping completed. Found: {stats.get('listings_found', 0)}, "
                f"New: {stats.get('new_listings', 0)}, "
                f"Updated: {stats.get('updated_listings', 0)}")
        
        # Update UI
        self._update_status("Completed", "green")
        self._reset_ui()
        
        # Refresh results
        self._refresh_results()
    
    def stop_scraping(self):
        """Stop the scraping process"""
        if not self.is_scraping:
            return
            
        self._log("Stopping scraping...")
        self._update_status("Stopping...", "orange")
        
        # Stop the scraper
        if self.scraper:
            # Get scraping state for possible resume
            self.scraper_state = self.scraper.pause_scraping()
            
            # Clean up
            self.scraper.cleanup()
            self.scraper = None
        
        # Reset UI
        self._reset_ui()
        
        # Update log
        self._log("Scraping stopped.")
    
    def _toggle_pause(self):
        """Pause or resume scraping"""
        if not self.is_scraping or not self.scraper:
            return
            
        # Check if we're already paused
        if self.pause_btn["text"] == "Resume":
            # Resume
            self._log("Resuming scraping...")
            self._update_status("Resuming...", "blue")
            
            # Change button
            self.pause_btn.config(text="Pause")
            
            # Resume in a new thread
            url = self.url_var.get().strip()
            thread = threading.Thread(target=lambda: self.scraper.resume_scraping(
                self.scraper_state, url), daemon=True)
            thread.start()
            
        else:
            # Pause
            self._log("Pausing scraping...")
            self._update_status("Paused", "orange")
            
            # Get state for resume
            self.scraper_state = self.scraper.pause_scraping()
            
            # Change button
            self.pause_btn.config(text="Resume")
    
    def _reset_ui(self):
        """Reset UI after scraping completes or stops"""
        self.is_scraping = False
        self.start_btn.config(state=tk.NORMAL)
        self.stop_btn.config(state=tk.DISABLED)
        self.pause_btn.config(state=tk.DISABLED)
        self.pause_btn.config(text="Pause")
    
    def _update_timer(self):
        """Update the elapsed time display"""
        if not self.is_scraping:
            return
            
        # Calculate elapsed time
        elapsed = time.time() - self.start_time
        minutes = int(elapsed // 60)
        seconds = int(elapsed % 60)
        
        # Update display
        self.time_var.set(f"Time: {minutes}:{seconds:02d}")
        
        # Update resource usage
        if self.scraper:
            memory_mb = getattr(self.scraper, "stats", {}).get("memory_usage_mb", 0)
            cpu = getattr(self.scraper, "stats", {}).get("cpu_usage", 0)
            
            self.memory_var.set(f"Memory: {memory_mb:.1f} MB")
            self.cpu_var.set(f"CPU: {cpu:.1f}%")
            
            # Check throttling
            is_throttling = getattr(self.scraper, "is_throttling", False)
            if is_throttling:
                self.throttle_var.set("Yes")
                self.throttle_label.config(foreground="red")
            else:
                self.throttle_var.set("No")
                self.throttle_label.config(foreground="black")
            
            # Update progress
            listings_found = getattr(self.scraper, "stats", {}).get("listings_found", 0)
            max_listings = self.scraper.max_listings
            
            if max_listings > 0:
                progress = (listings_found / max_listings) * 100
                self.progress_var.set(progress)
                self.progress_text.config(text=f"{listings_found}/{max_listings} listings ({progress:.1f}%)")
                
            # Update stats
            self.found_var.set(str(listings_found))
            self.new_var.set(str(getattr(self.scraper, "stats", {}).get("new_listings", 0)))
            self.updated_var.set(str(getattr(self.scraper, "stats", {}).get("updated_listings", 0)))
        
        # Schedule next update
        self.after(1000, self._update_timer)
    
    def _refresh_results(self):
        """Refresh the results display with recent listings"""
        # Clear current results
        for item in self.results_tree.get_children():
            self.results_tree.delete(item)
            
        # Load recent listings
        conn = self.db_manager.connect()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
            SELECT id, title, price, year, make, model, mileage, location, listing_date
            FROM car_listings
            ORDER BY listing_date DESC
            LIMIT 100
            """)
            
            listings = cursor.fetchall()
            
            # Add to treeview
            for listing in listings:
                listing_id, title, price, year, make, model, mileage, location, date = listing
                
                # Format values
                price_str = f"${price:,}" if price else ""
                year_str = str(year) if year else ""
                mileage_str = f"{mileage:,}" if mileage else ""
                
                # Format date
                try:
                    date_dt = datetime.fromisoformat(date)
                    date_str = date_dt.strftime("%Y-%m-%d")
                except (ValueError, TypeError):
                    date_str = str(date)
                
                self.results_tree.insert("", "end", text=listing_id, values=(
                    title, price_str, year_str, make, model, mileage_str, location, date_str
                ))
                
            # Update status
            self.results_status.config(text=f"{len(listings)} listings displayed")
            
        except Exception as e:
            self._log(f"Error loading results: {e}", "error")
    
    def _view_selected_listing(self, event=None):
        """View the selected listing"""
        selection = self.results_tree.selection()
        if not selection:
            return
            
        item = self.results_tree.item(selection[0])
        listing_id = item["text"]
        
        # Get listing URL
        conn = self.db_manager.connect()
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT url FROM car_listings WHERE id = ?", (listing_id,))
            result = cursor.fetchone()
            
            if result and result[0]:
                url = result[0]
                
                # Open URL in browser
                import webbrowser
                webbrowser.open(url)
            else:
                messagebox.showinfo("No URL", "This listing does not have a URL.")
                
        except Exception as e:
            messagebox.showerror("Error", f"Error opening listing: {e}")
    
    def _copy_listing_url(self):
        """Copy the URL of the selected listing to clipboard"""
        selection = self.results_tree.selection()
        if not selection:
            return
            
        item = self.results_tree.item(selection[0])
        listing_id = item["text"]
        
        # Get listing URL
        conn = self.db_manager.connect()
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT url FROM car_listings WHERE id = ?", (listing_id,))
            result = cursor.fetchone()
            
            if result and result[0]:
                url = result[0]
                
                # Copy to clipboard
                self.clipboard_clear()
                self.clipboard_append(url)
                
                messagebox.showinfo("URL Copied", "The listing URL has been copied to clipboard.")
            else:
                messagebox.showinfo("No URL", "This listing does not have a URL.")
                
        except Exception as e:
            messagebox.showerror("Error", f"Error copying URL: {e}")
    
    def _show_tree_menu(self, event):
        """Show context menu for the results tree"""
        # Select row under mouse
        item = self.results_tree.identify_row(event.y)
        if item:
            self.results_tree.selection_set(item)
            self.tree_menu.post(event.x_root, event.y_root)
    
    def _log(self, message, level="info"):
        """Add a message to the log"""
        # Add timestamp
        timestamp = datetime.now().strftime("%H:%M:%S")
        
        # Color based on level
        tag = level.lower()
        log_line = f"[{timestamp}] {message}"
        
        # Add to queue
        self.log_queue.append((log_line, tag))
        
        # Trim queue if too long
        if len(self.log_queue) > self.max_log_entries:
            self.log_queue.pop(0)
            
        # Update log text
        self._update_log()
    
    def _clear_log(self):
        """Clear the log display"""
        self.log_queue = []
        self.log_text.config(state=tk.NORMAL)
        self.log_text.delete(1.0, tk.END)
        self.log_text.config(state=tk.DISABLED)
    
    def _update_log(self):
        """Update the log text widget with current queue"""
        self.log_text.config(state=tk.NORMAL)
        
        # Clear current content
        self.log_text.delete(1.0, tk.END)
        
        # Add tags for coloring
        self.log_text.tag_configure("info", foreground="black")
        self.log_text.tag_configure("error", foreground="red")
        self.log_text.tag_configure("warning", foreground="orange")
        
        # Add each log line with appropriate tag
        for line, tag in self.log_queue:
            self.log_text.insert(tk.END, f"{line}\n", tag)
            
        # Scroll to end
        self.log_text.see(tk.END)
        
        # Disable editing
        self.log_text.config(state=tk.DISABLED)
    
    def _update_status(self, status, color="black"):
        """Update the status display with color"""
        self.status_value.config(text=status, foreground=color)
    
    def refresh(self):
        """Refresh the scraper frame"""
        # Reload saved searches
        self._load_saved_searches()
        
        # Refresh results
        self._refresh_results()
        
        # Apply settings from config
        self.max_listings_var.set(str(self.config.get("scraper", "max_listings", 500)))
        self.batch_size_var.set(str(self.config.get("scraper", "batch_size", 50)))
        self.headless_var.set(self.config.get("scraper", "headless", True))
        self.disable_images_var.set(self.config.get("scraper", "disable_images", True))
        self.low_resource_var.set(self.config.get("system", "low_resource_mode", False))
    
    def cleanup(self):
        """Clean up resources when frame is unloaded"""
        # Stop scraping if active
        if self.is_scraping and self.scraper:
            try:
                self.scraper.cleanup()
            except:
                pass
                
        self.scraper = None