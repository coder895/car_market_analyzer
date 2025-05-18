"""
Dashboard frame showing market overview and resource monitoring
"""

import tkinter as tk
from tkinter import ttk
from typing import Dict, List, Optional
import threading
import time
from datetime import datetime, timedelta

# Import matplotlib for charts with Tkinter backend
import matplotlib
matplotlib.use('TkAgg')
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import numpy as np

# Local imports
from src.utils.config import Config
from src.database.db_manager import DatabaseManager
from src.analysis.market_analyzer import MarketAnalyzer


class DashboardFrame(ttk.Frame):
    """Main dashboard showing overview of car market data and system resources"""
    
    def __init__(self, parent, config: Config, db_manager: DatabaseManager, 
                market_analyzer: MarketAnalyzer):
        """Initialize the dashboard frame"""
        super().__init__(parent)
        self.parent = parent
        self.config = config
        self.db_manager = db_manager
        self.market_analyzer = market_analyzer
        
        # Configure grid layout
        self.grid_columnconfigure(0, weight=1)
        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(0, weight=0)  # Header
        self.grid_rowconfigure(1, weight=1)  # Charts
        self.grid_rowconfigure(2, weight=1)  # Details
        
        # Dashboard variables
        self.total_listings = tk.StringVar(value="0")
        self.active_listings = tk.StringVar(value="0")
        self.avg_price = tk.StringVar(value="$0")
        self.price_trend = tk.StringVar(value="Stable")
        self.last_update = tk.StringVar(value="Never")
        
        # State variables
        self.figures = {}  # Store figure references
        self.loading = False
        
        # Create UI elements
        self._create_header()
        self._create_stats_panel()
        self._create_charts()
        self._create_popular_lists()
        
        # After initialization, load data
        self.refresh()
    
    def _create_header(self):
        """Create the dashboard header"""
        header_frame = ttk.Frame(self)
        header_frame.grid(row=0, column=0, columnspan=2, sticky="ew", padx=10, pady=(10, 5))
        
        # Dashboard title
        title_label = ttk.Label(header_frame, text="Market Dashboard", 
                              style="Title.TLabel")
        title_label.pack(side=tk.LEFT, padx=5)
        
        # Last update time
        update_frame = ttk.Frame(header_frame)
        update_frame.pack(side=tk.RIGHT, padx=5)
        
        update_label = ttk.Label(update_frame, text="Last Updated: ")
        update_label.pack(side=tk.LEFT)
        
        last_update_label = ttk.Label(update_frame, textvariable=self.last_update)
        last_update_label.pack(side=tk.LEFT)
        
        # Refresh button
        refresh_button = ttk.Button(update_frame, text="↻ Refresh",
                                  command=self.refresh)
        refresh_button.pack(side=tk.LEFT, padx=(10, 0))
    
    def _create_stats_panel(self):
        """Create the statistics overview panel"""
        stats_frame = ttk.LabelFrame(self, text="Market Overview")
        stats_frame.grid(row=1, column=0, sticky="nsew", padx=10, pady=5)
        
        # Total listings
        total_frame = ttk.Frame(stats_frame)
        total_frame.pack(fill=tk.X, padx=10, pady=5)
        
        ttk.Label(total_frame, text="Total Listings:").pack(side=tk.LEFT)
        ttk.Label(total_frame, textvariable=self.total_listings,
                font=("Arial", 11, "bold")).pack(side=tk.RIGHT)
        
        # Active listings
        active_frame = ttk.Frame(stats_frame)
        active_frame.pack(fill=tk.X, padx=10, pady=5)
        
        ttk.Label(active_frame, text="Active Listings:").pack(side=tk.LEFT)
        ttk.Label(active_frame, textvariable=self.active_listings,
                font=("Arial", 11, "bold")).pack(side=tk.RIGHT)
        
        # Average price
        price_frame = ttk.Frame(stats_frame)
        price_frame.pack(fill=tk.X, padx=10, pady=5)
        
        ttk.Label(price_frame, text="Average Price:").pack(side=tk.LEFT)
        ttk.Label(price_frame, textvariable=self.avg_price,
                font=("Arial", 11, "bold")).pack(side=tk.RIGHT)
        
        # Price trend
        trend_frame = ttk.Frame(stats_frame)
        trend_frame.pack(fill=tk.X, padx=10, pady=5)
        
        ttk.Label(trend_frame, text="Price Trend:").pack(side=tk.LEFT)
        self.trend_label = ttk.Label(trend_frame, textvariable=self.price_trend,
                                   font=("Arial", 11, "bold"))
        self.trend_label.pack(side=tk.RIGHT)
        
        # Separator
        ttk.Separator(stats_frame).pack(fill=tk.X, padx=10, pady=10)
        
        # Recent activity
        ttk.Label(stats_frame, text="Recent Activity",
                font=("Arial", 11, "bold")).pack(anchor=tk.W, padx=10, pady=5)
        
        # Activity frame for the last few scraping sessions
        self.activity_frame = ttk.Frame(stats_frame)
        self.activity_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
    
    def _create_charts(self):
        """Create the chart area"""
        chart_frame = ttk.LabelFrame(self, text="Market Trends")
        chart_frame.grid(row=1, column=1, sticky="nsew", padx=10, pady=5)
        
        # Configure the chart frame
        chart_frame.grid_columnconfigure(0, weight=1)
        chart_frame.grid_rowconfigure(0, weight=1)
        
        # Add tabs for different charts
        self.chart_notebook = ttk.Notebook(chart_frame)
        self.chart_notebook.grid(row=0, column=0, sticky="nsew", padx=5, pady=5)
        
        # Price trends tab
        self.price_trend_frame = ttk.Frame(self.chart_notebook)
        self.chart_notebook.add(self.price_trend_frame, text="Price Trends")
        
        # Price distribution tab
        self.price_dist_frame = ttk.Frame(self.chart_notebook)
        self.chart_notebook.add(self.price_dist_frame, text="Price Distribution")
        
        # Make vs. Price tab
        self.make_price_frame = ttk.Frame(self.chart_notebook)
        self.chart_notebook.add(self.make_price_frame, text="Make vs. Price")
        
        # Create placeholder for charts - will be populated in refresh
        self._create_price_trend_chart()
        self._create_price_distribution_chart()
        self._create_make_price_chart()
    
    def _create_price_trend_chart(self):
        """Create the price trend chart"""
        # Configure the frame
        self.price_trend_frame.grid_columnconfigure(0, weight=1)
        self.price_trend_frame.grid_rowconfigure(0, weight=1)
        
        # Create figure and canvas
        figure = Figure(figsize=(5, 4), dpi=100)
        self.price_trend_canvas = FigureCanvasTkAgg(figure, self.price_trend_frame)
        self.price_trend_canvas.get_tk_widget().grid(row=0, column=0, sticky="nsew")
        
        # Store figure reference for later updates
        self.figures["price_trend"] = figure
        
        # Add placeholder text
        ax = figure.add_subplot(111)
        ax.text(0.5, 0.5, "Loading price trend data...", 
              horizontalalignment='center', verticalalignment='center',
              transform=ax.transAxes)
        ax.set_title("Average Price Over Time")
        figure.tight_layout()
        self.price_trend_canvas.draw()
    
    def _create_price_distribution_chart(self):
        """Create the price distribution chart"""
        # Configure the frame
        self.price_dist_frame.grid_columnconfigure(0, weight=1)
        self.price_dist_frame.grid_rowconfigure(0, weight=1)
        
        # Create figure and canvas
        figure = Figure(figsize=(5, 4), dpi=100)
        self.price_dist_canvas = FigureCanvasTkAgg(figure, self.price_dist_frame)
        self.price_dist_canvas.get_tk_widget().grid(row=0, column=0, sticky="nsew")
        
        # Store figure reference for later updates
        self.figures["price_dist"] = figure
        
        # Add placeholder text
        ax = figure.add_subplot(111)
        ax.text(0.5, 0.5, "Loading price distribution data...", 
              horizontalalignment='center', verticalalignment='center',
              transform=ax.transAxes)
        ax.set_title("Price Distribution")
        figure.tight_layout()
        self.price_dist_canvas.draw()
    
    def _create_make_price_chart(self):
        """Create the make vs price chart"""
        # Configure the frame
        self.make_price_frame.grid_columnconfigure(0, weight=1)
        self.make_price_frame.grid_rowconfigure(0, weight=1)
        
        # Create figure and canvas
        figure = Figure(figsize=(5, 4), dpi=100)
        self.make_price_canvas = FigureCanvasTkAgg(figure, self.make_price_frame)
        self.make_price_canvas.get_tk_widget().grid(row=0, column=0, sticky="nsew")
        
        # Store figure reference for later updates
        self.figures["make_price"] = figure
        
        # Add placeholder text
        ax = figure.add_subplot(111)
        ax.text(0.5, 0.5, "Loading make vs. price data...", 
              horizontalalignment='center', verticalalignment='center',
              transform=ax.transAxes)
        ax.set_title("Average Price by Make")
        figure.tight_layout()
        self.make_price_canvas.draw()
    
    def _create_popular_lists(self):
        """Create the popular makes/models list area"""
        popular_frame = ttk.LabelFrame(self, text="Popular Vehicles")
        popular_frame.grid(row=2, column=0, columnspan=2, sticky="nsew", padx=10, pady=5)
        
        # Configure the frame
        popular_frame.grid_columnconfigure(0, weight=1)
        popular_frame.grid_columnconfigure(1, weight=1)
        popular_frame.grid_rowconfigure(0, weight=0)  # Title
        popular_frame.grid_rowconfigure(1, weight=1)  # Content
        
        # Create frames for makes and models
        makes_frame = ttk.LabelFrame(popular_frame, text="Top Makes")
        makes_frame.grid(row=0, column=0, sticky="nsew", padx=5, pady=5)
        
        models_frame = ttk.LabelFrame(popular_frame, text="Top Models")
        models_frame.grid(row=0, column=1, sticky="nsew", padx=5, pady=5)
        
        # Create treeview for makes
        self.makes_tree = ttk.Treeview(makes_frame, columns=("make", "count"),
                                     show="headings", height=5)
        self.makes_tree.heading("make", text="Make")
        self.makes_tree.heading("count", text="Count")
        self.makes_tree.column("make", width=150)
        self.makes_tree.column("count", width=70, anchor="e")
        self.makes_tree.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Create treeview for models
        self.models_tree = ttk.Treeview(models_frame, columns=("make", "model", "count"),
                                      show="headings", height=5)
        self.models_tree.heading("make", text="Make")
        self.models_tree.heading("model", text="Model")
        self.models_tree.heading("count", text="Count")
        self.models_tree.column("make", width=100)
        self.models_tree.column("model", width=100)
        self.models_tree.column("count", width=70, anchor="e")
        self.models_tree.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Create database info frame
        db_frame = ttk.LabelFrame(popular_frame, text="Database Status")
        db_frame.grid(row=1, column=0, columnspan=2, sticky="nsew", padx=5, pady=5)
        
        # Add database information
        db_info_frame = ttk.Frame(db_frame)
        db_info_frame.pack(fill=tk.X, padx=10, pady=5)
        
        # Two-column layout for database info
        left_info = ttk.Frame(db_info_frame)
        left_info.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        right_info = ttk.Frame(db_info_frame)
        right_info.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)
        
        # Database size
        self.db_size_var = tk.StringVar(value="0 MB")
        ttk.Label(left_info, text="Database Size:").grid(row=0, column=0, sticky="w", padx=5, pady=2)
        ttk.Label(left_info, textvariable=self.db_size_var).grid(row=0, column=1, sticky="e", padx=5, pady=2)
        
        # Total records
        self.db_records_var = tk.StringVar(value="0")
        ttk.Label(left_info, text="Total Records:").grid(row=1, column=0, sticky="w", padx=5, pady=2)
        ttk.Label(left_info, textvariable=self.db_records_var).grid(row=1, column=1, sticky="e", padx=5, pady=2)
        
        # Last scrape
        self.last_scrape_var = tk.StringVar(value="Never")
        ttk.Label(right_info, text="Last Scrape:").grid(row=0, column=0, sticky="w", padx=5, pady=2)
        ttk.Label(right_info, textvariable=self.last_scrape_var).grid(row=0, column=1, sticky="e", padx=5, pady=2)
        
        # Next scheduled scrape
        self.next_scrape_var = tk.StringVar(value="Not scheduled")
        ttk.Label(right_info, text="Next Scrape:").grid(row=1, column=0, sticky="w", padx=5, pady=2)
        ttk.Label(right_info, textvariable=self.next_scrape_var).grid(row=1, column=1, sticky="e", padx=5, pady=2)
    
    def refresh(self):
        """Refresh all dashboard data"""
        if self.loading:
            return
            
        self.loading = True
        
        # Start loading in a background thread
        threading.Thread(target=self._load_data, daemon=True).start()
    
    def _load_data(self):
        """Load data in background thread"""
        try:
            # Set status
            self.last_update.set("Loading...")
            
            # Load overview data
            self._load_overview_stats()
            
            # Load popular makes and models
            self._load_popular_vehicles()
            
            # Load database info
            self._load_database_info()
            
            # Load charts data
            self._load_price_trend_chart()
            self._load_price_distribution_chart()
            self._load_make_price_chart()
            
            # Load activity data
            self._load_recent_activity()
            
            # Update status
            self.last_update.set(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            self.last_update.set(f"Error: {str(e)}")
            
        finally:
            self.loading = False
    
    def _load_overview_stats(self):
        """Load basic market overview statistics"""
        conn = self.db_manager.connect()
        cursor = conn.cursor()
        
        try:
            # Get total listings count
            cursor.execute("SELECT COUNT(*) FROM car_listings")
            total = cursor.fetchone()[0]
            self.total_listings.set(f"{total:,}")
            
            # Get active listings count
            cursor.execute("SELECT COUNT(*) FROM car_listings WHERE status = 'active'")
            active = cursor.fetchone()[0]
            self.active_listings.set(f"{active:,}")
            
            # Get average price
            cursor.execute("""
            SELECT AVG(price) FROM car_listings 
            WHERE price > 0 AND status = 'active'
            """)
            avg_price = cursor.fetchone()[0]
            if avg_price:
                self.avg_price.set(f"${int(avg_price):,}")
            else:
                self.avg_price.set("$0")
            
            # Get price trend (using last 30 days of data)
            month_ago = (datetime.now() - timedelta(days=30)).isoformat()
            cursor.execute("""
            SELECT listing_date, AVG(price) 
            FROM car_listings 
            WHERE price > 0 AND listing_date > ?
            GROUP BY substr(listing_date, 1, 10)
            ORDER BY listing_date
            """, (month_ago,))
            
            trend_data = cursor.fetchall()
            
            if len(trend_data) >= 3:
                # Calculate trend slope
                prices = [row[1] for row in trend_data]
                x = list(range(len(prices)))
                
                # Using numpy for linear regression
                slope, _ = np.polyfit(x, prices, 1)
                
                # Set trend text and color
                if abs(slope) < 10:  # Threshold for "stable"
                    self.price_trend.set("Stable")
                    self.trend_label.configure(foreground="black")
                elif slope > 0:
                    percent = (slope * len(prices) / prices[0]) * 100 if prices[0] else 0
                    self.price_trend.set(f"↑ Up {abs(percent):.1f}%")
                    self.trend_label.configure(foreground="green")
                else:
                    percent = (slope * len(prices) / prices[0]) * 100 if prices[0] else 0
                    self.price_trend.set(f"↓ Down {abs(percent):.1f}%")
                    self.trend_label.configure(foreground="red")
            else:
                self.price_trend.set("Insufficient Data")
                self.trend_label.configure(foreground="black")
                
        except Exception as e:
            print(f"Error loading overview stats: {e}")
    
    def _load_popular_vehicles(self):
        """Load and display popular vehicle makes and models"""
        try:
            # Get popular makes and models from market analyzer
            popular_data = self.market_analyzer.get_popular_makes_models(limit=10)
            
            if "error" in popular_data:
                return
                
            # Update makes treeview
            for item in self.makes_tree.get_children():
                self.makes_tree.delete(item)
                
            for make_data in popular_data.get("popular_makes", []):
                self.makes_tree.insert("", "end", values=(
                    make_data["make"],
                    f"{make_data['count']:,}"
                ))
                
            # Update models treeview
            for item in self.models_tree.get_children():
                self.models_tree.delete(item)
                
            for model_data in popular_data.get("popular_models", []):
                self.models_tree.insert("", "end", values=(
                    model_data["make"],
                    model_data["model"],
                    f"{model_data['count']:,}"
                ))
                
        except Exception as e:
            print(f"Error loading popular vehicles: {e}")
    
    def _load_database_info(self):
        """Load and display database information"""
        conn = self.db_manager.connect()
        cursor = conn.cursor()
        
        try:
            # Get database file size
            import os
            db_path = self.config.get("database", "path")
            
            if os.path.exists(db_path):
                size_bytes = os.path.getsize(db_path)
                size_mb = size_bytes / (1024 * 1024)
                self.db_size_var.set(f"{size_mb:.1f} MB")
            else:
                self.db_size_var.set("Unknown")
            
            # Get total records count
            cursor.execute("SELECT COUNT(*) FROM car_listings")
            total = cursor.fetchone()[0]
            self.db_records_var.set(f"{total:,}")
            
            # Get last scrape time
            cursor.execute("""
            SELECT MAX(end_time) FROM scrape_sessions
            WHERE status = 'completed'
            """)
            
            last_scrape = cursor.fetchone()[0]
            if last_scrape:
                # Format the datetime
                try:
                    last_dt = datetime.fromisoformat(last_scrape)
                    self.last_scrape_var.set(last_dt.strftime("%Y-%m-%d %H:%M"))
                except ValueError:
                    self.last_scrape_var.set(last_scrape)
            else:
                self.last_scrape_var.set("Never")
            
            # Get next scheduled scrape
            # This would typically come from the scheduler
            from src.scraper.scheduler import TaskScheduler
            scheduler = TaskScheduler(self.config, self.db_manager)
            status = scheduler.get_status()
            
            if "next_run" in status:
                try:
                    next_dt = datetime.fromisoformat(status["next_run"])
                    self.next_scrape_var.set(next_dt.strftime("%Y-%m-%d %H:%M"))
                except ValueError:
                    self.next_scrape_var.set(status["next_run"])
            else:
                self.next_scrape_var.set("Not scheduled")
                
        except Exception as e:
            print(f"Error loading database info: {e}")
    
    def _load_price_trend_chart(self):
        """Load and display price trend chart"""
        try:
            # Get price trend data for the last 90 days
            trend_data = self.market_analyzer.analyze_price_trends(
                time_period="quarter"
            )
            
            if "error" in trend_data:
                return
                
            # Get figure and clear it
            figure = self.figures["price_trend"]
            figure.clear()
            
            # Create new subplot
            ax = figure.add_subplot(111)
            
            # Plot the data
            dates = trend_data.get("dates", [])
            avg_prices = trend_data.get("avg_prices", [])
            
            if dates and avg_prices:
                ax.plot(dates, avg_prices, marker='o', linestyle='-', color='blue')
                
                # Add trend line if available
                if "trend" in trend_data:
                    # Create simple trend line from first to last point
                    x = [0, len(dates) - 1]
                    first_price = avg_prices[0]
                    last_price = avg_prices[-1]
                    y = [first_price, last_price]
                    
                    trend_color = 'green' if trend_data["trend"]["direction"] == "up" else 'red'
                    ax.plot([dates[i] for i in x], y, linestyle='--', color=trend_color, alpha=0.7)
                
                # Format the chart
                ax.set_title("Average Price Over Time")
                ax.set_xlabel("Date")
                ax.set_ylabel("Price ($)")
                
                # Format y-axis as currency
                ax.yaxis.set_major_formatter(matplotlib.ticker.StrMethodFormatter('${x:,.0f}'))
                
                # Rotate x-axis labels for better readability
                if len(dates) > 10:
                    # Only show every nth label to avoid crowding
                    n = len(dates) // 10 + 1
                    for i, label in enumerate(ax.get_xticklabels()):
                        if i % n != 0:
                            label.set_visible(False)
                
                plt_dates = [datetime.strptime(d, "%Y-%m-%d") for d in dates]
                ax.set_xticklabels([d.strftime("%m/%d") for d in plt_dates], rotation=45)
                
                # Add grid
                ax.grid(True, linestyle='--', alpha=0.7)
                
                # Add text showing overall trend
                if "trend" in trend_data:
                    trend_info = trend_data["trend"]
                    direction = trend_info["direction"]
                    trend_text = f"Trend: {direction.title()}"
                    
                    if abs(trend_info["slope"]) > 10:
                        trend_text += f" (${abs(trend_info['slope']):.0f}/day)"
                        
                    ax.text(0.02, 0.95, trend_text, transform=ax.transAxes,
                          fontsize=10, verticalalignment='top',
                          bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
            else:
                ax.text(0.5, 0.5, "No price trend data available", 
                      horizontalalignment='center', verticalalignment='center',
                      transform=ax.transAxes)
            
            # Update the figure
            figure.tight_layout()
            self.price_trend_canvas.draw()
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"Error loading price trend chart: {e}")
            
            # Show error in chart
            figure = self.figures["price_trend"]
            figure.clear()
            ax = figure.add_subplot(111)
            ax.text(0.5, 0.5, f"Error loading chart: {str(e)}", 
                  horizontalalignment='center', verticalalignment='center',
                  transform=ax.transAxes, color='red')
            figure.tight_layout()
            self.price_trend_canvas.draw()
    
    def _load_price_distribution_chart(self):
        """Load and display price distribution chart"""
        try:
            # Get price distribution data
            dist_data = self.market_analyzer.analyze_price_distribution()
            
            if "error" in dist_data:
                return
                
            # Get figure and clear it
            figure = self.figures["price_dist"]
            figure.clear()
            
            # Create new subplot
            ax = figure.add_subplot(111)
            
            # Plot the data
            buckets = dist_data.get("buckets", [])
            counts = dist_data.get("counts", [])
            
            if buckets and counts:
                # Plot histogram
                bars = ax.bar(range(len(buckets)), counts, color='skyblue')
                
                # Add values on top of bars
                for i, bar in enumerate(bars):
                    height = bar.get_height()
                    if height > 0:
                        ax.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                              f'{height:,}', ha='center', va='bottom', 
                              fontsize=8, rotation=0)
                
                # Format the chart
                ax.set_title("Price Distribution")
                ax.set_xlabel("Price Range")
                ax.set_ylabel("Number of Listings")
                
                # Set x-tick labels (bucket names)
                ax.set_xticks(range(len(buckets)))
                
                # If we have too many buckets, show fewer labels
                if len(buckets) > 8:
                    # Only show every nth label to avoid crowding
                    n = len(buckets) // 8 + 1
                    shortened_buckets = []
                    for i, bucket in enumerate(buckets):
                        if i % n == 0:
                            # Shorten the bucket label
                            parts = bucket.split(" - ")
                            if len(parts) == 2:
                                shortened_buckets.append(parts[0])
                            else:
                                shortened_buckets.append(bucket)
                        else:
                            shortened_buckets.append("")
                    ax.set_xticklabels(shortened_buckets, rotation=45, ha='right')
                else:
                    # Show all labels with shortened text
                    shortened_buckets = []
                    for bucket in buckets:
                        # Shorten the bucket label
                        parts = bucket.split(" - ")
                        if len(parts) == 2:
                            shortened_buckets.append(parts[0])
                        else:
                            shortened_buckets.append(bucket)
                    ax.set_xticklabels(shortened_buckets, rotation=45, ha='right')
                
                # Add grid
                ax.grid(True, linestyle='--', alpha=0.7, axis='y')
                
                # Add stats to the chart
                stats = dist_data.get("stats", {})
                if stats:
                    stats_text = (
                        f"Count: {stats.get('count', 0):,}\n"
                        f"Average: ${stats.get('avg', 0):,.0f}\n"
                        f"Median: ${stats.get('median', 0):,.0f}"
                    )
                    ax.text(0.02, 0.95, stats_text, transform=ax.transAxes,
                          fontsize=9, verticalalignment='top',
                          bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
            else:
                ax.text(0.5, 0.5, "No price distribution data available", 
                      horizontalalignment='center', verticalalignment='center',
                      transform=ax.transAxes)
            
            # Update the figure
            figure.tight_layout()
            self.price_dist_canvas.draw()
            
        except Exception as e:
            print(f"Error loading price distribution chart: {e}")
            
            # Show error in chart
            figure = self.figures["price_dist"]
            figure.clear()
            ax = figure.add_subplot(111)
            ax.text(0.5, 0.5, f"Error loading chart: {str(e)}", 
                  horizontalalignment='center', verticalalignment='center',
                  transform=ax.transAxes, color='red')
            figure.tight_layout()
            self.price_dist_canvas.draw()
    
    def _load_make_price_chart(self):
        """Load and display make vs price chart"""
        try:
            # We need to calculate this data manually
            conn = self.db_manager.connect()
            cursor = conn.cursor()
            
            # Get top makes with their average prices
            cursor.execute("""
            SELECT make, AVG(price) as avg_price, COUNT(*) as count
            FROM car_listings
            WHERE make IS NOT NULL AND make != '' AND price > 0
            GROUP BY make
            HAVING count >= 5
            ORDER BY count DESC
            LIMIT 10
            """)
            
            data = cursor.fetchall()
            
            # Get figure and clear it
            figure = self.figures["make_price"]
            figure.clear()
            
            # Create new subplot
            ax = figure.add_subplot(111)
            
            if data:
                # Extract makes and prices
                makes = [row[0] for row in data]
                prices = [row[1] for row in data]
                counts = [row[2] for row in data]
                
                # Sort by price for better visualization
                sorted_data = sorted(zip(makes, prices, counts), key=lambda x: x[1], reverse=True)
                makes = [item[0] for item in sorted_data]
                prices = [item[1] for item in sorted_data]
                counts = [item[2] for item in sorted_data]
                
                # Create horizontal bar chart
                bars = ax.barh(makes, prices, color='green')
                
                # Add price values
                for i, bar in enumerate(bars):
                    width = bar.get_width()
                    ax.text(width + (max(prices) * 0.02), bar.get_y() + bar.get_height()/2,
                          f"${width:,.0f} ({counts[i]:,})", va='center', fontsize=8)
                
                # Format the chart
                ax.set_title("Average Price by Make")
                ax.set_xlabel("Average Price ($)")
                ax.set_ylabel("Make")
                
                # Format x-axis as currency
                ax.xaxis.set_major_formatter(matplotlib.ticker.StrMethodFormatter('${x:,.0f}'))
                
                # Add grid
                ax.grid(True, linestyle='--', alpha=0.7, axis='x')
                
                # Add average line
                avg_all = sum(prices) / len(prices) if prices else 0
                ax.axvline(x=avg_all, color='red', linestyle='--', alpha=0.7)
                ax.text(avg_all + (max(prices) * 0.02), len(makes) - 0.5, 
                      f"Avg: ${avg_all:,.0f}", va='center', fontsize=8, color='red')
            else:
                ax.text(0.5, 0.5, "No make vs. price data available", 
                      horizontalalignment='center', verticalalignment='center',
                      transform=ax.transAxes)
            
            # Update the figure
            figure.tight_layout()
            self.make_price_canvas.draw()
            
        except Exception as e:
            print(f"Error loading make vs price chart: {e}")
            
            # Show error in chart
            figure = self.figures["make_price"]
            figure.clear()
            ax = figure.add_subplot(111)
            ax.text(0.5, 0.5, f"Error loading chart: {str(e)}", 
                  horizontalalignment='center', verticalalignment='center',
                  transform=ax.transAxes, color='red')
            figure.tight_layout()
            self.make_price_canvas.draw()
    
    def _load_recent_activity(self):
        """Load and display recent scraping activity"""
        conn = self.db_manager.connect()
        cursor = conn.cursor()
        
        # Clear existing widgets
        for widget in self.activity_frame.winfo_children():
            widget.destroy()
            
        try:
            # Get recent scrape sessions
            cursor.execute("""
            SELECT id, start_time, end_time, status, listings_found, new_listings, updated_listings
            FROM scrape_sessions
            ORDER BY start_time DESC
            LIMIT 3
            """)
            
            sessions = cursor.fetchall()
            
            if sessions:
                for i, session in enumerate(sessions):
                    session_id, start_time, end_time, status, listings_found, new_listings, updated_listings = session
                    
                    # Format times
                    try:
                        start_dt = datetime.fromisoformat(start_time)
                        start_str = start_dt.strftime("%m/%d %H:%M")
                    except (ValueError, TypeError):
                        start_str = str(start_time)
                        
                    try:
                        if end_time:
                            end_dt = datetime.fromisoformat(end_time)
                            end_str = end_dt.strftime("%H:%M")
                            duration = (end_dt - start_dt).total_seconds() / 60
                            duration_str = f"{duration:.1f} min"
                        else:
                            end_str = "N/A"
                            duration_str = "N/A"
                    except (ValueError, TypeError):
                        end_str = str(end_time)
                        duration_str = "N/A"
                    
                    # Create session frame
                    if i > 0:
                        ttk.Separator(self.activity_frame).pack(fill=tk.X, padx=5, pady=2)
                        
                    session_frame = ttk.Frame(self.activity_frame)
                    session_frame.pack(fill=tk.X, padx=5, pady=2)
                    
                    # Session time and status
                    time_frame = ttk.Frame(session_frame)
                    time_frame.pack(fill=tk.X, expand=True)
                    
                    # Set color based on status
                    status_color = "green" if status == "completed" else "red" if status == "failed" else "black"
                    
                    time_label = ttk.Label(time_frame, text=f"{start_str} - {end_str} ({duration_str})")
                    time_label.pack(side=tk.LEFT)
                    
                    status_label = ttk.Label(time_frame, text=status.title())
                    status_label.pack(side=tk.RIGHT)
                    status_label.configure(foreground=status_color)
                    
                    # Session results
                    results_frame = ttk.Frame(session_frame)
                    results_frame.pack(fill=tk.X, expand=True)
                    
                    results_text = f"Found: {listings_found or 0:,} | New: {new_listings or 0:,} | Updated: {updated_listings or 0:,}"
                    results_label = ttk.Label(results_frame, text=results_text)
                    results_label.pack(side=tk.LEFT)
            else:
                no_data_label = ttk.Label(self.activity_frame, text="No recent scraping activity")
                no_data_label.pack(padx=5, pady=10)
                
        except Exception as e:
            print(f"Error loading recent activity: {e}")
            error_label = ttk.Label(self.activity_frame, text=f"Error: {str(e)}")
            error_label.pack(padx=5, pady=10)
    
    def cleanup(self):
        """Clean up resources when frame is unloaded"""
        # Close any figure windows that might be open
        for figure_key in self.figures:
            plt_figure = self.figures[figure_key]
            plt_figure.clear()
            
        # Clear references to large objects
        self.figures.clear()