"""
Resource-aware scheduler for car listing scraper
Implements intelligent scheduling that respects system resources
"""

import os
import time
import logging
import platform
import threading
import signal
import json
from datetime import datetime, timedelta
from pathlib import Path
import psutil

# Local imports
from src.utils.config import Config
from src.database.db_manager import DatabaseManager
from src.scraper.fb_marketplace_scraper import ResourceEfficientScraper


class TaskScheduler:
    """Windows-optimized task scheduler for resource-efficient scraping"""
    
    def __init__(self, config: Config, db_manager: DatabaseManager):
        """Initialize the scheduler with configuration"""
        self.config = config
        self.db_manager = db_manager
        self.logger = logging.getLogger(__name__)
        self.running = False
        self.paused = False
        self.current_task = None
        self.scraper = None
        self.thread = None
        self.exit_event = threading.Event()
        
        # Scheduler settings
        self.enabled = config.get("scheduler", "enabled", True)
        self.frequency_hours = config.get("scheduler", "scrape_frequency_hours", 24)
        self.scan_when_idle = config.get("scheduler", "scan_when_idle", True)
        self.idle_threshold_minutes = config.get("scheduler", "idle_threshold_minutes", 10)
        
        # Resource limits
        self.memory_limit_mb = config.get("system", "memory_limit_mb", 512)
        self.cpu_usage_limit = config.get("system", "cpu_usage_limit", 50)
        self.pause_on_battery = config.get("system", "pause_on_battery", True)
        
        # State file for persistence
        self.state_dir = Path(config.get("database", "path")).parent
        self.state_file = self.state_dir / "scheduler_state.json"
        
        # Last run time
        self.last_run_time = self._load_last_run_time()
    
    def _load_last_run_time(self):
        """Load the last run time from state file"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                    return datetime.fromisoformat(state.get('last_run', '2000-01-01T00:00:00'))
            except (json.JSONDecodeError, ValueError, KeyError) as e:
                self.logger.error(f"Error loading scheduler state: {e}")
        
        return datetime.now() - timedelta(days=1)
    
    def _save_last_run_time(self):
        """Save the last run time to state file"""
        try:
            os.makedirs(self.state_dir, exist_ok=True)
            with open(self.state_file, 'w') as f:
                json.dump({
                    'last_run': self.last_run_time.isoformat(),
                    'status': 'paused' if self.paused else 'running' if self.running else 'idle'
                }, f)
        except Exception as e:
            self.logger.error(f"Error saving scheduler state: {e}")
    
    def _is_system_idle(self):
        """Check if system is idle based on CPU and user activity"""
        # Check CPU usage
        cpu_usage = psutil.cpu_percent(interval=1)
        
        # On Windows, check user idle time
        if platform.system() == "Windows":
            try:
                import win32api
                import win32con
                
                last_input_info = win32api.GetLastInputInfo()
                tick_count = win32api.GetTickCount()
                idle_minutes = (tick_count - last_input_info) / 60000  # Convert milliseconds to minutes
                
                return (cpu_usage < 15) and (idle_minutes > self.idle_threshold_minutes)
            except ImportError:
                self.logger.warning("win32api not available, falling back to CPU usage only")
                return cpu_usage < 10
        else:
            # On non-Windows systems, just use CPU usage
            return cpu_usage < 10
    
    def _is_on_battery(self):
        """Check if system is running on battery power"""
        if platform.system() == "Windows":
            try:
                import win32api
                import win32con
                
                status = win32api.GetSystemPowerStatus()
                ac_status = status.get("ACLineStatus", 1)  # 1 = AC power
                return ac_status == 0
            except (ImportError, AttributeError):
                self.logger.warning("Could not check battery status, assuming AC power")
                return False
        else:
            # On non-Windows systems, check with psutil
            try:
                battery = psutil.sensors_battery()
                return battery is not None and not battery.power_plugged
            except (AttributeError, psutil.Error):
                return False
    
    def _check_resource_constraints(self):
        """Check if system resources allow running tasks"""
        # Check memory usage
        memory_usage_percent = psutil.virtual_memory().percent
        if memory_usage_percent > 90:
            self.logger.warning(f"Memory usage too high ({memory_usage_percent}%), postponing task")
            return False
        
        # Check CPU usage
        cpu_usage = psutil.cpu_percent(interval=1)
        if cpu_usage > self.cpu_usage_limit:
            self.logger.warning(f"CPU usage too high ({cpu_usage}%), postponing task")
            return False
        
        # Check if on battery
        if self.pause_on_battery and self._is_on_battery():
            self.logger.info("System running on battery, postponing task")
            return False
        
        return True
    
    def _should_run_task(self):
        """Determine if a task should be run based on schedule and resources"""
        # Check if scheduler is enabled
        if not self.enabled:
            return False
        
        # Check time since last run
        time_since_last_run = datetime.now() - self.last_run_time
        hours_since_last_run = time_since_last_run.total_seconds() / 3600
        
        if hours_since_last_run < self.frequency_hours:
            # If we're not due for a regular run, check if we should run when idle
            if self.scan_when_idle and self._is_system_idle():
                self.logger.info("System idle, running task early")
                return self._check_resource_constraints()
            return False
        
        # Time for a regular run, check resource constraints
        return self._check_resource_constraints()
    
    def start(self):
        """Start the scheduler in a background thread"""
        if self.running:
            return
            
        self.running = True
        self.exit_event.clear()
        self.thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self.thread.start()
        self.logger.info("Scheduler started")
    
    def _scheduler_loop(self):
        """Main scheduler loop running in background thread"""
        while self.running and not self.exit_event.is_set():
            try:
                if not self.paused and self._should_run_task():
                    self._run_scheduled_tasks()
                
                # Sleep for a while before checking again
                # Sleep in small increments to respond quickly to stop requests
                for _ in range(60):  # Check every minute
                    if not self.running or self.exit_event.is_set():
                        break
                    time.sleep(1)
                    
            except Exception as e:
                self.logger.error(f"Error in scheduler loop: {e}")
                time.sleep(60)  # Wait a bit before retrying
    
    def _run_scheduled_tasks(self):
        """Run the scheduled scraping tasks"""
        self.logger.info("Running scheduled scraping tasks")
        
        # Get saved searches from database
        conn = self.db_manager.connect()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
            SELECT id, name, search_params FROM saved_searches
            WHERE auto_run = 1
            ORDER BY last_run ASC
            """)
            
            searches = cursor.fetchall()
            
            if not searches:
                self.logger.info("No scheduled searches found")
                # Create a default search if none exist
                default_url = "https://www.facebook.com/marketplace/category/vehicles"
                self._run_scraping_task(default_url)
            else:
                # Run each saved search
                for search_id, name, search_params in searches:
                    if not self.running or self.exit_event.is_set():
                        break
                        
                    try:
                        params = json.loads(search_params)
                        url = params.get("url", "https://www.facebook.com/marketplace/category/vehicles")
                        self.logger.info(f"Running saved search: {name}")
                        self._run_scraping_task(url)
                        
                        # Update last run time
                        cursor.execute("""
                        UPDATE saved_searches SET last_run = ? WHERE id = ?
                        """, (datetime.now().isoformat(), search_id))
                        conn.commit()
                        
                    except Exception as e:
                        self.logger.error(f"Error running saved search {name}: {e}")
                    
                    # Wait between searches to avoid overloading the system
                    time.sleep(60)
            
            # Update the last run time
            self.last_run_time = datetime.now()
            self._save_last_run_time()
            
        except Exception as e:
            self.logger.error(f"Error retrieving saved searches: {e}")
        finally:
            if self.scraper:
                self.scraper.cleanup()
                self.scraper = None
    
    def _run_scraping_task(self, url):
        """Run a single scraping task"""
        try:
            self.current_task = {
                "type": "scrape",
                "url": url,
                "start_time": datetime.now().isoformat()
            }
            
            # Create scraper instance
            self.scraper = ResourceEfficientScraper(self.config, self.db_manager)
            
            # Run the scraper
            self.scraper.scrape_marketplace(url)
            
            self.current_task["status"] = "completed"
            self.current_task["end_time"] = datetime.now().isoformat()
            
        except Exception as e:
            self.logger.error(f"Error in scraping task: {e}")
            if self.current_task:
                self.current_task["status"] = "failed"
                self.current_task["error"] = str(e)
        finally:
            if self.scraper:
                self.scraper.cleanup()
                self.scraper = None
    
    def pause(self):
        """Pause the scheduler"""
        if not self.paused:
            self.paused = True
            self.logger.info("Scheduler paused")
            self._save_last_run_time()
    
    def resume(self):
        """Resume the scheduler"""
        if self.paused:
            self.paused = False
            self.logger.info("Scheduler resumed")
            self._save_last_run_time()
    
    def stop(self):
        """Stop the scheduler"""
        self.running = False
        self.exit_event.set()
        
        if self.thread:
            self.thread.join(timeout=2)
            
        if self.scraper:
            self.scraper.cleanup()
            self.scraper = None
            
        self.logger.info("Scheduler stopped")
    
    def get_status(self):
        """Get the current status of the scheduler"""
        return {
            "running": self.running,
            "paused": self.paused,
            "last_run": self.last_run_time.isoformat(),
            "next_run": (self.last_run_time + timedelta(hours=self.frequency_hours)).isoformat(),
            "current_task": self.current_task
        }
    
    def run_now(self, url=None):
        """
        Run a scraping task immediately
        
        Args:
            url (str): URL to scrape, or None to use default
        """
        if not url:
            url = "https://www.facebook.com/marketplace/category/vehicles"
            
        # Run in a separate thread to avoid blocking
        task_thread = threading.Thread(
            target=self._run_scraping_task,
            args=(url,),
            daemon=True
        )
        task_thread.start()
        
        return {
            "status": "started",
            "url": url,
            "time": datetime.now().isoformat()
        }
    
    def register_with_windows_scheduler(self):
        """Register the application with Windows Task Scheduler for auto-start"""
        if platform.system() != "Windows":
            self.logger.warning("Windows Task Scheduler registration only available on Windows")
            return False
            
        try:
            import subprocess
            
            # Get the Python executable path
            python_exe = sys.executable
            
            # Get the main script path - assuming this is run from main.py
            script_path = os.path.abspath(sys.argv[0])
            
            # Create the task XML
            task_name = "CarMarketAnalyzer"
            
            # Build the command
            cmd = [
                "schtasks", "/create", "/tn", task_name, "/sc", "DAILY",
                "/st", "19:00", "/ru", "SYSTEM",
                "/tr", f'"{python_exe}" "{script_path}" --scrape'
            ]
            
            # Run the command
            subprocess.run(cmd, check=True)
            
            self.logger.info(f"Registered with Windows Task Scheduler as '{task_name}'")
            return True
            
        except Exception as e:
            self.logger.error(f"Error registering with Windows Task Scheduler: {e}")
            return False
    
    def remove_from_windows_scheduler(self):
        """Remove the application from Windows Task Scheduler"""
        if platform.system() != "Windows":
            return False
            
        try:
            import subprocess
            
            task_name = "CarMarketAnalyzer"
            
            # Build the command
            cmd = ["schtasks", "/delete", "/tn", task_name, "/f"]
            
            # Run the command
            subprocess.run(cmd, check=True)
            
            self.logger.info(f"Removed from Windows Task Scheduler: '{task_name}'")
            return True
            
        except Exception as e:
            self.logger.error(f"Error removing from Windows Task Scheduler: {e}")
            return False