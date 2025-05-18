"""
Resource Monitor for tracking and managing system resource usage
"""

import threading
import time
import logging
import platform
import psutil
from typing import Dict, List, Optional, Callable

# Local imports
from src.utils.config import Config


class ResourceMonitor:
    """Monitors system resources and manages application resource usage"""
    
    def __init__(self, update_callback: Callable, threshold_callback: Callable, config: Config):
        """
        Initialize resource monitor
        
        Args:
            update_callback (callable): Function to call with resource updates
            threshold_callback (callable): Function to call when thresholds are exceeded
            config (Config): Application configuration
        """
        self.update_callback = update_callback
        self.threshold_callback = threshold_callback
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Resource limits from config
        self.memory_limit_mb = config.get("system", "memory_limit_mb", 512)
        self.cpu_limit_percent = config.get("system", "cpu_usage_limit", 50)
        
        # Critical thresholds (90% of limit)
        self.memory_critical_mb = self.memory_limit_mb * 0.9
        self.cpu_critical_percent = self.cpu_limit_percent * 0.9
        
        # Throttling state
        self.is_throttling = False
        
        # Peak memory usage tracking
        self.peak_memory_mb = 0
        
        # Process for monitoring
        self.process = psutil.Process()
        
        # Thread for monitoring
        self.monitor_thread = None
        self.running = False
        
        # Throttling history for adaptive throttling
        self.throttle_history = []
        self.max_history_size = 10
    
    def start(self):
        """Start resource monitoring in a background thread"""
        if self.monitor_thread and self.monitor_thread.is_alive():
            return
            
        self.running = True
        self.monitor_thread = threading.Thread(target=self._monitor_resources, daemon=True)
        self.monitor_thread.start()
        self.logger.info("Resource monitor started")
    
    def stop(self):
        """Stop resource monitoring"""
        self.running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=1.0)
        self.logger.info("Resource monitor stopped")
    
    def _monitor_resources(self):
        """Background thread for monitoring system resources"""
        while self.running:
            try:
                # Get memory usage
                memory_info = self.process.memory_info()
                memory_mb = memory_info.rss / (1024 * 1024)
                
                # Update peak memory if needed
                if memory_mb > self.peak_memory_mb:
                    self.peak_memory_mb = memory_mb
                
                # Calculate memory percentage relative to system memory
                system_memory_mb = psutil.virtual_memory().total / (1024 * 1024)
                memory_percent = (memory_mb / system_memory_mb) * 100
                
                # Get CPU usage
                cpu_percent = self.process.cpu_percent(interval=0.1)
                
                # Check if we should be throttling
                prev_throttling = self.is_throttling
                self.is_throttling = memory_mb > self.memory_limit_mb or cpu_percent > self.cpu_limit_percent
                
                # Update throttling history
                self.throttle_history.append(self.is_throttling)
                if len(self.throttle_history) > self.max_history_size:
                    self.throttle_history.pop(0)
                
                # Apply adaptive throttling if needed
                if sum(self.throttle_history) >= self.max_history_size // 2:
                    # We've been throttling frequently, increase thresholds temporarily
                    self._adjust_thresholds(True)
                elif not any(self.throttle_history):
                    # We haven't been throttling at all, reset thresholds
                    self._adjust_thresholds(False)
                
                # Call update callback
                self.update_callback(memory_percent, memory_mb, cpu_percent, self.is_throttling)
                
                # Check thresholds
                self._check_thresholds(memory_mb, cpu_percent)
                
                # If throttling state changed, log it
                if prev_throttling != self.is_throttling:
                    if self.is_throttling:
                        self.logger.warning(f"Resource throttling activated: Memory={memory_mb:.1f}MB, CPU={cpu_percent:.1f}%")
                    else:
                        self.logger.info("Resource throttling deactivated")
                
                # Sleep between updates (configurable)
                interval_sec = self.config.get("system", "resource_check_interval_sec", 2.0)
                time.sleep(interval_sec)
                
            except Exception as e:
                self.logger.error(f"Error monitoring resources: {e}")
                time.sleep(5)  # Wait longer before retrying if there was an error
    
    def _adjust_thresholds(self, increase: bool):
        """
        Adjust resource thresholds dynamically
        
        Args:
            increase (bool): Whether to increase thresholds
        """
        if increase:
            # Increase thresholds by 20%
            new_memory_limit = self.memory_limit_mb * 1.2
            new_cpu_limit = min(95, self.cpu_limit_percent * 1.2)
            
            self.logger.info(f"Increasing resource thresholds: Memory={new_memory_limit:.1f}MB, CPU={new_cpu_limit:.1f}%")
            
            self.memory_limit_mb = new_memory_limit
            self.cpu_limit_percent = new_cpu_limit
            self.memory_critical_mb = self.memory_limit_mb * 0.9
            self.cpu_critical_percent = self.cpu_limit_percent * 0.9
        else:
            # Reset to config values
            original_memory_limit = self.config.get("system", "memory_limit_mb", 512)
            original_cpu_limit = self.config.get("system", "cpu_usage_limit", 50)
            
            if self.memory_limit_mb != original_memory_limit or self.cpu_limit_percent != original_cpu_limit:
                self.logger.info(f"Resetting resource thresholds to original values")
                
                self.memory_limit_mb = original_memory_limit
                self.cpu_limit_percent = original_cpu_limit
                self.memory_critical_mb = self.memory_limit_mb * 0.9
                self.cpu_critical_percent = self.cpu_limit_percent * 0.9
    
    def _check_thresholds(self, memory_mb: float, cpu_percent: float):
        """
        Check if resource thresholds are exceeded and call callback if needed
        
        Args:
            memory_mb (float): Current memory usage in MB
            cpu_percent (float): Current CPU usage percent
        """
        # Check memory critical threshold
        if memory_mb > self.memory_critical_mb:
            self.threshold_callback("memory", memory_mb, self.memory_critical_mb, True)
        # Check memory warning threshold
        elif memory_mb > self.memory_limit_mb * 0.7:
            self.threshold_callback("memory", memory_mb, self.memory_limit_mb * 0.7, False)
            
        # Check CPU critical threshold
        if cpu_percent > self.cpu_critical_percent:
            self.threshold_callback("cpu", cpu_percent, self.cpu_critical_percent, True)
        # Check CPU warning threshold
        elif cpu_percent > self.cpu_limit_percent * 0.7:
            self.threshold_callback("cpu", cpu_percent, self.cpu_limit_percent * 0.7, False)
    
    def get_peak_memory(self) -> float:
        """Get peak memory usage in MB"""
        return self.peak_memory_mb
    
    def reset_peak_memory(self):
        """Reset peak memory tracking"""
        self.peak_memory_mb = 0
    
    def is_system_idle(self) -> bool:
        """
        Check if the system is currently idle
        
        Returns:
            bool: True if system is idle
        """
        cpu_idle_threshold = 15  # % CPU usage
        
        # Check CPU usage
        cpu_usage = psutil.cpu_percent(interval=0.5)
        
        # On Windows, also check user idle time
        if platform.system() == "Windows":
            try:
                import win32api
                
                last_input_info = win32api.GetLastInputInfo()
                tick_count = win32api.GetTickCount()
                idle_time_ms = tick_count - last_input_info
                
                # Convert to minutes
                idle_time_min = idle_time_ms / (1000 * 60)
                
                # Consider idle if CPU is low and no user input for 5+ minutes
                return cpu_usage < cpu_idle_threshold and idle_time_min >= 5
            except ImportError:
                return cpu_usage < cpu_idle_threshold
        
        # On non-Windows platforms, just use CPU
        return cpu_usage < cpu_idle_threshold
    
    def is_on_battery(self) -> bool:
        """
        Check if system is running on battery power
        
        Returns:
            bool: True if on battery
        """
        try:
            battery = psutil.sensors_battery()
            return battery is not None and not battery.power_plugged
        except (AttributeError, psutil.Error):
            return False
    
    def get_resource_summary(self) -> Dict:
        """
        Get detailed resource usage summary
        
        Returns:
            dict: Resource usage summary
        """
        # Current process info
        process = self.process
        
        # Memory info
        memory_info = process.memory_info()
        memory_mb = memory_info.rss / (1024 * 1024)
        
        # System memory info
        system_memory = psutil.virtual_memory()
        
        # CPU info
        cpu_percent = process.cpu_percent(interval=0.1)
        system_cpu_percent = psutil.cpu_percent(interval=0.1)
        
        # Disk IO
        try:
            disk_io = process.io_counters()
            read_mb = disk_io.read_bytes / (1024 * 1024)
            write_mb = disk_io.write_bytes / (1024 * 1024)
        except (psutil.Error, AttributeError):
            read_mb = write_mb = 0
        
        return {
            "memory": {
                "current_mb": memory_mb,
                "peak_mb": self.peak_memory_mb,
                "limit_mb": self.memory_limit_mb,
                "percent": (memory_mb / (system_memory.total / (1024 * 1024))) * 100,
                "system_total_gb": system_memory.total / (1024 ** 3),
                "system_available_gb": system_memory.available / (1024 ** 3),
                "system_percent": system_memory.percent
            },
            "cpu": {
                "process_percent": cpu_percent,
                "system_percent": system_cpu_percent,
                "limit_percent": self.cpu_limit_percent,
                "cores": psutil.cpu_count(logical=False),
                "logical_cores": psutil.cpu_count(logical=True)
            },
            "disk": {
                "read_mb": read_mb,
                "write_mb": write_mb
            },
            "status": {
                "is_throttling": self.is_throttling,
                "is_idle": self.is_system_idle(),
                "is_on_battery": self.is_on_battery()
            }
        }