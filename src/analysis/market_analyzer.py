"""
Car market trend analyzer with optimized resource usage
Implements efficient batch processing and data caching
"""

import json
import logging
import time
import hashlib
import gc
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Union, Any
from functools import lru_cache

import numpy as np
import pandas as pd
from scipy import stats

# Local imports
from src.utils.config import Config
from src.database.db_manager import DatabaseManager


class MarketAnalyzer:
    """Resource-efficient market trend analyzer for car listings"""
    
    def __init__(self, config: Config, db_manager: DatabaseManager):
        """Initialize the analyzer with configuration"""
        self.config = config
        self.db_manager = db_manager
        self.logger = logging.getLogger(__name__)
        
        # Resource optimization settings
        self.batch_size = config.get("analysis", "batch_size", 1000)
        self.precompute_common_metrics = config.get("analysis", "precompute_common_metrics", True)
        self.cache_results = config.get("analysis", "cache_results", True)
        self.cache_ttl_minutes = config.get("analysis", "cache_ttl_minutes", 60)
        self.parallel_processing = config.get("analysis", "parallel_processing", False)
        self.max_chart_points = config.get("analysis", "max_chart_points", 100)
        
        # For progressive computation
        self.current_analysis_job = None
    
    def _generate_cache_key(self, analysis_type: str, params: Dict) -> str:
        """
        Generate a unique cache key for the analysis
        
        Args:
            analysis_type (str): Type of analysis
            params (dict): Analysis parameters
            
        Returns:
            str: Cache key
        """
        # Sort params to ensure consistent keys
        param_str = json.dumps(params, sort_keys=True)
        key_str = f"{analysis_type}:{param_str}"
        
        # Create MD5 hash as cache key
        return hashlib.md5(key_str.encode('utf-8')).hexdigest()
    
    def _get_cached_result(self, analysis_type: str, params: Dict) -> Optional[Dict]:
        """
        Try to get a cached result
        
        Args:
            analysis_type (str): Type of analysis
            params (dict): Analysis parameters
            
        Returns:
            dict: Cached results or None if not found
        """
        if not self.cache_results:
            return None
            
        cache_key = self._generate_cache_key(analysis_type, params)
        return self.db_manager.get_from_cache(cache_key)
    
    def _cache_result(self, analysis_type: str, params: Dict, result: Dict) -> bool:
        """
        Cache analysis results
        
        Args:
            analysis_type (str): Type of analysis
            params (dict): Analysis parameters
            result (dict): Results to cache
            
        Returns:
            bool: Success status
        """
        if not self.cache_results:
            return False
            
        cache_key = self._generate_cache_key(analysis_type, params)
        return self.db_manager.save_to_cache(cache_key, result, self.cache_ttl_minutes)
    
    def _process_data_batches(self, filters: Dict, processor_func: callable, 
                             initial_state: Dict = None) -> Dict:
        """
        Process data in batches to minimize memory usage
        
        Args:
            filters (dict): Database filters to apply
            processor_func (callable): Function to process each batch
            initial_state (dict): Initial state for the processor
            
        Returns:
            dict: Processed results
        """
        conn = self.db_manager.connect()
        cursor = conn.cursor()
        
        # Count total matching records
        total_count = self.db_manager.count_listings(filters)
        
        if total_count == 0:
            return {"error": "No data found matching the filters"}
            
        # Initialize state
        state = initial_state or {}
        state["processed_count"] = 0
        state["total_count"] = total_count
        
        # Process in batches
        offset = 0
        while state["processed_count"] < total_count:
            # Check if processing should be paused/canceled
            if self.current_analysis_job and self.current_analysis_job.get("cancel", False):
                return {"error": "Analysis canceled", "partial_results": state}
            
            # Fetch a batch of data
            batch = self.db_manager.get_listings(
                filters=filters, 
                limit=self.batch_size, 
                offset=offset
            )
            
            if not batch:
                break
                
            # Process the batch
            state = processor_func(batch, state)
            state["processed_count"] += len(batch)
            
            # Update progress if we're tracking a job
            if self.current_analysis_job:
                self.current_analysis_job["progress"] = state["processed_count"] / total_count
                
            # Move to next batch
            offset += self.batch_size
            
            # Force garbage collection to free memory
            if state["processed_count"] % (self.batch_size * 5) == 0:
                gc.collect()
        
        return state
    
    def analyze_price_trends(self, filters: Dict = None, 
                            time_period: str = "all") -> Dict:
        """
        Analyze price trends over time
        
        Args:
            filters (dict): Filters to apply (make, model, year, etc.)
            time_period (str): Time period to analyze (week, month, quarter, year, all)
            
        Returns:
            dict: Analysis results
        """
        # Check for cached results
        params = {"filters": filters or {}, "time_period": time_period}
        cached = self._get_cached_result("price_trends", params)
        if cached:
            return cached
            
        # Prepare filters
        if filters is None:
            filters = {}
            
        # Add time period filter
        if time_period != "all":
            today = datetime.now()
            if time_period == "week":
                cutoff = today - timedelta(days=7)
            elif time_period == "month":
                cutoff = today - timedelta(days=30)
            elif time_period == "quarter":
                cutoff = today - timedelta(days=90)
            elif time_period == "year":
                cutoff = today - timedelta(days=365)
            else:
                cutoff = today - timedelta(days=30)  # Default to month
                
            filters["listing_date_min"] = cutoff.isoformat()
        
        # Check for precomputed stats first
        if self.precompute_common_metrics and filters.get("make") and filters.get("model"):
            stats = self._get_precomputed_price_trends(filters, time_period)
            if stats:
                self._cache_result("price_trends", params, stats)
                return stats
        
        # Define batch processor function
        def process_batch(batch, state):
            if "prices_by_date" not in state:
                state["prices_by_date"] = {}
                
            for listing in batch:
                # Extract date (just the date part, not time)
                date_str = listing.get("listing_date", "").split("T")[0]
                if not date_str:
                    continue
                    
                price = listing.get("price")
                if price is None or price <= 0:
                    continue
                
                if date_str not in state["prices_by_date"]:
                    state["prices_by_date"][date_str] = []
                    
                state["prices_by_date"][date_str].append(price)
            
            return state
        
        # Process data in batches
        result = self._process_data_batches(filters, process_batch)
        
        if "error" in result:
            return result
            
        # Calculate statistics from the collected data
        dates = sorted(result["prices_by_date"].keys())
        avg_prices = []
        median_prices = []
        min_prices = []
        max_prices = []
        counts = []
        
        for date in dates:
            prices = result["prices_by_date"][date]
            avg_prices.append(sum(prices) / len(prices))
            median_prices.append(sorted(prices)[len(prices) // 2])
            min_prices.append(min(prices))
            max_prices.append(max(prices))
            counts.append(len(prices))
        
        # Downsample if we have too many points
        if len(dates) > self.max_chart_points:
            # Group dates into bins and average the values
            bin_size = len(dates) // self.max_chart_points
            
            binned_dates = []
            binned_avg = []
            binned_median = []
            binned_min = []
            binned_max = []
            binned_counts = []
            
            for i in range(0, len(dates), bin_size):
                end_idx = min(i + bin_size, len(dates))
                bin_dates = dates[i:end_idx]
                binned_dates.append(bin_dates[-1])  # Use last date in bin
                
                bin_avg_prices = avg_prices[i:end_idx]
                bin_median_prices = median_prices[i:end_idx]
                bin_min_prices = min_prices[i:end_idx]
                bin_max_prices = max_prices[i:end_idx]
                bin_counts = counts[i:end_idx]
                
                binned_avg.append(sum(bin_avg_prices) / len(bin_avg_prices))
                binned_median.append(sum(bin_median_prices) / len(bin_median_prices))
                binned_min.append(min(bin_min_prices))
                binned_max.append(max(bin_max_prices))
                binned_counts.append(sum(bin_counts))
            
            dates = binned_dates
            avg_prices = binned_avg
            median_prices = binned_median
            min_prices = binned_min
            max_prices = binned_max
            counts = binned_counts
        
        # Calculate overall statistics
        all_prices = []
        for price_list in result["prices_by_date"].values():
            all_prices.extend(price_list)
            
        stats = {
            "dates": dates,
            "avg_prices": avg_prices,
            "median_prices": median_prices,
            "min_prices": min_prices,
            "max_prices": max_prices,
            "counts": counts,
            "overall_stats": {
                "count": len(all_prices),
                "avg": sum(all_prices) / len(all_prices) if all_prices else 0,
                "median": sorted(all_prices)[len(all_prices) // 2] if all_prices else 0,
                "min": min(all_prices) if all_prices else 0,
                "max": max(all_prices) if all_prices else 0,
                "std_dev": np.std(all_prices) if all_prices else 0
            }
        }
        
        # Try to detect trend direction
        if len(dates) >= 3:
            try:
                # Use linear regression to determine trend
                x = list(range(len(dates)))
                slope, _, r_value, p_value, _ = stats.linregress(x, avg_prices)
                
                # Calculate and include trend info
                stats["trend"] = {
                    "direction": "up" if slope > 0 else "down",
                    "slope": slope,
                    "r_squared": r_value ** 2,
                    "p_value": p_value,
                    "significant": p_value < 0.05
                }
            except Exception as e:
                self.logger.error(f"Error calculating trend: {e}")
        
        # Save to cache
        self._cache_result("price_trends", params, stats)
        
        # If this is for a make/model combo, save as precomputed stat
        if self.precompute_common_metrics and filters.get("make") and filters.get("model"):
            self._save_precomputed_price_trends(filters, stats)
        
        return stats
    
    def _get_precomputed_price_trends(self, filters: Dict, time_period: str) -> Optional[Dict]:
        """
        Get precomputed price trends from database
        
        Args:
            filters (dict): Filters that were applied
            time_period (str): Time period for the analysis
            
        Returns:
            dict: Precomputed statistics or None if not found
        """
        make = filters.get("make")
        model = filters.get("model")
        year_min = filters.get("year_min")
        year_max = filters.get("year_max")
        
        if not make or not model:
            return None
            
        # Date range for market stats
        if time_period == "week":
            date_from = (datetime.now() - timedelta(days=7)).isoformat().split("T")[0]
        elif time_period == "month":
            date_from = (datetime.now() - timedelta(days=30)).isoformat().split("T")[0]
        elif time_period == "quarter":
            date_from = (datetime.now() - timedelta(days=90)).isoformat().split("T")[0]
        elif time_period == "year":
            date_from = (datetime.now() - timedelta(days=365)).isoformat().split("T")[0]
        else:
            date_from = None
            
        # Get pre-aggregated stats
        market_stats = self.db_manager.get_market_stats(
            make=make,
            model=model,
            year_min=year_min,
            year_max=year_max,
            date_from=date_from
        )
        
        if not market_stats:
            return None
            
        # Organize by date and stat type
        dates = sorted(list({stat['date'] for stat in market_stats}))
        
        avg_prices = []
        median_prices = []
        min_prices = []
        max_prices = []
        counts = []
        
        for date in dates:
            # Get stats for this date
            date_stats = [s for s in market_stats if s['date'] == date]
            
            avg_stat = next((s for s in date_stats if s['stat_type'] == 'avg_price'), None)
            median_stat = next((s for s in date_stats if s['stat_type'] == 'median_price'), None)
            min_stat = next((s for s in date_stats if s['stat_type'] == 'min_price'), None)
            max_stat = next((s for s in date_stats if s['stat_type'] == 'max_price'), None)
            count_stat = next((s for s in date_stats if s['stat_type'] == 'count'), None)
            
            avg_prices.append(avg_stat['stat_value'] if avg_stat else 0)
            median_prices.append(median_stat['stat_value'] if median_stat else 0)
            min_prices.append(min_stat['stat_value'] if min_stat else 0)
            max_prices.append(max_stat['stat_value'] if max_stat else 0)
            counts.append(count_stat['stat_value'] if count_stat else 0)
        
        # Calculate overall stats
        total_count = sum(counts)
        
        # Since we don't have all raw prices, estimate overall stats
        weighted_avg = sum(avg_prices[i] * counts[i] for i in range(len(avg_prices))) / total_count if total_count else 0
        
        # Build result in same format as the regular function
        result = {
            "dates": dates,
            "avg_prices": avg_prices,
            "median_prices": median_prices,
            "min_prices": min_prices,
            "max_prices": max_prices,
            "counts": counts,
            "overall_stats": {
                "count": total_count,
                "avg": weighted_avg,
                "median": median_prices[len(median_prices) // 2] if median_prices else 0,
                "min": min(min_prices) if min_prices else 0,
                "max": max(max_prices) if max_prices else 0,
                "std_dev": 0  # Can't calculate without raw data
            },
            "precomputed": True
        }
        
        # Try to detect trend direction
        if len(dates) >= 3:
            try:
                # Use linear regression to determine trend
                x = list(range(len(dates)))
                slope, _, r_value, p_value, _ = stats.linregress(x, avg_prices)
                
                # Calculate and include trend info
                result["trend"] = {
                    "direction": "up" if slope > 0 else "down",
                    "slope": slope,
                    "r_squared": r_value ** 2,
                    "p_value": p_value,
                    "significant": p_value < 0.05
                }
            except Exception as e:
                self.logger.error(f"Error calculating trend: {e}")
        
        return result
    
    def _save_precomputed_price_trends(self, filters: Dict, stats: Dict) -> None:
        """
        Save precomputed price trends to the database
        
        Args:
            filters (dict): Filters that were applied
            stats (dict): Calculated statistics
        """
        make = filters.get("make")
        model = filters.get("model")
        year_min = filters.get("year_min")
        year_max = filters.get("year_max")
        
        if not make or not model:
            return
            
        # Get the latest date
        today = datetime.now().isoformat().split("T")[0]
        
        # Save aggregate stats
        try:
            # Average price
            self.db_manager.save_market_stat(
                date=today,
                make=make,
                model=model,
                year_min=year_min,
                year_max=year_max,
                stat_type="avg_price",
                stat_value=stats["overall_stats"]["avg"],
                sample_size=stats["overall_stats"]["count"]
            )
            
            # Median price
            self.db_manager.save_market_stat(
                date=today,
                make=make,
                model=model,
                year_min=year_min,
                year_max=year_max,
                stat_type="median_price",
                stat_value=stats["overall_stats"]["median"],
                sample_size=stats["overall_stats"]["count"]
            )
            
            # Min price
            self.db_manager.save_market_stat(
                date=today,
                make=make,
                model=model,
                year_min=year_min,
                year_max=year_max,
                stat_type="min_price",
                stat_value=stats["overall_stats"]["min"],
                sample_size=stats["overall_stats"]["count"]
            )
            
            # Max price
            self.db_manager.save_market_stat(
                date=today,
                make=make,
                model=model,
                year_min=year_min,
                year_max=year_max,
                stat_type="max_price",
                stat_value=stats["overall_stats"]["max"],
                sample_size=stats["overall_stats"]["count"]
            )
            
            # Count
            self.db_manager.save_market_stat(
                date=today,
                make=make,
                model=model,
                year_min=year_min,
                year_max=year_max,
                stat_type="count",
                stat_value=stats["overall_stats"]["count"],
                sample_size=stats["overall_stats"]["count"]
            )
            
        except Exception as e:
            self.logger.error(f"Error saving precomputed stats: {e}")
    
    def analyze_price_distribution(self, filters: Dict = None) -> Dict:
        """
        Analyze the distribution of prices
        
        Args:
            filters (dict): Filters to apply
            
        Returns:
            dict: Price distribution analysis
        """
        # Check for cached results
        params = {"filters": filters or {}}
        cached = self._get_cached_result("price_distribution", params)
        if cached:
            return cached
            
        # Prepare filters
        if filters is None:
            filters = {}
            
        # Define batch processor function
        def process_batch(batch, state):
            if "prices" not in state:
                state["prices"] = []
                
            for listing in batch:
                price = listing.get("price")
                if price is not None and price > 0:
                    state["prices"].append(price)
            
            return state
        
        # Process data in batches
        result = self._process_data_batches(filters, process_batch)
        
        if "error" in result:
            return result
            
        prices = result["prices"]
        
        if not prices:
            return {"error": "No valid price data found"}
            
        # Calculate distribution statistics
        min_price = min(prices)
        max_price = max(prices)
        
        # Create price buckets
        num_buckets = min(20, max(5, len(prices) // 10))
        bucket_size = (max_price - min_price) / num_buckets
        
        buckets = []
        counts = []
        
        for i in range(num_buckets):
            bucket_min = min_price + i * bucket_size
            bucket_max = min_price + (i + 1) * bucket_size
            
            # For the last bucket, include the max price
            if i == num_buckets - 1:
                bucket_max = max_price
                
            bucket_label = f"${int(bucket_min):,} - ${int(bucket_max):,}"
            buckets.append(bucket_label)
            
            count = sum(1 for p in prices if bucket_min <= p <= bucket_max)
            counts.append(count)
        
        # Calculate overall statistics
        stats = {
            "buckets": buckets,
            "counts": counts,
            "stats": {
                "count": len(prices),
                "min": min_price,
                "max": max_price,
                "avg": sum(prices) / len(prices),
                "median": sorted(prices)[len(prices) // 2],
                "mode": max(set(prices), key=prices.count) if len(set(prices)) < len(prices) / 2 else None,
                "std_dev": np.std(prices)
            }
        }
        
        # Save to cache
        self._cache_result("price_distribution", params, stats)
        
        return stats
    
    def analyze_mileage_vs_price(self, filters: Dict = None) -> Dict:
        """
        Analyze relationship between mileage and price
        
        Args:
            filters (dict): Filters to apply
            
        Returns:
            dict: Analysis results
        """
        # Check for cached results
        params = {"filters": filters or {}}
        cached = self._get_cached_result("mileage_vs_price", params)
        if cached:
            return cached
            
        # Prepare filters
        if filters is None:
            filters = {}
            
        # Define batch processor function
        def process_batch(batch, state):
            if "data_points" not in state:
                state["data_points"] = []
                
            for listing in batch:
                price = listing.get("price")
                mileage = listing.get("mileage")
                
                if price is not None and price > 0 and mileage is not None and mileage > 0:
                    state["data_points"].append((mileage, price))
            
            return state
        
        # Process data in batches
        result = self._process_data_batches(filters, process_batch)
        
        if "error" in result:
            return result
            
        data_points = result["data_points"]
        
        if not data_points:
            return {"error": "No valid mileage and price data found"}
            
        # Extract mileage and price arrays
        mileages = [point[0] for point in data_points]
        prices = [point[1] for point in data_points]
        
        # Calculate regression line
        slope, intercept, r_value, p_value, std_err = stats.linregress(mileages, prices)
        
        # Calculate predicted prices
        min_mileage = min(mileages)
        max_mileage = max(mileages)
        
        # Create prediction points for visualization
        prediction_mileages = []
        prediction_prices = []
        
        # Create evenly spaced points for prediction line
        num_predictions = min(20, len(data_points) // 5)
        for i in range(num_predictions + 1):
            mileage = min_mileage + (max_mileage - min_mileage) * i / num_predictions
            price = intercept + slope * mileage
            
            prediction_mileages.append(mileage)
            prediction_prices.append(price)
        
        # Create scatter plot data
        # If we have too many points, downsample
        max_points = self.max_chart_points
        if len(data_points) > max_points:
            # Randomly sample points
            indices = np.random.choice(len(data_points), max_points, replace=False)
            scatter_mileages = [mileages[i] for i in indices]
            scatter_prices = [prices[i] for i in indices]
        else:
            scatter_mileages = mileages
            scatter_prices = prices
        
        # Results
        analysis = {
            "scatter_mileages": scatter_mileages,
            "scatter_prices": scatter_prices,
            "prediction_mileages": prediction_mileages,
            "prediction_prices": prediction_prices,
            "regression": {
                "slope": slope,
                "intercept": intercept,
                "r_squared": r_value ** 2,
                "p_value": p_value,
                "std_err": std_err
            },
            "stats": {
                "count": len(data_points),
                "avg_mileage": sum(mileages) / len(mileages),
                "avg_price": sum(prices) / len(prices),
                "correlation": r_value
            }
        }
        
        # Calculate price depreciation per mile
        if slope < 0:  # Negative slope means price decreases with mileage
            avg_price = sum(prices) / len(prices)
            depreciation_per_mile = abs(slope)
            depreciation_per_1000_miles = depreciation_per_mile * 1000
            depreciation_percent_per_1000_miles = (depreciation_per_1000_miles / avg_price) * 100
            
            analysis["depreciation"] = {
                "per_mile": depreciation_per_mile,
                "per_1000_miles": depreciation_per_1000_miles,
                "percent_per_1000_miles": depreciation_percent_per_1000_miles
            }
        
        # Save to cache
        self._cache_result("mileage_vs_price", params, analysis)
        
        return analysis
    
    def analyze_year_vs_price(self, filters: Dict = None) -> Dict:
        """
        Analyze relationship between vehicle year and price
        
        Args:
            filters (dict): Filters to apply
            
        Returns:
            dict: Analysis results
        """
        # Check for cached results
        params = {"filters": filters or {}}
        cached = self._get_cached_result("year_vs_price", params)
        if cached:
            return cached
            
        # Prepare filters
        if filters is None:
            filters = {}
            
        # Define batch processor function
        def process_batch(batch, state):
            if "data_by_year" not in state:
                state["data_by_year"] = {}
                
            for listing in batch:
                year = listing.get("year")
                price = listing.get("price")
                
                if year is not None and price is not None and price > 0:
                    if year not in state["data_by_year"]:
                        state["data_by_year"][year] = []
                        
                    state["data_by_year"][year].append(price)
            
            return state
        
        # Process data in batches
        result = self._process_data_batches(filters, process_batch)
        
        if "error" in result:
            return result
            
        data_by_year = result["data_by_year"]
        
        if not data_by_year:
            return {"error": "No valid year and price data found"}
            
        # Calculate statistics for each year
        years = sorted(data_by_year.keys())
        avg_prices = []
        median_prices = []
        min_prices = []
        max_prices = []
        counts = []
        
        for year in years:
            prices = data_by_year[year]
            avg_prices.append(sum(prices) / len(prices))
            median_prices.append(sorted(prices)[len(prices) // 2])
            min_prices.append(min(prices))
            max_prices.append(max(prices))
            counts.append(len(prices))
        
        # Calculate regression line
        slope, intercept, r_value, p_value, std_err = stats.linregress(years, avg_prices)
        
        # Calculate appreciation/depreciation per year
        avg_overall_price = sum(avg_prices) / len(avg_prices)
        price_change_per_year = slope
        percent_change_per_year = (price_change_per_year / avg_overall_price) * 100
        
        analysis = {
            "years": years,
            "avg_prices": avg_prices,
            "median_prices": median_prices,
            "min_prices": min_prices,
            "max_prices": max_prices,
            "counts": counts,
            "regression": {
                "slope": slope,
                "intercept": intercept,
                "r_squared": r_value ** 2,
                "p_value": p_value,
                "std_err": std_err
            },
            "price_change": {
                "per_year": price_change_per_year,
                "percent_per_year": percent_change_per_year,
                "direction": "appreciation" if slope > 0 else "depreciation"
            }
        }
        
        # Save to cache
        self._cache_result("year_vs_price", params, analysis)
        
        return analysis
    
    def get_popular_makes_models(self, limit: int = 10) -> Dict:
        """
        Get most popular vehicle makes and models by listing count
        
        Args:
            limit (int): Maximum number of results to return
            
        Returns:
            dict: Popular makes and models
        """
        # Check for cached results
        params = {"limit": limit}
        cached = self._get_cached_result("popular_makes_models", params)
        if cached:
            return cached
            
        conn = self.db_manager.connect()
        cursor = conn.cursor()
        
        try:
            # Get popular makes
            cursor.execute("""
            SELECT make, COUNT(*) as count
            FROM car_listings
            WHERE make IS NOT NULL AND make != ''
            GROUP BY make
            ORDER BY count DESC
            LIMIT ?
            """, (limit,))
            
            popular_makes = []
            for row in cursor.fetchall():
                popular_makes.append({
                    "make": row[0],
                    "count": row[1]
                })
            
            # Get popular models
            cursor.execute("""
            SELECT make, model, COUNT(*) as count
            FROM car_listings
            WHERE make IS NOT NULL AND model IS NOT NULL
            AND make != '' AND model != ''
            GROUP BY make, model
            ORDER BY count DESC
            LIMIT ?
            """, (limit,))
            
            popular_models = []
            for row in cursor.fetchall():
                popular_models.append({
                    "make": row[0],
                    "model": row[1],
                    "count": row[2]
                })
            
            result = {
                "popular_makes": popular_makes,
                "popular_models": popular_models
            }
            
            # Save to cache
            self._cache_result("popular_makes_models", params, result)
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error getting popular makes/models: {e}")
            return {"error": str(e)}
    
    def start_analysis_job(self, analysis_type: str, params: Dict) -> Dict:
        """
        Start an analysis job that can be paused/resumed
        
        Args:
            analysis_type (str): Type of analysis to run
            params (dict): Analysis parameters
            
        Returns:
            dict: Job information
        """
        # Cancel any running job
        if self.current_analysis_job:
            self.current_analysis_job["cancel"] = True
            time.sleep(0.5)
            
        # Create a new job
        job_id = f"job_{int(time.time())}"
        self.current_analysis_job = {
            "id": job_id,
            "type": analysis_type,
            "params": params,
            "start_time": datetime.now().isoformat(),
            "progress": 0.0,
            "cancel": False,
            "status": "running"
        }
        
        # Start the job in a separate thread
        thread = threading.Thread(
            target=self._run_analysis_job,
            args=(job_id, analysis_type, params),
            daemon=True
        )
        thread.start()
        
        return {
            "job_id": job_id,
            "status": "started",
            "type": analysis_type
        }
    
    def _run_analysis_job(self, job_id: str, analysis_type: str, params: Dict) -> None:
        """Run an analysis job in a background thread"""
        try:
            result = None
            
            if analysis_type == "price_trends":
                filters = params.get("filters", {})
                time_period = params.get("time_period", "all")
                result = self.analyze_price_trends(filters, time_period)
                
            elif analysis_type == "price_distribution":
                filters = params.get("filters", {})
                result = self.analyze_price_distribution(filters)
                
            elif analysis_type == "mileage_vs_price":
                filters = params.get("filters", {})
                result = self.analyze_mileage_vs_price(filters)
                
            elif analysis_type == "year_vs_price":
                filters = params.get("filters", {})
                result = self.analyze_year_vs_price(filters)
                
            # Update job with result
            if self.current_analysis_job and self.current_analysis_job["id"] == job_id:
                self.current_analysis_job["result"] = result
                self.current_analysis_job["end_time"] = datetime.now().isoformat()
                self.current_analysis_job["status"] = "completed"
                self.current_analysis_job["progress"] = 1.0
                
        except Exception as e:
            self.logger.error(f"Error in analysis job {job_id}: {e}")
            
            if self.current_analysis_job and self.current_analysis_job["id"] == job_id:
                self.current_analysis_job["error"] = str(e)
                self.current_analysis_job["end_time"] = datetime.now().isoformat()
                self.current_analysis_job["status"] = "error"
    
    def get_job_status(self, job_id: str = None) -> Dict:
        """
        Get status of an analysis job
        
        Args:
            job_id (str): Job ID to check, or None for current job
            
        Returns:
            dict: Job status information
        """
        if not self.current_analysis_job:
            return {"error": "No active job"}
            
        if job_id and self.current_analysis_job["id"] != job_id:
            return {"error": f"Job {job_id} not found"}
            
        return {
            "id": self.current_analysis_job["id"],
            "type": self.current_analysis_job["type"],
            "status": self.current_analysis_job["status"],
            "progress": self.current_analysis_job["progress"],
            "start_time": self.current_analysis_job["start_time"],
            "end_time": self.current_analysis_job.get("end_time")
        }
    
    def cancel_job(self, job_id: str = None) -> Dict:
        """
        Cancel an analysis job
        
        Args:
            job_id (str): Job ID to cancel, or None for current job
            
        Returns:
            dict: Result of cancellation
        """
        if not self.current_analysis_job:
            return {"error": "No active job"}
            
        if job_id and self.current_analysis_job["id"] != job_id:
            return {"error": f"Job {job_id} not found"}
            
        self.current_analysis_job["cancel"] = True
        self.current_analysis_job["status"] = "canceled"
        
        return {"status": "canceled", "id": self.current_analysis_job["id"]}
    
    def get_job_result(self, job_id: str = None) -> Dict:
        """
        Get the result of a completed analysis job
        
        Args:
            job_id (str): Job ID to get result for, or None for current job
            
        Returns:
            dict: Job result or error
        """
        if not self.current_analysis_job:
            return {"error": "No active job"}
            
        if job_id and self.current_analysis_job["id"] != job_id:
            return {"error": f"Job {job_id} not found"}
            
        if self.current_analysis_job["status"] != "completed":
            return {
                "error": f"Job not completed (status: {self.current_analysis_job['status']})",
                "progress": self.current_analysis_job["progress"]
            }
            
        return self.current_analysis_job.get("result", {"error": "No result available"})
    
    @lru_cache(maxsize=32)
    def get_makes_list(self) -> List[str]:
        """
        Get list of all car makes in the database
        
        Returns:
            list: List of make names
        """
        conn = self.db_manager.connect()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
            SELECT DISTINCT make FROM car_listings
            WHERE make IS NOT NULL AND make != ''
            ORDER BY make
            """)
            
            return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            self.logger.error(f"Error getting makes list: {e}")
            return []
    
    def get_models_for_make(self, make: str) -> List[str]:
        """
        Get list of models for a specific make
        
        Args:
            make (str): Car make name
            
        Returns:
            list: List of model names
        """
        conn = self.db_manager.connect()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
            SELECT DISTINCT model FROM car_listings
            WHERE make = ? AND model IS NOT NULL AND model != ''
            ORDER BY model
            """, (make,))
            
            return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            self.logger.error(f"Error getting models for {make}: {e}")
            return []
    
    def get_year_range(self, make: str = None, model: str = None) -> Tuple[int, int]:
        """
        Get min and max years in the database
        
        Args:
            make (str): Optional make to filter by
            model (str): Optional model to filter by
            
        Returns:
            tuple: (min_year, max_year)
        """
        conn = self.db_manager.connect()
        cursor = conn.cursor()
        
        query = """
        SELECT MIN(year), MAX(year) FROM car_listings
        WHERE year IS NOT NULL
        """
        
        params = []
        
        if make:
            query += " AND make = ?"
            params.append(make)
            
        if model:
            query += " AND model = ?"
            params.append(model)
        
        try:
            cursor.execute(query, params)
            row = cursor.fetchone()
            
            if row and row[0] and row[1]:
                return row[0], row[1]
            else:
                return 1990, datetime.now().year
        except Exception as e:
            self.logger.error(f"Error getting year range: {e}")
            return 1990, datetime.now().year