"""
Database Manager for Car Market Analyzer
Handles creation, connection, and efficient storage of car listing data
"""

import sqlite3
import json
import zlib
import logging
import os
import time
from pathlib import Path
from datetime import datetime, timedelta


class DatabaseManager:
    """Manages SQLite database operations with compression support"""
    
    def __init__(self, config):
        """Initialize database manager with configuration"""
        self.config = config
        self.db_path = Path(config.get("database", "path"))
        self.connection = None
        self.compression_enabled = config.get("database", "compression_enabled", True)
        self.max_size_mb = config.get("database", "max_size_mb", 500)
        self.retention_days = config.get("database", "retention_days", 90)
        
        # Ensure database directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Configure logger
        self.logger = logging.getLogger(__name__)
    
    def connect(self):
        """Establish database connection with pragmas for performance"""
        if self.connection is None:
            self.connection = sqlite3.connect(self.db_path)
            
            # Set pragmas for better performance
            self.connection.execute("PRAGMA journal_mode=WAL")
            self.connection.execute("PRAGMA synchronous=NORMAL")
            self.connection.execute("PRAGMA temp_store=MEMORY")
            self.connection.execute("PRAGMA mmap_size=30000000")  # Use memory-mapped I/O
            
            # Enable foreign keys
            self.connection.execute("PRAGMA foreign_keys=ON")
            
            # Register custom functions
            self.connection.create_function("decompress", 1, self._decompress_data)
        
        return self.connection
    
    def close(self):
        """Close database connection if open"""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def initialize_database(self):
        """Create database schema if it doesn't exist"""
        conn = self.connect()
        cursor = conn.cursor()
        
        # Create tables with appropriate indices
        cursor.executescript('''
        -- Car listings table
        CREATE TABLE IF NOT EXISTS car_listings (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            price REAL,
            year INTEGER,
            make TEXT,
            model TEXT,
            mileage INTEGER,
            location TEXT,
            listing_date TEXT,
            last_updated TEXT,
            url TEXT,
            data BLOB,  -- Compressed JSON data
            image_urls TEXT,
            status TEXT DEFAULT 'active',
            raw_html BLOB  -- Compressed HTML for fallback parsing
        );
        
        -- Create indices for common queries
        CREATE INDEX IF NOT EXISTS idx_listings_make_model ON car_listings(make, model);
        CREATE INDEX IF NOT EXISTS idx_listings_price ON car_listings(price);
        CREATE INDEX IF NOT EXISTS idx_listings_year ON car_listings(year);
        CREATE INDEX IF NOT EXISTS idx_listings_date ON car_listings(listing_date);
        CREATE INDEX IF NOT EXISTS idx_listings_status ON car_listings(status);
        
        -- Market stats table for pre-aggregated data
        CREATE TABLE IF NOT EXISTS market_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            make TEXT,
            model TEXT,
            year_min INTEGER,
            year_max INTEGER,
            stat_type TEXT NOT NULL,
            stat_value REAL,
            sample_size INTEGER,
            UNIQUE(date, make, model, year_min, year_max, stat_type)
        );
        
        CREATE INDEX IF NOT EXISTS idx_stats_make_model ON market_stats(make, model);
        CREATE INDEX IF NOT EXISTS idx_stats_date ON market_stats(date);
        CREATE INDEX IF NOT EXISTS idx_stats_type ON market_stats(stat_type);
        
        -- Scrape sessions table to track scraping history
        CREATE TABLE IF NOT EXISTS scrape_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            start_time TEXT NOT NULL,
            end_time TEXT,
            status TEXT DEFAULT 'in_progress',
            listings_found INTEGER DEFAULT 0,
            new_listings INTEGER DEFAULT 0,
            updated_listings INTEGER DEFAULT 0,
            error_message TEXT,
            search_params TEXT  -- JSON string of search parameters
        );
        
        -- User saved searches
        CREATE TABLE IF NOT EXISTS saved_searches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            search_params TEXT NOT NULL,  -- JSON string
            created_date TEXT NOT NULL,
            last_run TEXT,
            auto_run BOOLEAN DEFAULT 0
        );
        
        -- Search alerts for price changes
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            search_id INTEGER,
            alert_type TEXT NOT NULL,
            threshold REAL,
            active BOOLEAN DEFAULT 1,
            last_triggered TEXT,
            FOREIGN KEY (search_id) REFERENCES saved_searches(id) ON DELETE CASCADE
        );
        
        -- Cache table for analysis results
        CREATE TABLE IF NOT EXISTS analysis_cache (
            cache_key TEXT PRIMARY KEY,
            data BLOB,
            created TEXT NOT NULL,
            expires TEXT NOT NULL
        );
        
        CREATE INDEX IF NOT EXISTS idx_cache_expires ON analysis_cache(expires);
        ''')
        
        conn.commit()
        
        # Check if we need to perform maintenance
        self._perform_maintenance()
    
    def _compress_data(self, data):
        """Compress JSON data for efficient storage"""
        if not self.compression_enabled:
            return json.dumps(data)
        
        try:
            json_str = json.dumps(data)
            return zlib.compress(json_str.encode('utf-8'))
        except Exception as e:
            self.logger.error(f"Compression error: {e}")
            # Fallback to uncompressed
            return json.dumps(data)
    
    def _decompress_data(self, compressed_data):
        """Decompress data from the database"""
        if not compressed_data:
            return '{}'
            
        try:
            # Check if the data is compressed
            if isinstance(compressed_data, bytes) and compressed_data.startswith(b'x\x9c'):
                decompressed = zlib.decompress(compressed_data).decode('utf-8')
                return decompressed
            elif isinstance(compressed_data, str):
                return compressed_data
            else:
                return compressed_data.decode('utf-8')
        except Exception as e:
            self.logger.error(f"Decompression error: {e}")
            return '{}'
    
    def save_car_listing(self, listing_data):
        """
        Save a car listing to the database
        
        Args:
            listing_data (dict): Car listing information
            
        Returns:
            bool: Success status
        """
        conn = self.connect()
        cursor = conn.cursor()
        
        now = datetime.now().isoformat()
        
        try:
            # Check if listing already exists
            cursor.execute("SELECT id, last_updated FROM car_listings WHERE id = ?", 
                          (listing_data.get('id'),))
            existing = cursor.fetchone()
            
            # Prepare compressed data
            if 'raw_html' in listing_data:
                if self.compression_enabled:
                    listing_data['raw_html'] = zlib.compress(listing_data['raw_html'].encode('utf-8'))
                else:
                    listing_data['raw_html'] = listing_data['raw_html'].encode('utf-8')
            
            # Extract fields that are stored in separate columns
            main_data = {}
            for key, value in listing_data.items():
                if key not in ['id', 'title', 'price', 'year', 'make', 'model', 
                              'mileage', 'location', 'listing_date', 'url', 
                              'image_urls', 'status', 'raw_html']:
                    main_data[key] = value
            
            # Compress the main data
            compressed_data = self._compress_data(main_data)
            
            # Process image URLs
            image_urls = json.dumps(listing_data.get('image_urls', []))
            
            if existing:
                # Update existing listing
                cursor.execute('''
                UPDATE car_listings SET
                    title = ?,
                    price = ?,
                    year = ?,
                    make = ?,
                    model = ?,
                    mileage = ?,
                    location = ?,
                    last_updated = ?,
                    url = ?,
                    data = ?,
                    image_urls = ?,
                    status = ?
                WHERE id = ?
                ''', (
                    listing_data.get('title', ''),
                    listing_data.get('price'),
                    listing_data.get('year'),
                    listing_data.get('make', ''),
                    listing_data.get('model', ''),
                    listing_data.get('mileage'),
                    listing_data.get('location', ''),
                    now,
                    listing_data.get('url', ''),
                    compressed_data,
                    image_urls,
                    listing_data.get('status', 'active'),
                    listing_data['id']
                ))
                
                # Only update raw_html if it's provided
                if 'raw_html' in listing_data:
                    cursor.execute('''
                    UPDATE car_listings SET raw_html = ? WHERE id = ?
                    ''', (listing_data['raw_html'], listing_data['id']))
            else:
                # Insert new listing
                cursor.execute('''
                INSERT INTO car_listings (
                    id, title, price, year, make, model, mileage, location, 
                    listing_date, last_updated, url, data, image_urls, status, raw_html
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    listing_data['id'],
                    listing_data.get('title', ''),
                    listing_data.get('price'),
                    listing_data.get('year'),
                    listing_data.get('make', ''),
                    listing_data.get('model', ''),
                    listing_data.get('mileage'),
                    listing_data.get('location', ''),
                    listing_data.get('listing_date', now),
                    now,
                    listing_data.get('url', ''),
                    compressed_data,
                    image_urls,
                    listing_data.get('status', 'active'),
                    listing_data.get('raw_html', None)
                ))
            
            conn.commit()
            return True
            
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Error saving listing: {e}")
            return False
    
    def save_listings_batch(self, listings):
        """
        Save multiple car listings in a batch for better performance
        
        Args:
            listings (list): List of car listing dictionaries
            
        Returns:
            tuple: (success_count, error_count)
        """
        conn = self.connect()
        cursor = conn.cursor()
        
        now = datetime.now().isoformat()
        success_count = 0
        error_count = 0
        
        try:
            conn.execute("BEGIN TRANSACTION")
            
            for listing_data in listings:
                try:
                    # Check if listing already exists
                    cursor.execute("SELECT id FROM car_listings WHERE id = ?", 
                                  (listing_data.get('id'),))
                    existing = cursor.fetchone()
                    
                    # Prepare compressed data
                    if 'raw_html' in listing_data:
                        if self.compression_enabled:
                            listing_data['raw_html'] = zlib.compress(listing_data['raw_html'].encode('utf-8'))
                        else:
                            listing_data['raw_html'] = listing_data['raw_html'].encode('utf-8')
                    
                    # Extract fields that are stored in separate columns
                    main_data = {}
                    for key, value in listing_data.items():
                        if key not in ['id', 'title', 'price', 'year', 'make', 'model', 
                                      'mileage', 'location', 'listing_date', 'url', 
                                      'image_urls', 'status', 'raw_html']:
                            main_data[key] = value
                    
                    # Compress the main data
                    compressed_data = self._compress_data(main_data)
                    
                    # Process image URLs
                    image_urls = json.dumps(listing_data.get('image_urls', []))
                    
                    if existing:
                        # Update existing listing
                        cursor.execute('''
                        UPDATE car_listings SET
                            title = ?,
                            price = ?,
                            year = ?,
                            make = ?,
                            model = ?,
                            mileage = ?,
                            location = ?,
                            last_updated = ?,
                            url = ?,
                            data = ?,
                            image_urls = ?,
                            status = ?
                        WHERE id = ?
                        ''', (
                            listing_data.get('title', ''),
                            listing_data.get('price'),
                            listing_data.get('year'),
                            listing_data.get('make', ''),
                            listing_data.get('model', ''),
                            listing_data.get('mileage'),
                            listing_data.get('location', ''),
                            now,
                            listing_data.get('url', ''),
                            compressed_data,
                            image_urls,
                            listing_data.get('status', 'active'),
                            listing_data['id']
                        ))
                        
                        # Only update raw_html if it's provided
                        if 'raw_html' in listing_data:
                            cursor.execute('''
                            UPDATE car_listings SET raw_html = ? WHERE id = ?
                            ''', (listing_data['raw_html'], listing_data['id']))
                    else:
                        # Insert new listing
                        cursor.execute('''
                        INSERT INTO car_listings (
                            id, title, price, year, make, model, mileage, location, 
                            listing_date, last_updated, url, data, image_urls, status, raw_html
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            listing_data['id'],
                            listing_data.get('title', ''),
                            listing_data.get('price'),
                            listing_data.get('year'),
                            listing_data.get('make', ''),
                            listing_data.get('model', ''),
                            listing_data.get('mileage'),
                            listing_data.get('location', ''),
                            listing_data.get('listing_date', now),
                            now,
                            listing_data.get('url', ''),
                            compressed_data,
                            image_urls,
                            listing_data.get('status', 'active'),
                            listing_data.get('raw_html', None)
                        ))
                    
                    success_count += 1
                    
                except Exception as e:
                    self.logger.error(f"Error in batch processing listing {listing_data.get('id', 'unknown')}: {e}")
                    error_count += 1
            
            conn.commit()
            
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Batch save transaction failed: {e}")
            error_count += len(listings) - success_count
            
        return success_count, error_count
    
    def get_car_listing(self, listing_id):
        """
        Get a car listing by ID
        
        Args:
            listing_id (str): Listing ID
            
        Returns:
            dict: Car listing data or None if not found
        """
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            # Get the listing data
            cursor.execute('''
            SELECT 
                id, title, price, year, make, model, mileage, location, 
                listing_date, last_updated, url, data, image_urls, status
            FROM car_listings 
            WHERE id = ?
            ''', (listing_id,))
            
            row = cursor.fetchone()
            if not row:
                return None
                
            # Build the result dictionary
            result = {
                'id': row[0],
                'title': row[1],
                'price': row[2],
                'year': row[3],
                'make': row[4],
                'model': row[5],
                'mileage': row[6],
                'location': row[7],
                'listing_date': row[8],
                'last_updated': row[9],
                'url': row[10],
                'image_urls': json.loads(row[12]) if row[12] else [],
                'status': row[13]
            }
            
            # Decompress and add the remaining data
            if row[11]:
                additional_data = json.loads(self._decompress_data(row[11]))
                result.update(additional_data)
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error retrieving listing {listing_id}: {e}")
            return None
    
    def get_listings(self, filters=None, sort_by="listing_date", sort_order="DESC", limit=100, offset=0):
        """
        Get car listings with optional filtering and sorting
        
        Args:
            filters (dict): Filters to apply (optional)
            sort_by (str): Column to sort by
            sort_order (str): ASC or DESC
            limit (int): Maximum number of results
            offset (int): Offset for pagination
            
        Returns:
            list: List of car listing dictionaries
        """
        conn = self.connect()
        cursor = conn.cursor()
        
        # Build query
        query = '''
        SELECT 
            id, title, price, year, make, model, mileage, location, 
            listing_date, last_updated, url, data, image_urls, status
        FROM car_listings
        '''
        
        params = []
        where_clauses = []
        
        # Apply filters
        if filters:
            for key, value in filters.items():
                if key == 'price_min':
                    where_clauses.append("price >= ?")
                    params.append(value)
                elif key == 'price_max':
                    where_clauses.append("price <= ?")
                    params.append(value)
                elif key == 'year_min':
                    where_clauses.append("year >= ?")
                    params.append(value)
                elif key == 'year_max':
                    where_clauses.append("year <= ?")
                    params.append(value)
                elif key == 'mileage_max':
                    where_clauses.append("mileage <= ?")
                    params.append(value)
                elif key == 'make':
                    where_clauses.append("make LIKE ?")
                    params.append(f"%{value}%")
                elif key == 'model':
                    where_clauses.append("model LIKE ?")
                    params.append(f"%{value}%")
                elif key == 'status':
                    where_clauses.append("status = ?")
                    params.append(value)
                elif key == 'search_term':
                    where_clauses.append("(title LIKE ? OR make LIKE ? OR model LIKE ?)")
                    params.extend([f"%{value}%", f"%{value}%", f"%{value}%"])
        
        # Default to active listings only
        if filters is None or 'status' not in filters:
            where_clauses.append("status = 'active'")
        
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)
        
        # Add sorting
        valid_sort_columns = [
            'price', 'year', 'mileage', 'listing_date', 'last_updated', 
            'make', 'model', 'title'
        ]
        valid_sort_orders = ['ASC', 'DESC']
        
        if sort_by not in valid_sort_columns:
            sort_by = 'listing_date'
        
        if sort_order not in valid_sort_orders:
            sort_order = 'DESC'
            
        query += f" ORDER BY {sort_by} {sort_order}"
        
        # Add pagination
        query += " LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        
        try:
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            results = []
            for row in rows:
                listing = {
                    'id': row[0],
                    'title': row[1],
                    'price': row[2],
                    'year': row[3],
                    'make': row[4],
                    'model': row[5],
                    'mileage': row[6],
                    'location': row[7],
                    'listing_date': row[8],
                    'last_updated': row[9],
                    'url': row[10],
                    'image_urls': json.loads(row[12]) if row[12] else [],
                    'status': row[13]
                }
                
                # Decompress additional data
                if row[11]:
                    additional_data = json.loads(self._decompress_data(row[11]))
                    listing.update(additional_data)
                
                results.append(listing)
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error retrieving listings: {e}")
            return []
    
    def count_listings(self, filters=None):
        """
        Count listings matching the filters
        
        Args:
            filters (dict): Filters to apply (optional)
            
        Returns:
            int: Count of matching listings
        """
        conn = self.connect()
        cursor = conn.cursor()
        
        # Build query
        query = "SELECT COUNT(*) FROM car_listings"
        
        params = []
        where_clauses = []
        
        # Apply filters
        if filters:
            for key, value in filters.items():
                if key == 'price_min':
                    where_clauses.append("price >= ?")
                    params.append(value)
                elif key == 'price_max':
                    where_clauses.append("price <= ?")
                    params.append(value)
                elif key == 'year_min':
                    where_clauses.append("year >= ?")
                    params.append(value)
                elif key == 'year_max':
                    where_clauses.append("year <= ?")
                    params.append(value)
                elif key == 'mileage_max':
                    where_clauses.append("mileage <= ?")
                    params.append(value)
                elif key == 'make':
                    where_clauses.append("make LIKE ?")
                    params.append(f"%{value}%")
                elif key == 'model':
                    where_clauses.append("model LIKE ?")
                    params.append(f"%{value}%")
                elif key == 'status':
                    where_clauses.append("status = ?")
                    params.append(value)
                elif key == 'search_term':
                    where_clauses.append("(title LIKE ? OR make LIKE ? OR model LIKE ?)")
                    params.extend([f"%{value}%", f"%{value}%", f"%{value}%"])
        
        # Default to active listings only
        if filters is None or 'status' not in filters:
            where_clauses.append("status = 'active'")
        
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)
        
        try:
            cursor.execute(query, params)
            count = cursor.fetchone()[0]
            return count
        except Exception as e:
            self.logger.error(f"Error counting listings: {e}")
            return 0
    
    def start_scrape_session(self, search_params=None):
        """
        Record the start of a scraping session
        
        Args:
            search_params (dict): Search parameters
            
        Returns:
            int: Session ID
        """
        conn = self.connect()
        cursor = conn.cursor()
        
        now = datetime.now().isoformat()
        search_params_json = json.dumps(search_params) if search_params else None
        
        try:
            cursor.execute('''
            INSERT INTO scrape_sessions (start_time, status, search_params)
            VALUES (?, 'in_progress', ?)
            ''', (now, search_params_json))
            
            conn.commit()
            return cursor.lastrowid
        except Exception as e:
            self.logger.error(f"Error starting scrape session: {e}")
            return None
    
    def end_scrape_session(self, session_id, status='completed', listings_found=0, 
                           new_listings=0, updated_listings=0, error_message=None):
        """
        Record the end of a scraping session
        
        Args:
            session_id (int): Session ID
            status (str): Session status
            listings_found (int): Total listings found
            new_listings (int): New listings added
            updated_listings (int): Existing listings updated
            error_message (str): Error message if failed
            
        Returns:
            bool: Success status
        """
        if session_id is None:
            return False
            
        conn = self.connect()
        cursor = conn.cursor()
        
        now = datetime.now().isoformat()
        
        try:
            cursor.execute('''
            UPDATE scrape_sessions SET
                end_time = ?,
                status = ?,
                listings_found = ?,
                new_listings = ?,
                updated_listings = ?,
                error_message = ?
            WHERE id = ?
            ''', (now, status, listings_found, new_listings, updated_listings, 
                 error_message, session_id))
            
            conn.commit()
            return True
        except Exception as e:
            self.logger.error(f"Error ending scrape session: {e}")
            return False
    
    def save_market_stat(self, date, make, model, year_min, year_max, stat_type, stat_value, sample_size):
        """
        Save a pre-calculated market statistic
        
        Args:
            date (str): Date of the statistic
            make (str): Car make
            model (str): Car model
            year_min (int): Minimum year in range
            year_max (int): Maximum year in range
            stat_type (str): Type of statistic (avg_price, median_price, etc.)
            stat_value (float): Value of the statistic
            sample_size (int): Number of listings used in calculation
            
        Returns:
            bool: Success status
        """
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
            INSERT OR REPLACE INTO market_stats 
                (date, make, model, year_min, year_max, stat_type, stat_value, sample_size)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (date, make, model, year_min, year_max, stat_type, stat_value, sample_size))
            
            conn.commit()
            return True
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Error saving market stat: {e}")
            return False
    
    def get_market_stats(self, make=None, model=None, year_min=None, year_max=None, 
                         stat_type=None, date_from=None, date_to=None):
        """
        Get market statistics with optional filtering
        
        Args:
            Various filter parameters
            
        Returns:
            list: List of statistic dictionaries
        """
        conn = self.connect()
        cursor = conn.cursor()
        
        query = "SELECT * FROM market_stats"
        params = []
        where_clauses = []
        
        if make:
            where_clauses.append("make = ?")
            params.append(make)
        
        if model:
            where_clauses.append("model = ?")
            params.append(model)
            
        if year_min:
            where_clauses.append("year_min >= ?")
            params.append(year_min)
            
        if year_max:
            where_clauses.append("year_max <= ?")
            params.append(year_max)
            
        if stat_type:
            where_clauses.append("stat_type = ?")
            params.append(stat_type)
            
        if date_from:
            where_clauses.append("date >= ?")
            params.append(date_from)
            
        if date_to:
            where_clauses.append("date <= ?")
            params.append(date_to)
        
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)
            
        query += " ORDER BY date DESC"
        
        try:
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            # Convert to dictionaries
            results = []
            for row in rows:
                results.append({
                    'id': row[0],
                    'date': row[1],
                    'make': row[2],
                    'model': row[3],
                    'year_min': row[4],
                    'year_max': row[5],
                    'stat_type': row[6],
                    'stat_value': row[7],
                    'sample_size': row[8]
                })
                
            return results
        except Exception as e:
            self.logger.error(f"Error retrieving market stats: {e}")
            return []
    
    def save_to_cache(self, cache_key, data, ttl_minutes=60):
        """
        Save analysis results to cache
        
        Args:
            cache_key (str): Unique identifier for the cached data
            data (any): Data to cache (will be JSON serialized and compressed)
            ttl_minutes (int): Cache TTL in minutes
            
        Returns:
            bool: Success status
        """
        conn = self.connect()
        cursor = conn.cursor()
        
        now = datetime.now()
        created = now.isoformat()
        expires = (now + timedelta(minutes=ttl_minutes)).isoformat()
        
        try:
            # Compress the data
            compressed_data = self._compress_data(data)
            
            cursor.execute('''
            INSERT OR REPLACE INTO analysis_cache (cache_key, data, created, expires)
            VALUES (?, ?, ?, ?)
            ''', (cache_key, compressed_data, created, expires))
            
            conn.commit()
            return True
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Error saving to cache: {e}")
            return False
    
    def get_from_cache(self, cache_key):
        """
        Get data from cache if it exists and hasn't expired
        
        Args:
            cache_key (str): Cache key
            
        Returns:
            any: Cached data or None if not found or expired
        """
        conn = self.connect()
        cursor = conn.cursor()
        
        now = datetime.now().isoformat()
        
        try:
            cursor.execute('''
            SELECT data FROM analysis_cache
            WHERE cache_key = ? AND expires > ?
            ''', (cache_key, now))
            
            row = cursor.fetchone()
            if not row:
                return None
                
            # Decompress the data
            decompressed = self._decompress_data(row[0])
            return json.loads(decompressed)
        except Exception as e:
            self.logger.error(f"Error retrieving from cache: {e}")
            return None
    
    def clear_expired_cache(self):
        """Clear expired cache entries"""
        conn = self.connect()
        cursor = conn.cursor()
        
        now = datetime.now().isoformat()
        
        try:
            cursor.execute("DELETE FROM analysis_cache WHERE expires <= ?", (now,))
            conn.commit()
            return cursor.rowcount
        except Exception as e:
            self.logger.error(f"Error clearing expired cache: {e}")
            return 0
    
    def _perform_maintenance(self):
        """Perform database maintenance operations"""
        # Check database size
        try:
            db_size_mb = os.path.getsize(self.db_path) / (1024 * 1024)
            
            if db_size_mb > self.max_size_mb:
                self._prune_old_data()
            
            # Clear expired cache entries
            self.clear_expired_cache()
            
            # Check if vacuum is needed
            vacuum_threshold = self.config.get("database", "vacuum_threshold", 0.2)
            if db_size_mb > 10 and random.random() < vacuum_threshold:
                self._vacuum_database()
                
        except Exception as e:
            self.logger.error(f"Error during database maintenance: {e}")
    
    def _prune_old_data(self):
        """Remove old data to keep database size in check"""
        conn = self.connect()
        cursor = conn.cursor()
        
        cutoff_date = (datetime.now() - timedelta(days=self.retention_days)).isoformat()
        
        try:
            # Mark old inactive listings as archived
            cursor.execute('''
            UPDATE car_listings 
            SET status = 'archived', raw_html = NULL
            WHERE last_updated < ? AND status = 'inactive'
            ''', (cutoff_date,))
            
            # Delete very old archived listings
            very_old_cutoff = (datetime.now() - timedelta(days=self.retention_days * 2)).isoformat()
            cursor.execute('''
            DELETE FROM car_listings
            WHERE status = 'archived' AND last_updated < ?
            ''', (very_old_cutoff,))
            
            conn.commit()
            return True
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Error pruning old data: {e}")
            return False
    
    def _vacuum_database(self):
        """Run VACUUM to reclaim space and defragment the database"""
        conn = self.connect()
        
        try:
            conn.execute("VACUUM")
            return True
        except Exception as e:
            self.logger.error(f"Error vacuuming database: {e}")
            return False