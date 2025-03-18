"""
Alarm Data Monitor

A streamlined application that monitors a directory for Automation Studios alarm CSV files,
parses them, and stores the data in a MySQL database for later analysis.

This is the simplified version focused solely on continuous monitoring mode.
"""

import os
import glob
import time
import csv
import mysql.connector
import logging
import configparser
from datetime import datetime
from typing import List, Dict, Union, Optional
from contextlib import contextmanager

# Create logger without handlers first (will be properly configured in _setup_logging)
logger = logging.getLogger("AlarmMonitor")
# Prevent logger from propagating messages to the root logger
logger.propagate = False
# Remove any existing handlers to avoid duplicates
if logger.handlers:
    logger.handlers = []

class AlarmMonitor:
    """
    A class to monitor, parse, and store Automation Studios alarm data.
    
    This application continuously monitors a specified directory for new alarm CSV files,
    processes them, and stores the data in a MySQL database. It's designed to run
    indefinitely until manually stopped.
    """
    
    def __init__(self, config_file: str = "config.ini"):
        """
        Initialize the AlarmMonitor with configuration from a file.
        
        Args:
            config_file: Path to the configuration file (default: "config.ini")
        """
        self.config_file = config_file
        self.config = self._load_config()
        self.db_connection = None
        self.latest_processed_file = None
        self.is_running = False
        self.max_reconnect_attempts = 5
        self.reconnect_delay = 5  # seconds
        
        # Check MySQL service availability before starting
        if self.check_mysql_availability():
            self.connect_to_database()
        else:
            # Error details already logged in check_mysql_availability
            print("MySQL service is not available. Please ensure MySQL is running before proceeding.")
    
    @contextmanager
    def get_cursor(self, dictionary=False):
        """
        Context manager for database cursors.
        
        This method is decorated with @contextmanager to automatically handle
        resource acquisition and release with the 'with' statement.
        
        Usage example:
            with self.get_cursor() as cursor:
                cursor.execute("SELECT * FROM table")
                results = cursor.fetchall()
            # cursor is automatically closed here, even if an exception occurred
        
        Args:
            dictionary: If True, returns rows as dictionaries instead of tuples
            
        Yields:
            A database cursor
        """
        # Ensure connection is active
        if not self.db_connection or not self.db_connection.is_connected():
            self.reconnect_to_database()
        
        cursor = None
        try:
            cursor = self.db_connection.cursor(dictionary=dictionary)
            yield cursor
        finally:
            if cursor is not None:
                cursor.close()
                
    def check_mysql_availability(self) -> bool:
        """
        Check if MySQL service is available by attempting a connection.
        
        This method first tries to connect to the MySQL server without a specific database,
        then attempts to connect to the configured database.
        
        Returns:
            bool: True if MySQL service and database are available, False otherwise
        """
        try:
            # First, check if MySQL server is running (without specifying a database)
            try:
                conn = mysql.connector.connect(
                    host=self.config['DEFAULT']['db_host'],
                    port=int(self.config['DEFAULT']['db_port']),
                    user=self.config['DEFAULT']['db_user'],
                    password=self.config['DEFAULT']['db_password'],
                    connection_timeout=5,
                    database=None
                )
                conn.close()
                logger.info("MySQL server is running and accessible")
                
                # Now try connecting to the specific database
                try:
                    conn = mysql.connector.connect(
                        host=self.config['DEFAULT']['db_host'],
                        port=int(self.config['DEFAULT']['db_port']),
                        user=self.config['DEFAULT']['db_user'],
                        password=self.config['DEFAULT']['db_password'],
                        connection_timeout=5,
                        database=self.config['DEFAULT']['db_name']
                    )
                    conn.close()
                    logger.info(f"Successfully connected to database '{self.config['DEFAULT']['db_name']}'")
                    return True
                except mysql.connector.Error as db_err:
                    # Database-specific errors
                    err_msg = str(db_err)
                    if "1049" in err_msg:  # Unknown database
                        error_msg = f"Database '{self.config['DEFAULT']['db_name']}' does not exist. Please create it first."
                        logger.error(error_msg)
                        print(error_msg)
                    elif "1045" in err_msg:  # Access denied
                        error_msg = f"Access denied to database '{self.config['DEFAULT']['db_name']}'. Please check username and password."
                        logger.error(error_msg)
                        print(error_msg)
                    else:
                        logger.error(f"Database connection error: {db_err}")
                    return False
                    
            except mysql.connector.Error as server_err:
                # Server-level errors
                err_msg = str(server_err)
                if "2003" in err_msg:  # Can't connect to MySQL server
                    logger.error("MySQL server is not running or not accessible")
                    print("MySQL server is not running. Please start the MySQL service.")
                elif "1045" in err_msg:  # Access denied for server
                    error_msg = "Access denied to MySQL server. Please check username and password."
                    logger.error(error_msg)
                    print(error_msg)
                else:
                    logger.error(f"MySQL server error: {server_err}")
                return False
                
        except Exception as e:
            logger.error(f"Unexpected error during MySQL availability check: {e}")
            return False
            
    def reconnect_to_database(self) -> bool:
        """
        Attempt to reconnect to the database with multiple retries.
        
        Returns:
            bool: True if reconnection successful, False otherwise
        """
        attempts = 0
        while attempts < self.max_reconnect_attempts:
            # Log detailed information on first attempt, simplified logs for subsequent attempts
            if attempts == 0:
                logger.info(f"Attempting to reconnect to database (max attempts: {self.max_reconnect_attempts})")
            else:
                logger.debug(f"Reconnection attempt {attempts + 1}/{self.max_reconnect_attempts}")
                
            if self.connect_to_database():
                logger.info("Successfully reconnected to database")
                return True
                
            attempts += 1
            if attempts < self.max_reconnect_attempts:
                if attempts == 1:  # Only log wait information on first failure
                    logger.info(f"Reconnection failed. Will retry with {self.reconnect_delay} second intervals...")
                time.sleep(self.reconnect_delay)
                
        logger.error(f"Failed to reconnect to database after {self.max_reconnect_attempts} attempts")
        return False
    
    def _load_config(self) -> configparser.ConfigParser:
        """
        Load configuration from file or create default if not found.
        
        Returns:
            ConfigParser object with loaded configuration
        """
        config = configparser.ConfigParser()
        
        # Default configuration values
        config['DEFAULT'] = {
            'monitoring_dir': './alarm_files',  # Directory to watch for alarm files
            'file_pattern': 'Alarms_*.csv',     # Pattern to match alarm files
            'polling_interval': '10',           # Seconds between checks for new files
            'db_host': 'localhost',             # Database connection parameters
            'db_port': '3306',
            'db_user': 'alarm_user',
            'db_password': 'password',
            'db_name': 'alarm_db',
            'table_name': 'alarms'              # Database table name
        }
        
        # Default logging configuration
        config['LOGGING'] = {
            'log_level': 'INFO',
            'log_file': 'alarm_monitor.log'
        }
        
        # Try to load from file if it exists
        if os.path.exists(self.config_file):
            try:
                # Explicitly use UTF-8 encoding for config file reading
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    config.read_file(f)
                logger.info(f"Configuration loaded from {self.config_file} with UTF-8 encoding")
            except UnicodeDecodeError:
                # Fallback to other encodings if UTF-8 fails
                try:
                    with open(self.config_file, 'r', encoding='gbk') as f:
                        config.read_file(f)
                    logger.info(f"Configuration loaded from {self.config_file} with GBK encoding")
                except Exception as e:
                    logger.error(f"Error loading configuration file with GBK encoding: {e}")
                    logger.info("Using default configuration")
            except Exception as e:
                logger.error(f"Error loading configuration file: {e}")
                logger.info("Using default configuration")
        else:
            # Create config file with defaults if it doesn't exist
            try:
                # Explicitly use UTF-8 encoding for config file writing
                with open(self.config_file, 'w', encoding='utf-8') as f:
                    config.write(f)
                logger.info(f"Created default configuration file at {self.config_file} with UTF-8 encoding")
            except Exception as e:
                logger.error(f"Error creating default configuration file: {e}")
        
        # Set up logging based on loaded configuration
        self._setup_logging(config)
        
        return config
    
    def _setup_logging(self, config: configparser.ConfigParser) -> None:
        """
        Configure logging based on settings from config file.
        
        Args:
            config: ConfigParser object with loaded configuration
        """
        global logger
        
        # Get logging configuration from config file
        log_level_name = config.get('LOGGING', 'log_level', fallback='INFO')
        log_file = config.get('LOGGING', 'log_file', fallback='alarm_monitor.log')
        
        # Map string log level to logging constants
        log_level = getattr(logging, log_level_name.upper(), logging.INFO)
        
        # Clean up the root logger to prevent duplicate messages
        root_logger = logging.getLogger()
        if root_logger.handlers:
            for handler in root_logger.handlers[:]:
                root_logger.removeHandler(handler)
                
        # Reset the AlarmMonitor logger completely
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)

        # Make sure our logger doesn't propagate to the root logger
        logger.propagate = False
            
        # Create file handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        
        # Create console handler with simplified format
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        
        # Configure logger
        logger.setLevel(log_level)
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        logger.info(f"Logging configured with level {log_level_name} to file {log_file}")
    
    def connect_to_database(self) -> bool:
        """
        Connect to the MySQL database using configuration parameters.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            # Create connection to MySQL database
            self.db_connection = mysql.connector.connect(
                host=self.config['DEFAULT']['db_host'],
                port=int(self.config['DEFAULT']['db_port']),
                user=self.config['DEFAULT']['db_user'],
                password=self.config['DEFAULT']['db_password'],
                database=self.config['DEFAULT']['db_name']
            )
            logger.info("Connected to the database successfully")
            
            # Ensure table exists for storing alarm data
            self._ensure_table_exists()
            return True
        except mysql.connector.Error as err:
            logger.error(f"Database connection error: {err}")
            return False
    
    def _ensure_table_exists(self) -> None:
        """
        Create the alarms table if it doesn't exist in the database.
        
        This method creates a table with columns exactly matching the CSV file structure
        plus additional metadata columns for tracking and querying.
        
        The Time field has a unique index to improve performance of duplicate checking.
        """
        table_name = self.config['DEFAULT']['table_name']
        
        # Using context manager for cursor
        with self.get_cursor() as cursor:
            # SQL for creating table if it doesn't exist
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INT AUTO_INCREMENT PRIMARY KEY,              /* Unique identifier for each record */
                Time DATETIME(3) NOT NULL,                      /* Time field from CSV - timestamp with milliseconds */
                Instance INT NOT NULL,                          /* Instance ID from CSV */
                Name VARCHAR(255) NOT NULL,                     /* Alarm name from CSV */
                Code INT NOT NULL,                              /* Alarm code from CSV */
                Severity INT NOT NULL,                          /* Severity level from CSV */
                AdditionalInformation1 VARCHAR(255),            /* Additional information field 1 */
                AdditionalInformation2 VARCHAR(255),            /* Additional information field 2 */
                `Change` TEXT NOT NULL,                         /* Alarm status change (e.g., "Inactive -> Active") */
                Message TEXT NOT NULL,                          /* Alarm message text */
                file_source VARCHAR(255) NOT NULL,              /* Source file name for tracking */
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, /* Record creation timestamp */
                INDEX idx_time (Time)                           /* Index on Time for faster lookups */
            )
            """
            
            try:
                cursor.execute(create_table_query)
                self.db_connection.commit()
                logger.info(f"Table '{table_name}' checked/created successfully")
            except mysql.connector.Error as err:
                logger.error(f"Error creating table: {err}")
    
    def find_latest_alarm_file(self) -> Optional[str]:
        """
        Find the latest alarm CSV file in the monitoring directory based on filename timestamp.
        
        The function expects files named in format: Alarms_YYYY_MM_DD_HH_MM_SS.csv
        
        Returns:
            str: Path to the latest alarm file, or None if no files found
        """
        # Get search configuration
        monitoring_dir = self.config['DEFAULT']['monitoring_dir']
        file_pattern = self.config['DEFAULT']['file_pattern']
        
        # Handle path encoding for non-ASCII characters (like Chinese)
        try:
            # Normalize the path to handle Windows path separators and encoding
            monitoring_dir = os.path.normpath(monitoring_dir)
            search_pattern = os.path.join(monitoring_dir, file_pattern)
            
            # Find all files matching pattern
            files = glob.glob(search_pattern)
            
            # If no files found, try alternative approach for paths with special characters
            if not files and not os.path.exists(monitoring_dir):
                logger.warning(f"Monitoring directory not found with direct path: {monitoring_dir}")
                # Try to use the raw path with utf-8 encoding
                monitoring_dir = monitoring_dir.encode('utf-8').decode('utf-8')
                search_pattern = os.path.join(monitoring_dir, file_pattern)
                files = glob.glob(search_pattern)
                logger.info(f"Retried with UTF-8 encoding, found {len(files)} files")
            
            if not files:
                logger.debug(f"No files found matching pattern {search_pattern}")
                return None
            
            # Sort files by timestamp in the filename rather than modification time
            def extract_timestamp(filename):
                try:
                    # Extract parts from filename (Alarms_YYYY_MM_DD_HH_MM_SS.csv)
                    parts = os.path.basename(filename).split('.')[0].split('_')
                    if len(parts) >= 7:  # Alarms_YYYY_MM_DD_HH_MM_SS
                        # Reconstruct the timestamp string
                        year, month, day, hour, minute, second = parts[1:7]
                        # Handle milliseconds if present
                        msec = parts[7] if len(parts) > 7 else "00"
                        # Create datetime object
                        return datetime(
                            int(year), int(month), int(day), 
                            int(hour), int(minute), int(second), 
                            int(msec) * 1000 if msec else 0
                        )
                except (ValueError, IndexError) as e:
                    # If parsing fails, fall back to file modification time
                    logger.warning(f"Could not parse timestamp from filename {filename}: {e}")
                    return datetime.fromtimestamp(os.path.getmtime(filename))
                return datetime.fromtimestamp(os.path.getmtime(filename))
            
            # Sort files by extracted timestamp (newest first)
            files.sort(key=extract_timestamp, reverse=True)
            
            if files:
                logger.debug(f"Found latest file by timestamp: {files[0]}")
                return files[0]
            
            return None
            
        except Exception as e:
            logger.error(f"Error finding alarm files: {e}")
            return None

    def find_unprocessed_alarm_files(self) -> List[str]:
        """
        Find all unprocessed alarm CSV files in the monitoring directory.
        
        This method finds all alarm files that have not been processed yet,
        sorted from oldest to newest to ensure sequential processing.
        
        Returns:
            List[str]: Paths to unprocessed alarm files, or empty list if none found
        """
        # Get search configuration
        monitoring_dir = self.config['DEFAULT']['monitoring_dir']
        file_pattern = self.config['DEFAULT']['file_pattern']
        
        # Handle path encoding for non-ASCII characters (like Chinese)
        try:
            # Normalize the path to handle Windows path separators and encoding
            monitoring_dir = os.path.normpath(monitoring_dir)
            search_pattern = os.path.join(monitoring_dir, file_pattern)
            
            # Find all files matching pattern
            files = glob.glob(search_pattern)
            
            # If no files found, try alternative approach for paths with special characters
            if not files and not os.path.exists(monitoring_dir):
                logger.warning(f"Monitoring directory not found with direct path: {monitoring_dir}")
                # Try to use the raw path with utf-8 encoding
                monitoring_dir = monitoring_dir.encode('utf-8').decode('utf-8')
                search_pattern = os.path.join(monitoring_dir, file_pattern)
                files = glob.glob(search_pattern)
                logger.info(f"Retried with UTF-8 encoding, found {len(files)} files")
            
            if not files:
                logger.debug(f"No files found matching pattern {search_pattern}")
                return []
            
            # Sort files by timestamp in the filename rather than modification time
            def extract_timestamp(filename):
                try:
                    # Extract parts from filename (Alarms_YYYY_MM_DD_HH_MM_SS.csv)
                    parts = os.path.basename(filename).split('.')[0].split('_')
                    if len(parts) >= 7:  # Alarms_YYYY_MM_DD_HH_MM_SS
                        # Reconstruct the timestamp string
                        year, month, day, hour, minute, second = parts[1:7]
                        # Handle milliseconds if present
                        msec = parts[7] if len(parts) > 7 else "00"
                        # Create datetime object
                        return datetime(
                            int(year), int(month), int(day), 
                            int(hour), int(minute), int(second), 
                            int(msec) * 1000 if msec else 0
                        )
                except (ValueError, IndexError) as e:
                    # If parsing fails, fall back to file modification time
                    logger.warning(f"Could not parse timestamp from filename {filename}: {e}")
                    return datetime.fromtimestamp(os.path.getmtime(filename))
                return datetime.fromtimestamp(os.path.getmtime(filename))
            
            # Sort files by extracted timestamp (oldest first for sequential processing)
            files.sort(key=extract_timestamp)
            
            # Filter for files we haven't processed yet
            if self.latest_processed_file:
                unprocessed_files = []
                latest_basename = os.path.basename(self.latest_processed_file)
                
                # Get the timestamp of the latest processed file
                latest_timestamp = extract_timestamp(self.latest_processed_file)
                
                # Keep only files newer than our latest processed file
                for file in files:
                    file_timestamp = extract_timestamp(file)
                    if file_timestamp > latest_timestamp:
                        unprocessed_files.append(file)
                    elif os.path.basename(file) == latest_basename:
                        # Skip the file we've already processed (by exact name match)
                        continue
                    elif file_timestamp == latest_timestamp:
                        # Edge case: files with identical timestamps
                        # Include the file if it has a different name (could be a different file with same timestamp)
                        if os.path.basename(file) != latest_basename:
                            unprocessed_files.append(file)
                
                logger.debug(f"Found {len(unprocessed_files)} unprocessed files")
                return unprocessed_files
            else:
                # If we haven't processed any files yet, return all files
                logger.debug(f"No files processed yet. Found {len(files)} files to process")
                return files
            
        except Exception as e:
            logger.error(f"Error finding alarm files: {e}")
            return []
    
    def parse_alarm_file(self, file_path: str) -> List[Dict[str, Union[str, int, datetime]]]:
        """
        Parse an alarm CSV file into a list of alarm data dictionaries.
        
        This method reads the CSV file, attempting different encodings to handle
        potential issues with Chinese characters. It processes each row in the CSV
        and converts the data into structured dictionaries with proper typing.
        
        The Time field is parsed as a datetime object with milliseconds precision,
        which is crucial for duplicate detection since this field serves as a unique
        identifier for each alarm entry.
        
        Args:
            file_path: Path to the CSV file to parse
            
        Returns:
            List of dictionaries containing normalized alarm data
        """
        alarm_data = []
        
        try:
            # Try multiple encodings to handle potential issues with Chinese characters
            encodings = ['utf-8', 'gbk', 'gb18030', 'cp936']  # Common Chinese encodings
            file_content = None
            successful_encoding = None
            
            # Try each encoding until one works
            for encoding in encodings:
                try:
                    with open(file_path, 'r', encoding=encoding) as f:
                        file_content = f.read()
                    successful_encoding = encoding
                    break
                except UnicodeDecodeError:
                    continue
            
            # If all encodings failed, log error and return
            if file_content is None:
                logger.error(f"Failed to read file {file_path} with any of the attempted encodings")
                return []
                
            logger.debug(f"Successfully read file with encoding: {successful_encoding}")
            
            # Process the CSV content
            import io
            csv_file = io.StringIO(file_content)
            # Read the first line to get headers
            header_line = csv_file.readline().strip()
            headers = [h.strip() for h in header_line.split(',')]
            
            reader = csv.reader(csv_file)
            
            # Process each row in the CSV
            for row in reader:
                # Validate row has minimum required fields
                if len(row) < 9:  
                    logger.warning(f"Skipping invalid row: {row}")
                    continue
                
                # Parse the timestamp - this has special formatting
                try:
                    # Format: 2025-01-06 16:54:22:455
                    timestamp = datetime.strptime(row[0].strip(), '%Y-%m-%d %H:%M:%S:%f')
                except ValueError:
                    logger.warning(f"Invalid timestamp format in row: {row}")
                    continue
                
                # Create structured dictionary with alarm data
                alarm = {
                    'Time': timestamp,
                    'Instance': int(row[1].strip()),
                    'Name': row[2].strip(),
                    'Code': int(row[3].strip()),
                    'Severity': int(row[4].strip()),
                    'AdditionalInformation1': row[5].strip(),
                    'AdditionalInformation2': row[6].strip(),
                    'Change': row[7].strip(),
                    'Message': row[8].strip(),
                    'file_source': os.path.basename(file_path)  # Track source file
                }
                
                alarm_data.append(alarm)
            
            # Log summary once at the end instead of duplicating logs
            if alarm_data:
                logger.info(f"Parsed {len(alarm_data)} alarm entries from {file_path}")
            return alarm_data
        except Exception as e:
            logger.error(f"Error parsing alarm file {file_path}: {e}")
            return []
    
    def insert_alarms_to_database(self, alarms: List[Dict[str, Union[str, int, datetime]]]) -> bool:
        """
        Insert parsed alarm data into the database with duplicate prevention.
        
        This method checks each alarm record against existing database entries 
        to avoid duplicate insertions. It uses the Time field as unique identifier
        since each alarm has a unique timestamp down to milliseconds.
        
        Args:
            alarms: List of alarm dictionaries to insert
            
        Returns:
            bool: True if insertion successful, False otherwise
        """
        # Handle empty input case
        if not alarms:
            logger.debug("No alarms to insert")
            return True
        
        table_name = self.config['DEFAULT']['table_name']
        
        # Ensure database connection is active
        if not self.db_connection or not self.db_connection.is_connected():
            if not self.reconnect_to_database():
                return False
        
        # Using context manager for cursor
        with self.get_cursor() as cursor:
            # Enhanced duplicate prevention - check combination of Time, Instance, Code and Name
            # This provides stronger duplicate detection than timestamp alone
            try:
                # First, verify and repair the Time index to ensure it's working properly
                try:
                    # use execute ANALYZE TABLE and immediately get its result set
                    with self.get_cursor() as analyze_cursor:
                        analyze_cursor.execute(f"ANALYZE TABLE {table_name}")
                        # must get the result set, avoid "Unread result found" error
                        analyze_result = analyze_cursor.fetchall()
                    
                    # check if the index exists
                    cursor.execute(f"""
                        SELECT COUNT(*) 
                        FROM information_schema.statistics 
                        WHERE table_schema = DATABASE() 
                        AND table_name = '{table_name}' 
                        AND index_name = 'idx_time'
                    """)
                    
                    if cursor.fetchone()[0] == 0:
                        logger.warning(f"Time index missing, recreating it")
                        cursor.execute(f"ALTER TABLE {table_name} ADD INDEX idx_time (Time)")
                        self.db_connection.commit()
                except mysql.connector.Error as err:
                    logger.warning(f"Error checking/fixing Time index: {err}")
                
                # Prepare to check existing data using multiple fields for better uniqueness
                unique_alarms = []
                duplicate_count = 0
                
                # check in batches, avoid processing too many records at once
                batch_size = 50
                for i in range(0, len(alarms), batch_size):
                    batch = alarms[i:i+batch_size]
                    
                    # use IN query to check this group of records
                    placeholders = []
                    values = []
                    for alarm in batch:
                        placeholders.append("(Time = %s AND Instance = %s AND Code = %s AND Name = %s)")
                        values.extend([
                            alarm['Time'], 
                            alarm['Instance'],
                            alarm['Code'],
                            alarm['Name']
                        ])
                    
                    if placeholders:
                        query = f"""
                        SELECT Time, Instance, Code, Name FROM {table_name} 
                        WHERE {" OR ".join(placeholders)}
                        """
                        
                        cursor.execute(query, values)
                        existing_records = cursor.fetchall()
                        
                        # create a unique identifier set for existing records
                        existing_keys = set()
                        for record in existing_records:
                            key = (record[0], record[1], record[2], record[3])  # (Time, Instance, Code, Name)
                            existing_keys.add(key)
                        
                        # add records that are not in the existing set to the unique record list
                        for alarm in batch:
                            key = (alarm['Time'], alarm['Instance'], alarm['Code'], alarm['Name'])
                            if key not in existing_keys:
                                unique_alarms.append(alarm)
                            else:
                                duplicate_count += 1
                
                # Only log a message if duplicates were found
                if duplicate_count > 0:
                    logger.info(f"Found {duplicate_count} duplicate alarms that already exist in the database")
                
                # If no unique alarms remain after filtering out duplicates, we're done
                if not unique_alarms:
                    logger.debug("All alarm records already exist in the database, skipping insertion")
                    return True
                
                # Log how many unique records will be inserted
                logger.info(f"Preparing to insert {len(unique_alarms)} unique records")
                
                # SQL for inserting alarm records with parameterized query for security
                insert_query = f"""
                INSERT INTO {table_name} 
                (Time, Instance, Name, Code, Severity, 
                AdditionalInformation1, AdditionalInformation2, `Change`, Message, file_source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                # Only proceed with insertion if we have unique records
                if unique_alarms:
                    # Prepare values for batch insert
                    values = [
                        (
                            alarm['Time'],
                            alarm['Instance'],
                            alarm['Name'],
                            alarm['Code'],
                            alarm['Severity'],
                            alarm['AdditionalInformation1'],
                            alarm['AdditionalInformation2'],
                            alarm['Change'],
                            alarm['Message'],
                            alarm['file_source']
                        )
                        for alarm in unique_alarms
                    ]
                    
                    # Execute batch insert (more efficient than individual inserts)
                    cursor.executemany(insert_query, values)
                    self.db_connection.commit()
                    
                    # Only log the final summary to reduce log volume
                    logger.info(f"Successfully inserted {len(unique_alarms)} unique alarm records into database")
                
                return True
                
            except mysql.connector.Error as err:
                logger.error(f"Error inserting alarms into database: {err}")
                self.db_connection.rollback()
                return False
    
    def process_new_alarm_file(self) -> bool:
        """
        Check for new alarm files and process the latest one if it's new.
        
        Returns:
            bool: True if a new file was processed, False otherwise
        """
        # This legacy method now just calls the new multi-file processing method
        # Kept for backward compatibility
        return self.process_new_alarm_files()
    
    def process_new_alarm_files(self) -> bool:
        """
        Check for and process all new alarm files.
        
        This method ensures that all new CSV files are processed, not just the latest one,
        which is important if multiple files are generated between polling intervals.
        
        Returns:
            bool: True if one or more new files were processed, False otherwise
        """
        # Find all unprocessed alarm files
        unprocessed_files = self.find_unprocessed_alarm_files()
        
        # Handle case where no files are found
        if not unprocessed_files:
            logger.debug("No unprocessed alarm files found in monitoring directory")
            return False
        
        files_processed = 0
        files_failed = 0
        
        # Process each file in order (oldest first)
        for file_path in unprocessed_files:
            logger.info(f"Processing alarm file: {file_path}")
            
            # Parse the file and extract alarm data
            alarms = self.parse_alarm_file(file_path)
            
            # Insert data into database and update tracking status
            if alarms and self.insert_alarms_to_database(alarms):
                self.latest_processed_file = file_path
                logger.info(f"Successfully processed {file_path}")
                files_processed += 1
            else:
                # Only log warning if we had alarms but insertion failed
                if alarms:
                    logger.warning(f"Failed to insert alarms from {file_path} into database")
                files_failed += 1
        
        # Summary logging
        if files_processed > 0:
            logger.info(f"Processed {files_processed} new alarm files in this cycle")
            if files_failed > 0:
                logger.warning(f"Failed to process {files_failed} files")
            return True
        
        return False
    
    def start_monitoring(self) -> None:
        """
        Start continuous monitoring for new alarm files.
        
        This method enters a loop that runs until stopped, periodically checking
        for new files and processing them. It handles database reconnection
        automatically if the connection is lost.
        """
        self.is_running = True
        polling_interval = int(self.config['DEFAULT']['polling_interval'])
        
        logger.info(f"Starting alarm file monitoring with interval {polling_interval} seconds")
        
        # More concise startup output with all information in a structured format
        print("\n=== Alarm Monitor Configuration ===")
        print(f"Monitoring directory: {self.config['DEFAULT']['monitoring_dir']}")
        print(f"File pattern: {self.config['DEFAULT']['file_pattern']}")
        print(f"Database: {self.config['DEFAULT']['db_name']}")
        print(f"Table: {self.config['DEFAULT']['table_name']}")
        print(f"Polling interval: {polling_interval} seconds")
        print("=================================")
        print("Processing all new alarm files every cycle")
        print("Press Ctrl+C to stop monitoring")
        
        try:
            # Main monitoring loop
            while self.is_running:
                # Check database connection and reconnect if needed
                if not self.db_connection or not self.db_connection.is_connected():
                    logger.warning("Database connection lost. Attempting to reconnect...")
                    if not self.reconnect_to_database():
                        logger.error("Failed to reconnect to database. Will retry on next cycle.")
                        time.sleep(polling_interval)
                        continue
                
                # Check for and process all new files
                self.process_new_alarm_files()
                # Wait for configured interval before next check
                time.sleep(polling_interval)
        except KeyboardInterrupt:
            # Handle clean shutdown on Ctrl+C
            logger.info("Monitoring stopped by user")
            self.is_running = False
        except Exception as e:
            # Handle unexpected errors
            logger.error(f"Monitoring error: {e}")
            self.is_running = False
            raise
    
    def stop_monitoring(self) -> None:
        """
        Stop the monitoring process gracefully.
        
        This method sets a flag to exit the monitoring loop on the next iteration.
        """
        self.is_running = False
        logger.info("Monitoring stop signal received")
    
    def close(self) -> None:
        """
        Close database connection and clean up resources.
        
        This method should be called before program exit to ensure proper cleanup.
        """
        self.stop_monitoring()
        if self.db_connection and self.db_connection.is_connected():
            self.db_connection.close()
            logger.info("Database connection closed")

# Start monitoring when run directly
if __name__ == "__main__":
    print("Starting Alarm Data Monitor...")
    
    try:
        # Create handler instance and start monitoring
        monitor = AlarmMonitor()
        monitor.start_monitoring()
    except KeyboardInterrupt:
        print("\nMonitoring stopped by user. Shutting down...")
    except Exception as e:
        print(f"\nError occurred: {e}")
    finally:
        if 'monitor' in locals():
            monitor.close()
        print("Alarm Data Monitor stopped.") 