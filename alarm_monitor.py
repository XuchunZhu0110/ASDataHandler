"""
This module monitors a directory for Automation Studios generated alarm CSV files,
parses them, and stores the data in a MySQL database for later analysis.

This is the main monitoring application that focuses on:
- File monitoring and CSV parsing
- Data processing and database operations
- Core business logic for alarm handling

Detailed system recovery and maintenance functionality is handled by the SystemRecoveryManager
in the system_recovery module.
"""

import os
import glob
import time
import csv
import io
import mysql.connector
import logging
import logging.handlers
import configparser
from datetime import datetime
from typing import List, Dict, Union
from contextlib import contextmanager

# Import system recovery functionality
from system_recovery import SystemRecoveryManager


class TimestampRotatingFileHandler(logging.FileHandler):
    """Custom log handler that rotates files using timestamp-based naming"""
    
    def __init__(self, log_dir, base_name, max_bytes, backup_count, encoding=None):
        self.log_dir = log_dir
        self.base_name = base_name
        self.max_bytes = max_bytes
        self.backup_count = backup_count    
        # Clean up old files first to ensure limits are not exceeded
        self._cleanup_old_files()
        # Generate initial filename and initialize parent class
        self.current_filename = self._generate_filename()
        super().__init__(self.current_filename, encoding=encoding)
        
    def _generate_filename(self):
        """Generate a new filename with current timestamp"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return os.path.join(self.log_dir, f'{self.base_name}_{timestamp}.log')
    
    def _cleanup_old_files(self):
        """Clean up old log files to maintain backup_count limit"""
        try:
            pattern = os.path.join(self.log_dir, f'{self.base_name}_*.log')
            log_files = glob.glob(pattern)
            if len(log_files) < self.backup_count:
                return
            
            # Sort by modification time (oldest first for deletion)
            log_files.sort(key=os.path.getmtime)
            
            # Delete excess files to maintain backup_count limit
            # When rotating: if we have N files and create 1 new, we need to delete (N+1-backup_count) files
            files_to_delete = len(log_files) - self.backup_count + 1
            
            # Delete oldest files
            deleted_files = []
            for file_path in log_files[:files_to_delete]:
                try:
                    filename = os.path.basename(file_path)
                    os.remove(file_path)
                    deleted_files.append(filename)
                except Exception as e:
                    print(f"Warning: Could not delete {os.path.basename(file_path)}: {e}")
            
            if deleted_files:
                deleted_count = len(deleted_files)
                files_list = ", ".join(deleted_files)
                cleanup_msg = f"Cleaned up {deleted_count} old log files to maintain limit of {self.backup_count}: {files_list}"
                
                try:
                    cleanup_record = logging.LogRecord('AlarmMonitor', logging.INFO, '', 0, cleanup_msg, (), None)
                    if hasattr(self, 'stream') and self.stream:
                        super().emit(cleanup_record)
                    else:
                        print(cleanup_msg)
                except:
                    print(cleanup_msg)
                
        except Exception as e:
            print(f"Warning: Log file cleanup failed: {e}")
    
    def _should_rotate(self):
        """Check if current file should be rotated"""
        try:
            return os.path.exists(self.current_filename) and os.path.getsize(self.current_filename) >= self.max_bytes
        except OSError:
            return False
    
    def _rotate(self):
        """Rotate to a new file with timestamp"""
        try:
            # Close current file
            if self.stream:
                self.stream.close()
                self.stream = None
            
            # Clean up old files before creating new one
            self._cleanup_old_files()
            
            # Create new file
            self.current_filename = self._generate_filename()
            self.baseFilename = self.current_filename
            self.stream = self._open()
            
            # Log the rotation
            msg = f'Log rotated to new file: {os.path.basename(self.current_filename)}'
            rotation_record = logging.LogRecord('AlarmMonitor', logging.INFO, '', 0, msg, (), None)
            super().emit(rotation_record)
            
        except Exception as e:
            print(f"Error during log rotation: {e}")
    
    def emit(self, record):
        """Emit a log record, rotating if necessary"""
        try:
            if self._should_rotate():
                self._rotate()
            super().emit(record)
        except Exception:
            self.handleError(record)


def _initialize_logger():
    """Initialize logger without handlers (configured in _setup_logging)"""
    logger = logging.getLogger("AlarmMonitor")
    logger.propagate = False
    if logger.handlers:
        logger.handlers = []
    return logger


# Create logger instance
logger = _initialize_logger()


class AlarmMonitor:
    """
    Core alarm monitoring and data processing functionality
    Handles file monitoring, CSV parsing, and database operations
    """
    
    def __init__(self, config_file: str = "config.ini"):
        """Initialize the AlarmMonitor with configuration and system setup"""
        self.config_file = config_file
        self.config = self._load_config()
        self.db_connection = None
        self.latest_processed_file = None
        self.is_running = False
        self.max_reconnect_attempts = 5
        self.reconnect_delay = 5
        self.last_cleanup_time = datetime.now()
        
        # Initialize recovery manager
        self.recovery_manager = SystemRecoveryManager(self)
        
        # Check MySQL service and establish connection
        if self.check_mysql_availability():
            self.connect_database()
            # Perform system recovery
            self.recovery_manager.perform_full_recovery()
        else:
            print("MySQL service is not available. Please ensure MySQL is running before proceeding.")
    
    @contextmanager
    def get_cursor(self, dictionary=False):
        """Context manager for database cursors with automatic cleanup"""
        if not self.db_connection or not self.db_connection.is_connected():
            self.reconnect_to_database()
        
        cursor = None
        try:
            cursor = self.db_connection.cursor(dictionary=dictionary)
            yield cursor
        finally:
            if cursor is not None:
                cursor.close()
                
    def start_monitoring(self) -> None:
        """Main monitoring loop that continuously processes new alarm files"""
        self.is_running = True
        polling_interval = int(self.config['DEFAULT']['polling_interval'])
        
        logger.info(f"Starting alarm monitoring with {polling_interval}s interval")
        
        print("\n=== Alarm Monitor Configuration ===")
        print(f"Monitoring directory: {self.config['DEFAULT']['monitoring_dir']}")
        print(f"File pattern: {self.config['DEFAULT']['file_pattern']}")
        print(f"Database: {self.config['DEFAULT']['db_name']}")
        print(f"Table: {self.config['DEFAULT']['table_name']}")
        print(f"Polling interval: {polling_interval} seconds")
        print("=====================================")
        print("Press Ctrl+C to stop monitoring")
        
        try:
            while self.is_running:
                # Ensure database connection is active
                if not self.db_connection or not self.db_connection.is_connected():
                    logger.warning("Database connection lost, attempting reconnection")
                    if not self.reconnect_to_database():
                        logger.error("Failed to reconnect, will retry next cycle")
                        time.sleep(polling_interval)
                        continue
                
                # Process new files
                files_processed = self.process_new_files()
                
                # Time-based periodic maintenance
                if self._should_perform_cleanup():
                    logger.debug("Performing scheduled maintenance")
                    self.recovery_manager.perform_maintenance()
                    self.last_cleanup_time = datetime.now()
                
                time.sleep(polling_interval)
                
        except KeyboardInterrupt:
            logger.info("Monitoring stopped by user")
            self.is_running = False
        except Exception as e:
            logger.error(f"Monitoring error: {e}")
            self.is_running = False
            raise
    
    def process_new_files(self) -> bool:
        """Check for and process all new alarm files"""
        unprocessed_files = self.find_unprocessed_files()
        
        if not unprocessed_files:
            logger.debug("No unprocessed files found")
            return False
            
        files_processed = 0
        
        for file_path in unprocessed_files:
            if self.process_single_file(file_path):
                files_processed += 1
        
        if files_processed > 0:
            logger.info(f"Processed {files_processed} new files")
            return True
                
        return False
    
    def process_single_file(self, file_path: str) -> bool:
        """Process a single alarm file completely"""
        logger.info(f"Processing file: {os.path.basename(file_path)}")
        
        try:
            # Mark processing started
            self._mark_processing_started(file_path)
            
            # Parse CSV file
            alarms = self.parse_csv_file(file_path)
            
            if not alarms:
                logger.warning(f"No valid alarms found in {os.path.basename(file_path)}")
                self._mark_processing_completed(file_path, 0)
                return True
            
            # Insert alarms with optimization
            inserted_count = self.insert_alarms(alarms)
            
            if inserted_count >= 0:
                self.latest_processed_file = file_path
                self._mark_processing_completed(file_path, inserted_count)
                logger.info(f"Successfully processed {os.path.basename(file_path)}")
                return True
            else:
                self._mark_processing_failed(file_path, "Insert operation failed")
                return False
                
        except Exception as e:
            logger.error(f"Error processing {os.path.basename(file_path)}: {e}")
            self._mark_processing_failed(file_path, str(e))
            return False
    
    def insert_alarms(self, alarms: List[Dict[str, Union[str, int, datetime]]]) -> int:
        """
        Main alarm insertion with automatic optimization and fallback
        Tries optimized temp table approach first, falls back to batch method
        """
        if not alarms:
            logger.debug("No alarms to insert")
            return 0
        
        try:
            # Primary method: optimized temporary table approach
            # Under normal circumstances, use temporary table approach for better performance(insert_alarms_optimized)
            # Otherwise, use batch checking approach(insert_alarms_batch)
            if self.config.getboolean('PERFORMANCE', 'enable_temp_table_optimization', fallback=True):
                return self.insert_alarms_optimized(alarms)
            else:
                return self.insert_alarms_batch(alarms)
                
        except Exception as e:
            logger.error(f"Optimized insertion failed: {e}")
            logger.info("Falling back to batch checking method")
            
            # Fallback method: proven batch checking approach  
            return self.insert_alarms_batch(alarms)
    
    def insert_alarms_optimized(self, alarms: List[Dict[str, Union[str, int, datetime]]]) -> int:
        """
        Optimized insertion using temporary table approach
        Uses database-level duplicate detection for better performance
        
        Temporary table lifecycle:
        1. Create unique temp table for this processing session
        2. Bulk insert all data to temp table
        3. Use JOIN to find new records and insert to main table
        4. Immediately delete temp table (total lifecycle: several seconds)
        
        NOTE: Each call creates its own temp table and deletes it immediately after processing
        """
        table_name = self.config['DEFAULT']['table_name']
        # Create unique temp table name: process_id + timestamp ensures no conflicts
        temp_table = f"temp_alarms_{os.getpid()}_{int(time.time())}"
        
        try:
            with self.get_cursor() as cursor:
                # Ensure no pending transactions before starting new one
                try:
                    # Check if there's already a transaction in progress
                    if self.db_connection.in_transaction:
                        self.db_connection.rollback()
                    # Start transaction for atomicity
                    self.db_connection.start_transaction()
                except Exception as trans_err:
                    # If transaction management fails, try to recover
                    logger.debug(f"Transaction management issue: {trans_err}")
                    self.db_connection.rollback()
                    self.db_connection.start_transaction()
                
                # Create temporary table with same collation as main table
                cursor.execute(f"""
                    CREATE TEMPORARY TABLE {temp_table} (
                        Time DATETIME(3) NOT NULL,
                        Instance INT NOT NULL,
                        Name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
                        Code INT NOT NULL,
                        Severity INT NOT NULL,
                        AdditionalInformation1 VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
                        AdditionalInformation2 VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
                        `Change` TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
                        Message TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL
                    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci
                """)
                
                # Bulk insert to temporary table
                insert_temp_query = f"""
                    INSERT INTO {temp_table} 
                    (Time, Instance, Name, Code, Severity, 
                     AdditionalInformation1, AdditionalInformation2, `Change`, Message)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                values = [
                    (
                        alarm['Time'], alarm['Instance'], alarm['Name'],
                        alarm['Code'], alarm['Severity'],
                        alarm['AdditionalInformation1'], alarm['AdditionalInformation2'],
                        alarm['Change'], alarm['Message']
                    )
                    for alarm in alarms
                ]
                
                cursor.executemany(insert_temp_query, values)
                
                # Find and insert only new records
                cursor.execute(f"""
                    INSERT INTO {table_name} 
                    (Time, Instance, Name, Code, Severity, 
                     AdditionalInformation1, AdditionalInformation2, `Change`, Message)
                    SELECT t.Time, t.Instance, t.Name, t.Code, t.Severity,
                           t.AdditionalInformation1, t.AdditionalInformation2, 
                           t.`Change`, t.Message
                    FROM {temp_table} t
                    LEFT JOIN {table_name} m ON (
                        t.Time = m.Time AND t.Instance = m.Instance 
                        AND t.Code = m.Code AND t.Name = m.Name
                    )
                    WHERE m.id IS NULL
                """)
                
                new_count = cursor.rowcount
                duplicate_count = len(alarms) - new_count
                
                # Commit transaction
                self.db_connection.commit()
                
                # Log results
                logger.info(f"Data processing completed: {new_count} new records inserted, {duplicate_count} duplicates skipped.")
                
                return new_count
            
        except Exception as e:
            # Ensure proper rollback even if transaction state is unclear
            try:
                if self.db_connection.in_transaction:
                    self.db_connection.rollback()
            except Exception:
                pass  # Ignore rollback errors during error handling
            logger.error(f"Optimized insertion failed: {e}")
            raise
        finally:
            # Immediate cleanup: delete temp table after processing (normal lifecycle: seconds)
            try:
                with self.get_cursor() as cursor:
                    cursor.execute(f"DROP TEMPORARY TABLE IF EXISTS {temp_table}")
            except Exception:
                pass  # Ignore cleanup errors - orphaned table will be cleaned up later
    
    def insert_alarms_batch(self, alarms: List[Dict[str, Union[str, int, datetime]]]) -> int:
        """
        Reliable insertion using batch checking approach
        Fallback method with proven reliability
        """
        table_name = self.config['DEFAULT']['table_name']
        batch_size = self.config.getint('PERFORMANCE', 'batch_size', fallback=500)
        
        if not self.db_connection or not self.db_connection.is_connected():
            if not self.reconnect_to_database():
                return -1
        
        with self.get_cursor() as cursor:
            try:
                unique_alarms = []
                duplicate_count = 0
                
                # Process in batches for better performance
                for i in range(0, len(alarms), batch_size):
                    batch = alarms[i:i+batch_size]
                    
                    # Build query to check existing records
                    placeholders = []
                    values = []
                    for alarm in batch:
                        placeholders.append("(Time = %s AND Instance = %s AND Code = %s AND Name = %s)")
                        values.extend([alarm['Time'], alarm['Instance'], alarm['Code'], alarm['Name']])
                    
                    if placeholders:
                        query = f"""
                        SELECT Time, Instance, Code, Name FROM {table_name} 
                        WHERE {" OR ".join(placeholders)}
                        """
                        
                        cursor.execute(query, values)
                        existing_records = cursor.fetchall()
                        
                        # Create set of existing keys
                        existing_keys = set()
                        for record in existing_records:
                            key = (record[0], record[1], record[2], record[3])
                            existing_keys.add(key)
                        
                        # Filter out duplicates
                        for alarm in batch:
                            key = (alarm['Time'], alarm['Instance'], alarm['Code'], alarm['Name'])
                            if key not in existing_keys:
                                unique_alarms.append(alarm)
                            else:
                                duplicate_count += 1
                
                # Insert unique records
                if unique_alarms:
                    insert_query = f"""
                    INSERT INTO {table_name} 
                    (Time, Instance, Name, Code, Severity, 
                     AdditionalInformation1, AdditionalInformation2, `Change`, Message)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    
                    values = [
                        (
                            alarm['Time'], alarm['Instance'], alarm['Name'],
                            alarm['Code'], alarm['Severity'],
                            alarm['AdditionalInformation1'], alarm['AdditionalInformation2'],
                            alarm['Change'], alarm['Message']
                        )
                        for alarm in unique_alarms
                    ]
                    
                    cursor.executemany(insert_query, values)
                    self.db_connection.commit()
                
                # Log results
                logger.info(f"Data processing completed: {len(unique_alarms)} new records inserted, {duplicate_count} duplicates skipped.")
                
                return len(unique_alarms)
                
            except mysql.connector.Error as err:
                logger.error(f"Batch insertion failed: {err}")
                self.db_connection.rollback()
                return -1
    
    def parse_csv_file(self, file_path: str) -> List[Dict[str, Union[str, int, datetime]]]:
        """Parse alarm CSV file into structured data"""
        alarm_data = []
        
        try:
            # Try multiple encodings
            encodings = ['utf-8', 'gbk', 'gb18030', 'cp936']
            file_content = None
            successful_encoding = None
            
            for encoding in encodings:
                try:
                    with open(file_path, 'r', encoding=encoding) as f:
                        file_content = f.read()
                    successful_encoding = encoding
                    break
                except UnicodeDecodeError:
                    continue
            
            if file_content is None:
                logger.error(f"Failed to read file with supported encodings: {os.path.basename(file_path)}")
                return []
                
            logger.debug(f"File read with encoding: {successful_encoding}")
            
            # Process CSV content
            csv_file = io.StringIO(file_content)
            csv_file.readline()  # Skip header
            
            reader = csv.reader(csv_file)
            
            for row in reader:
                if len(row) < 9:  
                    logger.warning(f"Skipping invalid row: {row}")
                    continue
                
                try:
                    # Parse timestamp from CSV content format: YYYY-MM-DD HH:MM:SS
                    timestamp = datetime.strptime(row[0].strip(), '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    logger.warning(f"Invalid timestamp in row: {row}")
                    continue
                
                alarm = {
                    'Time': timestamp,
                    'Instance': int(row[1].strip()),
                    'Name': row[2].strip(),
                    'Code': int(row[3].strip()),
                    'Severity': int(row[4].strip()),
                    'AdditionalInformation1': row[5].strip(),
                    'AdditionalInformation2': row[6].strip(),
                    'Change': row[7].strip(),
                    'Message': row[8].strip()
                }
                
                alarm_data.append(alarm)
            
            if alarm_data:
                logger.info(f"Parsed {len(alarm_data)} records from {os.path.basename(file_path)}")
                
            return alarm_data
            
        except Exception as e:
            logger.error(f"Error parsing file {os.path.basename(file_path)}: {e}")
            return []
    
    @staticmethod
    def extract_timestamp_from_filename(filename: str) -> datetime:
        """
        Extract timestamp from alarm filename
        Expected format: Alarms_YYYY_MM_DD_HH_MM_SS_MSS.csv
        Falls back to file modification time if parsing fails
        """
        try:
            # Extract filename without extension and split by underscore
            base_name = os.path.basename(filename).split('.')[0]
            parts = base_name.split('_')
            
            # Validate format and extract timestamp components
            if len(parts) >= 7:
                year, month, day, hour, minute, second = parts[1:7]
                msec = parts[7] if len(parts) > 7 else "00"
                
                return datetime(
                    int(year), int(month), int(day), 
                    int(hour), int(minute), int(second), 
                    int(msec) * 1000 if msec else 0
                )
        except (ValueError, IndexError):
            # Fallback to file modification time
            pass
        
        # If timestamp extraction fails, use file modification time
        return datetime.fromtimestamp(os.path.getmtime(filename))

    def find_unprocessed_files(self) -> List[str]:
        """Find all alarm files that need processing, sorted chronologically"""
        monitoring_dir = self.config['DEFAULT']['monitoring_dir']
        file_pattern = self.config['DEFAULT']['file_pattern']
        
        try:
            monitoring_dir = os.path.normpath(monitoring_dir)
            search_pattern = os.path.join(monitoring_dir, file_pattern)
            files = glob.glob(search_pattern)
            
            if not files:
                logger.debug(f"No files found matching pattern {search_pattern}")
                return []
            
            # Sort files by timestamp in filename
            files.sort(key=self.extract_timestamp_from_filename)
            
            # Filter for unprocessed files
            if self.latest_processed_file:
                latest_timestamp = self.extract_timestamp_from_filename(self.latest_processed_file)
                unprocessed_files = []
                
                for file in files:
                    file_timestamp = self.extract_timestamp_from_filename(file)
                    if file_timestamp > latest_timestamp:
                        unprocessed_files.append(file)
                
                logger.debug(f"Found {len(unprocessed_files)} unprocessed files")
                return unprocessed_files
            else:
                logger.debug(f"No previous files processed, found {len(files)} files")
                return files
                
        except Exception as e:
            logger.error(f"Error finding files: {e}")
            return []
    
    def _mark_processing_started(self, file_path: str) -> None:
        """Mark file processing as started in state table"""
        file_name = os.path.basename(file_path)
        
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    INSERT INTO processing_state 
                    (file_name, file_path, processing_status) 
                    VALUES (%s, %s, 'started')
                    ON DUPLICATE KEY UPDATE 
                    processing_status = 'started',
                    started_at = CURRENT_TIMESTAMP,
                    error_message = NULL
                """, (file_name, file_path))
                
                self.db_connection.commit()
                
        except Exception as e:
            logger.warning(f"Failed to mark processing started for {file_name}: {e}")
    
    def _mark_processing_completed(self, file_path: str, record_count: int) -> None:
        """Mark file processing as completed"""
        file_name = os.path.basename(file_path)
        
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    UPDATE processing_state 
                    SET processing_status = 'completed',
                        completed_at = CURRENT_TIMESTAMP,
                        record_count = %s,
                        error_message = NULL
                    WHERE file_name = %s
                """, (record_count, file_name))
                
                self.db_connection.commit()
                
        except Exception as e:
            logger.warning(f"Failed to mark processing completed for {file_name}: {e}")
    
    def _mark_processing_failed(self, file_path: str, error_message: str) -> None:
        """Mark file processing as failed"""
        file_name = os.path.basename(file_path)
        
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    UPDATE processing_state 
                    SET processing_status = 'failed',
                        error_message = %s
                    WHERE file_name = %s
                """, (error_message, file_name))
                
                self.db_connection.commit()
                
        except Exception as e:
            logger.warning(f"Failed to mark processing failed for {file_name}: {e}")
    
    def check_mysql_availability(self) -> bool:
        """Check if MySQL service is available by attempting connection"""
        try:
            # Check MySQL server connectivity
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
                logger.info("MySQL server is accessible")
                
                # Check specific database
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
                    logger.info(f"Database '{self.config['DEFAULT']['db_name']}' is accessible")
                    return True
                    
                except mysql.connector.Error as db_err:
                    if "1049" in str(db_err):
                        error_msg = f"Database '{self.config['DEFAULT']['db_name']}' does not exist"
                        logger.error(error_msg)
                        print(error_msg)
                    elif "1045" in str(db_err):
                        error_msg = "Access denied to database. Check credentials"
                        logger.error(error_msg)
                        print(error_msg)
                    else:
                        logger.error(f"Database connection error: {db_err}")
                    return False
                    
            except mysql.connector.Error as server_err:
                if "2003" in str(server_err):
                    logger.error("MySQL server is not running")
                    print("MySQL server is not running. Please start MySQL service.")
                elif "1045" in str(server_err):
                    error_msg = "Access denied to MySQL server. Check credentials"
                    logger.error(error_msg)
                    print(error_msg)
                else:
                    logger.error(f"MySQL server error: {server_err}")
                return False
                
        except Exception as e:
            logger.error(f"Unexpected error checking MySQL availability: {e}")
            return False
    
    def connect_database(self) -> bool:
        """Establish database connection"""
        try:
            self.db_connection = mysql.connector.connect(
                host=self.config['DEFAULT']['db_host'],
                port=int(self.config['DEFAULT']['db_port']),
                user=self.config['DEFAULT']['db_user'],
                password=self.config['DEFAULT']['db_password'],
                database=self.config['DEFAULT']['db_name']
            )
            logger.info("Database connection established")
            return True
            
        except mysql.connector.Error as err:
            logger.error(f"Database connection failed: {err}")
            return False
    
    def reconnect_to_database(self) -> bool:
        """Attempt to reconnect to database with retries"""
        attempts = 0
        while attempts < self.max_reconnect_attempts:
            if attempts == 0:
                logger.info(f"Attempting database reconnection (max attempts: {self.max_reconnect_attempts})")
            else:
                logger.debug(f"Reconnection attempt {attempts + 1}/{self.max_reconnect_attempts}")
                
            if self.connect_database():
                logger.info("Database connection restored")
                return True
                
            attempts += 1
            if attempts < self.max_reconnect_attempts:
                if attempts == 1:
                    logger.info(f"Reconnection failed, retrying every {self.reconnect_delay}s...")
                time.sleep(self.reconnect_delay)
                
        logger.error(f"Failed to reconnect after {self.max_reconnect_attempts} attempts")
        return False
    
    @staticmethod
    def _read_config_file(file_path: str) -> configparser.ConfigParser:
        """Read configuration file with encoding fallback (UTF-8 -> GBK)"""
        config = configparser.ConfigParser()
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                config.read_file(f)
            logger.info(f"Configuration loaded from {file_path}")
            return config
        except UnicodeDecodeError:
            try:
                with open(file_path, 'r', encoding='gbk') as f:
                    config.read_file(f)
                logger.info(f"Configuration loaded from {file_path} with GBK encoding")
                return config
            except Exception as e:
                logger.error(f"Error loading configuration with GBK encoding: {e}")
                raise
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            raise

    def _load_config(self) -> configparser.ConfigParser:
        """Load configuration from file or create from template"""
        # Load from existing config.ini if exists
        if os.path.exists(self.config_file):
            try:
                config = self._read_config_file(self.config_file)
            except Exception as e:
                logger.info("Will create configuration from template or defaults")
                config = self._create_config_from_template()
        else:
            # Create config from template or defaults
            config = self._create_config_from_template()
        
        # Setup logging
        self._setup_logging(config)
        
        return config
    
    def _create_default_config(self) -> configparser.ConfigParser:
        """Create configuration with built-in default values"""
        config = configparser.ConfigParser()
        
        config['DEFAULT'] = {
            'monitoring_dir': './alarm_files',
            'log_file_path': './logs/',
            'file_pattern': 'Alarms_*.csv',
            'polling_interval': '10',
            'db_host': 'localhost',
            'db_port': '3306',
            'db_user': 'alarm_user',
            'db_password': 'password',
            'db_name': 'alarm_db',
            'table_name': 'alarms'
        }
        
        config['PERFORMANCE'] = {
            'batch_size': '2000',
            'enable_temp_table_optimization': 'true',
            'temp_table_cleanup_interval': '3600'
        }
        
        config['DATA_MANAGEMENT'] = {
            'data_retention_value': '12',
            'data_retention_unit': 'months',
            'auto_cleanup_enabled': 'true',
            'cleanup_check_interval_value': '24',
            'cleanup_check_interval_unit': 'hours',
            'max_processing_state_records': '100'
        }
        
        config['LOGGING'] = {
            'log_level': 'INFO',
            'log_rotation_enabled': 'true',
            'log_rotation_max_size_mb': '30',
            'log_rotation_backup_count': '10'
        }
        
        return config
    
    def _save_config_file(self, config: configparser.ConfigParser) -> None:
        """Save configuration to file"""
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                config.write(f)
            logger.info(f"Created configuration file: {self.config_file}")
        except Exception as e:
            logger.error(f"Error creating configuration file: {e}")
    
    def _create_config_from_template(self) -> configparser.ConfigParser:
        """Create configuration from template file or use built-in defaults"""
        template_file = "config.ini.template"
        
        # Try to load from template file first
        if os.path.exists(template_file):
            try:
                config = self._read_config_file(template_file)
                self._save_config_file(config)
                return config
            except Exception as e:
                logger.error(f"Error loading template configuration: {e}")
                logger.info("Using built-in default configuration")
        else:
            logger.info(f"Template file {template_file} not found, using built-in defaults")
        
        # Fallback to built-in defaults
        config = self._create_default_config()
        self._save_config_file(config)
        return config
    
    def _setup_logging(self, config: configparser.ConfigParser) -> None:
        """Configure logging system with rotation support based on configuration"""
        global logger
        
        log_level_name = config.get('LOGGING', 'log_level', fallback='INFO')
        
        # Try to get log_file_path from DEFAULT section first, then fallback to LOGGING section
        log_file_path = config.get('DEFAULT', 'log_file_path', fallback=None)
        if log_file_path is None:
            log_file_path = config.get('LOGGING', 'log_file', fallback='alarm_monitor.log')
        
        # Generate timestamped log filename in specified directory
        log_dir = log_file_path
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_filename = f'alarm_monitor_{timestamp}.log'
        log_file_path = os.path.join(log_dir, log_filename)
        
        # Read rotation settings
        rotation_enabled = config.getboolean('LOGGING', 'log_rotation_enabled', fallback=True)
        rotation_max_size_mb = config.getint('LOGGING', 'log_rotation_max_size_mb', fallback=30)
        rotation_backup_count = config.getint('LOGGING', 'log_rotation_backup_count', fallback=10)
        
        # Ensure log directory exists
        if log_dir and not os.path.exists(log_dir):
            try:
                os.makedirs(log_dir, exist_ok=True)
                print(f"Created log directory: {log_dir}")
            except Exception as e:
                print(f"Error: Could not create log directory {log_dir}: {e}")
                print("Please check the log directory path in your configuration.")
                raise RuntimeError(f"Failed to create log directory: {e}")
        
        log_level = getattr(logging, log_level_name.upper(), logging.INFO)
        
        # Clean up existing handlers
        root_logger = logging.getLogger()
        if root_logger.handlers:
            for handler in root_logger.handlers[:]:
                root_logger.removeHandler(handler)
                
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        logger.propagate = False
        
        # Create file handler with or without rotation
        try:
            if rotation_enabled:
                # Use custom timestamp-based rotation
                max_bytes = rotation_max_size_mb * 1024 * 1024  # Convert MB to bytes
                file_handler = TimestampRotatingFileHandler(
                    log_dir,
                    'alarm_monitor',
                    max_bytes,
                    rotation_backup_count,
                    encoding='utf-8'
                )
                rotation_info = f"timestamp-based ({rotation_max_size_mb}MB), {rotation_backup_count} backups"
            else:
                # No rotation, use standard FileHandler with timestamp
                file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
                rotation_info = "disabled"
            
            file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
            
        except Exception as e:
            print(f"Error: Could not create log file {log_file_path}: {e}")
            print("Please check the log file path in your configuration.")
            raise RuntimeError(f"Failed to create log file: {e}")
        
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        
        # Configure logger
        logger.setLevel(log_level)
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        # Log configuration info
        if rotation_enabled:
            current_log_file = file_handler.current_filename
            logger.info(f"Logging configured: level={log_level_name}, file={os.path.abspath(current_log_file)}, rotation={rotation_info}")
        else:
            logger.info(f"Logging configured: level={log_level_name}, file={os.path.abspath(log_file_path)}, rotation={rotation_info}")
    

    def _should_perform_cleanup(self) -> bool:
        """Check if it's time to perform cleanup based on flexible interval configuration"""
        try:
            # Get cleanup interval from recovery manager
            interval_seconds = self.recovery_manager.get_cleanup_interval_seconds()
            
            # Check if enough time has passed since last cleanup
            time_since_last_cleanup = datetime.now() - self.last_cleanup_time
            return time_since_last_cleanup.total_seconds() >= interval_seconds
            
        except Exception as e:
            logger.warning(f"Error checking cleanup interval: {e}, using default")
            # Fallback to 24 hours if configuration is invalid
            time_since_last_cleanup = datetime.now() - self.last_cleanup_time
            return time_since_last_cleanup.total_seconds() >= 24 * 3600
    
    def close(self) -> None:
        """Clean shutdown of monitoring system"""
        self.is_running = False
        if self.db_connection and self.db_connection.is_connected():
            self.db_connection.close()
            logger.info("Database connection closed")


def main():
    """
    Main program entry point with complete error handling
    Single entry point for the entire alarm monitoring application
    """
    print("Starting Alarm Data Monitor...")
    monitor = None
    
    try:
        # Initialize monitor instance
        monitor = AlarmMonitor("config.ini")
        
        # Verify system is ready
        if not monitor.db_connection:
            print("Failed to initialize database connection. Exiting...")
            return 1
        
        # Start monitoring (runs indefinitely until interrupted)
        monitor.start_monitoring()
        
        return 0
        
    except KeyboardInterrupt:
        print("\nMonitoring stopped by user. Shutting down gracefully...")
        return 0
        
    except Exception as e:
        print(f"\nCritical error occurred: {e}")
        logger.critical(f"Program crashed: {e}")
        return 1
        
    finally:
        # Ensure clean shutdown
        if monitor is not None:
            try:
                monitor.close()
            except Exception as e:
                print(f"Error during shutdown: {e}")
        
        print("Alarm Data Monitor stopped.") 


if __name__ == "__main__":
    """Script execution entry point"""
    import sys
    exit_code = main()
    sys.exit(exit_code) 
    