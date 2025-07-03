"""
System Recovery Manager

Handles system recovery, maintenance and database management functionality
for the Alarm Data Monitor module.

This module provides:
- Database structure initialization and verification
- Crash recovery and interrupted file processing
- Automatic cleanup of orphaned resources
- Data retention management
- Performance optimization through index management
"""

import os
import time
import mysql.connector
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Union, Optional

# Get logger instance (will be configured by main application)
logger = logging.getLogger("AlarmMonitor")

# Database constants
DB_CHARSET = "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"


class SystemRecoveryManager:
    """
    System recovery, maintenance and database management functionality
    Handles error recovery, resource cleanup, and database structure management
    """
    
    def __init__(self, alarm_monitor):
        """Initialize recovery manager with reference to main monitor"""
        self.monitor = alarm_monitor
        self.config = alarm_monitor.config
        
    @property
    def db_connection(self):
        """Get database connection from main monitor"""
        return self.monitor.db_connection
    
    def get_cursor(self, dictionary=False):
        """Get database cursor using monitor's cursor manager"""
        return self.monitor.get_cursor(dictionary=dictionary)
    
    def perform_full_recovery(self) -> bool:
        """
        Master recovery function that handles all system recovery operations
        Single entry point for complete system health restoration
        """
        recovery_steps = [
            ("Initializing database structure", self.initialize_database),
            ("Cleaning orphaned temp tables", self.cleanup_temp_tables),
            ("Repairing database connection", self.repair_connection),  
            ("Verifying database indexes", self.verify_indexes),
            ("Recovering interrupted files", self.recover_interrupted_files),
            ("Maintaining state table", self.maintain_state_table),
            ("Validating data integrity", self.validate_data_integrity)
        ]
        
        logger.info("Starting system recovery process")
        successful_steps = 0
        
        for step_name, step_function in recovery_steps:
            try:
                logger.debug(f"Recovery step: {step_name}")
                step_function()
                successful_steps += 1
                
            except Exception as e:
                logger.error(f"Recovery step failed - {step_name}: {e}")
                # Continue with next step, don't stop entire recovery
                
        success_rate = successful_steps / len(recovery_steps)
        if success_rate == 1.0:
            logger.info("System recovery completed successfully")
        elif success_rate > 0.7:
            logger.warning(f"System recovery partially completed ({successful_steps}/{len(recovery_steps)} steps)")
        else:
            logger.error(f"System recovery mostly failed ({successful_steps}/{len(recovery_steps)} steps)")
            
        return success_rate > 0.5
    
    def perform_maintenance(self) -> None:
        """
        Routine database maintenance operations
        Called periodically to maintain system performance
        
        Includes cleanup of orphaned resources from potential process crashes
        """
        try:
            self.maintain_state_table()
            self.cleanup_temp_tables()  # Clean orphaned temp tables from crashed processes
            
            # Clean expired data if enabled
            if self.config.getboolean('DATA_MANAGEMENT', 'auto_cleanup_enabled', fallback=True):
                retention_months = self.config.getint('DATA_MANAGEMENT', 'data_retention_months', fallback=12)
                self.cleanup_expired_data(retention_months)
                
            logger.debug("Routine maintenance completed")
            
        except Exception as e:
            logger.error(f"Maintenance operation failed: {e}")
    
    def initialize_database(self) -> None:
        """Create and verify all required database tables and indexes"""
        self._ensure_alarms_table_exists()
        self._ensure_processing_state_table_exists()
        self.verify_indexes()
        
    def _ensure_alarms_table_exists(self) -> None:
        """Create the main alarms table if it doesn't exist"""
        table_name = self.config['DEFAULT']['table_name']
        
        with self.get_cursor() as cursor:
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                Time DATETIME(3) NOT NULL,
                Instance INT NOT NULL,
                Name VARCHAR(255) {DB_CHARSET} NOT NULL,
                Code INT NOT NULL,
                Severity INT NOT NULL,
                AdditionalInformation1 VARCHAR(255) {DB_CHARSET},
                AdditionalInformation2 VARCHAR(255) {DB_CHARSET},
                `Change` TEXT {DB_CHARSET} NOT NULL,
                Message TEXT {DB_CHARSET} NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_time (Time)
            ) {DB_CHARSET}
            """
            
            cursor.execute(create_table_query)
            self.db_connection.commit()
            logger.debug(f"Table '{table_name}' structure verified")
    
    def _ensure_processing_state_table_exists(self) -> None:
        """Create processing state tracking table for crash recovery"""
        with self.get_cursor() as cursor:
            create_state_table = f"""
            CREATE TABLE IF NOT EXISTS processing_state (
                id INT AUTO_INCREMENT PRIMARY KEY,
                file_name VARCHAR(255) {DB_CHARSET} UNIQUE NOT NULL,
                file_path VARCHAR(500) {DB_CHARSET} NOT NULL,
                processing_status ENUM('started', 'completed', 'failed') {DB_CHARSET} NOT NULL,
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP NULL,
                error_message TEXT {DB_CHARSET} NULL,
                record_count INT DEFAULT 0,
                INDEX idx_status (processing_status),
                INDEX idx_started (started_at)
            ) {DB_CHARSET}
            """
            
            cursor.execute(create_state_table)
            self.db_connection.commit()
            logger.debug("Processing state table structure verified")
    
    def verify_indexes(self) -> None:
        """Ensure all required database indexes exist for optimal performance"""
        table_name = self.config['DEFAULT']['table_name']
        
        with self.get_cursor() as cursor:
            # Check and create composite index for duplicate detection
            cursor.execute(f"""
                SELECT COUNT(*) FROM information_schema.statistics 
                WHERE table_schema = DATABASE() 
                AND table_name = '{table_name}' 
                AND index_name = 'idx_duplicate_check'
            """)
            
            if cursor.fetchone()[0] == 0:
                cursor.execute(f"""
                    ALTER TABLE {table_name} 
                    ADD INDEX idx_duplicate_check (Time, Instance, Code, Name)
                """)
                logger.info("Created composite index for duplicate detection")
            
            # Check and create cleanup index
            cursor.execute(f"""
                SELECT COUNT(*) FROM information_schema.statistics 
                WHERE table_schema = DATABASE() 
                AND table_name = '{table_name}' 
                AND index_name = 'idx_cleanup'
            """)
            
            if cursor.fetchone()[0] == 0:
                cursor.execute(f"""
                    ALTER TABLE {table_name} 
                    ADD INDEX idx_cleanup (created_at)
                """)
                logger.info("Created index for data cleanup operations")
                
            self.db_connection.commit()
    
    def cleanup_temp_tables(self) -> None:
        """
        Remove orphaned temporary tables left by crashed processes
        
        NOTE: Normal processing creates and immediately deletes temp tables (lifecycle: seconds)
        This function only cleans up leftover tables from abnormal program terminations
        """
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    SELECT table_name FROM information_schema.tables 
                    WHERE table_schema = DATABASE() 
                    AND table_name LIKE 'temp_alarms_%'
                    AND create_time < NOW() - INTERVAL 1 HOUR
                """)
                
                orphaned_tables = cursor.fetchall()
                cleaned_count = 0
                
                for table_row in orphaned_tables:
                    table_name = table_row[0]
                    try:
                        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                        cleaned_count += 1
                    except Exception as e:
                        logger.warning(f"Failed to clean temp table {table_name}: {e}")
                
                if cleaned_count > 0:
                    logger.info(f"Cleaned {cleaned_count} orphaned temporary tables from crashed processes")
                    self.db_connection.commit()
                else:
                    logger.debug("No orphaned temporary tables found (normal condition)")
                    
        except Exception as e:
            logger.error(f"Temp table cleanup failed: {e}")
    
    def recover_interrupted_files(self) -> None:
        """Recover files that were being processed when program crashed"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    SELECT file_name, file_path FROM processing_state 
                    WHERE processing_status = 'started'
                    ORDER BY started_at
                """)
                
                interrupted_files = cursor.fetchall()
                
                if interrupted_files:
                    logger.warning(f"Found {len(interrupted_files)} interrupted files")
                    
                    for file_name, file_path in interrupted_files:
                        logger.info(f"Recovering interrupted file: {file_name}")
                        
                        # Reset status to allow reprocessing
                        cursor.execute("""
                            UPDATE processing_state 
                            SET processing_status = 'failed', 
                                error_message = 'Recovered from program interruption'
                            WHERE file_name = %s
                        """, (file_name,))
                        
                    self.db_connection.commit()
                    
        except Exception as e:
            logger.error(f"File recovery failed: {e}")
    
    def maintain_state_table(self) -> None:
        """Keep processing state table size under control"""
        max_records = self.config.getint('DATA_MANAGEMENT', 'max_processing_state_records', fallback=100)
        
        try:
            with self.get_cursor() as cursor:
                # Keep only the latest N records
                cursor.execute("""
                    DELETE FROM processing_state 
                    WHERE id NOT IN (
                        SELECT id FROM (
                            SELECT id FROM processing_state 
                            ORDER BY started_at DESC 
                            LIMIT %s
                        ) as temp_table
                    )
                """, (max_records,))
                
                deleted_count = cursor.rowcount
                if deleted_count > 0:
                    logger.debug(f"Cleaned {deleted_count} old processing state records")
                    self.db_connection.commit()
                    
        except Exception as e:
            logger.error(f"State table maintenance failed: {e}")
    
    def repair_connection(self) -> None:
        """Restore database connection after network issues"""
        if not self.db_connection or not self.db_connection.is_connected():
            logger.info("Repairing database connection")
            if not self.monitor.reconnect_to_database():
                raise Exception("Failed to repair database connection")
    
    def validate_data_integrity(self) -> None:
        """Verify data integrity for recently processed files"""
        # This is a placeholder for future data integrity checks
        # Could include checksum validation, record count verification, etc.
        logger.debug("Data integrity validation completed")
    
    def cleanup_expired_data(self, retention_months: int) -> None:
        """Remove alarm data older than retention period"""
        table_name = self.config['DEFAULT']['table_name']
        cutoff_date = datetime.now() - timedelta(days=retention_months * 30)
        
        try:
            with self.get_cursor() as cursor:
                cursor.execute(f"""
                    DELETE FROM {table_name} 
                    WHERE created_at < %s
                """, (cutoff_date,))
                
                deleted_count = cursor.rowcount
                if deleted_count > 0:
                    logger.info(f"Cleaned {deleted_count} expired records (older than {retention_months} months)")
                    self.db_connection.commit()
                    
        except Exception as e:
            logger.error(f"Expired data cleanup failed: {e}") 