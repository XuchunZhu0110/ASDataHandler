#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Clear Alarm Table Data

A utility script to truncate (empty) the alarms table in the database.
This allows for a fresh start without deleting the table structure itself.
"""

import sys
import os
import configparser
import mysql.connector
from mysql.connector import Error


def load_config(config_file='config.ini'):
    """Load configuration from the config file."""
    if not os.path.exists(config_file):
        print(f"Error: Configuration file '{config_file}' not found.")
        sys.exit(1)
    
    config = configparser.ConfigParser()
    try:
        # Force reading config file with UTF-8 encoding
        with open(config_file, 'r', encoding='utf-8') as f:
            config.read_file(f)
        return config
    except Exception as e:
        print(f"Error reading configuration: {e}")
        sys.exit(1)


def connect_to_database(config):
    """Connect to MySQL database using configuration settings."""
    db_config = {
        'host': config['DEFAULT']['db_host'],
        'port': int(config['DEFAULT']['db_port']),
        'user': config['DEFAULT']['db_user'],
        'password': config['DEFAULT']['db_password'],
        'database': config['DEFAULT']['db_name'],
    }
    
    try:
        conn = mysql.connector.connect(**db_config)
        if conn.is_connected():
            print(f"Connected to MySQL database: {config['DEFAULT']['db_name']}")
            return conn
    except Error as e:
        print(f"Error connecting to MySQL database: {e}")
        sys.exit(1)
    
    print("Failed to connect to the database.")
    sys.exit(1)


def clear_table(conn, table_name):
    """
    Clear all data from the specified table.
    
    This function uses TRUNCATE TABLE which is faster than DELETE FROM
    for removing all records. It also resets the auto-increment counter.
    """
    cursor = conn.cursor()
    try:
        print(f"\nPreparing to clear all data from table '{table_name}'...")
        
        # First get the current count to show the user
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        print(f"Current number of records in the table: {count}")
        
        # Get confirmation from the user
        confirmation = input("\nWARNING: This will permanently delete ALL records from the table.\n"
                           "This action cannot be undone. Do you want to continue? (yes/no): ")
        
        if confirmation.lower() not in ['yes', 'y']:
            print("Operation cancelled. No data was deleted.")
            return False
        
        # Proceed with truncation
        print("Clearing table data...")
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        conn.commit()
        
        print(f"Successfully removed all {count} records from '{table_name}'.")
        print("The table structure has been preserved.")
        return True
    
    except Error as e:
        print(f"Error clearing table: {e}")
        return False
    
    finally:
        cursor.close()


def main():
    # Load configuration
    config = load_config()
    table_name = config['DEFAULT']['table_name']
    
    # Connect to database
    conn = connect_to_database(config)
    
    try:
        # Clear the table
        clear_table(conn, table_name)
    
    finally:
        # Close connection
        if conn and conn.is_connected():
            conn.close()
            print("\nDatabase connection closed.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
        sys.exit(0) 