#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
MySQL Alarm Data Viewer
A simple tool to view alarm data stored in the MySQL database.
"""

import sys
import os
import configparser
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import argparse


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
            return conn
    except Error as e:
        print(f"Error connecting to MySQL database: {e}")
        sys.exit(1)
    
    print("Failed to connect to the database.")
    sys.exit(1)


def get_all_alarms(conn, table_name, limit=100, order_by='Time DESC'):
    """Retrieve all alarms from the database."""
    cursor = conn.cursor(dictionary=True)
    try:
        query = f"SELECT * FROM {table_name} ORDER BY {order_by} LIMIT {limit}"
        cursor.execute(query)
        return cursor.fetchall()
    except Error as e:
        print(f"Error retrieving alarms: {e}")
        return []
    finally:
        cursor.close()


def get_alarms_by_code(conn, table_name, code, limit=100):
    """Retrieve alarms with the specified code."""
    cursor = conn.cursor(dictionary=True)
    try:
        query = f"SELECT * FROM {table_name} WHERE Code = %s ORDER BY Time DESC LIMIT {limit}"
        cursor.execute(query, (code,))
        return cursor.fetchall()
    except Error as e:
        print(f"Error retrieving alarms by code: {e}")
        return []
    finally:
        cursor.close()


def get_alarms_by_severity(conn, table_name, severity, limit=100):
    """Retrieve alarms with the specified severity."""
    cursor = conn.cursor(dictionary=True)
    try:
        query = f"SELECT * FROM {table_name} WHERE Severity = %s ORDER BY Time DESC LIMIT {limit}"
        cursor.execute(query, (severity,))
        return cursor.fetchall()
    except Error as e:
        print(f"Error retrieving alarms by severity: {e}")
        return []
    finally:
        cursor.close()


def get_alarms_by_timerange(conn, table_name, start_time, end_time, limit=100):
    """Retrieve alarms within the specified time range."""
    cursor = conn.cursor(dictionary=True)
    try:
        query = (f"SELECT * FROM {table_name} WHERE Time BETWEEN %s AND %s "
                 f"ORDER BY Time DESC LIMIT {limit}")
        cursor.execute(query, (start_time, end_time))
        return cursor.fetchall()
    except Error as e:
        print(f"Error retrieving alarms by time range: {e}")
        return []
    finally:
        cursor.close()


def get_alarm_statistics(conn, table_name):
    """Get statistics about alarms stored in the database."""
    cursor = conn.cursor(dictionary=True)
    try:
        # Get total count of alarms
        cursor.execute(f"SELECT COUNT(*) as total_count FROM {table_name}")
        total_count = cursor.fetchone()['total_count']
        
        # Get count by severity
        cursor.execute(f"SELECT Severity, COUNT(*) as count FROM {table_name} GROUP BY Severity")
        severity_counts = cursor.fetchall()
        
        # Get count by code (top 10)
        cursor.execute(f"SELECT Code, COUNT(*) as count FROM {table_name} GROUP BY Code ORDER BY count DESC LIMIT 10")
        code_counts = cursor.fetchall()
        
        # Get count by name (top 10)
        cursor.execute(f"SELECT Name, COUNT(*) as count FROM {table_name} GROUP BY Name ORDER BY count DESC LIMIT 10")
        name_counts = cursor.fetchall()
        
        # Get earliest and latest alarm timestamps
        cursor.execute(f"SELECT MIN(Time) as earliest, MAX(Time) as latest FROM {table_name}")
        timestamps = cursor.fetchone()
        
        return {
            'total_count': total_count,
            'severity_counts': severity_counts,
            'top_codes': code_counts,
            'top_names': name_counts,
            'earliest': timestamps['earliest'],
            'latest': timestamps['latest']
        }
    except Error as e:
        print(f"Error retrieving alarm statistics: {e}")
        return {}
    finally:
        cursor.close()


def display_alarms(alarms):
    """Display the alarms in a readable format."""
    if not alarms:
        print("No alarms found.")
        return
    
    # Get column widths based on data content
    widths = {
        'Time': 19,  # Fixed width for timestamp
        'Instance': max(5, max(len(str(alarm['Instance'])) for alarm in alarms)),
        'Name': max(15, max(len(str(alarm['Name'])) for alarm in alarms)),
        'Code': max(4, max(len(str(alarm['Code'])) for alarm in alarms)),
        'Severity': max(4, max(len(str(alarm['Severity'])) for alarm in alarms)),
        'AdditionalInformation1': max(10, max(len(str(alarm['AdditionalInformation1'] or "")) for alarm in alarms)),
        'AdditionalInformation2': max(10, max(len(str(alarm['AdditionalInformation2'] or "")) for alarm in alarms)),
        'Change': max(6, max(len(str(alarm['Change'])) for alarm in alarms)),
        'Message': max(7, max(len(str(alarm['Message'])) for alarm in alarms if alarm['Message']))
    }
    
    # Limit column widths to prevent excessively wide display
    max_column_width = 30
    for key in ['Name', 'AdditionalInformation1', 'AdditionalInformation2', 'Message']:
        if widths[key] > max_column_width:
            widths[key] = max_column_width
    
    # Print headers with all fields in a single row (without ID column)
    header = (f"{'Time':<{widths['Time']}} | {'Inst':<{widths['Instance']}} | "
             f"{'Name':<{widths['Name']}} | {'Code':<{widths['Code']}} | {'Sev':<{widths['Severity']}} | "
             f"{'AddInfo1':<{widths['AdditionalInformation1']}} | {'AddInfo2':<{widths['AdditionalInformation2']}} | "
             f"{'Change':<{widths['Change']}} | {'Message':<{widths['Message']}}")
    
    separator = '-' * len(header)
    
    print(separator)
    print(header)
    print(separator)
    
    # Print each alarm with all information in a single row (without ID column)
    for alarm in alarms:
        # Format timestamp for display
        timestamp = alarm['Time'].strftime('%Y-%m-%d %H:%M:%S') if alarm['Time'] else 'N/A'
        
        # Truncate very long text fields for better display
        message = str(alarm['Message'])
        if len(message) > widths['Message']:
            message = message[:widths['Message']-3] + '...'
            
        name = str(alarm['Name'])
        if len(name) > widths['Name']:
            name = name[:widths['Name']-3] + '...'
        
        # Handle potentially None values in additional info fields
        add_info1 = str(alarm['AdditionalInformation1'] or "")
        if len(add_info1) > widths['AdditionalInformation1']:
            add_info1 = add_info1[:widths['AdditionalInformation1']-3] + '...'
            
        add_info2 = str(alarm['AdditionalInformation2'] or "")
        if len(add_info2) > widths['AdditionalInformation2']:
            add_info2 = add_info2[:widths['AdditionalInformation2']-3] + '...'
        
        # Print all information in one row (without ID column)
        print(f"{timestamp:<{widths['Time']}} | "
              f"{alarm['Instance']:<{widths['Instance']}} | {name:<{widths['Name']}} | "
              f"{alarm['Code']:<{widths['Code']}} | {alarm['Severity']:<{widths['Severity']}} | "
              f"{add_info1:<{widths['AdditionalInformation1']}} | {add_info2:<{widths['AdditionalInformation2']}} | "
              f"{alarm['Change']:<{widths['Change']}} | {message}")
    
    print(separator)
    print(f"Total: {len(alarms)} records")


def display_statistics(stats):
    """Display alarm statistics in a readable format."""
    if not stats:
        print("No statistics available.")
        return
    
    print("\n===== ALARM STATISTICS =====")
    print(f"Total alarms: {stats['total_count']}")
    
    if stats.get('earliest') and stats.get('latest'):
        print(f"Time range: {stats['earliest']} to {stats['latest']}")
    
    if stats.get('severity_counts'):
        print("\nAlarms by severity:")
        for severity in stats['severity_counts']:
            print(f"  Severity {severity['Severity']}: {severity['count']} alarms")
    
    if stats.get('top_codes'):
        print("\nTop alarm codes:")
        for code in stats['top_codes']:
            print(f"  Code {code['Code']}: {code['count']} alarms")
            
    if stats.get('top_names'):
        print("\nTop alarm names:")
        for name in stats['top_names']:
            print(f"  {name['Name']}: {name['count']} alarms")


def main():
    parser = argparse.ArgumentParser(description='View alarm data from MySQL database')
    parser.add_argument('--config', default='config.ini', help='Path to configuration file')
    parser.add_argument('--limit', type=int, default=300, help='Maximum number of records to show')
    parser.add_argument('--stats', action='store_true', help='Show alarm statistics')
    parser.add_argument('--code', type=int, help='Filter alarms by code')
    parser.add_argument('--severity', type=int, help='Filter alarms by severity')
    parser.add_argument('--start-time', help='Start time for filtering (format: YYYY-MM-DD HH:MM:SS)')
    parser.add_argument('--end-time', help='End time for filtering (format: YYYY-MM-DD HH:MM:SS)')
    
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    table_name = config['DEFAULT']['table_name']
    
    # Connect to database
    print("Connecting to database...")
    conn = connect_to_database(config)
    print("Connected successfully!")
    
    try:
        # Show statistics if requested
        if args.stats:
            stats = get_alarm_statistics(conn, table_name)
            display_statistics(stats)
        
        # Apply filters based on arguments
        if args.code is not None:
            print(f"\nShowing alarms with code {args.code}:")
            alarms = get_alarms_by_code(conn, table_name, args.code, args.limit)
        elif args.severity is not None:
            print(f"\nShowing alarms with severity {args.severity}:")
            alarms = get_alarms_by_severity(conn, table_name, args.severity, args.limit)
        elif args.start_time and args.end_time:
            try:
                start = datetime.strptime(args.start_time, '%Y-%m-%d %H:%M:%S')
                end = datetime.strptime(args.end_time, '%Y-%m-%d %H:%M:%S')
                print(f"\nShowing alarms between {start} and {end}:")
                alarms = get_alarms_by_timerange(conn, table_name, start, end, args.limit)
            except ValueError:
                print("Error: Invalid time format. Use YYYY-MM-DD HH:MM:SS")
                return
        else:
            print(f"\nShowing latest {args.limit} alarms:")
            alarms = get_all_alarms(conn, table_name, args.limit)
        
        # Display the results
        display_alarms(alarms)
    
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