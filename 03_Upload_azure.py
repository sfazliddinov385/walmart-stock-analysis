import pyodbc
import pandas as pd
import os
import getpass

def upload_walmart_data_to_azure():
    """Upload Walmart stock data to Azure SQL Database"""
    
    print("=== Walmart Stock Data Upload to Azure SQL ===\n")
    
    # Connection details
    SERVER = 'mygithubprojects.database.windows.net'
    DATABASE = 'WalmartStockMarketHistorty'  # Note: typo in database name
    
    # Get credentials
    print("Enter your SQL Server credentials:")
    username = input("Username (e.g., CloudSA9979afc6): ")
    password = getpass.getpass("Password: ")
    
    # Build connection string
    conn_string = (
        f'DRIVER={{ODBC Driver 18 for SQL Server}};'
        f'SERVER={SERVER};'
        f'DATABASE={DATABASE};'
        f'UID={username};'
        f'PWD={password};'
        f'Encrypt=yes;'
        f'TrustServerCertificate=no;'
        f'Connection Timeout=30;'
    )
    
    try:
        # Connect to database
        print("\nConnecting to Azure SQL Database...")
        conn = pyodbc.connect(conn_string)
        cursor = conn.cursor()
        print("✅ Connected successfully!\n")
        
        # Create table
        print("Creating table structure...")
        create_table_sql = """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='WalmartStock' AND xtype='U')
        CREATE TABLE WalmartStock (
            Date DATE PRIMARY KEY,
            [Open] DECIMAL(10, 4),
            High DECIMAL(10, 4),
            Low DECIMAL(10, 4),
            [Close] DECIMAL(10, 4),
            Volume BIGINT,
            Dividends DECIMAL(10, 4),
            StockSplits DECIMAL(10, 4)
        );
        """
        cursor.execute(create_table_sql)
        conn.commit()
        print("✅ Table created/verified!\n")
        
        # Check if data already exists
        cursor.execute("SELECT COUNT(*) FROM WalmartStock")
        existing_count = cursor.fetchone()[0]
        
        if existing_count > 0:
            print(f"⚠️  Table already contains {existing_count} rows.")
            response = input("Do you want to delete existing data and reload? (y/n): ")
            if response.lower() == 'y':
                print("Deleting existing data...")
                cursor.execute("DELETE FROM WalmartStock")
                conn.commit()
                print("✅ Existing data deleted.\n")
            else:
                print("Keeping existing data. Exiting...")
                conn.close()
                return
        
        # Load CSV file
        csv_file = 'walmart_stock_history_clean2.csv'
        if not os.path.exists(csv_file):
            print(f"❌ Error: {csv_file} not found in current directory!")
            print(f"Current directory: {os.getcwd()}")
            conn.close()
            return
        
        print(f"Loading data from {csv_file}...")
        df = pd.read_csv(csv_file)
        total_rows = len(df)
        print(f"✅ Loaded {total_rows:,} rows from CSV\n")
        
        # Upload data
        print("Uploading data to Azure SQL Database...")
        print("This may take a few minutes...\n")
        
        success_count = 0
        error_count = 0
        batch_size = 1000
        
        for i in range(0, total_rows, batch_size):
            batch = df.iloc[i:i+batch_size]
            
            for _, row in batch.iterrows():
                try:
                    cursor.execute(
                        """INSERT INTO WalmartStock 
                           (Date, [Open], High, Low, [Close], Volume, Dividends, StockSplits) 
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                        row['Date'], 
                        float(row['Open']), 
                        float(row['High']), 
                        float(row['Low']), 
                        float(row['Close']), 
                        int(row['Volume']), 
                        float(row['Dividends']), 
                        float(row['Stock Splits'])
                    )
                    success_count += 1
                except Exception as e:
                    error_count += 1
                    if error_count <= 5:  # Only show first 5 errors
                        print(f"  Error on row {row['Date']}: {str(e)[:50]}...")
            
            # Commit batch
            conn.commit()
            
            # Progress update
            uploaded = min(i + batch_size, total_rows)
            percent = (uploaded / total_rows) * 100
            print(f"Progress: {uploaded:,}/{total_rows:,} rows ({percent:.1f}%)")
        
        print(f"\n✅ Upload complete!")
        print(f"   Successfully uploaded: {success_count:,} rows")
        if error_count > 0:
            print(f"   Errors: {error_count} rows")
        
        # Verify and show statistics
        print("\n📊 Verifying data in database...")
        
        # Get statistics
        cursor.execute("""
            SELECT 
                COUNT(*) as TotalRows,
                MIN(Date) as FirstDate,
                MAX(Date) as LastDate,
                MIN([Close]) as MinPrice,
                MAX([Close]) as MaxPrice,
                AVG([Close]) as AvgPrice,
                SUM(CASE WHEN StockSplits > 0 THEN 1 ELSE 0 END) as TotalSplits
            FROM WalmartStock
        """)
        
        stats = cursor.fetchone()
        print(f"\nDatabase Statistics:")
        print(f"  📈 Total Rows: {stats[0]:,}")
        print(f"  📅 Date Range: {stats[1]} to {stats[2]}")
        print(f"  💰 Price Range: ${stats[3]:.4f} to ${stats[4]:.2f}")
        print(f"  💵 Average Price: ${stats[5]:.2f}")
        print(f"  🔄 Stock Splits: {stats[6]}")
        
        # Calculate total return
        cursor.execute("""
            WITH PriceData AS (
                SELECT 
                    [Close] as FirstPrice
                FROM WalmartStock 
                WHERE Date = (SELECT MIN(Date) FROM WalmartStock)
            ),
            LastPriceData AS (
                SELECT 
                    [Close] as LastPrice
                FROM WalmartStock 
                WHERE Date = (SELECT MAX(Date) FROM WalmartStock)
            )
            SELECT 
                p.FirstPrice,
                l.LastPrice,
                ((l.LastPrice / p.FirstPrice) - 1) * 100 as TotalReturn
            FROM PriceData p, LastPriceData l
        """)
        
        returns = cursor.fetchone()
        if returns:
            print(f"  📊 Total Return: {returns[2]:,.2f}% (${returns[0]:.4f} → ${returns[1]:.2f})")
        
        # Show recent data
        print("\n📈 Most recent 5 days:")
        cursor.execute("""
            SELECT TOP 5 Date, [Close], Volume 
            FROM WalmartStock 
            ORDER BY Date DESC
        """)
        
        for row in cursor.fetchall():
            print(f"  {row[0]}: Close=${row[1]:.2f}, Volume={row[2]:,}")
        
        print("\n✅ SUCCESS! Your Walmart stock data is now in Azure SQL Database!")
        print("\n📝 Next steps:")
        print("  1. Use Query Editor in Azure Portal to run SQL queries")
        print("  2. Connect from Power BI to create visualizations")
        print("  3. Use Python/pandas to analyze the data")
        print("  4. Connect from Excel for reports")
        
        # Close connection
        cursor.close()
        conn.close()
        
    except pyodbc.Error as e:
        print(f"\n❌ Database error: {e}")
        if "Login failed" in str(e):
            print("\n🔍 Check:")
            print("  1. Username is correct (from connection string)")
            print("  2. Password is what you just set")
            print("  3. SQL authentication is enabled")
    except Exception as e:
        print(f"\n❌ Error: {e}")

if __name__ == "__main__":
    # Clear screen for better visibility
    os.system('cls' if os.name == 'nt' else 'clear')
    
    # Check if CSV exists before starting
    if not os.path.exists('walmart_stock_history_clean2.csv'):
        print("❌ Error: walmart_stock_history_clean2.csv not found!")
        print(f"Please make sure the file is in: {os.getcwd()}")
    else:
        upload_walmart_data_to_azure()