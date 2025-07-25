import pandas as pd

# Method 1: Using pandas to clean dates during reading
def clean_dates_method1():
    """
    Clean dates while reading the CSV file
    """
    # Read the CSV and parse dates
    df = pd.read_csv('walmart_stock_history.csv', parse_dates=['Date'])
    
    # Convert Date column to just the date (no time)
    df['Date'] = df['Date'].dt.date
    
    # Save the cleaned data
    df.to_csv('walmart_stock_history_clean.csv', index=False)
    print("Method 1: Cleaned data saved to walmart_stock_history_clean.csv")
    return df

# Method 2: String manipulation approach
def clean_dates_method2():
    """
    Clean dates using string manipulation
    """
    df = pd.read_csv('walmart_stock_history.csv')
    
    # Remove everything after the space (including time and timezone)
    df['Date'] = df['Date'].str.split(' ').str[0]
    
    # Save the cleaned data
    df.to_csv('walmart_stock_history_clean2.csv', index=False)
    print("Method 2: Cleaned data saved to walmart_stock_history_clean2.csv")
    return df

# Method 3: Using string replacement
def clean_dates_method3():
    """
    Clean dates using string replacement for specific patterns
    """
    df = pd.read_csv('walmart_stock_history.csv')
    
    # Remove the time patterns
    df['Date'] = df['Date'].str.replace(' 00:00:00-04:00', '', regex=False)
    df['Date'] = df['Date'].str.replace(' 00:00:00-05:00', '', regex=False)
    
    # Save the cleaned data
    df.to_csv('walmart_stock_history_clean3.csv', index=False)
    print("Method 3: Cleaned data saved to walmart_stock_history_clean3.csv")
    return df

# Method 4: Using regex for more flexibility
def clean_dates_method4():
    """
    Clean dates using regex to remove any time pattern
    """
    import re
    
    df = pd.read_csv('walmart_stock_history.csv')
    
    # Remove any time pattern (everything after the date)
    df['Date'] = df['Date'].str.replace(r'\s+\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}', '', regex=True)
    
    # Save the cleaned data
    df.to_csv('walmart_stock_history_clean4.csv', index=False)
    print("Method 4: Cleaned data saved to walmart_stock_history_clean4.csv")
    return df

# Main function to demonstrate all methods
def main():
    """
    Clean the date column in Walmart stock data
    """
    print("Cleaning Walmart stock data dates...")
    print("=" * 50)
    
    # You can choose any method you prefer
    # Method 1 is recommended as it properly handles dates
    
    try:
        # Use Method 1 (recommended)
        df = clean_dates_method1()
        
        print("\nFirst 5 rows of cleaned data:")
        print(df.head())
        
        print("\nDate column now contains only dates (YYYY-MM-DD format)")
        
    except Exception as e:
        print(f"Error: {e}")
        print("Trying alternative method...")
        
        # Fallback to Method 2
        df = clean_dates_method2()
        print("\nFirst 5 rows of cleaned data:")
        print(df.head())

# Quick one-liner if you just want to clean it fast
def quick_clean():
    """
    Quick one-liner to clean the dates
    """
    # Read, clean, and save in one go
    pd.read_csv('walmart_stock_history.csv').assign(
        Date=lambda x: x['Date'].str.split(' ').str[0]
    ).to_csv('walmart_stock_history_clean.csv', index=False)
    
    print("Data cleaned and saved!")

if __name__ == "__main__":
    main()
    
    # Or use the quick method:
    # quick_clean()