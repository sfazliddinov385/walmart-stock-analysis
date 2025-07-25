import yfinance as yf
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt

def get_walmart_stock_history():
    """
    Extract Walmart's complete stock market history from IPO to today
    """
    # Create Walmart ticker object
    wmt = yf.Ticker("WMT")
    
    # Get historical data from the earliest available date
    # yfinance will automatically get data from the earliest available date
    history = wmt.history(period="max")
    
    # Reset index to make Date a column
    history.reset_index(inplace=True)
    
    # Display basic information
    print("Walmart Stock Market History")
    print("=" * 50)
    print(f"Stock Symbol: WMT")
    print(f"IPO Date: October 1, 1970 (trading began August 25, 1972)")
    print(f"Data Available From: {history['Date'].min().strftime('%Y-%m-%d')}")
    print(f"Data Available To: {history['Date'].max().strftime('%Y-%m-%d')}")
    print(f"Total Trading Days: {len(history)}")
    print("=" * 50)
    
    # Display first few rows
    print("\nFirst 5 days of trading:")
    print(history.head())
    
    # Display last few rows
    print("\nLast 5 days of trading:")
    print(history.tail())
    
    # Calculate some key statistics
    print("\nKey Statistics:")
    print(f"Starting Price (Close): ${history['Close'].iloc[0]:.2f}")
    print(f"Current Price (Close): ${history['Close'].iloc[-1]:.2f}")
    print(f"All-Time High: ${history['High'].max():.2f}")
    print(f"All-Time Low: ${history['Low'].min():.2f}")
    print(f"Total Return: {((history['Close'].iloc[-1] / history['Close'].iloc[0]) - 1) * 100:.2f}%")
    
    # Save to CSV
    csv_filename = 'walmart_stock_history.csv'
    history.to_csv(csv_filename, index=False)
    print(f"\nData saved to: {csv_filename}")
    
    return history

def plot_walmart_history(history):
    """
    Create visualizations of Walmart stock history
    """
    # Create figure with subplots
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
    
    # Plot 1: Price History
    ax1.plot(history['Date'], history['Close'], label='Close Price', color='blue', linewidth=0.5)
    ax1.set_title('Walmart (WMT) Stock Price History', fontsize=16)
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Price ($)')
    ax1.grid(True, alpha=0.3)
    ax1.legend()
    
    # Plot 2: Trading Volume
    ax2.bar(history['Date'], history['Volume'], label='Volume', color='green', alpha=0.7)
    ax2.set_title('Walmart (WMT) Trading Volume History', fontsize=16)
    ax2.set_xlabel('Date')
    ax2.set_ylabel('Volume')
    ax2.grid(True, alpha=0.3)
    ax2.legend()
    
    plt.tight_layout()
    plt.savefig('walmart_stock_history.png', dpi=300, bbox_inches='tight')
    print("\nChart saved to: walmart_stock_history.png")
    plt.show()

def calculate_stock_splits(history):
    """
    Identify potential stock splits by detecting large price drops with volume spikes
    """
    print("\nAnalyzing for potential stock splits...")
    print("(Note: This is an approximation. For exact split dates, check official records)")
    
    # Calculate daily returns
    history['Daily_Return'] = history['Close'].pct_change()
    
    # Look for days with significant price drops (potential splits)
    # Stock splits typically show as 50% drops (2:1 split) or 66.67% drops (3:1 split)
    potential_splits = history[history['Daily_Return'] < -0.4].copy()
    
    if len(potential_splits) > 0:
        print("\nPotential stock split dates:")
        for idx, row in potential_splits.iterrows():
            print(f"Date: {row['Date'].strftime('%Y-%m-%d')}, "
                  f"Price drop: {row['Daily_Return']*100:.1f}%")
    
    # According to research: Walmart has had 11 two-for-one splits and 1 three-for-one split
    print("\nOfficial record: Walmart has had 11 two-for-one (2:1) splits and 1 three-for-one (3:1) split")

def main():
    """
    Main function to run the Walmart stock history extraction
    """
    try:
        # Get the data
        print("Fetching Walmart stock history...")
        history = get_walmart_stock_history()
        
        # Create visualizations
        plot_walmart_history(history)
        
        # Analyze for stock splits
        calculate_stock_splits(history)
        
        print("\nAnalysis complete!")
        
    except Exception as e:
        print(f"An error occurred: {e}")
        print("\nMake sure you have yfinance installed: pip install yfinance pandas matplotlib")

if __name__ == "__main__":
    main()