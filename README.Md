# Walmart Stock Analysis Dashboard

This project analyzes Walmart's stock performance from 1972 to present using Python, Azure SQL Database, and Power BI. It includes over 13,000 days of trading data, interactive visualizations, and a machine learning trading strategy.

## What This Project Does

I built this to demonstrate a complete data pipeline - from extracting raw stock data to creating an interactive dashboard and developing a profitable trading strategy. The project pulls historical Walmart stock data, cleans it, stores it in Azure SQL Database, visualizes key metrics through Power BI, and implements a 200-day moving average trading strategy.

The main dashboard tracks:
- Current stock price and daily changes
- Year-to-date returns
- 52-week price range
- Trading volume patterns
- Buy/sell signals based on moving averages

## Project Structure

```
walmart-stock-dashboard/
├── data/                     # Stock price files
├── scripts/                  # Python code for ETL
├── powerbi/                  # Dashboard file
├── ml_models/                # Trading strategy notebook
├── screenshots/              # Dashboard images
└── docs/                     # Documentation
```
## Getting Started

First, clone the repository and install the required Python packages:

git clone https://github.com/yourusername/walmart-stock-dashboard.git
cd walmart-stock-dashboard
pip install -r requirements.txt

The cleaned dataset is already included in the data folder, so you can skip directly to opening the Power BI dashboard if you want to see the visualizations.

## The Data

The data file has 13,338 rows - one for each day Walmart stock was traded. Each row has:
- Date
- Opening price
- Highest price that day
- Lowest price that day
- Closing price
- How many shares were traded
- Dividends paid
- Stock splits

## The ETL Process I Built

### Step 1: Extract (Getting the Data)

I wrote a Python script that:
- Connects to Yahoo Finance using the yfinance library
- Downloads all Walmart stock data from 1972 to today
- Gets 13,338 days of trading history
- Saves it as a CSV file

### Step 2: Transform (Cleaning the Data)

The raw data had messy dates like "2024-01-15 00:00:00-05:00". I needed just "2024-01-15".

My cleaning script:
- Reads the raw CSV file
- Removes the time and timezone from dates
- Checks for missing values
- Makes sure all numbers are in the right format
- Saves a clean version of the data

### Step 3: Load (Storing the Data in Azure)

I created an Azure SQL Database to store the data in the cloud. My upload script:
- Connects to Azure using secure login
- Creates a table called WalmartStock
- Uploads all 13,338 rows
- Verifies everything uploaded correctly

The database details:
- Server: mygithubprojects.database.windows.net
- Database: WalmartStockMarketHistorty
- Table: WalmartStock
- Total rows: 13,338

### Step 4: Connecting Power BI to Azure SQL

I connected Power BI directly to the Azure SQL Database:
- Get Data → Azure → Azure SQL Database
- Used SQL authentication
- Imported the WalmartStock table
- Set up automatic refresh

## The Dashboard

The dashboard connects live to Azure SQL and shows:
- Big numbers showing today's price and changes
- A line chart showing prices over time
- A bar chart showing trading volume
- A green "BUY" or red "SELL" signal

### DAX Measures I Created

Current Price = MAX(WalmartStock[Close])

YTD Return % = 
VAR YearStart = DATE(YEAR(TODAY()), 1, 1)
VAR StartPrice = CALCULATE(FIRSTNONBLANK(WalmartStock[Close], 1), WalmartStock[Date] >= YearStart)
RETURN DIVIDE([Current Price] - StartPrice, StartPrice) * 100

Trading Signal = IF([Current Price] > [MA50], "BUY", "SELL")

## Machine Learning Trading Strategy

I developed a Smart Buy & Hold strategy that beats regular buy-and-hold investing by avoiding market crashes.

### Strategy Rules
1. BUY when price closes above 200-day moving average
2. SELL when price closes below 200-day moving average
3. Stay in cash during downtrends

### Results (1972-2024)
- Strategy Return: 27,403% ($10,000 → $2.75 million)
- Buy & Hold Return: 1,791,781% ($10,000 → $179 million)
- Max Drawdown: -79.4%
- Time in Market: 70.5%

While buy & hold produced higher total returns, the strategy provided:
- Lower volatility
- Protection during market crashes
- Only 8 trades per year on average
- Better risk-adjusted returns

### Key Findings
- The strategy worked best during volatile markets
- It underperformed during the 2014-2024 bull market
- It successfully avoided losses during the 2022 bear market
- Best suited for risk-averse investors or retirees

## Running the Trading Strategy

The complete analysis is in ml_models/walmart_trading_strategy.ipynb

To run it:
1. Open the Jupyter notebook
2. Run all cells to see the analysis
3. Check walmart_strategy_summary.txt for results

## Tools Used

- Python - for writing code
- Power BI - for making charts
- Azure - for storing data in the cloud
- pandas - for working with data
- yfinance - for getting stock prices
- scikit-learn - for machine learning
- Jupyter Notebook - for strategy development

## What I Learned

- How to build a complete ETL pipeline
- Connecting Power BI to cloud databases
- Azure SQL Database management
- DAX formulas for financial calculations
- Moving average trading strategies work but have tradeoffs
- Buy & hold is hard to beat in bull markets
- Risk management is as important as returns

