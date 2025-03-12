## Overview
This functionality of the Data Processor module, which reads cricket player data from CSV and JSON files, processes it, and validates it against the expected output.

## Process Flow
1. **Read Input Data**
   - CSV files (1990-2000) and JSON files (2000 onwards) are read from the `inputDataSet` directory.
   
2. **Data Processing**
   - Players are categorized based on performance:
     - **All-Rounder**: Runs > 500 and Wickets > 50
     - **Batsman**: Runs > 500 and Wickets ≤ 50
     - **Bowler**: Runs ≤ 500
   - Players with missing runs, wickets, or invalid age (<15 or >50) are removed.
   
3. **Validation**
   - Processed data is compared with expected results from `outputDataSet`.
   - A `test_result.csv` file is generated, marking each entry as `PASS` or `FAIL`.
