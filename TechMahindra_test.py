import os
import json
import pandas as pd
from pathlib import Path

def read_csv_files(directory: str) -> pd.DataFrame:
    """Read CSV files from a given directory."""

    csv_files = Path(directory).rglob("*.csv")
    df_list = [pd.read_csv(file, sep=';') for file in csv_files]
    return pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()

def read_json_files(directory: str) -> pd.DataFrame:
    """Read JSON files from a given directory, handling potential trailing data issues."""

    json_files = Path(directory).rglob("*.json")
    df_list = []
    for file in json_files:
        try:
            df_list.append(pd.read_json(file, lines=True)) 
        except ValueError as e:
            raise e
    return pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()

def determine_player_type(row):

        if row['runs'] > 500 and row['wickets'] > 50:
            return "All-Rounder"
        elif row['runs'] > 500:
            return "Batsman"
        else:
            return "Bowler"

def process_data(df: pd.DataFrame) -> pd.DataFrame:
    """Process data to determine player type and filter valid records."""

    df = df.dropna(subset=['runs', 'wickets', 'age'])
    df = df[(df['age'] >= 15) & (df['age'] <= 50)]
    df['playerType'] = df.apply(determine_player_type, axis=1)
    return df

def validate_output(processed_df: pd.DataFrame, expected_output_path: str) -> pd.DataFrame:
    """Validate processed data against expected output."""
    
    expected_df = read_csv_files(expected_output_path)
    merged_df = processed_df.merge(expected_df, on=['eventType', 'playerName', 'age', 'runs', 'wickets', 'playerType'], how='outer', indicator=True)
    print(merged_df)
    merged_df['Result'] = merged_df['_merge'].apply(lambda x: "PASS" if x == "both" else "FAIL")
    merged_df = merged_df.drop(columns=['_merge'])
    # print(merged_df)

    # assert all(merged_df['Result'] == 'PASS')
    # pd.testing.assert_frame_equal(processed_df.reset_index(drop=True), expected_df.reset_index(drop=True))
    
    
    return merged_df

def main():
    """Main function to orchestrate data processing and validation."""
    
    input_directory = "inputDataSet"
    output_directory = "outputDataSet"
    temp_output = "tempDataSet"
    test_result_file = "test_result.csv"
    
    os.makedirs(temp_output, exist_ok=True)
    
    csv_data = read_csv_files(input_directory)
    json_data = read_json_files(input_directory)

    combined_df = pd.concat([csv_data, json_data], ignore_index=True)
   
    processed_df = process_data(combined_df)
    
    processed_df.to_csv(temp_output+"/processed_data.csv", index=False)
    
    validated_df = validate_output(processed_df, output_directory)
    # print(validated_df)

    validated_df.to_csv(test_result_file, index=False)
    
if __name__ == "__main__":
    main()