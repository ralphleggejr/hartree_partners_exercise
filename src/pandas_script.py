
""" 
Pandas Data Processing Script

This script processes two datasets by joining them on a common column, performing group-wise calculations, and
transforming the data to obtain summarized information based on specific business logic. It is designed to be run with
command-line arguments that specify the paths to the datasets.
"""
import pandas as pd
import numpy as np
import argparse

def main(args):
    """
    Main function to execute the script logic.
    
    Parameters:
    args (argparse.Namespace): Command-line arguments containing paths to datasets.
    
    The function performs the following steps:
    - Loads the datasets into pandas DataFrames.
    - Joins the datasets on the 'counter_party' column.
    - Performs group-wise calculations and adds summary columns to the joined DataFrame.
    """


    ## Load datasets
    df1 = pd.read_csv(args.dataset1)  # Load the first dataset from the specified CSV file
    df2 = pd.read_csv(args.dataset2)  # Load the second dataset from the specified CSV file


    ## Join datasets on the 'counter_party' column
    # An inner join is used to combine rows from both DataFrames based on common 'counter_party' values
    df_joined = pd.merge(df1, df2, on='counter_party', how='inner')


    ## Perform calculations
    # Calculate the maximum rating by counterparty and add it as a new column
    df_joined['max_rating_by_counterparty'] = df_joined.groupby('counter_party')['rating'].transform('max') 
    # Calculate the sum of 'value' for 'ARAP' status grouped by 'legal_entity'
    df_joined['sum_value_ARAP'] = df_joined[df_joined['status'] == 'ARAP'].groupby('legal_entity')['value'].transform('sum')
    # Calculate the sum of 'value' for 'ACCR' status grouped by 'legal_entity'
    df_joined['sum_value_ACCR'] = df_joined[df_joined['status'] == 'ACCR'].groupby('legal_entity')['value'].transform('sum')
    # # Why use both legal_entity and status?
    """
        1) Entity-specific Accounting: Each legal entity may have its own set of books and records. For a corporation with multiple subsidiaries (each being a separate legal entity), it's necessary to know how much each entity is owed (receivables) or owes (payables).
        2) Regulatory Compliance: Legal entities often have to report their financials separately for tax and regulatory reasons. Knowing the ARAP for each entity is crucial for accurate financial statements.
        3) Financial Analysis: For managing the cash flow and financial health of a company, finance departments analyze the ARAP balances by legal entity. This helps in understanding which entities are contributing to cash inflows and which are adding to liabilities.
        4) Risk Management: By tracking ARAP by legal entity, a company can manage risk at the entity level, which is important for internal controls over financial reporting.
        5) Intercompany Transactions: If the legal entities represent different segments or subsidiaries within a larger corporate structure, tracking ARAP by entity is necessary to manage intercompany transactions, which could include internal loans, services rendered, or transfer pricing.
    """


    ## Create Totals
    # Calculate sum of values where status is 'ARAP' and 'ACCR' across all counter_parties for each legal_entity
    df_joined['value_ARAP'] = df_joined['value'].where(df_joined['status'] == 'ARAP', 0)
    df_joined['value_ACCR'] = df_joined['value'].where(df_joined['status'] == 'ACCR', 0)

    # Create a total row for each legal_entity
    totals_legal_entity = df_joined.groupby(['legal_entity']).agg({
        'rating': 'max',
        'value_ARAP': 'sum',
        'value_ACCR': 'sum'
    }).reset_index()
    totals_legal_entity['counter_party'] = 'Total'
    totals_legal_entity['tier'] = 'Total'
    totals_legal_entity.rename(columns={'rating': 'max_rating_by_counterparty'}, inplace=True)
    totals_legal_entity.rename(columns={'value_ARAP': 'sum_value_ARAP'}, inplace=True)
    totals_legal_entity.rename(columns={'value_ACCR': 'sum_value_ACCR'}, inplace=True)
    totals_legal_entity = totals_legal_entity[['legal_entity', 'counter_party', 'tier', 'max_rating_by_counterparty', 'sum_value_ARAP', 'sum_value_ACCR']]
    
    # Create a total row for each legal_entity w/ counter_party
    totals = df_joined.groupby(['legal_entity', 'counter_party']).agg({
        'tier': 'first',  # Assuming tier is the same for all rows of the same group
        'max_rating_by_counterparty': 'max',
        'value_ARAP': 'sum',
        'value_ACCR': 'sum'
    }).reset_index()
    totals.rename(columns={'value_ARAP': 'sum_value_ARAP'}, inplace=True)
    totals.rename(columns={'value_ACCR': 'sum_value_ACCR'}, inplace=True)
    totals = totals[['legal_entity', 'counter_party', 'tier', 'max_rating_by_counterparty', 'sum_value_ARAP', 'sum_value_ACCR']]
    
    # Create a total row for each counter_party
    totals_counter_party = df_joined.groupby(['counter_party']).agg({
        'max_rating_by_counterparty': 'max',
        'value_ARAP': 'sum',
        'value_ACCR': 'sum'
    }).reset_index()
    totals_counter_party['legal_entity'] = 'Total'
    totals_counter_party['tier'] = 'Total'
    totals_counter_party.rename(columns={'value_ARAP': 'sum_value_ARAP'}, inplace=True)
    totals_counter_party.rename(columns={'value_ACCR': 'sum_value_ACCR'}, inplace=True)
    totals_counter_party = totals_counter_party[['legal_entity', 'counter_party', 'tier', 'max_rating_by_counterparty', 'sum_value_ARAP', 'sum_value_ACCR']]
    
    # Create a total row for each tier
    totals_tier = df_joined.groupby(['tier']).agg({
        'max_rating_by_counterparty': 'max',
        'value_ARAP': 'sum',
        'value_ACCR': 'sum'
    }).reset_index()
    totals_tier['legal_entity'] = 'Total'
    totals_tier['counter_party'] = 'Total'
    totals_tier.rename(columns={'value_ARAP': 'sum_value_ARAP'}, inplace=True)
    totals_tier.rename(columns={'value_ACCR': 'sum_value_ACCR'}, inplace=True)
    totals_tier = totals_tier[['legal_entity', 'counter_party', 'tier', 'max_rating_by_counterparty', 'sum_value_ARAP', 'sum_value_ACCR']]


    ## Finalize and save output to CSV
    # Clean up df_joined for joining
    df_joined = df_joined[['legal_entity', 'counter_party', 'tier', 'max_rating_by_counterparty', 'sum_value_ARAP', 'sum_value_ACCR']]
    # Concatenate the original data with totals
    final_df = pd.concat([df_joined, totals_legal_entity, totals, totals_counter_party, totals_tier], ignore_index=True)
    # Save to CSV
    final_df.to_csv(args.output, index=False)


# Runs the above functions when the script is called
if __name__ == "__main__":
    ## Parse the arguments and send to main
    parser = argparse.ArgumentParser(description='Join two datasets and perform aggregations.')
    parser.add_argument('--dataset1', required=True, help='Path to the first dataset')
    parser.add_argument('--dataset2', required=True, help='Path to the second dataset')
    parser.add_argument('--output', required=True, help='Path to the output CSV file')
    # Set up the argument parser and add arguments for dataset paths
    args = parser.parse_args()
    # Call the main function with parsed arguments
    main(args)

#EOL