import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Replace this with your JSON URL
# url = 'https://usc1.contabostorage.com/sravz-data/historical/earnings/stk_us_nvda.json?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=aec6391c26dc4354b66d92fdf426ffeb%2F20241111%2Fcustom%2Fs3%2Faws4_request&X-Amz-Date=20241111T213115Z&X-Amz-Expires=300&X-Amz-Signature=c069a57b04206417d43d7b4802a4629f303fcf0b141b747456041bf0c86bb549&X-Amz-SignedHeaders=host'


def main(sravz_id, df_parquet_file_path: str, output_file_path: str) -> str:
    # Load JSON data from the URL into a Pandas DataFrame
    df = pd.read_parquet(df_parquet_file_path)

    # Display the first few rows of the DataFrame
    print(df.head())
    print(df.tail())
    print(df.columns)

    # Select only the relevant columns
    adjusted_close_column = f"{sravz_id}_AdjustedClose"
    log_adjusted_close_column = f"{sravz_id}_log_AdjustedClose"

    df[log_adjusted_close_column] = np.log(
        df[adjusted_close_column])
    columns_to_plot = [log_adjusted_close_column]
    df_plot = df[columns_to_plot]

    # Set up the figure and axis for plotting
    plt.figure(figsize=(14, 8))

    # Plot each column in the selected list
    for column in columns_to_plot:
        plt.plot(df['DateTime'], df_plot[column], label=column)

    # Add bubbles for each non-null 'percent' value
    non_null_percent = df['percent'].notnull()
    # Calculate absolute values for size and set color based on sign
    # Use absolute values for size
    bubble_sizes = df['percent'][non_null_percent].abs()
    # Red for negative, blue for positive
    bubble_colors = df['percent'][non_null_percent].apply(
        lambda x: 'red' if x < 0 else 'green')

    plt.scatter(
        # X-axis values where percent is non-null
        df['DateTime'][non_null_percent],
        # Y-axis values from the first column in columns_to_plot
        df[log_adjusted_close_column][non_null_percent],
        s=bubble_sizes,  # Absolute values for bubble size
        c=bubble_colors,  # Color based on positive/negative
        alpha=0.6,
        label='Earnings Surprise Percent'
    )

    # Customize plot
    plt.xlabel('DateTime')
    plt.ylabel(log_adjusted_close_column)
    plt.title('Log Adjusted Close Price Vs Earnings Surprise')
    plt.legend(loc='upper left')
    plt.xticks(rotation=45)

    # Show plot
    plt.tight_layout()
    plt.savefig(output_file_path, format='png', dpi=300)
    return output_file_path
    # plt.show()
