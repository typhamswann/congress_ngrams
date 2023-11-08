import pandas as pd
from collections import Counter
import string
from itertools import chain

# Function to clean and tokenize speech into words
def tokenize(text):
    # Remove punctuation and convert to lower case
    text = text.translate(str.maketrans('', '', string.punctuation)).lower()
    # Split the text into words
    return text.split()

# Load the CSV file in chunks
file_path = '<congress data>'  # Replace with your input CSV file path

chunk_size = 250000
# Create an empty DataFrame to store the combined results
combined_unigram_time_series = pd.DataFrame()

n=0
for chunk in pd.read_csv(file_path, chunksize=chunk_size):
    # Filter out rows with invalid dates assuming a valid date format is "YYYY-MM-DD"
    chunk = chunk[chunk['date'].str.match(r'^\d{4}-\d{2}-\d{2}$', na=False)]
    
    # Tokenize each speech and count the word frequencies
    chunk['unigrams'] = chunk['speech'].map(tokenize)

    # Flatten the list of unigrams and create a new DataFrame with date and word columns
    unigram_df = pd.DataFrame({'date': chunk['date'].repeat(chunk['unigrams'].str.len()),
                               'word': list(chain.from_iterable(chunk['unigrams']))})

    # Count the occurrences of each word for each date
    unigram_time_series = unigram_df.groupby(['date', 'word']).size().unstack(fill_value=0)
    
    # Combine the results with the previous chunks
    combined_unigram_time_series = combined_unigram_time_series.add(unigram_time_series, fill_value=0)
    print(n)
    n = n + 1

# Save the pivoted data to a CSV file
output_file_path = '<output path>'  # Replace with your desired output CSV file path
combined_unigram_time_series.to_csv(output_file_path, index=True)

print(f"Unigram time series data saved to {output_file_path}")