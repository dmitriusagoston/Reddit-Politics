import csv
import os

def count_csv_rows(filename):
    with open(filename, 'r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        return sum(1 for row in reader)
    
def process_csv_files(folder_path):
    for file in os.listdir(folder_path):
        if file.endswith('.csv'):
            filename = os.path.join(folder_path, file)
            num_rows = count_csv_rows(filename)
            print(f'{file}: {num_rows} rows')

folder_path = 'data/'
process_csv_files(folder_path)