import binascii
import csv
import fastavro


# Define a filter function that returns True if a record should be included
def filter_record(record, target_value):
    if target_value is None:
        return True  # No filtering if target_value is None
    for value in record.values():
        if isinstance(value, bytes):
            value = binascii.hexlify(value).decode('utf-8')
        if target_value == str(value):
            return True
    return False


# Function to convert Avro to CSV with optional filtering and optional file splitting
def avro_to_csv(input_avro_file, output_csv_prefix, target_value=None, max_rows_per_file=None):
    with open(input_avro_file, 'rb') as fo:
        reader = fastavro.reader(fo)
        writer_schema = reader.writer_schema

        # Variables to track the number of rows
        current_row_count = 0
        current_file_index = 0

        # Writing CSV files
        while True:
            # Creating a new CSV file
            csv_file_name = f'{output_csv_prefix}{current_file_index}.csv' if max_rows_per_file else f'{output_csv_prefix}.csv'
            with open(csv_file_name, 'w', newline='') as csv_file:
                csv_writer = csv.writer(csv_file)

                # Writing header
                header = [field['name'] for field in writer_schema['fields']]
                csv_writer.writerow(header)

                # Writing data
                for record in reader:
                    if filter_record(record, target_value):
                        row = [binascii.hexlify(record[field['name']]).decode('utf-8')
                               if isinstance(record[field['name']], bytes) else record[field['name']]
                               for field in writer_schema['fields']]
                        csv_writer.writerow(row)

                        # Updating the row count
                        current_row_count += 1

                        # Checking if the row limit is reached
                        if max_rows_per_file and current_row_count >= max_rows_per_file:
                            break

                # Checking if the end of the file is reached
                if not max_rows_per_file or current_row_count < max_rows_per_file:
                    break

                # Resetting the row count for a new file
                current_row_count = 0
                current_file_index += 1

            # If max_rows_per_file is None, break after one iteration
            if not max_rows_per_file:
                break
