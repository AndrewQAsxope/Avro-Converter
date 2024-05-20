from avro_to_csv import avro_to_csv

# Configuration
input_avro_file = 'icdcm_codes_20230915033913.avro-00000-of-00001.avro'
output_csv_prefix = 'icdcm_codes'
filtered_value = None  # Put value for filter, or set None without '' for no filtering
max_rows_per_file = None  # Adjust the number of rows per CSV file

# Execute conversion
avro_to_csv(input_avro_file, output_csv_prefix, filtered_value, max_rows_per_file)
