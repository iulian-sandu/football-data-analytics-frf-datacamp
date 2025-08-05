import pandas as pd # type: ignore
import json

def infer_bigquery_type(pandas_dtype, column_series):
    """
    Infers the appropriate BigQuery data type and mode from a pandas Series' dtype
    and its content.
    """
    # Default to STRING and NULLABLE, then refine
    bq_type = "STRING"
    bq_mode = "NULLABLE"

    # Check if the column has any non-null values to infer if it might be required
    # Note: For simple inference, we assume if *any* nulls are present in the sample,
    # it's NULLABLE. If no nulls are present, we still default to NULLABLE for safety
    # unless explicitly marked as REQUIRED by manual review.
    if column_series.isnull().any():
        bq_mode = "NULLABLE"
    else:
        # If no nulls in the sample, still consider it NULLABLE by default
        # as a robust practice, unless you specifically want to infer REQUIRED.
        # For this "simplest mode", we stick to NULLABLE for safety.
        bq_mode = "NULLABLE"

    if pd.api.types.is_integer_dtype(pandas_dtype):
        # BigQuery's INTEGER is INT64. Check for potential overflow for BIGNUMERIC.
        # Max/min values for signed 64-bit integer: 9,223,372,036,854,775,807 / -9,223,372,036,854,775,808
        if column_series.dtype == 'int64': # Pandas int64 usually maps to BigQuery INT64
            bq_type = "INT64"
        else: # Handle larger integers if pandas infers them differently (e.g., object for huge numbers)
            # This is a basic check. For very large numbers, BIGNUMERIC is safer.
            # For simplicity, we'll assume pandas handles int64 correctly for typical data.
            bq_type = "INT64"
    elif pd.api.types.is_float_dtype(pandas_dtype):
        bq_type = "FLOAT64" # BigQuery's FLOAT is FLOAT64
    elif pd.api.types.is_bool_dtype(pandas_dtype):
        bq_type = "BOOL"
    elif pd.api.types.is_datetime64_any_dtype(pandas_dtype):
        bq_type = "TIMESTAMP" # Or "DATE" if you know it's only dates, or "DATETIME"
    elif pd.api.types.is_string_dtype(pandas_dtype) or pandas_dtype == object:
        # Try to detect if a string column could be a date or timestamp
        # This is a heuristic and might not be perfect for all date formats.
        try:
            # Drop NaNs to avoid errors in to_datetime conversion
            temp_series = pd.to_datetime(column_series.dropna(), errors='coerce')
            if not temp_series.isnull().all(): # If at least some values successfully converted
                # Check if it has time components
                if temp_series.dt.time.nunique() > 1: # More than just 00:00:00, likely a timestamp
                    bq_type = "TIMESTAMP"
                else:
                    bq_type = "DATE" # Only date part present
            else:
                bq_type = "STRING"
        except Exception:
            bq_type = "STRING" # Fallback if datetime conversion fails

    return bq_type, bq_mode

def sanitize_bigquery_column_name(name):
    """
    Sanitizes a column name to be compatible with BigQuery's naming rules.
    """
    # Replace spaces and non-alphanumeric characters with underscores
    sanitized = ''.join(c if c.isalnum() or c == '_' else '_' for c in name)
    # Ensure it starts with a letter or underscore
    if not sanitized[0].isalpha() and not sanitized[0] == '_':
        sanitized = '_' + sanitized
    # Remove consecutive underscores
    sanitized = '_'.join(filter(None, sanitized.split('_')))
    # Trim to BigQuery's max length if necessary (128 characters)
    return sanitized[:128]

def infer_bigquery_schema_from_csv(csv_file_path):
    """
    Infers a BigQuery schema from a CSV file using pandas for robust type detection.

    Args:
        csv_file_path (str): Path to the input CSV file.

    Returns:
        list: A list of dictionaries representing the inferred BigQuery schema.
              This format is directly usable by BigQuery.
    """
    # Read the CSV using pandas.
    # low_memory=False helps pandas infer types more accurately by reading the whole file.
    # keep_default_na=True and na_values handle common null representations.
    try:
        df = pd.read_csv(csv_file_path, low_memory=False, keep_default_na=True, na_values=['', 'NA', 'N/A', 'NULL'])
    except FileNotFoundError:
        print(f"Error: CSV file not found at {csv_file_path}")
        return []
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return []

    bq_schema_fields = []
    
    for column_name, pandas_dtype in df.dtypes.items():
        # Get the actual Series for the column to check for nulls and content
        column_series = df[column_name]

        sanitized_bq_name = sanitize_bigquery_column_name(column_name)
        bq_type, bq_mode = infer_bigquery_type(pandas_dtype, column_series)

        bq_schema_fields.append({
            "name": sanitized_bq_name,
            "type": bq_type,
            "mode": bq_mode,
            "description": f"Inferred from CSV column '{column_name}'"
        })
    return bq_schema_fields

if __name__ == "__main__":
    csv_file = "Dinamo_Bucuresti_2024_2025_events.csv"
    output_schema_file = "bigquery_schema_inferred.json"

    inferred_bq_schema = infer_bigquery_schema_from_csv(csv_file)

    if inferred_bq_schema:
        print("--- Inferred BigQuery Schema ---")
        print(json.dumps(inferred_bq_schema, indent=2))

        # Save the inferred schema to a JSON file
        with open(output_schema_file, "w") as f:
            json.dump(inferred_bq_schema, f, indent=2)
        print(f"\nInferred BigQuery schema saved to {output_schema_file}")
    else:
        print("Schema inference failed or returned an empty schema.")