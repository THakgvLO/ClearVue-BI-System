import pandas as pd
import numpy as np

def comprehensive_data_cleaning(file_path):
    """
    Comprehensive data cleaning for Excel files
    """
    # Load Excel
    print("📥 Loading Excel file...")
    df = pd.read_excel(file_path)
    
    print("\n🔍 BEFORE CLEANING:")
    print("Shape:", df.shape)
    print("Columns:", df.columns.tolist())
    print("\nFirst 5 rows:")
    print(df.head())
    print("\n" + "="*50)
    
    # --- Comprehensive Cleaning Steps ---
    
    # 1. Standardize column names
    original_columns = df.columns.tolist()
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    print("✅ Column names standardized")
    
    # 2. Remove completely empty columns and rows
    initial_shape = df.shape
    df = df.dropna(axis=1, how='all')
    df = df.dropna(axis=0, how='all')
    print(f"✅ Removed empty columns/rows. Shape changed from {initial_shape} to {df.shape}")
    
    # 3. Remove duplicate rows
    duplicates_removed = df.duplicated().sum()
    df = df.drop_duplicates()
    print(f"✅ Removed {duplicates_removed} duplicate rows")
    
    # 4. Data type analysis and cleaning
    print("\n📊 DATA TYPES ANALYSIS:")
    for col in df.columns:
        dtype = df[col].dtype
        non_null = df[col].count()
        null_count = df[col].isnull().sum()
        unique_count = df[col].nunique()
        
        print(f"   {col}: {dtype} | Non-null: {non_null} | Null: {null_count} | Unique: {unique_count}")
        
        # Show sample values for object columns
        if df[col].dtype == 'object' and non_null > 0:
            sample_values = df[col].dropna().unique()[:3]
            print(f"     Sample: {list(sample_values)}")
    
    # 5. Handle missing values strategically
    print("\n🔄 HANDLING MISSING VALUES:")
    
    for col in df.columns:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            dtype = df[col].dtype
            
            if dtype == 'object':
                df[col] = df[col].fillna("Unknown")
                print(f"   {col}: Filled {null_count} missing values with 'Unknown'")
            elif 'int' in str(dtype) or 'float' in str(dtype):
                df[col] = df[col].fillna(0)
                print(f"   {col}: Filled {null_count} missing values with 0")
            else:
                df[col] = df[col].fillna("Unknown")
                print(f"   {col}: Filled {null_count} missing values with 'Unknown'")
    
    # 6. Clean text data (strip whitespace, handle special cases)
    print("\n✨ CLEANING TEXT DATA:")
    text_columns = df.select_dtypes(include='object').columns
    for col in text_columns:
        # Strip whitespace
        df[col] = df[col].astype(str).str.strip()
        
        # Replace empty strings with "Unknown"
        empty_count = (df[col] == '').sum()
        if empty_count > 0:
            df[col] = df[col].replace('', 'Unknown')
            print(f"   {col}: Replaced {empty_count} empty strings with 'Unknown'")
    
    # 7. Validate numeric columns
    print("\n🔢 VALIDATING NUMERIC COLUMNS:")
    numeric_columns = df.select_dtypes(include=[np.number]).columns
    for col in numeric_columns:
        min_val = df[col].min()
        max_val = df[col].max()
        mean_val = df[col].mean()
        print(f"   {col}: Min={min_val:.2f}, Max={max_val:.2f}, Mean={mean_val:.2f}")
    
    # 8. Check for potential data quality issues
    print("\n🚨 DATA QUALITY CHECKS:")
    
    # Check for inconsistent values in key columns
    potential_key_columns = [col for col in df.columns if 'code' in col or 'id' in col]
    for col in potential_key_columns:
        if df[col].dtype == 'object':
            # Check for mixed formats
            unique_samples = df[col].dropna().unique()[:5]
            print(f"   {col} sample values: {list(unique_samples)}")
    
    # 9. Final data summary
    print("\n📈 FINAL DATA SUMMARY:")
    print(f"Final shape: {df.shape}")
    print(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024 ** 2:.2f} MB")
    
    print("\nFirst 5 rows of cleaned data:")
    print(df.head())
    
    return df

def save_cleaned_data(df, original_path):
    """Save the cleaned data with a modified filename"""
    # Create output path by adding '_cleaned' before extension
    if original_path.endswith('.xlsx'):
        output_path = original_path.replace('.xlsx', '_cleaned.xlsx')
    else:
        output_path = original_path + '_cleaned.xlsx'
    
    # Save to Excel
    df.to_excel(output_path, index=False)
    print(f"\n💾 CLEANED DATA SAVED TO: {output_path}")
    
    # Also save as CSV for easier analysis
    csv_path = output_path.replace('.xlsx', '.csv')
    df.to_csv(csv_path, index=False)
    print(f"📄 Also saved as CSV: {csv_path}")
    
    return output_path, csv_path

# Main execution
if __name__ == "__main__":
    # Your file path
    file_path = r"C:\Users\ibrah\Music\clearvue-bi-system\raw_data\Products.xlsx"
    
    try:
        # Perform comprehensive cleaning
        cleaned_df = comprehensive_data_cleaning(file_path)
        
        # Save the cleaned data
        excel_path, csv_path = save_cleaned_data(cleaned_df, file_path)
        
        print("\n🎉 CLEANING PROCESS COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print(f"Original file: {file_path}")
        print(f"Cleaned Excel: {excel_path}")
        print(f"Cleaned CSV: {csv_path}")
        print(f"Final data shape: {cleaned_df.shape}")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        print("Please check the file path and ensure the Excel file is not open in another program.")