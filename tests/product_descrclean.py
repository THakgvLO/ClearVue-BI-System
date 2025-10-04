import pandas as pd
import os

# Load Excel with the correct path
file_path = r"C:\Users\ibrah\Music\clearvue-bi-system\raw_data\Product Categories.xlsx"

# Check if file exists
if not os.path.exists(file_path):
    print(f"Error: File not found at {file_path}")
    print("Please check the file path and try again.")
else:
    print(f"File found: {file_path}")
    df = pd.read_excel(file_path)

    # Create a function to clean and categorize the product descriptions
    def clean_product_description(desc):
        desc = str(desc).strip()
        
        # Map to standardized categories
        if desc in ['Parts', 'Products', 'Packaging', 'PROMOTIONAL GOODS', 'SAMPLES', 'VIP']:
            return desc
        elif desc == 'SALE':
            return 'Sale Items'
        elif desc == 'DISCONTINUED':
            return 'Discontinued'
        elif desc in ['legal costs', 'Admin fees postage / courier']:
            return 'Administrative'
        elif desc in ['Unknown', 'LAST OF RANGE']:
            return desc
        # Handle year patterns
        elif any(year in desc for year in ['2018', '2019', '2017', '2000', '2003']):
            if '+' in desc:
                return 'Future Year Models'
            elif '/' in desc:
                return 'Transition Year Models'
            else:
                return 'Specific Year Models'
        else:
            return 'Other'

    # Apply the cleaning
    df_cleaned = df.copy()
    df_cleaned['PRODCAT_DESC_CLEANED'] = df_cleaned['PRODCAT_DESC'].apply(clean_product_description)

    # Create cleaned_data folder if it doesn't exist
    cleaned_dir = r"C:\Users\ibrah\Music\clearvue-bi-system\cleaned_data"
    if not os.path.exists(cleaned_dir):
        os.makedirs(cleaned_dir)
        print(f"Created directory: {cleaned_dir}")

    # Create the output file path
    output_path = os.path.join(cleaned_dir, "Product_Categories_Cleaned.xlsx")

    # Save to new Excel file
    df_cleaned.to_excel(output_path, index=False)

    print("Cleaned data saved successfully!")
    print(f"File saved to: {output_path}")
    print(f"Original rows: {len(df)}")
    print(f"Cleaned rows: {len(df_cleaned)}")
    print("\nPreview of cleaned data:")
    print(df_cleaned[['PRODCAT_CODE', 'PRODCAT_DESC', 'PRODCAT_DESC_CLEANED']].head(10))

    print("\nCleaned categories summary:")
    print(df_cleaned['PRODCAT_DESC_CLEANED'].value_counts())