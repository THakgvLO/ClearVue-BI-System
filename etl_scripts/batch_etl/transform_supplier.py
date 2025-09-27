import logging
try:
    import pandas as pd
except ImportError:
    logging.error("pandas module not found. Please install it using 'pip install pandas'")
    raise
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define file paths relative to etl_scripts/batch_etl/
DATA_DIR = "../raw_data/"

# Helper function to convert Excel serial date to YYYY-MM-DD
def excel_to_date(serial):
    try:
        base_date = datetime(1899, 12, 30)  # Excel 1900 date system
        return (base_date + timedelta(days=serial)).strftime("%Y-%m-%d")
    except Exception as e:
        logging.error(f"Error converting date {serial}: {e}")
        return None

# Helper function to clean supplier descriptions
def clean_supplier_desc(desc):
    try:
        if desc.startswith("DR"):
            shipment = desc.replace("DR ", "").replace("purch order ", "").strip()
            return {"name": "DR Supplier", "shipmentDetails": [shipment] if shipment else []}
        return {"name": desc, "shipmentDetails": []}
    except Exception as e:
        logging.error(f"Error cleaning supplier desc {desc}: {e}")
        return {"name": desc, "shipmentDetails": []}

# Step 1: Extract - Load Excel files from raw_data/
try:
    suppliers_df = pd.read_excel(f"{DATA_DIR}Suppliers.xlsx")
    headers_df = pd.read_excel(f"{DATA_DIR}Purchases Headers.xlsx")
    lines_df = pd.read_excel(f"{DATA_DIR}Purchases Lines.xlsx")
    logging.info("Successfully loaded Excel files")
except FileNotFoundError as e:
    logging.error(f"File not found: {e}")
    raise
except Exception as e:
    logging.error(f"Error loading Excel files: {e}")
    raise

# Step 2: Transform - Clean Suppliers
suppliers_df = suppliers_df[suppliers_df['SUPPLIER_CODE'] != "999999"]
suppliers_df['cleaned_supplier'] = suppliers_df['SUPPLIER_DESC'].apply(clean_supplier_desc)
suppliers_df['EXCLSV'] = suppliers_df['EXCLSV'].map({'Y': True, 'N': False})

# Create supplier lookup dictionary
supplier_lookup = {
    row['SUPPLIER_CODE']: {
        'supplierID': row['SUPPLIER_CODE'],
        'name': row['cleaned_supplier']['name'],
        'excludesVAT': row['EXCLSV'],
        'paymentTerms': row['NORMAL_PAYTERMS'],
        'creditLimit': row['CREDIT_LIMIT'],
        'shipmentDetails': row['cleaned_supplier']['shipmentDetails']
    } for _, row in suppliers_df.iterrows()
}

# Step 3: Transform - Clean Purchases Headers

# Convert the column to datetime objects, then format to string YYYY-MM-DD
headers_df['purchaseDate'] = pd.to_datetime( headers_df['PURCH_DATE'], errors='coerce').dt.strftime('%Y-%m-%d')

# FINANCIAL_PERIOD should be derived from the formatted string
# Note: Use .str to access string methods on the Pandas Series
headers_df['financialPeriod'] = headers_df['purchaseDate'].str.replace("-", "").str[:6]

# Validate SUPPLIER_CODE... (rest of step 3)

# Validate SUPPLIER_CODE
invalid_suppliers = headers_df[~headers_df['SUPPLIER_CODE'].isin(supplier_lookup.keys())]
if not invalid_suppliers.empty:
    logging.warning(f"Invalid supplier codes found: {invalid_suppliers['SUPPLIER_CODE'].tolist()}")

# Step 4: Transform - Clean Purchases Lines
lines_df['totalCost'] = lines_df['QUANTITY'] * lines_df['UNIT_COST_PRICE']
discrepancies = lines_df[abs(lines_df['totalCost'] - lines_df['TOTAL_LINE_COST']) > 0.01]
if not discrepancies.empty:
    logging.warning(f"Cost discrepancies in {discrepancies['PURCH_DOC_NO'].tolist()}")
    lines_df['TOTAL_LINE_COST'] = lines_df['totalCost']  # Correct discrepancies

#CLean up calculatedCost column
lines_df = lines_df.drop(columns=['calculatedCost'],errors='ignore')
# Rename fields for consistency
lines_df = lines_df.rename(columns={
    'INVENTORY_CODE': 'productID',
    'QUANTITY': 'quantity',
    'UNIT_COST_PRICE': 'unitCost',
    'TOTAL_LINE_COST': 'totalCost'
})

#Explicily drop the old ro prevent duplication warnings
lines_df = lines_df.drop(columns=['INVENTORY_CODE','QUANTITY','UNIT_COST_PRICE','TOTAL_LINE_COST'],errors='ignore')

# Step 5: Structure - Combine into MongoDB-compatible documents
purchases_documents = []
for _, header in headers_df.iterrows():
    doc_no = header['PURCH_DOC_NO']
    supplier_id = header['SUPPLIER_CODE']
    
    # Get line items for this PO
    line_items = lines_df[lines_df['PURCH_DOC_NO'] == doc_no][
        ['productID', 'quantity', 'unitCost', 'totalCost']
    ].to_dict('records')
    
    # Compute total purchase cost
    total_cost = sum(item['totalCost'] for item in line_items)
    
    # Build document
    document = {
        '_id': doc_no,
        'purchaseDate': header['purchaseDate'],
        'financialPeriod': header['financialPeriod'],
        'supplier': supplier_lookup.get(supplier_id, {
            'supplierID': supplier_id,
            'name': 'Unknown Supplier',
            'excludesVAT': False,
            'paymentTerms': 0,
            'creditLimit': 0,
            'shipmentDetails': []
        }),
        'lineItems': line_items,
        'totalPurchaseCost': round(total_cost, 2),
        'status': 'Open'
    }
    purchases_documents.append(document)

# Step 6: Output - Log results (for demo, not saving)
logging.info(f"Generated {len(purchases_documents)} documents")
for doc in purchases_documents[:1]:  # Show one example
    logging.info(doc)