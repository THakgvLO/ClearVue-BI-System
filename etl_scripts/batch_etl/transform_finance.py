import pandas as pd 

payment_header = pd.read_excel(r"raw_data/Payment Header.xlsx",sheet_name="Payment_Header")
payment_lines = pd.read_excel(r"raw_data/Payment Lines.xlsx",sheet_name="Payment_Lines")

# Clean Payment  file
payment_header = payment_header.drop_duplicates()

#Clean Payment Lines
payment_lines["DEPOSIT_DATE"] = pd.to_datetime(payment_lines["DEPOSIT_DATE"], errors="coerce")

# Remove duplicates
payment_lines = payment_lines.drop_duplicates()

# Check for missing values
missing_summary = payment_lines.isnull().sum()

# remove rows with missing deposit reference or customer number
payment_lines = payment_lines.dropna(subset=["CUSTOMER_NUMBER", "DEPOSIT_REF"])



#Cleaning Age_Analysis
age_df = pd.read_excel("raw_data\Age Analysis.xlsx", sheet_name="Age_Analysis")

# Remove duplicates
age_df = age_df.drop_duplicates()

# Ensure numeric columns are actually numeric
amount_cols = [col for col in age_df.columns if col.startswith("AMT_") or col == "TOTAL_DUE"]
age_df[amount_cols] = age_df[amount_cols].apply(pd.to_numeric, errors="coerce")

# Fill missing values with 0 for amounts
age_df[amount_cols] = age_df[amount_cols].fillna(0)

# Check consistency of totals
age_df["BUCKET_SUM"] = age_df[amount_cols].drop("TOTAL_DUE", axis=1).sum(axis=1)
age_df["CONSISTENT_PAYMENTS"] = age_df["TOTAL_DUE"].round(2) == age_df["BUCKET_SUM"].round(2)




#Cleaning Customer Account Parameters
custAcc_df = pd.read_excel(r"raw_data/Customer Account Parameters.xlsx", sheet_name="Customer_Account_Parameters")

# Remove duplicates
custAcc_df = custAcc_df.drop_duplicates()

# Drop rows with missing values in key columns
custAcc_df = custAcc_df.dropna(subset=["CUSTOMER_NUMBER", "PARAMETER"])

# Standardize text 
custAcc_df["CUSTOMER_NUMBER"] = custAcc_df["CUSTOMER_NUMBER"].str.strip()
custAcc_df["PARAMETER"] = custAcc_df["PARAMETER"].str.strip().str.capitalize()


