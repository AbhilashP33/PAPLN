import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import re

# --- 1. Configuration and Utility Functions (Replicating SAS Macros/Setup) ---

# Configuration variables (Replicating SAS macro variables and libname paths)
# NOTE: The remote SAS/Unix logic (RSUBMIT, libname server=_server_) is converted
# to local processing using Pandas on the assumption that the Python script
# will run where it can access the specified Excel files and output directory.

# Paths (Update these paths to be Python-friendly and accessible on the execution machine)
LOG_DIR = r"\\maple\data\Toronto\wrkgrp33\RSD Files\DataIntegrity\Regulatory\COB\Bread\Data\Dashboard\Log"
INPUT_PATH = r"\\maple\data\Toronto\wrkgrp33\RSD Files\DataIntegrity\Regulatory\COB\Bread\Data\Dashboard\SF_DataPull"
OUTPUT_DIR = r"\\maple\data\Toronto\wrkgrp33\RSD Files\DataIntegrity\Regulatory\COB\Bread\Data\Dashboard"
# DEV_OUTPUT_DIR = r"R:\DataIntegrity\Regulatory\COB\Bread\Data\Dashboard\dev" # Using main path for simplicity

# Emulating SAS functions/variables
today_date = datetime.now().date()
datetime_str = datetime.now().strftime('%Y%m%d_%H%M%S')
label_d_str = today_date.strftime('%Y%m%d')

def sas_intnx(interval, start_date, n, alignment='B'):
    """Simplified Python equivalent of SAS's INTNX('interval', start_date, n, 'alignment') for 'month'."""
    from dateutil.relativedelta import relativedelta
    from calendar import monthrange

    if interval.lower() == 'month':
        new_date = start_date + relativedelta(months=n)
        if alignment.upper() == 'B':
            # Beginning of month
            return new_date.replace(day=1)
        elif alignment.upper() == 'E':
            # End of month
            _, last_day = monthrange(new_date.year, new_date.month)
            return new_date.replace(day=last_day)
        else:
            return new_date
    return start_date

# Replicating SAS data _null_ for date macro variables
y_d = today_date - timedelta(days=1)
t_d = today_date

# Calculate month boundaries
mth_beg1 = sas_intnx('month', t_d, 0, 'B')
mth_end1 = sas_intnx('month', t_d, 0, 'E')

# Format as strings for comparison and output file names
yes_d = y_d.strftime('%Y-%m-%d')
tod_d = t_d.strftime('%Y-%m-%d')
mth_end2 = mth_end1.strftime('%y%m%d') # SAS nonyy7 format is YYMMDD without separators

# Create output filename based on SAS logic
OUTPUT_FILENAME = f"Payplan_Dashboard_{mth_end2}.xlsx"
FINAL_OUTPUT_PATH = os.path.join(OUTPUT_DIR, OUTPUT_FILENAME)
# DEV_OUTPUT_PATH = os.path.join(DEV_OUTPUT_DIR, OUTPUT_FILENAME)

print(f">>>>>>> This program is starting running at {datetime.now().strftime('%d%b%Y:%H:%M:%S')} by {os.getlogin()} <<<<<<<<<<")

# SAS Log/Printto is replaced by standard Python print/logging,
# which can be redirected using standard OS mechanisms if needed.

# --- 2. Data Import (Replicating PROC IMPORT) ---
print("--- Importing Data ---")

# Define full paths for Excel files
loan_app_file = os.path.join(INPUT_PATH, "SF_DataPull", "SF_Data.xlsx")
loan_label_file = os.path.join(INPUT_PATH, "SF_DataPull", "dev", "SF_Data.xlsx")

try:
    # Import loan_application (sheet='loan application')
    loan_application = pd.read_excel(
        loan_app_file,
        sheet_name='loan application',
        engine='openpyxl'
    )
    loan_application.columns = loan_application.columns.str.replace(r'[^A-Za-z0-9_]', '', regex=True) # Sanitize column names

    # Import loan_label (sheet='loan')
    loan_label = pd.read_excel(
        loan_label_file,
        sheet_name='loan',
        engine='openpyxl'
    )
    loan_label.columns = loan_label.columns.str.replace(r'[^A-Za-z0-9_]', '', regex=True) # Sanitize column names

except FileNotFoundError as e:
    print(f"Error: One of the input files was not found: {e}")
    exit()

# --- 3. SQL Join and Variable Selection (Replicating PROC SQL) ---
print("--- Joining Data (PROC SQL equivalent) ---")

# Perform a Full Outer Join on 'ID' (from loan_application) and 'PTR_Loan_Application_Name__c' (from loan_label)
stage1_accuracy = pd.merge(
    loan_application,
    loan_label,
    left_on='ID',
    right_on='PTR_Loan_Application_Name__c',
    how='outer',
    suffixes=('_1', '_2') # Suffixes for overlapping column names, replicating SAS's automatic renaming
)

# Rename the specific overlapping columns as done in SAS to match the derivation logic
stage1_accuracy = stage1_accuracy.rename(columns={
    'FT_Date_of_Advance__c_1': 'FT_Date_of_Advance__c_1',
    'FT_Date_of_Advance__c_2': 'FT_Date_of_Advance__c_2',
    'FT_First_Payment_Date__c_1': 'FT_First_Payment_Date__c_1',
    'FT_First_Payment_Date__c_2': 'FT_First_Payment_Date__c_2',
})

# Create the join success indicator
stage1_accuracy['APP_LOAN_JOIN_SUCCESS_IND'] = np.select(
    [
        (stage1_accuracy['FT_Loan_Application_ID__c'].astype(str).str.strip() != '') & (stage1_accuracy['FT_Loan_ID__c'].astype(str).str.strip() == ''),
        (stage1_accuracy['FT_Loan_Application_ID__c'].astype(str).str.strip() == '') & (stage1_accuracy['FT_Loan_ID__c'].astype(str).str.strip() != '')
    ],
    [
        "APP ONLY",
        "LOAN ONLY"
    ],
    default="APP_LOAN_Joined"
)

# NOTE: In Python, we select all columns by default and then drop/keep later,
# but the SAS PROC SQL explicitly selects a large list. We'll proceed with all columns
# and clean up later if necessary, as the subsequent data step uses most of them.

# --- 4. Data Step Logic and Derivations (Replicating Data Step) ---
print("--- Applying Derivation Logic (Data Step equivalent) ---")

# Use a copy to perform the transformation
df = stage1_accuracy.copy()

# Convert relevant date columns to datetime objects (handling various SAS/SF date formats)
def safe_date_convert(series):
    # Attempt to convert various date string formats to datetime, including YYYY-MM-DD from 'input(..., anydtdte10.)'
    series = series.astype(str).str.strip()
    series = series.replace('', np.nan)
    return pd.to_datetime(series, errors='coerce')

# Convert Esignature timestamp to date (SAS: input(substr(FT_e_signature_date_and_time_sta,1,10),anydtdte10.))
df['esign_date_str'] = df['FT_e_signature_date_and_time_sta'].astype(str).str.slice(0, 10)
df['esign_date'] = safe_date_convert(df['esign_date_str']).dt.strftime('%Y-%m-%d')

# Convert SAS date columns to Python datetime objects for calculations
date_cols = ['FT_Application_Date__c', 'FT_Loan_Settled_Date__c', 'FT_Date_of_Advance__c_1', 'FT_Date_of_Advance__c_2', 'FT_First_Payment_Date__c_1', 'FT_First_Payment_Date__c_2']
for col in date_cols:
    # The columns coming from the join might be objects/strings or numbers,
    # and the subsequent SAS logic uses input(trim(col), anydtdte10.) to get a date.
    df[f'{col}_dt'] = safe_date_convert(df[col])


# --- LAO Derivations (Accepted LAO) ---
# This replicates the long series of 'if/else if' blocks in the SAS data step.
# It selects the data for the first 'Accepted' LAO (1 to 6).
lao_vars = ['Status', 'Amortization_Term_Perio', 'Interest_Rate__c', 'Loan_Type__c', 'Client_APR__c', 'Term__c']
for var in lao_vars:
    df[f'LAO_{var}'] = np.nan

for i in range(1, 7):
    # Condition: If the current LAO is 'Accepted' AND the previous LAOs were NOT accepted (or it's the first one)
    condition = (df[f'FT_LAO_{i}_Status__c'] == 'Accepted')

    # Ensure this is the FIRST accepted one by checking if the LAO_status is still NaN
    for var in lao_vars:
        col_name = f'LAO_{var}'
        df.loc[condition & df[f'LAO_{col_name}'].isna(), f'LAO_{col_name}'] = df[f'FT_LAO_{i}_{var}']

# --- Total number of Application Offerings ---
# This is a bit complex in SAS, but in Pandas, we can count the statuses that are 'Accepted' or 'Not Selected'.
lao_statuses = [f'FT_LAO_{i}_Status__c' for i in range(1, 7)]
status_check = ['Accepted', 'Not Selected']
df['LAO_total_number'] = df[lao_statuses].apply(
    lambda row: sum(row.isin(status_check)),
    axis=1
)

# --- Number of accepted Offerings ---
# Replicating the 'A' count logic.
df['combined_AO_status_string'] = df[lao_statuses].apply(
    lambda row: ' '.join(row.astype(str).str.slice(0, 1).fillna('').tolist()),
    axis=1
)
df['accepted_count'] = df['combined_AO_status_string'].str.count('A')

# --- Principal Amount Validation ---
df['FT_Total_loan_Value_Estimated__c'] = pd.to_numeric(df['FT_Total_loan_Value_Estimated__c'], errors='coerce')
df['FT_Original_Principal_Balance__c'] = pd.to_numeric(df['FT_Original_Principal_Balance__c'], errors='coerce')

# Stage 1
df['pop1_principal_amt'] = df['FT_Total_loan_Value_Estimated__c'].isna().astype(int)
df['Ai_principal_amount'] = np.where(
    (df['FT_Total_loan_Value_Estimated__c'] > 1200) | (df['FT_Total_loan_Value_Estimated__c'] <= 0),
    1,
    0
)
df['Ai_principal_amount_word'] = np.where(df['Ai_principal_amount'] == 1, 'Fail', 'Pass')
df['principal_amt_plot_1'] = np.where(df['Ai_principal_amount'] == 1, df['FT_Total_loan_Value_Estimated__c'].astype(str), '<= $1200')

# Stage 2
df['pop2_principal_amt'] = df['FT_Original_Principal_Balance__c'].isna().astype(int)
df['Ai_principal_amount_2'] = np.where(
    (df['FT_Original_Principal_Balance__c'] > 1200) | (df['FT_Original_Principal_Balance__c'] <= 0),
    1,
    0
)
df['Ai_principal_amount_word_2'] = np.where(df['Ai_principal_amount_2'] == 1, 'Fail', 'Pass')
df['principal_amt_plot_2'] = np.where(df['Ai_principal_amount_2'] == 1, df['FT_Original_Principal_Balance__c'].astype(str), '<= $1200')


# --- Payment Amount Replication (R_payment_amt) ---
# Convert columns for numerical calculation
num_cols = ['FT_LAO_1_Amortization_Term_Perio', 'FT_LAO_1_Interest_Rate__c', 'FT_Total_loan_Value_Estimated__c',
            'FT_Origination_Payment_Amount_PI', 'FT_Original_Principal_Balance__c', 'FT_Original_Amortization__c',
            'FT_Settled_Payment_Amount_PI__c']

for col in num_cols:
    df[col] = pd.to_numeric(df[col], errors='coerce')

# Use LAO derived variables
df['LAO_Amort'] = pd.to_numeric(df['LAO_Amort'], errors='coerce')
df['LAO_Interest'] = pd.to_numeric(df['LAO_Interest'], errors='coerce')

# Define calculation function
def calculate_r_payment(row, stage):
    loan_type = row['LAO_Loan_Type__c']
    
    if stage == 1:
        principal = row['FT_Total_loan_Value_Estimated__c']
        amort = row['LAO_Amort']
        interest = row['LAO_Interest']
        orig_payment = row['FT_Origination_Payment_Amount_PI']
    elif stage == 2:
        principal = row['FT_Original_Principal_Balance__c']
        amort = row['FT_Original_Amortization__c']
        interest = row['LAO_Interest'] # Assuming LAO_Interest is used for lack of 'Activation COB Interest rate' as per comment
        orig_payment = row['FT_Settled_Payment_Amount_PI__c']
    else:
        return np.nan, np.nan, np.nan, np.nan, np.nan

    # Initialize results
    R_payment_amt = np.nan
    Ri_payment_amt = 1
    Ri_payment_amt_word = 'Fail'
    pop_payment_amt = 0

    if pd.isna(orig_payment):
        pop_payment_amt = 1

    if loan_type == '0% APR':
        if pd.notna(principal) and pd.notna(amort) and amort != 0:
            R_payment_amt = principal / amort
        
    elif loan_type == 'Interest Bearing':
        if pd.notna(interest) and pd.notna(principal) and pd.notna(amort) and interest != 0:
            # Monthly Interest Rate (Assuming LAO_Interest is the periodic rate, typically monthly, though the formula suggests a total rate if LAO_Amort is months)
            # The SAS code appears to use the annual rate as the periodic rate, which is mathematically incorrect for a standard annuity formula, but we must replicate the SAS logic.
            # Assuming LAO_Interest is the Periodic Interest Rate ($i$) and LAO_Amort is the number of periods ($n$).
            try:
                R_payment_amt = (interest * principal) / (1 - (1 + interest)**(-amort))
            except OverflowError:
                R_payment_amt = np.nan

    # Check for pass/fail (Tolerance is 0.01)
    if pd.notna(orig_payment) and pd.notna(R_payment_amt):
        diff = np.abs(orig_payment - R_payment_amt)
        if diff < 0.01:
            Ri_payment_amt = 0
            Ri_payment_amt_word = 'Pass'

    return R_payment_amt, np.round(np.abs(R_payment_amt - orig_payment), 3), Ri_payment_amt, Ri_payment_amt_word, pop_payment_amt

# Apply the function for Stage 1 (Origination)
results1 = df.apply(lambda row: calculate_r_payment(row, 1), axis=1, result_type='expand')
df['R_payment_amt'] = results1[0]
df['R_payment_difference'] = results1[1]
df['Ri_payment_amt'] = results1[2]
df['Ri_payment_amt_word'] = results1[3]
df['pop1_payment_amt'] = results1[4]

# Apply the function for Stage 2 (Settled)
results2 = df.apply(lambda row: calculate_r_payment(row, 2), axis=1, result_type='expand')
df['R_payment_amt_2'] = results2[0]
df['R_payment_difference_2'] = results2[1]
df['Ri_payment_amt_2'] = results2[2]
df['Ri_payment_amt_word_2'] = results2[3]
df['pop2_payment_amt'] = results2[4]

# If LAO_type is neither, force Fail (replicated from SAS 'else if LAO_type not in ...')
neither_condition = ~df['LAO_Loan_Type__c'].isin(['0% APR', 'Interest Bearing'])
df.loc[neither_condition, 'Ri_payment_amt'] = 1
df.loc[neither_condition, 'Ri_payment_amt_word'] = 'Fail'
df.loc[neither_condition, 'Ri_payment_amt_2'] = 1
df.loc[neither_condition, 'Ri_payment_amt_word_2'] = 'Fail'


# --- Total Payment at End of Term (TotalPay_EOT) ---
num_cols_eot = ['FT_Origination_Cost_Of_Borrowing', 'FT_Origination_Payment_Amount_Te',
                'FT_Settled_Cost_of_Borrowing__c', 'FT_Settled_Payment_Amount_Term__c']
for col in num_cols_eot:
    df[col] = pd.to_numeric(df[col], errors='coerce')

# Stage 1
df['pop1_payment_EOT'] = df['FT_Origination_Payment_Amount_Te'].isna().astype(int)
df['R_TotalPay_EOT'] = df['FT_Total_loan_Value_Estimated__c'] + df['FT_Origination_Cost_Of_Borrowing']
df['R_TotalPay_EOT_difference'] = np.round(np.abs(df['R_TotalPay_EOT'] - df['FT_Origination_Payment_Amount_Te']), 3)

EOT1_pass_cond = (
    (np.isclose(df['FT_Origination_Payment_Amount_Te'], df['R_TotalPay_EOT'], atol=0.01)) &
    (df['FT_Origination_Payment_Amount_Te'].notna()) &
    (df['R_TotalPay_EOT'].notna())
)
df['RI_TotalPay_EOT'] = np.where(EOT1_pass_cond, 0, 1)
df['RI_TotalPay_EOT_word'] = np.where(EOT1_pass_cond, 'Pass', 'Fail')

# Stage 2
df['pop2_payment_EOT'] = df['FT_Settled_Payment_Amount_Term__c'].isna().astype(int)
df['R_TotalPay_EOT_2'] = df['FT_Original_Principal_Balance__c'] + df['FT_Settled_Cost_of_Borrowing__c']
df['R_TotalPay_EOT_difference_2'] = np.round(np.abs(df['R_TotalPay_EOT_2'] - df['FT_Settled_Payment_Amount_Term__c']), 3)

EOT2_pass_cond = (
    (np.isclose(df['FT_Settled_Payment_Amount_Term__c'], df['R_TotalPay_EOT_2'], atol=0.01)) &
    (df['FT_Settled_Payment_Amount_Term__c'].notna()) &
    (df['R_TotalPay_EOT_2'].notna())
)
df['RI_TotalPay_EOT_2'] = np.where(EOT2_pass_cond, 0, 1)
df['RI_TotalPay_EOT_word_2'] = np.where(EOT2_pass_cond, 'Pass', 'Fail')


# --- Amortization, Term, APR, Interest, IOT, COB (Fixed Value Checks) ---
# A helper function for the series of 'if/else if' blocks checking for missing values (pop) and expected value (Ai)
def apply_fixed_value_check(df, col_prefix, stage_num, expected_value, is_numeric=True):
    col = df[f'{col_prefix}']
    pop_col = f'pop{stage_num}_{col_prefix.split("_")[-1].lower()}'
    ai_col = f'Ai_{col_prefix.split("_")[-1].lower()}_{stage_num}' if stage_num > 0 else f'Ai_{col_prefix.split("_")[-1].lower()}'

    if is_numeric:
        df[col] = pd.to_numeric(col, errors='coerce')
        df[pop_col] = df[col].isna().astype(int)
        df[ai_col] = np.where(df[col] == expected_value, 0, 1)
    else:
        # For string checks (e.g., Prepayment Charges)
        df[pop_col] = df[col].astype(str).str.strip().isin(['', 'nan']).astype(int)
        df[ai_col] = np.where(df[col].astype(str).str.strip() == expected_value, 0, 1)
    return df

# Stage 1 (LAO_)
df = apply_fixed_value_check(df, 'LAO_Amort', 1, 24)
df = apply_fixed_value_check(df, 'LAO_term', 1, 24)
df = apply_fixed_value_check(df, 'LAO_client_APR', 1, 0)
df = apply_fixed_value_check(df, 'LAO_Interest', 1, 0)
df = apply_fixed_value_check(df, 'FT_Origination_Interest_Over_Ter', 1, 0)
df = apply_fixed_value_check(df, 'FT_Origination_Cost_Of_Borrowing', 1, 0)

# Stage 2 (FT_Original_, FT_Settled_)
df = apply_fixed_value_check(df, 'FT_Original_Amortization__c', 2, 24)
df = apply_fixed_value_check(df, 'FT_Original_Term__c', 2, 24)
df = apply_fixed_value_check(df, 'FT_Settled_APR__c', 2, 0)
df = apply_fixed_value_check(df, 'FT_Settled_Interest_Rate__c', 2, 0)
df = apply_fixed_value_check(df, 'FT_Settled_Interest_Over_Term__c', 2, 0)
df = apply_fixed_value_check(df, 'FT_Settled_Cost_of_Borrowing__c', 2, 0)

# --- Date of Advance (DOA) Validation ---
# Stage 1: DOA = Application date + 3 days
df['pop1_DOA'] = df['FT_Date_of_Advance__c_1_dt'].isna().astype(int)

# Condition: DOA_1 is App_Date + 3 AND neither is missing
condition1_doa = (
    df['FT_Date_of_Advance__c_1_dt'].notna() &
    df['FT_Application_Date__c_dt'].notna() &
    (df['FT_Date_of_Advance__c_1_dt'].dt.date == (df['FT_Application_Date__c_dt'] + timedelta(days=3)).dt.date)
)
df['Ai_DOA_1'] = np.where(
    condition1_doa,
    0,
    np.where(
        df['FT_Date_of_Advance__c_1_dt'].notna() & df['FT_Application_Date__c_dt'].notna(),
        1,
        0 # SAS logic: Ai_DOA_1 = 0 if either is missing, despite pop1_DOA = 1
    )
)

# Stage 2: DOA = Loan Settled Date
df['pop2_DOA'] = df['FT_Date_of_Advance__c_2_dt'].isna().astype(int)

# Condition: DOA_2 is Settled_Date AND neither is missing
condition2_doa = (
    df['FT_Date_of_Advance__c_2_dt'].notna() &
    df['FT_Loan_Settled_Date__c_dt'].notna() &
    (df['FT_Date_of_Advance__c_2_dt'].dt.date == df['FT_Loan_Settled_Date__c_dt'].dt.date)
)
df['Ai_DOA_2'] = np.where(
    condition2_doa,
    0,
    np.where(
        df['FT_Date_of_Advance__c_2_dt'].notna() & df['FT_Loan_Settled_Date__c_dt'].notna(),
        1,
        0 # SAS logic: Ai_DOA_2 = 0 if either is missing
    )
)


# --- First Payment Date (FPD) Validation ---
def calculate_next_payment_date(doa_date_series):
    # Calculate 1 month later (sameday alignment)
    next_pmt_temp = doa_date_series.apply(lambda d: sas_intnx('month', d.date(), 1, 'sameday') if pd.notna(d) else np.nan)
    
    # Calculate difference in days (for the < 30 days check)
    diff_days = (next_pmt_temp.apply(lambda d: pd.to_datetime(d) if pd.notna(d) else np.nan) - doa_date_series).dt.days

    # If difference < 30, use DOA + 30 days
    is_less_than_30 = (diff_days < 30)
    
    final_pmt = next_pmt_temp.copy()
    final_pmt.loc[is_less_than_30 & pd.notna(doa_date_series)] = (doa_date_series.loc[is_less_than_30 & pd.notna(doa_date_series)] + timedelta(days=30)).dt.date
    
    return final_pmt.apply(lambda d: pd.to_datetime(d) if pd.notna(d) else np.nan)


# Stage 1
df['stage_1_nextpmt_cal'] = calculate_next_payment_date(df['FT_Date_of_Advance__c_1_dt'])
df['pop1_first_pmt_date'] = df['FT_First_Payment_Date__c_1_dt'].isna().astype(int)

condition1_fpd = (
    df['FT_First_Payment_Date__c_1_dt'].notna() &
    df['FT_Date_of_Advance__c_1_dt'].notna() &
    df['stage_1_nextpmt_cal'].notna() &
    (df['FT_First_Payment_Date__c_1_dt'].dt.date == df['stage_1_nextpmt_cal'].dt.date)
)
df['Ai_first_pmt_date_1'] = np.where(
    condition1_fpd,
    0,
    np.where(
        df['FT_First_Payment_Date__c_1_dt'].notna() & df['FT_Date_of_Advance__c_1_dt'].notna(),
        1,
        0
    )
)

# Stage 2
df['stage_2_nextpmt_cal'] = calculate_next_payment_date(df['FT_Date_of_Advance__c_2_dt'])
df['pop2_first_pmt_date'] = df['FT_First_Payment_Date__c_2_dt'].isna().astype(int)

condition2_fpd = (
    df['FT_First_Payment_Date__c_2_dt'].notna() &
    df['FT_Date_of_Advance__c_2_dt'].notna() &
    df['stage_2_nextpmt_cal'].notna() &
    (df['FT_First_Payment_Date__c_2_dt'].dt.date == df['stage_2_nextpmt_cal'].dt.date)
)
df['Ai_first_pmt_date_2'] = np.where(
    condition2_fpd,
    0,
    np.where(
        df['FT_First_Payment_Date__c_2_dt'].notna() & df['FT_Date_of_Advance__c_2_dt'].notna(),
        1,
        0
    )
)

# --- Other Fee, Prepayment Privilege, Prepayment Charges, Default Insurance (String Checks) ---
df = apply_fixed_value_check(df, 'PTR_Other_Fees__c', 1, 0)
df = apply_fixed_value_check(df, 'PTR_Prepayment_Privileges__c', 1, 'Open', is_numeric=False)
df = apply_fixed_value_check(df, 'PTR_Prepayment_Charges__c', 1, 'N/A', is_numeric=False)
df = apply_fixed_value_check(df, 'PTR_Default_Insurance__c', 1, 'N/A', is_numeric=False)


# --- Summing Errors and Blanks ---

# List of error/blank columns for summing (Stage 1 & 2)
initial_error_cols = [
    'Ai_principal_amount', 'Ri_payment_amt', 'RI_TotalPay_EOT', 'Ai_Amort_1', 'Ai_interest',
    'Ai_interest_over_term', 'Ai_COB', 'Ai_term_1', 'Ai_APR_1', 'Ai_DOA_1', 'Ai_first_pmt_date_1',
    'Ai_other_fee', 'Ai_ppp', 'Ai_pp_charge', 'Ai_DI'
]
activation_error_cols = [
    'Ai_principal_amount_2', 'Ri_payment_amt_2', 'RI_TotalPay_EOT_2', 'Ai_Amort_2', 'Ai_term_2',
    'Ai_interest_over_term_2', 'Ai_COB_2', 'Ai_interest_2', 'Ai_APR_2', 'Ai_DOA_2', 'Ai_first_pmt_date_2'
]

initial_blank_cols = [
    'pop1_principal_amt', 'pop1_payment_amt', 'pop1_payment_EOT', 'pop1_amort', 'pop1_interest',
    'pop1_OG_IOT', 'pop1_COB', 'pop1_term', 'pop1_APR', 'pop1_DOA', 'pop1_first_pmt_date',
    'pop1_other_fee', 'pop1_ppp', 'pop1_PP_charge', 'pop1_DI'
]
activation_blank_cols = [
    'pop2_principal_amt', 'pop2_payment_amt', 'pop2_payment_EOT', 'pop2_amort', 'pop2_term',
    'pop2_OG_IOT', 'pop2_COB', 'pop2_interest', 'pop2_APR', 'pop2_DOA', 'pop2_first_pmt_date'
]

# SAS SUM function handles missing values implicitly
df['Sum_Initial_COB_error'] = df[initial_error_cols].sum(axis=1)
df['Sum_activation_COB_error'] = df[activation_error_cols].sum(axis=1)
df['Sum_all_error'] = df['Sum_Initial_COB_error'] + df['Sum_activation_COB_error']

df['Sum_Initial_COB_blank'] = df[initial_blank_cols].sum(axis=1)
df['Sum_activation_COB_blank'] = df[activation_blank_cols].sum(axis=1)
df['Sum_all_blank'] = df['Sum_Initial_COB_blank'] + df['Sum_activation_COB_blank']

# --- Esignature Status ---
df['esignature_status'] = np.where(
    df['FT_e_signature_for_Loan_Agreemen'] == 1,
    'Provided E-signature',
    'Missing E-signature'
)

# --- Settled date Indicator ---
df['Settled_date_availability'] = np.where(
    df['FT_Loan_Settled_Date__c_dt'].notna(),
    'Yes',
    'No'
)

# --- Filtering (If/Else logic) ---
# Filter for records where Application Date OR Settled Date is within the current month
filter_condition = (
    (df['FT_Application_Date__c_dt'].dt.date >= mth_beg1) & (df['FT_Application_Date__c_dt'].dt.date <= mth_end1)
) | (
    (df['FT_Loan_Settled_Date__c_dt'].dt.date >= mth_beg1) & (df['FT_Loan_Settled_Date__c_dt'].dt.date <= mth_end1)
)

stage1_accuracy_cur_filtered = df[filter_condition].copy()

# Rename the final filtered dataset
stage1_accuracy_cur = stage1_accuracy_cur_filtered.copy()
# SAS final dataset name was 'bread.stage1_accuracy_cur_&label.' -> stage1_accuracy_cur_{label_d_str}
# We store it locally as a DataFrame for further processing.

# --- 5. Automation Logic (Replicating Zahra Added Logic) ---
print("--- Applying Automation Logic ---")

# Replicating dummy variables (to match SAS structure for export)
stage1_accuracy_cur['dummy5'] = ' '
stage1_accuracy_cur['dummy7'] = ' '
stage1_accuracy_cur['dummy8'] = ' '
stage1_accuracy_cur['dummy9'] = ' '
stage1_accuracy_cur['dummy10'] = ' '
stage1_accuracy_cur['dummy11'] = ' '
stage1_accuracy_cur['dummy12'] = ' '
stage1_accuracy_cur['dummy13'] = ' '
stage1_accuracy_cur['dummy14'] = ' '
stage1_accuracy_cur['dummy15'] = ' '

stage1_accuracy_cur2 = stage1_accuracy_cur.copy()

# Replicating date format change and rename/drop (This is often done in SAS for output formatting)
stage1_accuracy_cur2['FT_Loan_Settled_Date_c2'] = stage1_accuracy_cur2['FT_Loan_Settled_Date__c_dt'].dt.strftime('%Y-%m-%d')
stage1_accuracy_cur2 = stage1_accuracy_cur2.drop(columns=['FT_Loan_Settled_Date__c'], errors='ignore')
stage1_accuracy_cur2 = stage1_accuracy_cur2.rename(columns={'FT_Loan_Settled_Date_c2': 'FT_Loan_Settled_Date__c'})


# Replicating data stage1_accuracy_cur3 (which is essentially a full copy with the date change)
stage1_accuracy_cur3 = stage1_accuracy_cur2.copy()

# --- 6. Export Raw Data (Replicating PROC EXPORT Sheet='Raw') ---
print("--- Exporting Raw Data ---")

# Writer for Excel (to write multiple sheets)
with pd.ExcelWriter(FINAL_OUTPUT_PATH, engine='xlsxwriter') as writer:
    # Select columns for raw output. Since the SAS data step implicitly keeps all
    # variables created or kept, we'll try to select a reasonable subset for 'Raw' sheet.
    # The final set of columns is complex, so we'll just use the final DataFrame.
    # Drop intermediate date time columns used for calculation
    cols_to_drop = [col for col in stage1_accuracy_cur3.columns if col.endswith('_dt') or col.endswith('_str')]
    raw_df_output = stage1_accuracy_cur3.drop(columns=cols_to_drop, errors='ignore')
    
    raw_df_output.to_excel(writer, sheet_name='Raw', index=False)
    print(f"Exported 'Raw' sheet to {FINAL_OUTPUT_PATH}")

# --- 7. Update Variable Table (Replicating Date Range Logic) ---
print("--- Preparing Variables Sheet Data ---")

# Replicating SAS data _null_ for date range calculation
sdate = today_date
EsignDate_previous_month = sas_intnx('month', sdate, -1, 'B')
SettledDate_previous_month = EsignDate_previous_month # Same as EsignDate for 'SettledDate'

# Create the date range DataFrames
EsignDate_df = pd.DataFrame({
    'EsignDate': pd.date_range(start=EsignDate_previous_month, end=sdate, freq='D')
})
SettledDate_df = pd.DataFrame({
    'SettledDate': pd.date_range(start=SettledDate_previous_month, end=sdate, freq='D')
})

# Merge to get all combinations (similar to SAS MERGE without explicit BY)
# This usually results in a cross-join if no common column is used, but SAS MERGE without BY just interleaves.
# Since the length and dates are identical, a simple merge on index is the best replication of date alignment.
tot = pd.merge(EsignDate_df, SettledDate_df, left_index=True, right_index=True, how='outer')

# Final pre-processing
wanted = tot.copy()
wanted['EsignDate2'] = wanted['EsignDate'].dt.strftime('%Y-%m-%d')
wanted['SettledDate2'] = wanted['SettledDate'].dt.strftime('%Y-%m-%d')

prefinal = wanted.drop(columns=['EsignDate', 'SettledDate'], errors='ignore').rename(
    columns={'EsignDate2': 'EsignDate', 'SettledDate2': 'SettledDate'}
)

# Replicate PROC SQL INSERT (Adding MTD records)
mtd_row = pd.DataFrame({'EsignDate': ['MTD'], 'SettledDate': ['MTD']})
prefinal = pd.concat([prefinal, mtd_row], ignore_index=True)

# Replicate FINAL DATA STEP (Sorting/Merging - WHERE=(EsignDate='.'))
# SAS 'final_Variables' logic:
# 1. prefinal (where=(EsignDate='.')) - This is empty due to the MTD insert changing all NaNs to 'MTD'
# 2. prefinal (where=(EsignDate ne '.')) - This is the whole DataFrame
# The intent seems to be to put a placeholder record (if any were blank/missing) before the valid records.
# Since all dates are now strings or 'MTD', we'll just use the full 'prefinal' as the final output.
final_Variables = prefinal.copy()

# --- 8. Export Variables Data (Replicating PROC EXPORT Sheet='Variables') ---
print("--- Exporting Variables Sheet ---")

# Re-open the Excel writer in append mode or use the previous writer object
# Since the previous writer context closed, we need to append. Pandas requires a specific engine/writer for appending.
try:
    with pd.ExcelWriter(FINAL_OUTPUT_PATH, engine='openpyxl', mode='a') as writer:
        final_Variables.to_excel(writer, sheet_name='Variables', index=False)
    print(f"Exported 'Variables' sheet to {FINAL_OUTPUT_PATH}")
except FileNotFoundError:
    # Fallback to create if the Raw export failed for some reason
    with pd.ExcelWriter(FINAL_OUTPUT_PATH, engine='xlsxwriter') as writer:
        final_Variables.to_excel(writer, sheet_name='Variables', index=False)
    print(f"File not found, exported 'Variables' sheet to {FINAL_OUTPUT_PATH} (new file created)")

# --- 9. Final Steps (Replicating CHMOD and ENDSUBMIT) ---
print("--- Finalizing ---")
# Replicating the UNIX command x 'chmod 777 ...' is system-dependent.
# Assuming the Python script is run on a system with correct permissions for the output.

print(f">>>>>>> This program has finished running at {datetime.now().strftime('%d%b%Y:%H:%M:%S')} <<<<<<<<<<")
