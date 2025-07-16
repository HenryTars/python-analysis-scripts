import sqlite3
import pandas as pd
from datetime import datetime

# Connecting to the SQLite database
db_path = 'E:/cbs.db'
conn = sqlite3.connect(db_path)

# SQL query for agency banking deposits
query_deposits = """
SELECT 
    x.AC_BRANCH,
    x.AC_NO,
    acc.ACCOUNT_CLASS,
    x.DRCR_IND,
    x.EXCH_RATE,
    x.FINANCIAL_CYCLE,
    x.LCY_AMOUNT,
    x.PERIOD_CODE,
    x.PRODUCT,
    p.PRODUCT_DESCRIPTION AS PRODUCT_DESC,
    x.TRN_CODE,
    s.TRN_DESC AS TRN_DSC,
    x.TRN_DT,
    x.TRN_REF_NO,
    x.VALUE_DT
FROM 
    ACVWS_ALL_AC_ENTRIES_ACRJRNAL_2025 x
LEFT JOIN 
    STTM_TRN_CODE s ON x.TRN_CODE = s.TRN_CODE
LEFT JOIN 
    CSTM_PRODUCT p ON x.PRODUCT = p.PRODUCT_CODE
LEFT JOIN 
    STTM_CUST_ACCOUNT acc ON x.AC_NO = acc.CUST_AC_NO
WHERE 
    x.PRODUCT IN ('CDAU', 'DAGD', 'SECD') AND
    x.TRN_CODE IN ('705', 'D23', 'S09') AND
    x.DRCR_IND = 'C' AND
    x.CUST_GL = 'A' AND 
    acc.ACCOUNT_CLASS NOT IN ('AGBC', 'AGBA')
"""

# Creating a dataframe for all deposit entries
df_deposits = pd.read_sql_query(query_deposits, conn)

df_deposits.head(3)

# Exporting to All deposit entries to Excel
output_path = './../data/AGENCY_BANKING_DEPOSITS_ENTRIES.xlsx'
df_deposits.to_excel(output_path, index=False)

# Creating a dataframe for all deposit entries
df_agent_to_agent = pd.read_sql_query(agent_to_agent, conn)

# SQL query for withdrawals
query_withdrawals = """
SELECT 
    x.AC_BRANCH,
    x.AC_NO,
    x.DRCR_IND,
    x.FINANCIAL_CYCLE,
    x.LCY_AMOUNT,
    x.PERIOD_CODE,
    x.PRODUCT,
    p.PRODUCT_DESCRIPTION AS PRODUCT_DESC,
    x.TRN_CODE,
    s.TRN_DESC AS TRN_DSC,
    x.TRN_DT,
    x.TRN_REF_NO,
    x.VALUE_DT
FROM 
    ACVWS_ALL_AC_ENTRIES_ACRJRNAL_2025 x 
LEFT JOIN 
    STTM_TRN_CODE s ON x.TRN_CODE = s.TRN_CODE 
LEFT JOIN 
    CSTM_PRODUCT p ON x.PRODUCT = p.PRODUCT_CODE  
WHERE 
    x.PRODUCT IN ('CAAU', 'DAWM', 'SECW') AND
    x.TRN_CODE IN ('728', 'D26', 'S01') AND
    x.CUST_GL = 'A' AND
    x.DRCR_IND = 'D'
"""

df_withdrawals = pd.read_sql_query(query_withdrawals, conn)

df_withdrawals.head()

# Exporting to All Withdrawal entries to Excel
output_path = './../data/AGENCY_BANKING_WITHDRAWALS.xlsx'
df_deposits.to_excel(output_path, index=False)

# Convert TRN_DT from string to datetime
df_deposits['TRN_DT'] = pd.to_datetime(df_deposits['TRN_DT'], format="%m/%d/%Y %H:%M:%S")
df_withdrawals['TRN_DT'] = pd.to_datetime(df_withdrawals['TRN_DT'], format="%m/%d/%Y %H:%M:%S")

# Create a DATE column (only date part, drop time)
df_deposits['DATE'] = df_deposits['TRN_DT'].dt.date
df_withdrawals['DATE'] = df_withdrawals['TRN_DT'].dt.date

# Group deposits by DATE
deposits_summary = df_deposits.groupby('DATE').agg(
    Total_Deposit_Amount=('LCY_AMOUNT', 'sum'),
    Deposit_Transactions=('LCY_AMOUNT', 'count')
).reset_index()

# Group withdrawals by DATE
withdrawals_summary = df_withdrawals.groupby('DATE').agg(
    Total_Withdrawal_Amount=('LCY_AMOUNT', 'sum'),
    Withdrawal_Transactions=('LCY_AMOUNT', 'count')
).reset_index()

# Merge the two summaries on DATE
summary = pd.merge(deposits_summary, withdrawals_summary, on='DATE', how='outer')

# Format amounts with commas (optional)
summary['Total_Deposit_Amount'] = summary['Total_Deposit_Amount'].apply(lambda x: "{:,.2f}".format(x))
summary['Total_Withdrawal_Amount'] = summary['Total_Withdrawal_Amount'].apply(lambda x: "{:,.2f}".format(x))

summary.head()

# Save to Excel, Daily summary
output_path = './../data/daily_summary_agency_banking.xlsx'
summary.to_excel(output_path, index=False)

# We take agent debits during customer deposits
agent_debits = """
SELECT 
    x.AC_BRANCH,
    x.AC_NO,
    acc.ACCOUNT_CLASS,
    x.DRCR_IND,
    x.EXCH_RATE,
    x.FINANCIAL_CYCLE,
    x.LCY_AMOUNT,
    x.PERIOD_CODE,
    x.PRODUCT,
    p.PRODUCT_DESCRIPTION AS PRODUCT_DESC,
    x.TRN_CODE,
    s.TRN_DESC AS TRN_DSC,
    x.TRN_DT,
    x.TRN_REF_NO,
    x.VALUE_DT
FROM 
    ACVWS_ALL_AC_ENTRIES_ACRJRNAL_2025 x
LEFT JOIN 
    STTM_TRN_CODE s ON x.TRN_CODE = s.TRN_CODE
LEFT JOIN 
    CSTM_PRODUCT p ON x.PRODUCT = p.PRODUCT_CODE
LEFT JOIN 
    STTM_CUST_ACCOUNT acc ON x.AC_NO = acc.CUST_AC_NO
WHERE 
    x.PRODUCT IN ('CDAU', 'DAGD', 'SECD') AND
    x.TRN_CODE IN ('705', 'D23', 'S09') AND
    x.DRCR_IND = 'D' AND
    x.CUST_GL = 'A'
"""

df_agent_deposit_debits = pd.read_sql_query(agent_debits, conn)

merged_df = pd.merge(df_agent_deposit_debits, df_deposits, on='TRN_REF_NO', how='inner')

# 1️⃣ Find the last entry with PRODUCT_x == 'CDAU'
last_cdau_date = merged_df.loc[merged_df['PRODUCT_x'] == 'CDAU', 'TRN_DT_x'].max()

# 2️⃣ Filter dataset from that date onwards
filtered_df = merged_df[merged_df['TRN_DT_x'] >= last_cdau_date]

# 3️⃣ Group by AC_NO_x and AC_BRANCH_x, sum LCY_AMOUNT_x
summary = filtered_df.groupby(['AC_NO_x', 'AC_BRANCH_x']).agg(
    Total_LCY_Amount=('LCY_AMOUNT_x', 'sum')
).reset_index()

# 4️⃣ Sort by Total_LCY_Amount descending
summary = summary.sort_values(by='Total_LCY_Amount', ascending=False)

# Optional: Format amount with commas
summary['Total_LCY_Amount'] = summary['Total_LCY_Amount'].apply(lambda x: "{:,.2f}".format(x))

# 5️⃣ Load branch details (only required columns)
branches = pd.read_csv('./../data_tables/STTM_BRANCH.csv', usecols=['BRANCH_CODE', 'BRANCH_NAME'])

# 6️⃣ Merge summary with branch names
summary = summary.merge(
    branches,
    how='left',
    left_on='AC_BRANCH_x',
    right_on='BRANCH_CODE'
)

# 7️⃣ Drop unneeded columns
summary.drop(['AC_BRANCH_x', 'BRANCH_CODE'], axis=1, inplace=True)

# 8️⃣ Rename columns as requested
summary.rename(columns={
    'AC_NO_x': 'AGENT ACC',
    'BRANCH_NAME': 'BRANCH',
    'Total_LCY_Amount': 'AMOUNT'
}, inplace=True)

# ✅ Show result
summary.head()

# Save dataset to Excel
summary.to_excel('../data/agent_summary_with_branch_names.xlsx', index=False)

# Querying all agent deposits(both agent-to-agent transfers and customer-to-agent)
all_deposits = """
SELECT 
    x.AC_BRANCH,
    x.AC_NO,
    acc.ACCOUNT_CLASS,
    x.DRCR_IND,
    x.EXCH_RATE,
    x.FINANCIAL_CYCLE,
    x.LCY_AMOUNT,
    x.PERIOD_CODE,
    x.PRODUCT,
    p.PRODUCT_DESCRIPTION AS PRODUCT_DESC,
    x.TRN_CODE,
    s.TRN_DESC AS TRN_DSC,
    x.TRN_DT,
    x.TRN_REF_NO,
    x.VALUE_DT
FROM 
    ACVWS_ALL_AC_ENTRIES_ACRJRNAL_2025 x
LEFT JOIN 
    STTM_TRN_CODE s ON x.TRN_CODE = s.TRN_CODE
LEFT JOIN 
    CSTM_PRODUCT p ON x.PRODUCT = p.PRODUCT_CODE
LEFT JOIN 
    STTM_CUST_ACCOUNT acc ON x.AC_NO = acc.CUST_AC_NO
WHERE 
    x.PRODUCT IN ('CDAU', 'DAGD', 'SECD') AND
    x.TRN_CODE IN ('705', 'D23', 'S09') AND
    x.DRCR_IND = 'C' AND
    x.CUST_GL = 'A'
"""

# Creating a dataframe for all deposit entries
df_deposits_all = pd.read_sql_query(all_deposits, conn)

# Convert TRN_DT_x to datetime
df_deposits_all['TRN_DT'] = pd.to_datetime(df_deposits_all['TRN_DT'], format="%d/%m/%Y %H:%M:%S", errors='coerce')

# Create DATE column (only date part)
df_deposits_all['DATE'] = df_deposits_all['TRN_DT'].dt.date

# Classify transfers
def classify_transfer(row):
    if row['ACCOUNT_CLASS'] in ['AGBA', 'AGBC']:
        return 'Agent to Agent'
    else:
        return 'Customer to Agent'

df_deposits_all['TRANSFER_TYPE'] = df_deposits_all.apply(classify_transfer, axis=1)

# Group by DATE and TRANSFER_TYPE
summary = df_deposits_all.groupby(['DATE', 'TRANSFER_TYPE']).agg(
    Total_LCY_Amount=('LCY_AMOUNT', 'sum'),
    Transaction_Count=('LCY_AMOUNT', 'count')
).reset_index()

# Pivot so Agent to Agent and Customer to Agent appear side by side
pivot_summary = summary.pivot(index='DATE', columns='TRANSFER_TYPE', values=['Total_LCY_Amount', 'Transaction_Count']).fillna(0)

# Flatten multi-level column names
pivot_summary.columns = ['_'.join(col).strip() for col in pivot_summary.columns.values]

# Optional: Format amounts with commas
pivot_summary['Total_LCY_Amount_Agent to Agent'] = pivot_summary['Total_LCY_Amount_Agent to Agent'].apply(lambda x: "{:,.2f}".format(x))
pivot_summary['Total_LCY_Amount_Customer to Agent'] = pivot_summary['Total_LCY_Amount_Customer to Agent'].apply(lambda x: "{:,.2f}".format(x))

# Reset index to bring DATE back as a column
pivot_summary.reset_index(inplace=True)

pivot_summary.head()

# Save dataset to Excel
pivot_summary.to_excel('../data/agent_to_agent_trend.xlsx', index=False)

# Close DB connection
conn.close()