from flask import Flask, render_template, request, redirect, url_for
import os
import pandas as pd
import datetime
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

# Importing your provided functions (do not modify them)
import os
import pandas as pd
import re
import numpy as np

def html_to_csv(input_file, output_file):
    """Converts an HTML file with a table into a CSV file."""
    try:
        df = pd.read_html(input_file)[0]  # Read the first table from the HTML file
        df.to_csv(output_file, index=False)
        print(f"Data has been successfully converted to CSV and saved to {output_file}")
    except Exception as e:
        print(f"An error occurred: {e}")

def clean_csv(file_path, output_path):
    """Cleans a CSV file by handling headers, removing NaN values, and duplicates."""
    try:
        df = pd.read_csv(file_path, header=None)

        # Set the third row (index 2) as the header and clean the dataframe
        df.columns = df.iloc[2]
        df = df[3:].reset_index(drop=True)

        df.columns = df.columns.str.replace(r'\xa0', ' ', regex=True).str.strip()

        df_cleaned = df.dropna(axis=1, how='all')
        df_cleaned = df_cleaned.dropna(subset=['Doctor ID'])

        df_cleaned = df_cleaned[~df_cleaned['Doctor ID'].isin([1, '1', 'NaN', 'nan', None])]

        df_cleaned = df_cleaned.loc[:, ~df_cleaned.columns.duplicated()]

        df_cleaned.to_csv(output_path, index=False)
        print(f"Cleaned data has been successfully saved to {output_path}")

    except Exception as e:
        print(f"An error occurred: {e}")

def clean_and_save_data(input_html_file, output_csv_file):
    try:
        df = pd.read_html(input_html_file)[0]

        num_dash_re = re.compile(r"^\s*(\d+)\s*[-–]\s*(.+)$")
        rows = []
        current_dept_id = None
        current_dept_name = None
        current_header = None

        n = len(df)

        for i in range(n):
            row = df.iloc[i]
            first = row.iloc[0]
            next_first = df.iloc[i+1, 0] if i + 1 < n else None
            next_is_header = isinstance(next_first, str) and "doctor" in next_first.lower()

            is_number_dash = isinstance(first, str) and bool(num_dash_re.match(first))

            if is_number_dash and next_is_header:
                m = num_dash_re.match(first)
                current_dept_id = m.group(1).strip()
                current_dept_name = m.group(2).strip()
                current_header = None
                continue

            if isinstance(first, str) and "doctor" in first.lower():
                current_header = row.tolist()
                continue

            if row.isna().all():
                continue

            if current_dept_id is None or current_header is None:
                continue

            data = {
                "department_id": current_dept_id,
                "department_name": current_dept_name,
            }

            for col_name, value in zip(current_header, row.tolist()):
                if pd.isna(col_name):
                    continue
                col_clean = str(col_name).replace("\xa0", " ").strip()
                col_clean = re.sub(r"\s+", "_", col_clean).lower()
                data[col_clean] = value

            rows.append(data)

        df_cleaned = pd.DataFrame(rows)

        def normcol(c):
            c = str(c).replace("\xa0", " ").strip()
            c = re.sub(r"\s+", "_", c)
            return c.lower()

        df_cleaned.columns = [normcol(c) for c in df_cleaned.columns]

        if "doctor_name" in df_cleaned.columns:
            def split_doc(v):
                if pd.isna(v):
                    return None, None
                s = str(v)
                m = re.match(r"^\s*(\d+)\s*[-–]\s*(.+)$", s)
                if m:
                    return m.group(1).strip(), m.group(2).strip()
                return None, s.strip()

            parts = df_cleaned["doctor_name"].apply(split_doc)
            df_cleaned["doctor_id"] = parts.apply(lambda x: x[0])
            df_cleaned["doctor_name_clean"] = parts.apply(lambda x: x[1])

        df_cleaned = df_cleaned.dropna(subset=['doctor_id', 'doctor_name_clean'], how='any')
        df_cleaned = df_cleaned.drop(columns=['doctor_name'], errors='ignore')

        front = ["department_id", "department_name", "doctor_id", "doctor_name_clean"]
        rest = [c for c in df_cleaned.columns if c not in front]
        df_cleaned = df_cleaned[front + rest]

        output_dir = os.path.dirname(output_csv_file)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        df_cleaned.to_csv(output_csv_file, index=False)
        print(f"Cleaned data has been successfully saved to {output_csv_file}")

    except Exception as e:
        print(f"An error occurred: {e}")

from flask import Flask, request, render_template
import os
import pandas as pd
import numpy as np

app = Flask(__name__)

def merge_and_process_data(file_1, file_2, output_csv_file, clinic_id, date):
    """Merges two dataframes on 'doctor_id', processes them, and saves the result to a CSV with clinic_id and date."""
    try:
        # Load the two CSV files
        file_1 = pd.read_csv(file_1)
        file_2 = pd.read_csv(file_2)

        # Rename 'Doctor ID' in file_1 to 'doctor_id' to match file_2
        file_1 = file_1.rename(columns={'Doctor ID': 'doctor_id'})

        # Merging both datasets on 'doctor_id', keeping all unique doctors
        final_merged_df = pd.merge(file_1, file_2, on='doctor_id', how='outer')

        # Renaming columns to match the desired master sheet headers
        final_merged_df = final_merged_df.rename(columns={ 
            'department_id': 'department_id',
            'department_name': 'department_name',
            'doctor_id': 'doctor_id',
            'doctor_name_clean': 'doctor_name',
            'total': 'total',
            'new': 'new',
            'cons': 'cons',
            'flup': 'flup',
            'm_pro': 'm_pro',
            'wappt': 'wappt',
            'am': 'am',
            'pm': 'pm',
            'male': 'male',
            'female': 'female',
            'saudi': 'saudi',
            'non_sa': 'non_sa',
            'dir_co': 'dir_co',
            'per_cap': 'per_cap',
            'Consultation': 'consultation',
            'Laboratory': 'laboratory',
            'Radiology': 'radiology',
            'Medications': 'medications',
            'Services': 'services',
            'In-patients': 'in_patients',
            'Cash': 'cash',
            'Credit': 'credit'
        })

        # Adding calculated columns
        final_merged_df['Net_Revenue_for_Doctors'] = final_merged_df['consultation'] + final_merged_df['laboratory'] + final_merged_df['radiology'] + final_merged_df['services']
        final_merged_df['Average_bill_per_doctor'] = final_merged_df['Net_Revenue_for_Doctors'] / final_merged_df['total'].replace(0, np.nan)

        # Fill the entire columns with the clinic_id and date values
        final_merged_df['clinic_id'] = clinic_id
        final_merged_df['date'] = date

        # Add placeholders for columns that were not available in the original datasets
        final_merged_df['Total_number_of_slots'] = 780  # Set total slots as 780 for all doctors
        final_merged_df['Complaint'] = 'adjusted'  # Set Complaint column to 'adjusted' for all rows

        # Dropping the redundant 'Doctor ID' and 'Doctor Name' columns
        final_merged_df_cleaned = final_merged_df.drop(columns=['Doctor ID', 'Doctor Name'], errors='ignore')

        # Drop duplicates based on 'doctor_id'
        final_merged_df_cleaned = final_merged_df_cleaned.drop_duplicates(subset=['doctor_id'])

        # Calculate Slot Utilization: 780 slots per month
        total_slots = 780
        final_merged_df_cleaned['Slot_utilization'] = (final_merged_df_cleaned['total'] / total_slots) * 100

        # Drop rows where essential columns have NaN values (like 'doctor_id', 'doctor_name', 'total', 'Net_Revenue_for_Doctors')
        final_merged_df_cleaned = final_merged_df_cleaned.dropna(subset=['doctor_id', 'doctor_name', 'total', 'Net_Revenue_for_Doctors'])

        # Save the final cleaned dataframe to a CSV file
        final_merged_df_cleaned.to_csv(output_csv_file, index=False)

        # Optionally, print a success message
        print(f"Final merged sheet saved as {output_csv_file}")

    except Exception as e:
        print(f"An error occurred: {e}")

@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')  # Render the form for user input

@app.route('/submit', methods=['POST'])
def submit():
    try:
        # Capture form data
        clinic_id = request.form['clinic']  # Get clinic_id from the form
        date = request.form['date']  # Get date from the form

        # Capture the uploaded files
        ab_file = request.files['ab_file']
        ac_file = request.files['ac_file']

        # Save the files temporarily
        ab_file_path = os.path.join('uploads', ab_file.filename)
        ac_file_path = os.path.join('uploads', ac_file.filename)

        ab_file.save(ab_file_path)
        ac_file.save(ac_file_path)

        # Example output path for the merged CSV file
        output_csv_file = r'C:\Users\Kafeel\Desktop\looker bi project\master.csv'

        # Call the merge_and_process_data function with the form inputs and file paths
        merge_and_process_data(ab_file_path, ac_file_path, output_csv_file, clinic_id, date)

        # Notify the user that the data has been successfully processed
        return "Data has been successfully processed and saved!"

    except Exception as e:
        return f"An error occurred: {e}"



# Initialize Flask app
app = Flask(__name__)

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        # Get the files from the form
        ab_file = request.files['ab_file']
        ac_file = request.files['ac_file']

        # Get the selected clinic and date from the form
        clinic = request.form['clinic']  # Clinic selected from the dropdown
        date_str = request.form['date']  # Date selected from the date input
        date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()  # Convert string date to date object

        # Save files temporarily
        ab_file_path = os.path.join("uploads", "ab.html")
        ac_file_path = os.path.join("uploads", "ac.html")
        ab_file.save(ab_file_path)
        ac_file.save(ac_file_path)

        # Define paths for CSV files
        ab_csv_path = os.path.join("uploads", "ab.csv")
        ac_csv_path = os.path.join("uploads", "ac.csv")
        cleaned_ab_path = os.path.join("uploads", "cleaned_ab.csv")
        cleaned_ac_path = os.path.join("uploads", "cleaned_ac.csv")
        master_csv_path = os.path.join("uploads", "master.csv")

        # Use your provided functions to process files
        html_to_csv(ab_file_path, ab_csv_path)
        clean_csv(ab_csv_path, cleaned_ab_path)
        clean_and_save_data(ac_file_path, ac_csv_path)

        # Call merge_and_process_data with the required arguments
        merge_and_process_data(cleaned_ab_path, ac_csv_path, master_csv_path, clinic, date)

        # Upload to Google Sheets
        upload_to_google_sheets(master_csv_path, clinic, date)

        # Redirect to success page
        return redirect(url_for('success', clinic=clinic, date=date_str))

    return render_template('index.html')


@app.route("/success")
def success():
    # Get the clinic and date from the URL parameters
    clinic = request.args.get('clinic')
    date = request.args.get('date')
    return f"Data has been successfully uploaded to {clinic} and executive tab for {date}!"


def upload_to_google_sheets(master_csv_file, clinic, date):
    """Uploads the master CSV data to Google Sheets in the selected clinic and executive tab."""
    
    creds = Credentials.from_service_account_file(r'C:\Users\Kafeel\Desktop\final-01\credentials.json')
    service = build('sheets', 'v4', credentials=creds)

    # Read the CSV file into a DataFrame
    df = pd.read_csv(master_csv_file)

    # Convert the date to a string format and add clinic if not already added
    df['date'] = date.strftime('%Y-%m-%d')  # Ensure the date is correctly formatted
    df = df.where(pd.notna(df), None)  # Replaces NaN with None (for Google Sheets)
    df['clinic'] = clinic  # Add the clinic ID to each row

    # Convert the entire DataFrame to a list of lists (excluding headers initially)
    data = df.values.tolist()

    # Get the headers (column names)
    headers = df.columns.tolist()

    ### Handle Clinic-Specific Sheet ###
    
    # Check if headers already exist in the clinic-specific sheet by reading the first row
    sheet_range = f"{clinic}!A1:Z1"  # Read the first row (expand to the expected number of columns)
    result = service.spreadsheets().values().get(
        spreadsheetId='1xNmAf3xSAEHFv-BxQDdnA7jCuTh7Jh60zdvtazLgNFw',
        range=sheet_range
    ).execute()

    # If the first row is empty, insert the headers
    if not result.get('values'):
        # Insert the header row at the top
        body = {'values': [headers]}  # Insert headers
        service.spreadsheets().values().update(
            spreadsheetId='1xNmAf3xSAEHFv-BxQDdnA7jCuTh7Jh60zdvtazLgNFw',
            range=f"{clinic}!A1",  # Start from the first row
            valueInputOption="RAW",
            body=body
        ).execute()

    # Now, check for the first empty row in the clinic-specific sheet (starting from row 2)
    range_for_existing_data = f"{clinic}!A2:Z"  # Get the range starting from A2 to Z (assuming data starts from A2)
    existing_data = service.spreadsheets().values().get(
        spreadsheetId='1xNmAf3xSAEHFv-BxQDdnA7jCuTh7Jh60zdvtazLgNFw',
        range=range_for_existing_data
    ).execute()

    # Check where the first empty row is in the clinic-specific sheet
    existing_rows = existing_data.get('values', [])
    next_empty_row = len(existing_rows) + 2  # Account for header row

    # Insert the data starting from the first empty row in the clinic sheet
    body = {'values': data}
    service.spreadsheets().values().update(
        spreadsheetId='1xNmAf3xSAEHFv-BxQDdnA7jCuTh7Jh60zdvtazLgNFw',
        range=f"{clinic}!A{next_empty_row}",  # Start writing from the first empty row
        valueInputOption="RAW",
        body=body
    ).execute()

    ### Handle the "executive" tab ###

    # Check if headers already exist in the executive sheet by reading the first row
    exec_range = "executive!A1:Z1"  # Check the first row in the executive tab
    exec_result = service.spreadsheets().values().get(
        spreadsheetId='1xNmAf3xSAEHFv-BxQDdnA7jCuTh7Jh60zdvtazLgNFw',
        range=exec_range
    ).execute()

    # If the first row is empty in the executive sheet, insert the headers
    if not exec_result.get('values'):
        # Insert the header row at the top (only once)
        body = {'values': [headers]}  # Insert headers
        service.spreadsheets().values().update(
            spreadsheetId='1xNmAf3xSAEHFv-BxQDdnA7jCuTh7Jh60zdvtazLgNFw',
            range="executive!A1",  # Start from the first row in executive tab
            valueInputOption="RAW",
            body=body
        ).execute()

    # Now, check for the first empty row in the executive sheet (starting from row 2)
    range_for_existing_exec_data = "executive!A2:Z"  # Get the range starting from A2 to Z in executive tab
    existing_exec_data = service.spreadsheets().values().get(
        spreadsheetId='1xNmAf3xSAEHFv-BxQDdnA7jCuTh7Jh60zdvtazLgNFw',
        range=range_for_existing_exec_data
    ).execute()

    # Check where the first empty row is in the executive tab
    existing_exec_rows = existing_exec_data.get('values', [])
    next_empty_exec_row = len(existing_exec_rows) + 2  # Account for header row in executive

    # Insert the data into the executive tab from the first empty row
    body = {'values': data}
    service.spreadsheets().values().update(
        spreadsheetId='1xNmAf3xSAEHFv-BxQDdnA7jCuTh7Jh60zdvtazLgNFw',
        range=f"executive!A{next_empty_exec_row}",  # Start writing from the first empty row in executive tab
        valueInputOption="RAW",
        body=body
    ).execute()

if __name__ == "__main__":
    app.run(debug=True, port=5001)
