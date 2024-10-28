import pdfplumber
import pandas as pd
import os
import ocrmypdf
import re
import requests
import oracledb
#
# pdf_path = r'C:\Users\muhammad.ahsan\Downloads\levispdftemp(1).pdf'
user = 'apex_data'
cs = '10.10.204.109:1521/art'
pw = 'AM_APEXDATA'

## Run function for blob

try:
    connection = oracledb.connect(user=user, password=pw, dsn=cs)
    print('Connection is created')
except oracledb.DatabaseError as err:
    print('Connection not created:', err)
    connection = None


headers = {
    'User-Agent': 'My User Agent 1.0',
    'Content-Type': 'application/x-www-form-urlencoded',
}

# server_2_url = 'https://artlive.artisticmilliners.com:8081/ords/art/send_blob/send_pdf_file'
#
#
# try:
#     response = requests.get(server_2_url, headers=headers, verify=False)
#     if response.status_code == 200:
#         print('Response comes successfully')
#
#     else:
#         print(f'Error: Server 2 returned status code {response.status_code}')
#
# except requests.exceptions.RequestException as e:
#     print(f'Failed to connect to Server 2. Error: {e}')

all_text_temp_1 = ""
with pdfplumber.open('output.pdf') as pdf:
  for temp_1 in pdf.pages:
    temp_1_pdf_text = temp_1.extract_text()

    all_text_temp_1 += '\n' + temp_1_pdf_text


if "Empresa" in all_text_temp_1:
    # Extract text from the first page
    page1_temp1 = pdf.pages[0]
    pdf_tex_pg_1 = page1_temp1.extract_text()

    # Extract PO information using regular expressions
    po_number_match = re.search(r'PO NUMBER[:\s]*([A-Za-z0-9\-]+)', pdf_tex_pg_1)
    po_value_match = re.search(r'TOTAL PO VALUE[:\s]*([\d,]+\.\d{2})', pdf_tex_pg_1)
    total_po_value_match = re.search(r'TOTAL PO QUANTITY[:\s]*([A-Za-z0-9\-]+)', pdf_tex_pg_1)

    # Extract and print PO Number
    if po_number_match:
        po_number = po_number_match.group(1)
        print(po_number, 'PO')
    else:
        po_number = ''

    # Extract and print PO Value
    if po_value_match:
        po_value = po_value_match.group(1)
        print(po_value, 'PO_VALUE')
    else:
        po_value = ''

    # Extract and print Total PO Value
    if total_po_value_match:
        total_po_value = total_po_value_match.group(1)
        print(total_po_value, 'PO_TO_VAL')
    else:
        total_po_value = ''

    # Split page text into lines
    lines = pdf_tex_pg_1.split("\n")

    # Pattern to match the required data
    pattern = re.compile(
        r"(\d+)\s+([\w\s]+)\s+(\d+)\s+((?:[\d%]+\s*\w+\s*)+)\s+(\d+)\s+(\d+\w+)\s+(\d+)\s+([\d.]+)\s+([\d.]+)\s+(\d+)\s+([\d.]+)\s+([\d.]+)"
    )

    # Store extracted details
    extracted_values = []

    # Iterate through lines and extract data using the pattern
    for line in lines:
        match = pattern.match(line)
        if match:
            # Capture groups in a dictionary
            product_info = {
                "Product_Code": match.group(1),
                "Product_Description": match.group(2).strip(),
                "Tariff_Code": match.group(3),
                "Product_Content": match.group(4).strip(),
                "Color_Code": match.group(5),
                "Size": match.group(6),
                "UPC": match.group(7),
                "Ex_Fac_Date": match.group(8),
                "Planned_Del_Date": match.group(9),
                "Qty": match.group(10),
                "PO_Unit_Price": match.group(11),
                "Total_Value": match.group(12),
            }
            extracted_values.append(product_info)


    df2_temp_1 = pd.DataFrame(extracted_values)
    pd.set_option('display.max_columns', None)

    print(df2_temp_1)
    pd.reset_option('display.max_columns')

    try:
        cursor = connection.cursor()

        try:
            for_master_table ="""
                        INSERT INTO cust_pdf_extract_data_m (PO_NUMBER,PO_QUANTITY,PO_VALUE)
                        VALUES (:1,:2,:3)"""



            cursor.execute(for_master_table, [po_number, total_po_value, po_value])
            print('Data Inserted In MAster Table')
        except oracledb.DatabaseError as err:
            print('Error to insert master table', err)

        try:
            for index,row in df2_temp_1.iterrows():
                cursor.execute(
                """ insert into cust_pdf_extract_data_d(PRODUCT_CODE,PRODUCT_DESCRIPTION,TARRIF_CODE,PRODUCT_CONTENT,COLOR_CODE,item_SIZE
                ,UPC,PLANNED_EX_FAC_DATE,PLANED_DEL_DATE,QTY,PO_UNIT_PRICE,TOTAL_VALUE,PDF_TEMP_NUM
                )
                    values(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13)
                    """,[row['Product_Code'],row['Product_Description'],row['Tariff_Code'],row['Product_Content'],row['Color_Code'],row['Size'],
                 row['UPC'], row['Ex_Fac_Date'], row['Planned_Del_Date'], row['Qty'], row['PO_Unit_Price'],row['Total_Value'],'1'
                ])
                print('Data Storeed In Detail Table')
        except oracledb.DatabaseError as err:
            print('Error in Inserting Data in Detail Table',err)

        connection.commit()

    # Close the cursor and connection
        cursor.close()
        connection.close()
    except oracledb.DatabaseError as err:
        print('Error to insert in Data Base')

else:
    # If "Empresa" not found on the first page, extract tables from all pages


    all_tables = []
    first_page = True

    all_text_temp_2 = ''
    for temp2 in pdf.pages:
        temp_2_pdf_text = temp2.extract_text()
        temp_2_pdf_table = temp2.extract_table()

        po_order_match_temp2 = re.search(r'Purchase Order#[:\s]*([A-Za-z0-9\-]+)', temp_2_pdf_text)

        po_value_match_temp2 = re.search(r'PO Value[:\s]*([\d,]+\.\d{2})', temp_2_pdf_text)

        po_value_Quantity_match_temp2 = re.search(r'PO Quantity[:\s]*([A-Za-z0-9\-]+)', temp_2_pdf_text)

        if po_order_match_temp2:
          po_order_temp2 = po_order_match_temp2.group(1)
          print(po_order_temp2, 'PO')
        else:
          po_order_temp2 = ''

        if po_value_match_temp2:
          po_value_temp2 = po_value_match_temp2.group(1)
          print(po_value_temp2, 'P_VALUE')
        else:
          po_value_temp2 = ''


        if po_value_Quantity_match_temp2:
           po_value_Quantity_temp2 = po_value_Quantity_match_temp2.group(1)
           print(po_value_Quantity_temp2, 'P_VALUE_QUANTITY')
        else:
          po_value_Quantity_temp2 = ''




        # Concatenate the text of the pages
        all_text_temp_2 += '\n' + temp_2_pdf_text



        if temp_2_pdf_table:
            # Extract column headings on the first page
            if first_page:
                column_heading = temp_2_pdf_table[0]
                all_tables.extend(temp_2_pdf_table)  # Include column headings in the first table
                first_page = False
            else:
                new_data_rows = temp_2_pdf_table[1:] if temp_2_pdf_table[0] == column_heading else temp_2_pdf_table
                all_tables.extend(new_data_rows)  # Skip headings for subsequent pages

    # Create a DataFrame for all tables
    df_temp_2 = pd.DataFrame(all_tables, columns=column_heading)
    df_temp_2 = df_temp_2.drop(0).reset_index(drop=True)

    print(df_temp_2)

    try:
        cursor = connection.cursor()

        try:
            for_master_table = """
                              INSERT INTO cust_pdf_extract_data_m (PO_NUMBER,PO_QUANTITY,PO_VALUE)
                              VALUES (:1,:2,:3)"""

            cursor.execute(for_master_table, [po_order_temp2, po_value_Quantity_temp2, po_value_temp2])
            print('Data Inserted In MAster Table')
        except oracledb.DatabaseError as err:
            print('Error to insert master table', err)

            try:
                for index, row in df_temp_2.iterrows():
                    cursor.execute(
                        """ insert into cust_pdf_extract_data_d(PRODUCT_CODE,PRODUCT_DESCRIPTION,PRODUCT_CONTENT,item_SIZE
                        ,QTY,Transportation_Mode,INCO_TERMS,INCO_TERMS_LOC,PDF_TEMP_NUM
                        )
                            values(:1,:2,:3,:4,:5,:6,:7,:8,:9)
                            """,
                        [row['item#'], row['Description'], row['Variant Material'],
                        row['Size'],row['PO QTY'], row['Transportation Mode'], row['Inco Terms'], row['IncoTerm Location'], '1'
                         ])
                    print('Data Storeed In Detail Table')
            except oracledb.DatabaseError as err:
                print('Error in Inserting Data in Detail Table', err)

            connection.commit()

            # Close the cursor and connection
            cursor.close()
            connection.close()



    except oracledb.DatabaseError as err:
        print('Error in connection',err)