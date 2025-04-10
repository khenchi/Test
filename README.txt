



Function EscapeSQL(value As String) As String
    EscapeSQL = Replace(value, "'", "''")
End Function




Sub ColorSpecificRanges(rangesToColor As Range, Optional rangesToExclude As Range)

    'Define the color (e.g., RGB: Light Blue)
    Dim NewColor As Long
    NewColor = RGB(173, 216, 230)

    Dim Cell As Range

    'Loop through each cell in provided ranges
    For Each Cell In rangesToColor
        If Not rangesToExclude Is Nothing Then
            If Intersect(Cell, rangesToExclude) Is Nothing Then
                Cell.Interior.Color = NewColor
            Else
                Cell.Interior.ColorIndex = 0
            End If
        Else
            Cell.Interior.Color = NewColor
        End If
    Next Cell

End Sub


'Example of how to initialize and call the function:
Sub InitializeAndCall()

    'Open specific workbook (replace with your workbook's path)
    Dim wb As Workbook
    Set wb = Workbooks.Open("C:\\Path\\YourWorkbook.xlsx")

    'Set specific worksheet
    Dim ws As Worksheet
    Set ws = wb.Sheets("Sheet1")

    'Define ranges clearly using the worksheet reference
    Dim rangesToColor As Range
    Set rangesToColor = Union(ws.Range("A1:A10"), ws.Range("C1:D5"), ws.Range("F3:G8"))

    'Define optional exclusion ranges
    Dim rangesToExclude As Range
    Set rangesToExclude = Union(ws.Range("A5"), ws.Range("C3:C4"), ws.Range("F5"))

    'Call the macro
    Call ColorSpecificRanges(ws, rangesToColor, rangesToExclude)

End Sub



Sub ColorSpecificRanges()

    'Define the color (e.g., RGB: Light Blue)
    Dim NewColor As Long
    NewColor = RGB(173, 216, 230)

    'Define the ranges to color
    Dim RangesToColor As Range
    Set RangesToColor = Union(Range("A1:A10"), Range("C1:D5"), Range("F3:G8"))

    'Define ranges or cells to leave with default color
    Dim RangesToExclude As Range
    Set RangesToExclude = Union(Range("A5"), Range("C3:C4"), Range("F5"))

    Dim Cell As Range

    'Loop through each cell in RangesToColor
    For Each Cell In RangesToColor
        'Check if the current cell intersects with excluded ranges
        If Intersect(Cell, RangesToExclude) Is Nothing Then
            'Apply new color if not in excluded ranges
            Cell.Interior.Color = NewColor
        Else
            'Reset color to default (no fill) if explicitly excluded
            Cell.Interior.ColorIndex = 0
        End If
    Next Cell

End Sub






import cx_Oracle

def insert_dataframe_with_duplicate_check(
    conn,
    df,
    table_name,
    insert_columns,
    key_columns
):
    """
    Inserts a DataFrame into an Oracle table using a cx_Oracle connection,
    skipping rows where key_columns already exist in the target table.

    Args:
        conn: cx_Oracle connection object
        df: pandas DataFrame
        table_name: string, name of the Oracle table
        insert_columns: list of strings, column names to insert
        key_columns: list of strings, column names used to check duplicates

    Returns:
        dict with count of inserted and skipped rows
    """
    cursor = conn.cursor()
    inserted = 0
    skipped = 0

    # SQL statements
    placeholders = ", ".join([f":{i+1}" for i in range(len(insert_columns))])
    insert_sql = f"INSERT INTO {table_name} ({', '.join(insert_columns)}) VALUES ({placeholders})"

    check_placeholders = ", ".join([f"{col} = :{i+1}" for i, col in enumerate(key_columns)])
    check_sql = f"SELECT COUNT(*) FROM {table_name} WHERE {check_placeholders}"

    for _, row in df.iterrows():
        key_values = tuple(row[col] for col in key_columns)
        cursor.execute(check_sql, key_values)
        if cursor.fetchone()[0] == 0:
            values = tuple(row[col] for col in insert_columns)
            cursor.execute(insert_sql, values)
            inserted += 1
        else:
            skipped += 1

    conn.commit()
    cursor.close()

    return {"inserted": inserted, "skipped": skipped}






import cx_Oracle
import pandas as pd

# Étape 1 : Connexion Oracle
dsn = cx_Oracle.makedsn("host", 1521, service_name="service_name")  # À adapter
conn = cx_Oracle.connect(user="user", password="password", dsn=dsn)
cursor = conn.cursor()

# Étape 2 : Lecture du fichier Excel
df = pd.read_excel("chemin\\vers\\fichier.xlsx")  # Adapter le chemin

# Étape 3 : Définir les colonnes à insérer (doivent exister dans la table Oracle)
colonnes = ['col1', 'col2', 'col3']  # À adapter selon ton fichier et ta table

# Étape 4 : Construire la requête SQL dynamique
placeholders = ", ".join([f":{i+1}" for i in range(len(colonnes))])
sql = f"INSERT INTO nom_table ({', '.join(colonnes)}) VALUES ({placeholders})"

# Étape 5 : Convertir le DataFrame en liste de tuples
valeurs = [tuple(row[col] for col in colonnes) for _, row in df.iterrows()]

# Étape 6 : Insertion vectorisée avec executemany (rapide et sans boucle explicite)
try:
    cursor.executemany(sql, valeurs)
    conn.commit()
    print("Données insérées avec succès.")
except Exception as e:
    conn.rollback()
    print("Erreur d'insertion :", e)

# Étape 7 : Fermer la connexion
cursor.close()
conn.close()





import pandas as pd

# Sample DataFrame
df = pd.DataFrame({
    'A': [1, 2],
    'B': [3, 4],
    'C': [5, 6],
    'D': [7, 8]
})

# Columns to move and reference column
cols_to_move = ['B', 'C']
after_col = 'A'

# Find position after the reference column
position = df.columns.get_loc(after_col) + 1

# Extract the columns to move
moved_cols = df[cols_to_move]

# Drop them from original DataFrame
df = df.drop(columns=cols_to_move)

# Insert them back after the reference column
df = pd.concat(
    [df.iloc[:, :position], moved_cols, df.iloc[:, position:]],
    axis=1
)

print(df)



Sub RunPythonWithLog()
    Dim pythonExe As String
    Dim scriptPath As String
    Dim sourcePath As String
    Dim sheetName As String
    Dim outputPath As String
    Dim logFile As String
    Dim command As String

    ' Paths and arguments
    pythonExe = "py -3.11" ' Or "python" or full path to python.exe
    scriptPath = ThisWorkbook.Path & "\src\my_script.py"
    sourcePath = ThisWorkbook.FullName
    sheetName = "SheetToDelete"
    outputPath = ThisWorkbook.Path & "\output\my_cleaned_file.xlsb"

    ' Log file path
    logFile = ThisWorkbook.Path & "\debug_log.txt"

    ' Build the command
    command = pythonExe & " """ & scriptPath & """ """ & sourcePath & """ """ & outputPath & """ """ & sheetName & """ > """ & logFile & """ 2>&1"

    ' Execute the command silently
    Shell "cmd.exe /c " & command, vbHide

    MsgBox "Script executed. Check log: " & logFile
End Sub



import os
import shutil
import sys
import win32com.client

def clean_xlsb_macro(source_path, output_path, sheet_to_delete):
    # Step 1 – Copy the original file
    temp_path = output_path.replace(".xlsb", "_temp.xlsb")
    shutil.copy2(source_path, temp_path)

    # Step 2 – Open Excel with COM interface
    excel = win32com.client.DispatchEx("Excel.Application")
    excel.Visible = False
    excel.DisplayAlerts = False
    excel.AutomationSecurity = 3  # Force macros to be disabled

    try:
        # Step 3 – Open the temp file with macros disabled
        wb = excel.Workbooks.Open(temp_path)

        # Step 4 – Remove the VBA project (clear it completely)
        wb.VBProject.VBComponents.Clear()  # Requires Trust access to VBA project

        # Step 5 – Delete the specified sheet
        try:
            sheet = wb.Sheets(sheet_to_delete)
            sheet.Delete()
        except Exception as e:
            print(f"Could not delete sheet '{sheet_to_delete}': {e}")

        # Step 6 – Save to final output file (clean, no macros)
        wb.SaveAs(Filename=output_path, FileFormat=50)  # 50 = .xlsb
        wb.Close(SaveChanges=False)
        print(f"Saved macro-free version to: {output_path}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Clean up Excel COM object
        excel.Quit()
        if os.path.exists(temp_path):
            os.remove(temp_path)

# --- Command-line usage (like from VBA) ---
if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python clean_xlsb.py <source_path> <output_path> <sheet_to_delete>")
    else:
        source_path = sys.argv[1]
        output_path = sys.argv[2]
        sheet_to_delete = sys.argv[3]
        clean_xlsb_macro(source_path, output_path, sheet_to_delete)



import xlwings as xw
import shutil
import os

def process_xlsb(
    source_path,
    sheet_to_delete,
    final_name,
    destination_folder
):
    # Step 1 – Copy the source file
    temp_path = os.path.join(destination_folder, "temp_copy.xlsb")
    shutil.copy2(source_path, temp_path)

    # Step 2 – Open the copy
    app = xw.App(visible=False)
    wb = app.books.open(temp_path)

    # Step 3 – Delete the specified sheet
    try:
        wb.sheets(sheet_to_delete).delete()
    except Exception as e:
        print(f"Could not delete sheet '{sheet_to_delete}': {e}")

    # Step 4 – Save under new name
    final_path = os.path.join(destination_folder, final_name + ".xlsb")
    wb.save(final_path)

    # Step 5 – Clean up
    wb.close()
    app.quit()

    # Optional – Delete temp file
    os.remove(temp_path)

    print(f"File saved to: {final_path}")


Sub RunPythonWithArgs()
    Dim pythonExe As String
    Dim pythonScript As String
    Dim filePath As String
    Dim sheetNames As String
    Dim command As String

    pythonExe = "C:\Path\To\python.exe"
    pythonScript = "C:\Path\To\my_script.py"

    ' Arguments to pass
    filePath = "C:\Users\Me\Documents\Workbook.xlsx"
    sheetNames = "Sheet1,Sheet2"

    ' Combine everything into a single command
    command = """" & pythonExe & """ """ & pythonScript & """ """ & filePath & """ """ & sheetNames & """"

    ' Run the command
    Shell command, vbNormalFocus
End Sub




import sys

def main():
    # sys.argv[0] is the script name
    args = sys.argv[1:]  # Get all arguments passed from VBA
    print("Arguments received:", args)

    # Example: get a file path and sheet names
    if len(args) >= 2:
        filepath = args[0]
        sheet_names = args[1].split(",")  # Sheet names passed as comma-separated
        print("File path:", filepath)
        print("Sheets:", sheet_names)

if __name__ == "__main__":
    main()


import xlwings as xw

def export_selected_sheets():
    # Ouvre le classeur source
    wb = xw.Book(r"C:\Users\YourUsername\Documents\SourceWorkbook.xlsm")

    # Liste des feuilles à copier
    sheet_names = ["Sheet1", "Sheet2"]

    # Crée un nouveau classeur
    new_wb = xw.Book()
    # Supprime les feuilles par défaut
    for s in new_wb.sheets:
        s.delete()

    # Copie les feuilles choisies
    for name in sheet_names:
        wb.sheets[name].copy(after=new_wb.sheets[-1])

    # Enregistre le nouveau fichier en .xlsb
    new_wb.save(r"C:\Users\YourUsername\Documents\ExportedFromPython.xlsb")
    new_wb.close()
    wb.close()



Sub SaveSheetsWithoutButtons()
    Dim tempWb As Workbook
    Dim savePath As String
    Dim sheetList As Variant
    Dim shp As Shape
    Dim ws As Worksheet

    sheetList = Array("Sheet1", "Sheet2") ' Sheets to copy
    savePath = "C:\Users\YourUsername\Documents\Sheets_Without_Buttons.xlsb"

    ' Copy to a new workbook
    ThisWorkbook.Sheets(sheetList).Copy
    Set tempWb = ActiveWorkbook

    ' Remove buttons (shapes and ActiveX) from copied sheets
    For Each ws In tempWb.Worksheets
        For Each shp In ws.Shapes
            shp.Delete
        Next shp
    Next ws

    ' Save as .xlsb
    Application.DisplayAlerts = False
    tempWb.SaveAs Filename:=savePath, FileFormat:=50
    tempWb.Close SaveChanges:=False
    Application.DisplayAlerts = True

    MsgBox "Sheets saved without buttons."
End Sub







import xlwings as xw
import os

# Define file paths
source_xlsx = os.path.abspath("source.xlsx")
destination_xlsb = os.path.abspath("converted.xlsb")

# Launch Excel (invisible)
app = xw.App(visible=False)
wb = app.books.open(source_xlsx)

# Save the file as .xlsb (FileFormat = 50)
wb.api.SaveAs(Filename=destination_xlsb, FileFormat=50)

# Clean up
wb.close()
app.quit()


import xlwings as xw
import os

# Define file paths
source_xlsx = os.path.abspath("source.xlsx")
destination_xlsb = os.path.abspath("converted.xlsb")

# Launch Excel invisibly
app = xw.App(visible=False)
wb = app.books.open(source_xlsx)

# Inject VBA to save the current workbook as .xlsb
vba_code = f'''
Sub SaveAsXLSB()
    ThisWorkbook.SaveAs Filename:="{destination_xlsb.replace("\\", "\\\\")}", FileFormat:=50
End Sub
'''

# Add VBA module
vba_module = wb.api.VBProject.VBComponents.Add(1)
vba_module.CodeModule.AddFromString(vba_code)

# Run the macro (fully qualified name: workbook_name!macro_name)
macro_name = f"{wb.name}!SaveAsXLSB"
app.api.Application.Run(macro_name)

# Close workbooks and Excel
wb.close()
app.quit()









import xlwings as xw
import os

# Define file paths
source_xlsx = os.path.abspath("source.xlsx")
destination_xlsb = os.path.abspath("converted.xlsb")

# Launch Excel (invisible)
app = xw.App(visible=False)

# Open the source workbook
wb = app.books.open(source_xlsx)

# Inject VBA code to save the workbook as .xlsb
vba_code = f'''
Sub SaveAsXLSB()
    Dim wb As Workbook
    Set wb = Workbooks.Open("{source_xlsx.replace("\\", "\\\\")}")
    wb.SaveAs Filename:="{destination_xlsb.replace("\\", "\\\\")}", FileFormat:=50  ' 50 = xlExcel12 (.xlsb)
    wb.Close SaveChanges:=False
End Sub
'''

# Add VBA module and insert the code
vba_module = wb.api.VBProject.VBComponents.Add(1)  # 1 = standard module
vba_module.CodeModule.AddFromString(vba_code)

# Run the macro
app.api.Application.Run("SaveAsXLSB")

# Clean up
wb.close()
app.quit()






import xlwings as xw

# Open the source .xlsx workbook
source_wb = xw.Book('source.xlsx')
source_sheet = source_wb.sheets['Sheet1']  # Replace with your actual sheet name

# Open the destination .xlsb workbook
dest_wb = xw.Book('destination.xlsb')

# Copy the sheet to the destination workbook
source_sheet.api.Copy(Before=dest_wb.sheets[0].api)

# Optionally rename the copied sheet in the destination
dest_wb.sheets[0].name = 'CopiedSheet'

# Save and close both workbooks
dest_wb.save()
source_wb.close()
dest_wb.close()



La feuille « New_MASAI_names » apparaît lorsqu'une nouvelle ligne contenant un Massai Name est présente dans les données de la base, mais n'existe pas dans le fichier MappingDB_RM.xlsx, onglet « Known_masai_names ».
C’est par exemple le cas pour la catégorie « Other Adjustments YTD ».
Pour éviter que ce contrôle ne se déclenche, il faut ajouter manuellement la catégorie concernée dans l’onglet « Known_masai_names » du fichier de mapping.




Sub GroupColumns()
    Dim ws As Worksheet
    Set ws = ActiveSheet  ' Modify if needed

    ' Group Columns (Example: Group Columns B to D)
    ws.Columns("B:D").Group
    ws.Columns("F:H").Group
    ws.Columns("J:L").Group

    ' Show Outline Symbols (Plus/Minus Buttons)
    ws.Outline.ShowSymbols = True
End Sub

' Macro to Expand All Groups
Sub ExpandAllGroups()
    Dim ws As Worksheet
    Set ws = ActiveSheet
    ws.Outline.ShowLevels ColumnLevels:=2
End Sub

' Macro to Collapse All Groups
Sub CollapseAllGroups()
    Dim ws As Worksheet
    Set ws = ActiveSheet
    ws.Outline.ShowLevels ColumnLevels:=1
End Sub





Sub MoveColumnsInSheets()
    Dim ws As Worksheet
    Dim header As String
    Dim targetPos As Integer
    
    ' Define the worksheets, headers, and target positions
    Dim sheetConfigs As Variant
    sheetConfigs = Array( _
        Array("Sheet1", "Cheks", 2), _
        Array("Sheet2", "Amount", 3), _
        Array("Sheet3", "Date", 4) _
    )
    
    ' Loop through the configurations and apply the movement
    Dim i As Integer
    For i = LBound(sheetConfigs) To UBound(sheetConfigs)
        Set ws = ThisWorkbook.Sheets(sheetConfigs(i)(0))
        header = sheetConfigs(i)(1)
        targetPos = sheetConfigs(i)(2)
        MoveColumnByHeader ws, header, targetPos
    Next i
    
    ' Clear clipboard
    Application.CutCopyMode = False
End Sub

' Function to move a column by its header to a target position
Sub MoveColumnByHeader(ws As Worksheet, header As String, targetPos As Integer)
    Dim lastCol As Integer
    Dim colIndex As Integer
    
    ' Find the last used column
    lastCol = ws.Cells(1, ws.Columns.Count).End(xlToLeft).Column

    ' Search for the column with the specified header
    For colIndex = 1 To lastCol
        If ws.Cells(1, colIndex).Value = header Then
            ' Cut the column
            ws.Columns(colIndex).Cut
            ' Insert at the target position
            ws.Columns(targetPos).Insert Shift:=xlToRight
            ' Exit loop once found
            Exit For
        End If
    Next colIndex
End Sub


# Test

def get_quarterly_date(date_as_of: str) -> datetime:
    """Returns the last day of the current quarter given a reference date."""
    date = datetime.strptime(date_as_of, "%Y-%m-%d")
    quarter_end_month = ((date.month - 1) // 3) * 3 + 3  # Gets the last month of the quarter
    next_month = quarter_end_month + 1 if quarter_end_month < 12 else 1
    next_year = date.year if quarter_end_month < 12 else date.year + 1
    return datetime(next_year, next_month, 1) - timedelta(days=1)

 import unittest
from datetime import datetime, timedelta

def get_quarterly_date(date_as_of: str) -> datetime:
    """Returns the last day of the current quarter given a reference date."""
    date = datetime.strptime(date_as_of, "%Y-%m-%d")
    quarter_end_month = ((date.month - 1) // 3) * 3 + 3  # Gets the last month of the quarter
    next_month = quarter_end_month + 1 if quarter_end_month < 12 else 1
    next_year = date.year if quarter_end_month < 12 else date.year + 1
    return datetime(next_year, next_month, 1) - timedelta(days=1)

class TestQuarterlyDate(unittest.TestCase):
    def test_quarterly_dates(self):
        """Test quarterly date calculations."""
        test_cases = {
            "2025-03-14": "2025-03-31",  # Q1 (Jan-Mar) -> Mar 31
            "2025-06-20": "2025-06-30",  # Q2 (Apr-Jun) -> Jun 30
            "2025-09-10": "2025-09-30",  # Q3 (Jul-Sep) -> Sep 30
            "2025-12-05": "2025-12-31",  # Q4 (Oct-Dec) -> Dec 31
        }
        
        for date_as_of, expected in test_cases.items():
            with self.subTest(date_as_of=date_as_of):
                result = get_quarterly_date(date_as_of).strftime("%Y-%m-%d")
                self.assertEqual(result, expected)

# Run the unit test
if __name__ == "__main__":
    unittest.main()

def get_semestrial_date(date_as_of: str) -> datetime:
    """Returns the last day of the current semester given a reference date."""
    date = datetime.strptime(date_as_of, "%Y-%m-%d")
    semester_end_month = 6 if date.month <= 6 else 12  # End of first semester is June, second semester is December
    next_month = semester_end_month + 1 if semester_end_month < 12 else 1
    next_year = date.year if semester_end_month < 12 else date.year + 1
    return datetime(next_year, next_month, 1) - timedelta(days=1)

def get_yearly_date(date_as_of: str) -> datetime:
    """Returns the last day of the current year given a reference date."""
    date = datetime.strptime(date_as_of, "%Y-%m-%d")
    return datetime(date.year + 1, 1, 1) - timedelta(days=1)

class TestSemestrialAndYearlyDates(unittest.TestCase):
    def test_semestrial_dates(self):
        """Test semestrial date calculations."""
        test_cases = {
            "2025-03-14": "2025-06-30",  # First semester (Jan-Jun) -> Jun 30
            "2025-08-20": "2025-12-31",  # Second semester (Jul-Dec) -> Dec 31
        }
        
        for date_as_of, expected in test_cases.items():
            with self.subTest(date_as_of=date_as_of):
                result = get_semestrial_date(date_as_of).strftime("%Y-%m-%d")
                self.assertEqual(result, expected)

    def test_yearly_dates(self):
        """Test yearly date calculations."""
        test_cases = {
            "2025-03-14": "2025-12-31",  # End of year 2025 -> Dec 31
            "2025-11-10": "2025-12-31",  # End of year 2025 -> Dec 31
        }
        
        for date_as_of, expected in test_cases.items():
            with self.subTest(date_as_of=date_as_of):
                result = get_yearly_date(date_as_of).strftime("%Y-%m-%d")
                self.assertEqual(result, expected)

# Run the unit tests
if __name__ == "__main__":
    unittest.main()
