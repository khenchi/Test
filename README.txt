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
