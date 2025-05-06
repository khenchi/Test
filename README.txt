Okay, let's refactor the provided code to make it cleaner, more modular, faster, and follow best practices.

Key Improvements:

Modularity: Separate concerns:

Data Loading (functions to load main data and compositions).

Transformation Logic (the ETFTransparizer class).

Pipeline Execution (the main script logic).

Testing (kept separate, potentially in a different file like test_etf_transparizer.py).

Clarity & Readability:

Use more descriptive variable names where needed.

Add docstrings and type hints for better understanding and maintainability.

Simplify logic within methods.

Performance:

Replace iterrows() with vectorized Pandas operations (merge, direct column calculations) for significant speedup, especially on large DataFrames.

Robustness:

Handle edge cases like empty inputs or no ETFs needing transparization more explicitly.

Ensure consistent column sets for concatenation.

Code Structure:

Organize imports.

Use if name == "main": guard for executable script parts.

Slightly refactor testing for clarity and testing the public interface.

Revised Code Structure (Conceptual):

etf_utils/data_loader.py (or similar)

import pandas as pd

Mock data source (replace with actual loading logic)

_composition_data_store = { ... }

_main_data_store = pd.DataFrame({ ... })

def load_main_data() -> pd.DataFrame:

# Load from file, database, etc.

return _main_data_store.copy() # Return copy to avoid mutation

def load_composition(etf_id: str) -> pd.DataFrame:

# Load specific composition, handle not found

df = _composition_data_store.get(etf_id, pd.DataFrame(columns=['component_id', 'weight']))

return df.copy() # Return copy

etf_utils/transformer.py

import pandas as pd

from typing import Callable, Dict, List, Optional

class ETFTransparizer:

def __init__(self, composition_loader: Callable[[str], pd.DataFrame]):

    # ... (implementation) ...



def transparize(self, df_main: pd.DataFrame) -> pd.DataFrame:

    # ... (vectorized implementation) ...

main_pipeline.py

import pandas as pd

from etf_utils.data_loader import load_main_data, load_composition

from etf_utils.transformer import ETFTransparizer

def run_pipeline():

# ... (pipeline logic) ...

if name == "main":

run_pipeline()

tests/test_transformer.py

import pandas as pd

import pytest # (Optional but recommended)

from etf_utils.transformer import ETFTransparizer

... (test setup and functions) ...

Refactored Code (Single File for Simplicity Here):

import pandas as pd

from typing import Callable, Dict, List, Optional, Tuple

--- Configuration / Mock Data ---

In a real scenario, this would be loaded from files/DBs via functions below

COMPOSITION_DATA_STORE: Dict[str, pd.DataFrame] = {

'ETF1': pd.DataFrame({'component_id': ['A', 'B'], 'weight': [0.6, 0.4]}),

'ETF3': pd.DataFrame({'component_id': ['C', 'D', 'E'], 'weight': [0.5, 0.3, 0.2]})

}

MAIN_DATA_STORE: pd.DataFrame = pd.DataFrame({

'etf_id': ['ETF1', 'ETF2', 'ETF3'],

'instrument_id': ['ETF1', 'ETF2', 'ETF3'],

'amount': [1000, 800, 600],

'do_transparize': [True, False, True],

'other_col': ['val1', 'val2', 'val3'] # Example of extra columns to preserve

})

--- Data Loading Module (Conceptual) ---

def load_main_data() -> pd.DataFrame:

"""Loads the main ETF position data."""

print("Loading main data...")

# In real code: pd.read_csv, pd.read_sql, etc.

return MAIN_DATA_STORE.copy() # Return a copy to prevent accidental mutation

def load_composition(etf_id: str) -> pd.DataFrame:

"""

Loads the composition data for a single ETF.

Returns an empty DataFrame with expected columns if ETF is not found.

"""

print(f"Loading composition for: {etf_id}")

# In real code: Load from DB, API, file based on etf_id

composition = COMPOSITION_DATA_STORE.get(etf_id)

if composition is None:

    return pd.DataFrame(columns=['component_id', 'weight'])

# Ensure consistent columns and return a copy

return composition[['component_id', 'weight']].copy()

--- Transformation Module (Conceptual) ---

class ETFTransparizer:

"""

Handles the process of "transparizing" ETFs in a portfolio DataFrame.



Replaces ETF rows marked for transparization with rows representing

their underlying components, adjusting amounts based on weights.

"""

def __init__(self, composition_loader: Callable[[str], pd.DataFrame]):

    """

    Initializes the ETFTransparizer.



    Args:

        composition_loader: A function that takes an ETF ID (str) and

                            returns its composition DataFrame (must have

                            'component_id' and 'weight' columns).

    """

    if not callable(composition_loader):

        raise TypeError("composition_loader must be a callable function.")

    self.composition_loader = composition_loader

    self._composition_cache: Dict[str, pd.DataFrame] = {} # Cache compositions



def _get_composition(self, etf_id: str) -> pd.DataFrame:

    """Retrieves composition, using cache if available."""

    if etf_id not in self._composition_cache:

        self._composition_cache[etf_id] = self.composition_loader(etf_id)

    return self._composition_cache[etf_id]



def _load_all_compositions(self, etf_ids: List[str]) -> pd.DataFrame:

    """Loads compositions for multiple ETFs and combines them."""

    all_comps = []

    unique_etf_ids = set(etf_ids)

    for etf_id in unique_etf_ids:

        comp_df = self._get_composition(etf_id)

        if not comp_df.empty:

            comp_df['etf_id'] = etf_id # Add etf_id for joining

            all_comps.append(comp_df)



    if not all_comps:

        return pd.DataFrame(columns=['component_id', 'weight', 'etf_id'])



    return pd.concat(all_comps, ignore_index=True)



def transparize(self, df_main: pd.DataFrame) -> pd.DataFrame:

    """

    Performs the ETF transparization.



    Args:

        df_main: The input DataFrame with ETF positions. Must contain

                 'etf_id', 'instrument_id', 'amount', and 'do_transparize'

                 columns.



    Returns:

        A new DataFrame where specified ETFs are replaced by their

        components. Includes an 'is_transparized_component' boolean column

        and a 'source_etf' column indicating the origin for components.

    """

    if not isinstance(df_main, pd.DataFrame):

        raise TypeError("Input df_main must be a pandas DataFrame.")

    if df_main.empty:

        return pd.DataFrame(columns=list(df_main.columns) + ['is_transparized_component', 'source_etf'])



    required_cols = ['etf_id', 'instrument_id', 'amount', 'do_transparize']

    if not all(col in df_main.columns for col in required_cols):

        raise ValueError(f"Input DataFrame missing one or more required columns: {required_cols}")



    # --- Vectorized Approach ---



    # 1. Split data: rows to keep as is vs. rows to process

    df_to_process = df_main[df_main['do_transparize']].copy()

    df_keep = df_main[~df_main['do_transparize']].copy()



    # Add tracking columns to the rows we are keeping

    df_keep['is_transparized_component'] = False

    df_keep['source_etf'] = None # Or pd.NA for newer pandas versions



    if df_to_process.empty:

        print("No ETFs marked for transparization.")

        # Ensure consistent columns before returning

        final_cols = list(df_main.columns) + ['is_transparized_component', 'source_etf']

        return df_keep.reindex(columns=final_cols)



    # 2. Load compositions for ETFs needing processing

    etf_ids_to_load = df_to_process['etf_id'].unique().tolist()

    all_compositions = self._load_all_compositions(etf_ids_to_load)



    if all_compositions.empty:

        print("No compositions found for ETFs marked for transparization.")

        # If no compositions, treat processing rows like kept rows

        df_to_process['is_transparized_component'] = False

        df_to_process['source_etf'] = None

        final_cols = list(df_main.columns) + ['is_transparized_component', 'source_etf']

        combined_df = pd.concat([df_keep, df_to_process], ignore_index=True)

        return combined_df.reindex(columns=final_cols)





    # 3. Merge ETF data with composition data

    # Use suffixes to avoid column name collisions if 'etf_id' existed in composition_df

    merged_df = pd.merge(

        df_to_process,

        all_compositions,

        on='etf_id',

        how='inner' # Only keep ETFs for which we found compositions

    )



    if merged_df.empty:

         print("Merge resulted in empty DataFrame. Check composition data.")

         # Handle case where merge fails (e.g., no matching compositions)

         df_to_process['is_transparized_component'] = False # Treat as not transparized

         df_to_process['source_etf'] = None

         final_cols = list(df_main.columns) + ['is_transparized_component', 'source_etf']

         combined_df = pd.concat([df_keep, df_to_process], ignore_index=True)

         return combined_df.reindex(columns=final_cols)





    # 4. Calculate component amounts and add metadata

    transparized_components = merged_df.copy()

    transparized_components['amount'] = transparized_components['weight'] * transparized_components['amount']

    transparized_components['instrument_id'] = transparized_components['component_id']

    transparized_components['is_transparized_component'] = True

    transparized_components['source_etf'] = transparized_components['etf_id']



    # 5. Select and align columns for the final transparized part

    # Keep all original columns from df_main, plus the new/modified ones

    final_cols = list(df_main.columns) + ['is_transparized_component', 'source_etf']

    # Drop intermediate columns from merge/calculation

    cols_to_drop = ['component_id', 'weight']

    transparized_components = transparized_components.drop(columns=cols_to_drop, errors='ignore')

    # Ensure order and presence of all columns

    transparized_components = transparized_components.reindex(columns=final_cols)





    # 6. Concatenate the kept rows and the new component rows

    final_df = pd.concat([df_keep, transparized_components], ignore_index=True)



    # Ensure final column order matches expectation (optional but good practice)

    return final_df[final_cols]

--- Pipeline Execution Module (Conceptual) ---

def run_pipeline():

"""Runs the full ETF transparization pipeline."""

print("-" * 20)

print("Starting Pipeline")

print("-" * 20)



# 1. Load main data

df_main = load_main_data()

print("\nInitial Data:")

print(df_main)



# 2. Initialize Transparizer with the appropriate loader

# Use the actual loading function here

transparizer = ETFTransparizer(composition_loader=load_composition)



# 3. Perform transparization

print("\nPerforming Transparization...")

df_final = transparizer.transparize(df_main)



# 4. Show results

print("\nFinal Resulting DataFrame:")

print(df_final)

print("-" * 20)

print("Pipeline Finished")

print("-" * 20)

return df_final

--- Testing Module (Conceptual - often in separate files) ---

Using basic asserts here; pytest is recommended for larger projects

def mock_loader_test(etf_id: str) -> pd.DataFrame:

"""Mock loader specifically for tests."""

data = {

    'ETF1': pd.DataFrame({'component_id': ['A', 'B'], 'weight': [0.6, 0.4]}),

    'ETF_EMPTY': pd.DataFrame(columns=['component_id', 'weight']),

    'ETF_CDE': pd.DataFrame({'component_id': ['C', 'D', 'E'], 'weight': [0.5, 0.3, 0.2]})

}

return data.get(etf_id, pd.DataFrame(columns=['component_id', 'weight'])).copy()

def test_transparizer_initialization():

"""Tests if the transparizer initializes correctly."""

transparizer = ETFTransparizer(mock_loader_test)

assert callable(transparizer.composition_loader)

try:

    ETFTransparizer("not a function")

    assert False, "Should have raised TypeError for invalid loader"

except TypeError:

    assert True # Expected exception

def test_transparize_empty_input():

"""Tests transparizer with an empty input DataFrame."""

transparizer = ETFTransparizer(mock_loader_test)

empty_df = pd.DataFrame(columns=['etf_id', 'instrument_id', 'amount', 'do_transparize'])

result = transparizer.transparize(empty_df)

assert result.empty

assert list(result.columns) == ['etf_id', 'instrument_id', 'amount', 'do_transparize', 'is_transparized_component', 'source_etf']

def test_transparize_no_transparization_needed():

"""Tests when no rows have do_transparize = True."""

df_main = pd.DataFrame({

    'etf_id': ['ETF1', 'ETF2'],

    'instrument_id': ['ETF1', 'ETF2'],

    'amount': [1000, 800],

    'do_transparize': [False, False]

})

transparizer = ETFTransparizer(mock_loader_test)

result = transparizer.transparize(df_main)

pd.testing.assert_frame_equal(

    result[['etf_id', 'instrument_id', 'amount', 'do_transparize']],

    df_main[['etf_id', 'instrument_id', 'amount', 'do_transparize']]

)

assert not result['is_transparized_component'].any()

assert result['source_etf'].isnull().all()

assert len(result) == 2

def test_transparize_basic_case():

"""Tests a standard transparization case."""

df_main = pd.DataFrame({

    'etf_id': ['ETF1', 'ETF2'],

    'instrument_id': ['ETF1', 'ETF2'],

    'amount': [1000, 800],

    'do_transparize': [True, False],

    'other_col': ['v1','v2']

})

transparizer = ETFTransparizer(mock_loader_test)

result = transparizer.transparize(df_main)



assert len(result) == 3 # ETF2 (kept) + A (from ETF1) + B (from ETF1)



# Check kept row (ETF2)

kept_row = result[result['instrument_id'] == 'ETF2'].iloc[0]

assert kept_row['etf_id'] == 'ETF2'

assert kept_row['amount'] == 800

assert not kept_row['do_transparize']

assert not kept_row['is_transparized_component']

assert pd.isna(kept_row['source_etf'])

assert kept_row['other_col'] == 'v2'



# Check transparized components (A, B from ETF1)

comp_A = result[result['instrument_id'] == 'A'].iloc[0]

assert comp_A['amount'] == 600.0 # 0.6 * 1000

assert comp_A['is_transparized_component']

assert comp_A['source_etf'] == 'ETF1'

assert comp_A['other_col'] == 'v1' # Check other columns preserved



comp_B = result[result['instrument_id'] == 'B'].iloc[0]

assert comp_B['amount'] == 400.0 # 0.4 * 1000

assert comp_B['is_transparized_component']

assert comp_B['source_etf'] == 'ETF1'

assert comp_B['other_col'] == 'v1' # Check other columns preserved

def test_transparize_missing_composition():

"""Tests when an ETF marked for transparization has no composition data."""

df_main = pd.DataFrame({

    'etf_id': ['ETF_MISSING', 'ETF2'],

    'instrument_id': ['ETF_MISSING', 'ETF2'],

    'amount': [1000, 800],

    'do_transparize': [True, False]

})

transparizer = ETFTransparizer(mock_loader_test)

result = transparizer.transparize(df_main)



assert len(result) == 2 # Both ETFs should be present, none transparized

# ETF_MISSING should be kept as is, but marked as not transparized

missing_row = result[result['etf_id'] == 'ETF_MISSING'].iloc[0]

assert missing_row['amount'] == 1000

assert not missing_row['is_transparized_component'] # Crucial check

assert pd.isna(missing_row['source_etf'])



# ETF2 should be kept as is

kept_row = result[result['etf_id'] == 'ETF2'].iloc[0]

assert kept_row['amount'] == 800

assert not kept_row['is_transparized_component']

assert pd.isna(kept_row['source_etf'])

--- Main Execution Guard ---

if name == "main":

print("Running Tests...")

test_transparizer_initialization()

test_transparize_empty_input()

test_transparize_no_transparization_needed()

test_transparize_basic_case()

test_transparize_missing_composition()

print("Tests Passed!")



# Run the main pipeline example

run_pipeline()

IGNORE_WHEN_COPYING_START

content_copy

download

Use code with caution.

Python

IGNORE_WHEN_COPYING_END

Explanation of Changes:

Vectorization (transparize method):

The core logic now avoids row-by-row iteration (iterrows).

It splits the DataFrame into parts to keep and parts to process based on do_transparize.

It loads compositions only for the unique ETFs needing processing (_load_all_compositions).

pd.merge efficiently joins the ETF data with all relevant compositions.

Calculations (amount, setting instrument_id, etc.) are done on the entire merged_df at once.

pd.concat combines the kept rows and the newly generated component rows.

.reindex(columns=...) is used to ensure the final DataFrame and intermediate parts have consistent columns before concatenation.

Modularity:

Clear separation between data loading functions (load_main_data, load_composition), the ETFTransparizer class, the pipeline execution (run_pipeline), and tests.

The ETFTransparizer takes the composition_loader as a dependency (Dependency Injection), making it more flexible and testable.

Clarity & Readability:

Added docstrings explaining the purpose of the class, methods, and functions.

Added type hints (Callable, Dict, List, pd.DataFrame).

Added comments explaining steps within the transparize method.

Introduced helper methods like _get_composition and _load_all_compositions within the class.

Efficiency/Minor Improvements:

Added a simple cache (_composition_cache) within the transparizer to avoid reloading the same composition multiple times within a single transparize call.

Data loading functions now return copies (.copy()) to prevent modifying the original data store if it were mutable.

More robust checks for input types and required columns.

Handles cases where no ETFs need processing or no compositions are found more gracefully.

Testing:

Tests now primarily target the public transparize method, which is better practice.

Added tests for edge cases (empty input, no work to do, missing compositions).

Uses pd.testing.assert_frame_equal for more robust DataFrame comparisons in tests (though basic asserts are used here for simplicity).

A dedicated mock_loader_test is used for tests.

Removed build_final_df: The logic of combining original and transparized data is now handled directly within the transparize method, making the flow clearer and removing the need for a separate simple concatenation function. The transparize method now returns the complete final DataFrame.

This revised version should be significantly faster for larger datasets due to vectorization and provides a much cleaner, more maintainable, and testable structure.






Sub MergeAndCenterColumns()
    Dim ws As Worksheet
    Dim tableRange As Range
    Dim leftBottomCell As Range
    Dim lastRow As Long
    Dim colPositions As Variant
    Dim colPos As Variant
    Dim currentRow As Long
    Dim startRow As Long
    Dim endRow As Long
    Dim col As Long
    Dim cellValue As String
    
    ' --- Paramètres à adapter ---
    Set ws = ThisWorkbook.Sheets("Sheet1") ' Remplacer par le nom de ta feuille
    Set leftBottomCell = ws.Range("A100") ' Remplacer par la cellule en bas à gauche de ta table
    colPositions = Array(1, 2, 3) ' Colonnes à traiter : 1 = colonne A, 2 = colonne B, etc.
    
    ' Détermination de la dernière ligne de la table
    lastRow = leftBottomCell.End(xlUp).Row
    
    ' Boucle sur chaque colonne à traiter
    For Each colPos In colPositions
        col = colPos
        currentRow = 1
        
        ' Trouver la première cellule de la table pour la colonne
        currentRow = ws.Cells(1, col).End(xlDown).Row
        
        Do While currentRow <= lastRow
            ' Si cellule non vide
            If ws.Cells(currentRow, col).Value <> "" Then
                startRow = currentRow
                endRow = startRow
                
                ' Chercher les lignes vides en dessous
                Do While endRow + 1 <= lastRow And ws.Cells(endRow + 1, col).Value = ""
                    endRow = endRow + 1
                Loop
                
                ' Fusionner si plusieurs lignes
                If endRow > startRow Then
                    With ws.Range(ws.Cells(startRow, col), ws.Cells(endRow, col))
                        .Merge
                        .HorizontalAlignment = xlCenter
                        .VerticalAlignment = xlCenter
                    End With
                End If
                
                ' Passer à la prochaine cellule après le groupe fusionné
                currentRow = endRow + 1
            Else
                currentRow = currentRow + 1
            End If
        Loop
    Next colPos
End Sub

import pandas as pd from dataclasses import dataclass, field from typing import List

@dataclass class MaturityBucket: bucket_name: str consumption: float

def __post_init__(self):
    if self.consumption < 0:
        raise ValueError("Bucket consumption cannot be negative")

@dataclass class Curve: name: str limit: float total_consumption: float buckets: List[MaturityBucket] = field(default_factory=list)

def __post_init__(self):
    if self.limit <= 0:
        raise ValueError("Curve limit must be positive")
    if self.total_consumption < 0:
        raise ValueError("Total consumption cannot be negative")

def add_bucket(self, bucket: MaturityBucket):
    if not isinstance(bucket, MaturityBucket):
        raise TypeError("Expected a MaturityBucket instance")
    self.buckets.append(bucket)

@dataclass class Portfolio: curves: List[Curve] = field(default_factory=list)

def add_curve(self, curve: Curve):
    if not isinstance(curve, Curve):
        raise TypeError("Expected a Curve instance")
    self.curves.append(curve)

def create_dataframe(self) -> pd.DataFrame:
    rows = []
    for curve in self.curves:
        first_row = True
        for bucket in curve.buckets:
            row = {
                "Curve": curve.name if first_row else "",
                "Limit": curve.limit if first_row else "",
                "Total Consumption": curve.total_consumption if first_row else "",
                "Bucket": bucket.bucket_name,
                "Bucket Consumption": bucket.consumption,
                "% Bucket of Total": round((bucket.consumption / curve.total_consumption) * 100, 1)
            }
            rows.append(row)
            first_row = False
    return pd.DataFrame(rows)

Example Usage (for testing purpose)

if name == "main": portfolio = Portfolio()

curve_a = Curve('Curve A', 1000, 750)
curve_a.add_bucket(MaturityBucket('<1Y', 200))
curve_a.add_bucket(MaturityBucket('1-3Y', 150))
curve_a.add_bucket(MaturityBucket('3-5Y', 250))
curve_a.add_bucket(MaturityBucket('>5Y', 150))
portfolio.add_curve(curve_a)

curve_b = Curve('Curve B', 1200, 950)
curve_b.add_bucket(MaturityBucket('<1Y', 400))
curve_b.add_bucket(MaturityBucket('1-3Y', 200))
curve_b.add_bucket(MaturityBucket('3-5Y', 150))
curve_b.add_bucket(MaturityBucket('>5Y', 200))
portfolio.add_curve(curve_b)

# Generate DataFrame
df = portfolio.create_dataframe()
print(df)

# Optional: Export to Excel
df.to_excel("curve_consumption.xlsx", index=False)



import pytest from curve_maturity_oop import MaturityBucket, Curve, Portfolio

---------- Test MaturityBucket ----------

def test_bucket_creation(): bucket = MaturityBucket("<1Y", 100) assert bucket.bucket_name == "<1Y" assert bucket.consumption == 100

def test_bucket_negative_consumption(): with pytest.raises(ValueError): MaturityBucket("1-3Y", -50)

---------- Test Curve ----------

def test_curve_creation(): curve = Curve("Curve A", 1000, 800) assert curve.name == "Curve A" assert curve.limit == 1000 assert curve.total_consumption == 800 assert curve.buckets == []

def test_curve_invalid_limit(): with pytest.raises(ValueError): Curve("Invalid Curve", 0, 500)

def test_curve_negative_consumption(): with pytest.raises(ValueError): Curve("Invalid Curve", 1000, -500)

def test_add_valid_bucket(): curve = Curve("Test Curve", 1000, 500) bucket = MaturityBucket("3-5Y", 200) curve.add_bucket(bucket) assert len(curve.buckets) == 1 assert curve.buckets[0].bucket_name == "3-5Y"

def test_add_invalid_bucket(): curve = Curve("Test Curve", 1000, 500) with pytest.raises(TypeError): curve.add_bucket("not a bucket")

---------- Test Portfolio ----------

def test_portfolio_add_curve(): portfolio = Portfolio() curve = Curve("Curve A", 1000, 800) portfolio.add_curve(curve) assert len(portfolio.curves) == 1 assert portfolio.curves[0].name == "Curve A"

def test_portfolio_dataframe(): portfolio = Portfolio() curve = Curve("Curve A", 1000, 800) curve.add_bucket(MaturityBucket("<1Y", 200)) curve.add_bucket(MaturityBucket("1-3Y", 100)) portfolio.add_curve(curve) df = portfolio.create_dataframe() assert len(df) == 2 assert df.iloc[0]["Curve"] == "Curve A" assert df.iloc[1]["Curve"] == "" assert df.iloc[0]["Bucket"] == "<1Y" assert df.iloc[1]["Bucket"] == "1-3Y"





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
