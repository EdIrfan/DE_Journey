# we will be using pandas to read the csv file and also import logging module to log messages
# logging are of five levels: debug, info, warning, error and critical
import pandas as pd
import logging
import sys
import os

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================
# Logging replaces print(). It's the industry standard in Data Engineering.
# Five levels exist: DEBUG, INFO, WARNING, ERROR, CRITICAL
# We use INFO as default level—shows INFO, WARNING, ERROR, CRITICAL but hides DEBUG.
#
# Two handlers are set up:
# 1. FileHandler -> writes to 'validation_report.log' (audit trail)
# 2. StreamHandler -> prints to console (immediate feedback)
script_dir = os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(script_dir, 'validation_report.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

EXPECTED_SCHEMA = {
    'Sprint': 'object',
    'Sprint_Name': 'object',
    'Phase': 'object',
    'Story_ID': 'object',
    'Story_Name': 'object',
    'Type': 'object',
    'Description': 'object',
    'Tasks': 'object',
    'Est_Hours': 'float64',
    'Resources': 'object',
    'Acceptance_Criteria': 'object'
}

VALID_VALUES = {
    'Phase': [
        'Foundation', 'Core Tools', 'Capstone', 'Revision', 'Advanced Concepts',
        'Software Engineering', 'Cloud & Infrastructure', 'Interview Prep'
    ],
    'Type': [
        'Setup', 'Study', 'Practice', 'Design', 'Implementation', 'Documentation',
        'Interview Prep', 'Job Search', 'Project', 'Study & Practice', 'Revision'
    ]
}

MIN_EST_HOURS = 0.5
MAX_EST_HOURS = 100

# ============================================================================
# FUNCTION 1: SCHEMA VALIDATION
# ============================================================================

def validate_schema(df):
    """
    Checks if columns and data types match expectations.
    """
    logger.info("=" * 70)
    logger.info("STEP 1: SCHEMA VALIDATION")
    logger.info("=" * 70)

    issues = 0

    # Check for missing columns
    missing_cols = set(EXPECTED_SCHEMA.keys()) - set(df.columns)
    if missing_cols:
        logger.error(f"MISSING COLUMNS: {missing_cols}")
        issues += len(missing_cols)
    else:
        logger.info("  [OK] All expected columns are present")

    # Check data types for each column
    for col, expected_type in EXPECTED_SCHEMA.items():
        if col not in df.columns:
            continue

        actual_type = str(df[col].dtype)
        # Check if expected type is in actual type name
        if expected_type in actual_type:
            logger.info(f"  [OK] '{col}' -> {actual_type} (correct)")
        else:
            logger.warning(f"  [FAIL] '{col}' -> Expected {expected_type}, got {actual_type}")
            issues += 1

    if issues == 0:
        logger.info("Result: PASSED\n")
    else:
        logger.warning(f"Result: FAILED with {issues} issue(s)\n")

# ============================================================================
# FUNCTION 2: NULL VALUES CHECK
# ============================================================================

def check_nulls(df):
    """
    Finds missing (NULL) values in the dataset.
    """
    logger.info("=" * 70)
    logger.info("STEP 2: NULL VALUE CHECK")
    logger.info("=" * 70)

    null_counts = df.isnull().sum()
    cols_with_nulls = null_counts[null_counts > 0]

    if not cols_with_nulls.empty:
        logger.warning(f"Found NULL values in {len(cols_with_nulls)} column(s):")
        for col, count in cols_with_nulls.items():
            pct = (count / len(df)) * 100
            logger.warning(f"  - '{col}': {count} NULLs ({pct:.1f}% of rows)")
            null_row_indices = df[df[col].isnull()].index.tolist()
            logger.debug(f"    Row indices: {null_row_indices}")
        logger.warning("Result: FAILED (NULLs found)\n")
    else:
        logger.info("  [OK] No NULL values found")
        logger.info("Result: PASSED\n")

# ============================================================================
# FUNCTION 3: FULL ROW DUPLICATE CHECK
# ============================================================================

def check_full_duplicates(df):
    """
    Finds rows where ALL columns are identical.
    """
    logger.info("=" * 70)
    logger.info("STEP 3: FULL ROW DUPLICATE CHECK")
    logger.info("=" * 70)

    full_dupes_mask = df.duplicated(keep=False)
    n_full_dupes = full_dupes_mask.sum()

    if n_full_dupes > 0:
        logger.warning(f"Found {n_full_dupes} rows that are complete duplicates!")
        dupe_df = df[full_dupes_mask].sort_values(by=list(df.columns))
        logger.warning(f"Duplicate rows:\n{dupe_df.to_string()}")
        logger.warning("Result: FAILED (duplicates found)\n")
    else:
        logger.info("  [OK] No full row duplicates found")
        logger.info("Result: PASSED\n")

# ============================================================================
# FUNCTION 4: PRIMARY KEY DUPLICATE CHECK
# ============================================================================

def check_key_duplicates(df):
    """
    Checks if primary key column (Story_ID) has duplicates.
    """
    logger.info("=" * 70)
    logger.info("STEP 4: PRIMARY KEY DUPLICATE CHECK")
    logger.info("=" * 70)

    if 'Story_ID' not in df.columns:
        logger.warning("  Column 'Story_ID' not found. Skipping this check.")
        logger.info("Result: SKIPPED\n")
        return

    key_dupes_mask = df.duplicated(subset=['Story_ID'], keep=False)
    n_key_dupes = key_dupes_mask.sum()

    if n_key_dupes > 0:
        logger.error(f"CRITICAL: Found {n_key_dupes} rows with duplicate Story_IDs!")
        dupe_ids = df[key_dupes_mask]['Story_ID'].value_counts()
        logger.error("Duplicate Story_IDs and their counts:")
        for story_id, count in dupe_ids.items():
            logger.error(f"  - '{story_id}' appears {count} times")
        logger.error(f"Rows with duplicate Story_IDs:\n{df[key_dupes_mask].to_string()}")
        logger.error("Result: FAILED (primary key violation)\n")
    else:
        logger.info("  [OK] All Story_IDs are unique")
        logger.info("Result: PASSED\n")

# ============================================================================
# FUNCTION 5: BUSINESS LOGIC VALIDATION
# ============================================================================

def validate_business_logic(df):
    """
    Checks data against business rules (domain-specific validation).
    """
    logger.info("=" * 70)
    logger.info("STEP 5: BUSINESS LOGIC VALIDATION")
    logger.info("=" * 70)

    issues_found = False

    # CHECK 1: Estimate Hours in valid range
    logger.info("Checking Est_Hours range...")
    if 'Est_Hours' in df.columns:
        invalid_hours = df[
            (df['Est_Hours'] < MIN_EST_HOURS) | (df['Est_Hours'] > MAX_EST_HOURS)
        ]
        if not invalid_hours.empty:
            logger.warning(f"  Found {len(invalid_hours)} rows with invalid Est_Hours:")
            for idx, row in invalid_hours.iterrows():
                logger.warning(f"    Row {idx}: Story_ID '{row['Story_ID']}' has Est_Hours = {row['Est_Hours']}")
            issues_found = True
        else:
            logger.info("  [OK] All Est_Hours values are in valid range")

    # CHECK 2: Phase enum validation
    logger.info("Checking Phase enum values...")
    if 'Phase' in df.columns:
        invalid_phases = df[~df['Phase'].isin(VALID_VALUES['Phase'])]
        if not invalid_phases.empty:
            logger.warning(f"  Found {len(invalid_phases)} rows with invalid Phase:")
            for idx, row in invalid_phases.iterrows():
                logger.warning(f"    Row {idx}: Story_ID '{row['Story_ID']}' has Phase = '{row['Phase']}'")
            issues_found = True
        else:
            logger.info("  [OK] All Phase values are valid")

    # CHECK 3: Type enum validation
    logger.info("Checking Type enum values...")
    if 'Type' in df.columns:
        invalid_types = df[~df['Type'].isin(VALID_VALUES['Type'])]
        if not invalid_types.empty:
            logger.warning(f"  Found {len(invalid_types)} rows with invalid Type:")
            for idx, row in invalid_types.iterrows():
                logger.warning(f"    Row {idx}: Story_ID '{row['Story_ID']}' has Type = '{row['Type']}'")
            issues_found = True
        else:
            logger.info("  [OK] All Type values are valid")

    if not issues_found:
        logger.info("Result: PASSED\n")
    else:
        logger.warning("Result: FAILED (business logic violations found)\n")

# ============================================================================
# FUNCTION 6: DATA QUALITY SCORE (Advanced Learning)
# ============================================================================

def calculate_data_quality_score(df):
    """
    Calculates an overall Data Quality Score (0-100).
    """
    logger.info("=" * 70)
    logger.info("STEP 6: DATA QUALITY SCORE (Advanced)")
    logger.info("=" * 70)

    score = 100.0
    deductions = {}

    # Deduction 1: NULL percentage
    total_cells = len(df) * len(df.columns)
    null_cells = df.isnull().sum().sum()
    null_ratio = (null_cells / total_cells) * 100 if total_cells > 0 else 0
    null_deduction = min(20, null_ratio / 5)
    score -= null_deduction
    deductions['NULLs'] = null_deduction

    # Deduction 2: Full duplicate rows
    dupe_ratio = (df.duplicated().sum() / len(df)) * 100 if len(df) > 0 else 0
    dupe_deduction = min(15, dupe_ratio / 6.67)
    score -= dupe_deduction
    deductions['Duplicates'] = dupe_deduction

    # Deduction 3: Invalid Phase values
    if 'Phase' in df.columns:
        invalid_phase_ratio = (len(df[~df['Phase'].isin(VALID_VALUES['Phase'])]) / len(df)) * 100
        phase_deduction = min(15, invalid_phase_ratio / 6.67)
        score -= phase_deduction
        deductions['Invalid Phase'] = phase_deduction

    # Deduction 4: Invalid hours
    if 'Est_Hours' in df.columns:
        invalid_hours_count = len(df[
            (df['Est_Hours'] < MIN_EST_HOURS) | (df['Est_Hours'] > MAX_EST_HOURS)
        ])
        invalid_hours_ratio = (invalid_hours_count / len(df)) * 100
        hours_deduction = min(10, invalid_hours_ratio / 10)
        score -= hours_deduction
        deductions['Invalid Hours'] = hours_deduction

    score = max(0, score)

    logger.info(f"Quality Score: {score:.1f}/100")

    if score >= 90:
        rating = "EXCELLENT"
    elif score >= 70:
        rating = "GOOD"
    elif score >= 50:
        rating = "FAIR (needs work)"
    else:
        rating = "POOR (critical issues)"

    logger.info(f"Rating: {rating}")

    logger.info("\nDeduction Breakdown:")
    for category, deduction in deductions.items():
        if deduction > 0:
            logger.info(f"  - {category}: -{deduction:.1f} points")

    logger.info("Result: DATA QUALITY SCORE CALCULATED\n")

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """
    Main orchestrator. Runs all validation steps sequentially.
    """
    logger.info("\n" + "=" * 114)
    logger.info("DATA VALIDATION PIPELINE STARTED")
    logger.info("=" * 70 + "\n")

    filename = os.path.join(script_dir, 'test_data.csv')

    try:
        logger.info(f"Loading CSV: '{filename}'")
        df = pd.read_csv(filename, sep=',')
        logger.info("File loaded successfully")
        logger.info(f"  Shape: {df.shape[0]} rows x {df.shape[1]} columns\n")

        validate_schema(df)
        check_nulls(df)
        check_full_duplicates(df)
        check_key_duplicates(df)
        validate_business_logic(df)
        calculate_data_quality_score(df)

        logger.info("=" * 70)
        logger.info("VALIDATION PIPELINE COMPLETED")
        logger.info("=" * 70)
        logger.info("Full report saved to: validation_report.log\n")

    except FileNotFoundError:
        logger.critical(f"ERROR: File '{filename}' not found!")
        logger.critical("Make sure test_data.csv is in the current directory.")
        sys.exit(1)

    except pd.errors.EmptyDataError:
        logger.critical(f"ERROR: File '{filename}' is empty!")
        sys.exit(1)

    except Exception as e:
        logger.critical(f"UNEXPECTED ERROR: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
