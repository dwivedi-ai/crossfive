# stereotype_quiz_app/app.py
# Version 3.1: Cross-State Stereotype Annotation Quiz (User Selects 5 States, getlist fix)

import os
import csv
import io         # For in-memory file handling
import random     # To shuffle states
import mysql.connector
from mysql.connector import Error as MySQLError
from flask import (Flask, render_template, request, redirect, url_for, g,
                   flash, Response, send_file, session)
import pandas as pd
import numpy as np
import traceback # For detailed error logging
from datetime import datetime # To timestamp processed download filename
from werkzeug.datastructures import ImmutableMultiDict # Import for empty form data

# --- Configuration ---
# CSV Definitions File (relative to app.py)
CSV_FILE_PATH = os.path.join('data', 'stereotypes.csv')
# Schema file (assuming schema.sql is correct for results_cross and familiarity_ratings)
SCHEMA_FILE = 'schema.sql'

# Flask Secret Key (Essential for session management - CHANGE IN PRODUCTION)
SECRET_KEY = 'respai' # As provided by user

# --- MySQL Configuration ---
MYSQL_HOST = 'localhost'
MYSQL_USER = 'stereotype_user'
MYSQL_PASSWORD = 'RespAI@2025' # As provided by user
MYSQL_DB = 'stereotype_cross' # Using the cross-state DB
MYSQL_PORT = 3306
# Define table names used in this version
RESULTS_TABLE_CROSS = 'results_cross'
FAMILIARITY_TABLE = 'familiarity_ratings'

# Number of states user must select
NUM_STATES_TO_SELECT = 5

# --- Flask App Initialization ---
app = Flask(__name__)
app.config['SECRET_KEY'] = SECRET_KEY
app.config['MYSQL_HOST'] = MYSQL_HOST
app.config['MYSQL_USER'] = MYSQL_USER
app.config['MYSQL_PASSWORD'] = MYSQL_PASSWORD
app.config['MYSQL_DB'] = MYSQL_DB
app.config['MYSQL_PORT'] = MYSQL_PORT
# Optional: Configure session cookie settings for production
# app.config['SESSION_COOKIE_SECURE'] = True
# app.config['SESSION_COOKIE_HTTPONLY'] = True
# app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'

# --- Database Functions ---
def get_db():
    """Opens a new MySQL database connection and cursor if none exist for the current request context."""
    if 'db' not in g:
        try:
            g.db = mysql.connector.connect(
                host=app.config['MYSQL_HOST'], user=app.config['MYSQL_USER'],
                password=app.config['MYSQL_PASSWORD'], database=app.config['MYSQL_DB'],
                port=app.config['MYSQL_PORT'], autocommit=False # Disable autocommit
            )
            g.cursor = g.db.cursor(dictionary=True) # Use dictionary cursor
        except MySQLError as err:
            print(f"FATAL: Error connecting to MySQL: {err}")
            flash('Database connection error. Please contact admin.', 'error')
            g.db = None; g.cursor = None
    return getattr(g, 'cursor', None)

@app.teardown_appcontext
def close_db(error):
    """Closes the database cursor and connection at the end of the request."""
    cursor = g.pop('cursor', None)
    if cursor: cursor.close()
    db = g.pop('db', None)
    if db and hasattr(db, 'is_connected') and db.is_connected(): db.close()
    if error: print(f"App context teardown error: {error}")

def init_db():
    """Initializes the database schema if tables are missing."""
    temp_conn = None; temp_cursor = None
    print(f"Checking database '{app.config['MYSQL_DB']}' setup...")
    try:
        # Ensure DB exists
        try:
             temp_conn = mysql.connector.connect(
                host=app.config['MYSQL_HOST'], user=app.config['MYSQL_USER'],
                password=app.config['MYSQL_PASSWORD'], port=app.config['MYSQL_PORT']
            )
             temp_cursor = temp_conn.cursor()
             temp_cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{app.config['MYSQL_DB']}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
             print(f"Database '{app.config['MYSQL_DB']}' ensured.")
             temp_cursor.close(); temp_conn.close()
        except MySQLError as db_err:
             print(f"Warning: Could not ensure database exists (might need permissions or manual creation): {db_err}")

        # Connect to target DB for schema execution
        temp_conn = mysql.connector.connect(
            host=app.config['MYSQL_HOST'], user=app.config['MYSQL_USER'],
            password=app.config['MYSQL_PASSWORD'], database=app.config['MYSQL_DB'],
            port=app.config['MYSQL_PORT']
        )
        temp_cursor = temp_conn.cursor()
        base_dir = os.path.dirname(os.path.abspath(__file__))
        schema_path = os.path.join(base_dir, SCHEMA_FILE)

        try:
            with open(schema_path, mode='r', encoding='utf-8') as f: sql_script = f.read()
            # Split script into statements, ignore comments and empty lines
            sql_statements = [s.strip() for s in sql_script.split(';') if s.strip() and not s.strip().startswith('--')]
            if not sql_statements:
                print(f"Warning: No executable SQL statements found in {schema_path}")
            else:
                print(f"Executing {len(sql_statements)} statements from schema '{SCHEMA_FILE}'...")
                for statement in sql_statements:
                    try:
                        # print(f"Executing: {statement[:80]}...") # Optional debug print
                        temp_cursor.execute(statement)
                    except MySQLError as stmt_err:
                        # Handle expected errors gracefully if using 'IF NOT EXISTS'
                        print(f"Warning/Error executing statement (often OK if using 'IF NOT EXISTS'): {stmt_err}")
                        # Consider raising error for critical failures like syntax errors
                        # if "syntax error" in str(stmt_err): raise stmt_err
            temp_conn.commit() # Commit only after all statements succeed
            print(f"Database tables from '{SCHEMA_FILE}' created/verified successfully in '{app.config['MYSQL_DB']}'.")
        except FileNotFoundError:
            print(f"FATAL ERROR: Schema file '{schema_path}' not found.")
            raise # Re-raise critical error
        except MySQLError as err:
            print(f"FATAL ERROR executing schema file '{SCHEMA_FILE}': {err}")
            if temp_conn and temp_conn.is_connected(): temp_conn.rollback() # Rollback partial changes
            raise # Re-raise critical error
        except Exception as e:
            print(f"FATAL: Unexpected error initializing schema from file: {e}")
            if temp_conn and temp_conn.is_connected(): temp_conn.rollback()
            raise # Re-raise critical error
    except MySQLError as conn_err:
        # Error connecting to the specific database
        print(f"FATAL: Error connecting to database '{app.config['MYSQL_DB']}' during init: {conn_err}")
        raise # Re-raise essential connection errors
    finally:
        if temp_cursor: temp_cursor.close()
        if temp_conn and temp_conn.is_connected(): temp_conn.close()

# --- Data Loading Function ---
def load_stereotype_data(relative_filepath=CSV_FILE_PATH):
    """Loads stereotype definitions from the CSV."""
    stereotype_data = []
    base_dir = os.path.dirname(os.path.abspath(__file__))
    full_filepath = os.path.join(base_dir, relative_filepath)
    print(f"Attempting to load stereotype data from: {full_filepath}")
    try:
        if not os.path.exists(full_filepath):
            raise FileNotFoundError(f"File not found: {full_filepath}")
        with open(full_filepath, mode='r', encoding='utf-8-sig') as infile: # Handle potential BOM
            reader = csv.DictReader(infile)
            required_cols = ['State', 'Category', 'Superset', 'Subsets']
            # Check headers exist and contain required columns
            if not reader.fieldnames or not all(field in reader.fieldnames for field in required_cols):
                 missing = [c for c in required_cols if c not in (reader.fieldnames or [])]
                 raise ValueError(f"CSV missing required columns: {missing}. Found columns: {reader.fieldnames}")

            for i, row in enumerate(reader):
                try:
                    # Use .get with default empty string and strip whitespace
                    state = row.get('State','').strip()
                    category = row.get('Category','Uncategorized').strip()
                    superset = row.get('Superset','').strip()
                    subsets_str = row.get('Subsets','') # Default to empty string

                    # Basic validation: Ensure essential fields are not empty
                    if not state or not superset:
                        print(f"Warning: Skipping CSV row {i+1} due to missing State or Superset. Row: {row}")
                        continue

                    # Process subsets: split, strip, filter empty, sort
                    subsets = sorted([s.strip() for s in subsets_str.split(',') if s.strip()])

                    stereotype_data.append({
                        'state': state,
                        'category': category if category else 'Uncategorized', # Ensure category value
                        'superset': superset,
                        'subsets': subsets
                        })
                except Exception as row_err:
                    print(f"Error processing CSV row {i+1}: {row_err}. Row data: {row}");
                    continue # Skip problematic row

        print(f"Successfully loaded {len(stereotype_data)} stereotype entries from {full_filepath}")
        return stereotype_data
    except FileNotFoundError:
        print(f"FATAL ERROR: CSV file not found at {full_filepath}. App may not function correctly.");
        return [] # Return empty list to prevent crash, but log error
    except ValueError as ve:
        print(f"FATAL ERROR processing CSV structure: {ve}");
        return []
    except Exception as e:
        print(f"FATAL ERROR loading data: {e}\n{traceback.format_exc()}");
        return []

# --- Load Data & Get List of States ---
ALL_STEREOTYPE_DATA = load_stereotype_data()
ALL_DEFINED_STATES = sorted(list(set(item['state'] for item in ALL_STEREOTYPE_DATA)))
if not ALL_STEREOTYPE_DATA or not ALL_DEFINED_STATES:
    print("\nCRITICAL ERROR: Stereotype data loading failed or no states found! App cannot function.\n")
    ALL_STEREOTYPE_DATA = []
    ALL_DEFINED_STATES = ["Error: Check Logs"] # Signal error to template
else:
     print(f"All defined States/UTs available for selection: {ALL_DEFINED_STATES}")


# --- Flask Routes ---

@app.route('/', methods=['GET', 'POST'])
def index():
    """Handles user info form AND state selection."""
    if request.method == 'POST':
        user_name = request.form.get('name', '').strip()
        native_state = request.form.get('native_state')
        user_age_str = request.form.get('age')
        user_sex = request.form.get('sex')
        # Get list of selected target states from checkboxes
        selected_target_states = request.form.getlist('selected_states')

        errors = False
        # --- Basic Info Validation ---
        if not user_name: flash('Name is required.', 'error'); errors = True
        if not native_state or native_state not in ALL_DEFINED_STATES:
            flash('Please select your valid native state.', 'error'); errors = True
        if not user_sex: flash('Please select your sex.', 'error'); errors = True
        user_age = None
        if user_age_str:
            try:
                user_age = int(user_age_str)
                if user_age <= 0 or user_age > 120: # Basic sanity check
                     flash('Please enter a realistic age.', 'error'); errors = True
            except ValueError: flash('Please enter a valid number for age.', 'error'); errors = True
        else:
            # Make age required as per template change
            flash('Age is required.', 'error'); errors = True

        # --- Target State Selection Validation ---
        if "Error: Check Logs" in ALL_DEFINED_STATES:
             flash("Error loading state data. Cannot validate selection.", 'error'); errors = True
        elif len(selected_target_states) != NUM_STATES_TO_SELECT:
             flash(f'Please select exactly {NUM_STATES_TO_SELECT} target states/UTs.', 'error'); errors = True
        elif native_state in selected_target_states:
             # This check is important server-side, even if JS prevents it
             flash('You cannot select your native state as a target state. Please uncheck it.', 'error'); errors = True
        else:
             # Verify all selected states are actually in our master list
             invalid_states = [s for s in selected_target_states if s not in ALL_DEFINED_STATES]
             if invalid_states:
                  flash(f'Invalid target state(s) selected: {", ".join(invalid_states)}. Please refresh and try again.', 'error'); errors = True

        if errors:
            # Pass the actual request.form back to the template to repopulate fields,
            # including the selected checkboxes via form_data.getlist()
            return render_template('index.html',
                                   states=ALL_DEFINED_STATES,
                                   form_data=request.form) # Pass request.form on error

        # --- Setup Session for the Quiz ---
        session.clear()
        session['user_name'] = user_name
        session['native_state'] = native_state
        session['user_age'] = user_age
        session['user_sex'] = user_sex

        # Use the validated selected states
        target_states = selected_target_states
        random.shuffle(target_states) # Shuffle the order of the 5 selected states
        session['target_states'] = target_states
        session['current_state_index'] = 0

        print(f"User '{user_name}' (Native: {native_state}, Age: {user_age}, Sex: {user_sex}) starting quiz.")
        print(f"Selected target states ({len(target_states)}): {target_states}")
        return redirect(url_for('quiz_cross'))

    # --- GET request ---
    session.clear() # Clear session on fresh visit
    # Pass an empty ImmutableMultiDict instead of an empty dict for template compatibility
    empty_form_data = ImmutableMultiDict()
    return render_template('index.html',
                           states=ALL_DEFINED_STATES,
                           form_data=empty_form_data) # Use the empty MultiDict


@app.route('/quiz', methods=['GET', 'POST'])
def quiz_cross():
    """Handles the sequential display and submission for each SELECTED target state."""

    # --- Validate Session ---
    required_session_keys = ['user_name', 'native_state', 'target_states', 'current_state_index']
    if not all(key in session for key in required_session_keys):
        flash('Your session has expired or is invalid. Please start again.', 'warning')
        print(f"Warning: Invalid session state access attempt. Missing keys: {[k for k in required_session_keys if k not in session]}")
        return redirect(url_for('index'))

    target_states = session['target_states'] # This now holds the 5 selected states
    current_index = session['current_state_index']

    # --- Check if Quiz is Complete ---
    if current_index >= len(target_states): # Check if all 5 selected states are done
        print(f"User '{session.get('user_name', 'Unknown')}' finished all {len(target_states)} selected states.")
        return redirect(url_for('thank_you')) # Go to thank you page

    # Current target state based on index stored in session
    current_target_state = target_states[current_index]

    # --- Handle POST Request (Saving data for the state just completed) ---
    if request.method == 'POST':
        print(f"POST received for state index {current_index} ({current_target_state}) by user '{session.get('user_name')}'")
        cursor = get_db()
        if not cursor:
             flash('Database connection failed. Cannot save progress.', 'error')
             # Redirect back to GET for the same state, data won't be saved.
             return redirect(url_for('quiz_cross'))

        db_connection = g.db # Use the connection from g for transaction control

        try:
            # --- Retrieve & Validate Familiarity Rating ---
            familiarity_rating_str = request.form.get('familiarity_rating')
            familiarity_rating = None # Initialize
            if familiarity_rating_str is None:
                 # This should ideally be caught by client-side validation (required field)
                 flash('Error: Familiarity rating was not submitted. Please select a rating.', 'error')
                 print(f"Error: Familiarity rating missing in POST for {current_target_state}.")
                 # Stay on the same page (no db changes yet)
                 return redirect(url_for('quiz_cross'))

            try:
                familiarity_rating = int(familiarity_rating_str)
                if not (0 <= familiarity_rating <= 5): raise ValueError("Rating out of range")
            except (ValueError, TypeError):
                flash('Invalid familiarity rating submitted. Please select a value between 0 and 5.', 'error')
                print(f"Invalid familiarity rating received: '{familiarity_rating_str}' for {current_target_state}")
                return redirect(url_for('quiz_cross')) # Stay on same page

            # --- Save Familiarity Rating ---
            fam_sql = f"""
                INSERT INTO {FAMILIARITY_TABLE}
                (native_state, target_state, familiarity_rating, user_name, user_age, user_sex)
                VALUES (%(native_state)s, %(target_state)s, %(rating)s, %(name)s, %(age)s, %(sex)s)
            """
            fam_data = {
                'native_state': session['native_state'],
                'target_state': current_target_state,
                'rating': familiarity_rating,
                'name': session['user_name'],
                'age': session.get('user_age'), # Get potentially None value
                'sex': session.get('user_sex')  # Get potentially None value
            }
            cursor.execute(fam_sql, fam_data)
            print(f"Saved familiarity rating ({familiarity_rating}) for {current_target_state}")

            # --- Save Stereotype Annotations ---
            annotations_to_insert = []
            # Get the number of items PRESENTED on the previous page
            num_items_on_page_str = request.form.get('num_quiz_items')
            num_items_on_page = 0 # Default
            if num_items_on_page_str is None:
                 print(f"CRITICAL ERROR: Hidden input 'num_quiz_items' not found in form submission for state {current_target_state}.")
                 flash("A form processing error occurred (missing item count). Please try again.", 'error')
                 db_connection.rollback() # Rollback the familiarity rating insert
                 return redirect(url_for('quiz_cross')) # Stay on same page
            try:
                 num_items_on_page = int(num_items_on_page_str)
            except ValueError:
                 print(f"CRITICAL ERROR: Invalid value received for 'num_quiz_items': '{num_items_on_page_str}'")
                 flash("A form processing error occurred (invalid item count). Please try again.", 'error')
                 db_connection.rollback() # Rollback the familiarity rating insert
                 return redirect(url_for('quiz_cross')) # Stay on same page

            validation_passed_backend = True # Flag for backend validation
            for i in range(num_items_on_page): # Iterate based on items presented
                identifier = str(i) # Index used in the template loop

                # Retrieve data for this specific item
                superset = request.form.get(f'superset_{identifier}')
                category = request.form.get(f'category_{identifier}')
                annotation = request.form.get(f'annotation_{identifier}')
                rating_str = request.form.get(f'offensiveness_{identifier}')

                # Basic backend validation: Ensure core annotation data isn't missing
                if not annotation or not superset or not category:
                     print(f"Backend Validation Error: Missing core data (annotation/superset/category) for item index {identifier} on state {current_target_state}. Data: A='{annotation}', S='{superset}', C='{category}'")
                     flash(f"Incomplete data received for one of the items for {current_target_state}. Submission cancelled.", 'error')
                     db_connection.rollback() # Rollback familiarity insert too
                     validation_passed_backend = False # Set flag
                     break # Stop processing items for this state

                # Process Offensiveness Rating only if annotation is 'Stereotype'
                offensiveness = -1 # Default value as per schema
                if annotation == 'Stereotype':
                    if rating_str is not None:
                        try:
                            offensiveness = int(rating_str)
                            if not (0 <= offensiveness <= 5): # Validate range
                                print(f"Warning: Invalid offensiveness rating value ({offensiveness}) for item index {identifier}. Defaulting to -1.")
                                offensiveness = -1 # Reset to default if out of range
                        except (ValueError, TypeError):
                             print(f"Warning: Non-integer offensiveness rating ('{rating_str}') for item index {identifier}. Defaulting to -1.")
                             offensiveness = -1 # Reset to default if conversion fails
                    else:
                        # This *should* be blocked by client-side JS validation if required=true is working
                        print(f"ERROR: Offensiveness rating missing for Stereotype item index {identifier} despite client validation (indicates potential JS/HTML issue). Defaulting to -1.")
                        flash("Error: Offensiveness rating missing for a 'Stereotype' item. Submission cancelled.", 'error')
                        db_connection.rollback()
                        validation_passed_backend = False
                        break # Stop processing

                # Only append if no validation error occurred within the loop
                if validation_passed_backend:
                    annotations_to_insert.append({
                        'native_state': session['native_state'],
                        'target_state': current_target_state,
                        'user_name': session['user_name'],
                        'user_age': session.get('user_age'), # Handles None
                        'user_sex': session.get('user_sex'), # Handles None
                        'category': category,
                        'attribute_superset': superset,
                        'annotation': annotation,
                        'offensiveness_rating': offensiveness
                    })
            # End loop through quiz items

            if not validation_passed_backend: # If loop was broken due to error
                 return redirect(url_for('quiz_cross')) # Redirect back to the same page

            # --- Save Annotations if any were successfully processed ---
            if annotations_to_insert:
                # Optional: Check if count matches expected, log warning if not
                if len(annotations_to_insert) != num_items_on_page:
                     print(f"Warning: Annotation count mismatch for {current_target_state}. Expected {num_items_on_page}, saved {len(annotations_to_insert)}.")

                results_sql = f"""
                    INSERT INTO {RESULTS_TABLE_CROSS}
                    (native_state, target_state, user_name, user_age, user_sex, category, attribute_superset, annotation, offensiveness_rating)
                    VALUES (%(native_state)s, %(target_state)s, %(user_name)s, %(user_age)s, %(user_sex)s, %(category)s, %(attribute_superset)s, %(annotation)s, %(offensiveness_rating)s)
                """
                try:
                    cursor.executemany(results_sql, annotations_to_insert)
                    print(f"Saved {len(annotations_to_insert)} annotations for {current_target_state}")
                except MySQLError as exec_many_err:
                    print(f"DB Error during annotations batch insert for {current_target_state}: {exec_many_err}")
                    print(f"Data sample (first record): {annotations_to_insert[0] if annotations_to_insert else 'N/A'}")
                    flash("Database error saving your detailed responses. Please try again.", 'error');
                    db_connection.rollback() # Rollback everything for this state
                    return redirect(url_for('quiz_cross')) # Stay on the same page

            elif num_items_on_page > 0: # If there were items but list is empty (all failed validation?)
                 print(f"Warning: No annotations were saved for state {current_target_state} despite {num_items_on_page} items presented (check backend validation logic).")
                 # Decide if this state should be considered complete or if familiarity should be rolled back.
                 # Current logic proceeds, committing only familiarity.
                 # flash("Could not save any annotations for this state due to data issues.", 'warning')
            else: # num_items_on_page was 0
                 print(f"No quiz items were presented for state {current_target_state}, skipping annotation insert.")

            # --- Commit Transaction ---
            # If we reached here without errors causing a return/redirect, commit the transaction
            db_connection.commit()
            print(f"Committed transaction successfully for state {current_target_state}")

            # --- Advance to Next State ---
            session['current_state_index'] = current_index + 1
            session.modified = True # Explicitly mark session as modified
            print(f"Advanced user '{session.get('user_name')}' to state index {session['current_state_index']}")
            return redirect(url_for('quiz_cross'))

        except MySQLError as db_err:
             # Catch errors from the initial familiarity insert or commit
             print(f"DB Error during transaction for state {current_target_state}: {db_err}");
             print(traceback.format_exc()) # Print stack trace for DB errors
             try:
                 if db_connection and db_connection.is_connected():
                     db_connection.rollback()
                     print("Transaction rolled back due to DB error.")
             except Exception as rb_err:
                 print(f"Rollback failed after DB error: {rb_err}")
             flash("Database error saving responses. Your progress for this state was not saved. Please try again.", 'error');
             # Redirect back to GET for the *same* state to allow retry
             return redirect(url_for('quiz_cross'))

        except Exception as e:
             # Catch any other unexpected Python errors
             print(f"Unexpected Error processing POST for state {current_target_state}: {e}");
             print(traceback.format_exc()); # Print full stack trace
             try:
                 if db_connection and db_connection.is_connected():
                     db_connection.rollback()
                     print("Transaction rolled back due to unexpected error.")
             except Exception as rb_err:
                 print(f"Rollback failed after unexpected error: {rb_err}")
             flash("An unexpected error occurred processing your submission. Please restart the quiz.", 'error');
             # Redirect to index on completely unexpected errors
             return redirect(url_for('index'))


    # --- Handle GET Request (Display current state's questions) ---
    print(f"GET request for state index {current_index} ({current_target_state}) by user '{session.get('user_name')}'")

    # Filter ALL data to get only items for the current target state
    quiz_items_for_state = [item for item in ALL_STEREOTYPE_DATA if item.get('state') == current_target_state]
    # Sort items for consistent display (e.g., by category then superset)
    quiz_items_for_state.sort(key=lambda x: (x.get('category', ''), x.get('superset', '')))

    is_last_state = (current_index == len(target_states) - 1)
    num_items_to_display = len(quiz_items_for_state)

    print(f"Rendering quiz page for {current_target_state} with {num_items_to_display} items. Is last state: {is_last_state}")

    return render_template('quiz.html',
                           target_state=current_target_state,
                           quiz_items=quiz_items_for_state,
                           user_info=session, # Pass session data for context
                           current_index=current_index,
                           total_states=len(target_states), # Total is now NUM_STATES_TO_SELECT
                           is_last_state=is_last_state,
                           num_quiz_items=num_items_to_display) # Pass the correct count


@app.route('/thank_you')
def thank_you():
    """Displays the thank you page."""
    user_name = session.get('user_name', 'Participant')
    print(f"Displaying thank you page for user '{user_name}'.")
    # Optionally clear session here if the quiz is truly over and data saved
    # session.clear()
    return render_template('thank_you.html', user_name=user_name)


# --- Admin Routes ---
# !! Add proper authentication/authorization to admin routes in production !!
# Example: using a simple decorator or Flask-Login/Flask-Principal

@app.route('/admin')
# @login_required # Example if using Flask-Login
def admin_view():
    """Displays raw annotation results and familiarity ratings."""
    # Add Authentication Check Here (e.g., check session, user role)
    print("Admin view accessed.")
    cursor = get_db()
    if not cursor:
        flash('Database connection failed for admin view.', 'error')
        return redirect(url_for('index')) # Or redirect to login

    results_data = []; familiarity_data = [] # Initialize
    try:
        # Fetch annotations
        cursor.execute(f"SELECT * FROM {RESULTS_TABLE_CROSS} ORDER BY timestamp DESC, id DESC")
        results_data = cursor.fetchall() # fetchall() returns list of dicts

        # Fetch familiarity ratings
        cursor.execute(f"SELECT * FROM {FAMILIARITY_TABLE} ORDER BY timestamp DESC, id DESC")
        familiarity_data = cursor.fetchall()

        print(f"Admin view: Fetched {len(results_data)} annotations, {len(familiarity_data)} familiarity ratings.")
    except MySQLError as err:
        print(f"Error fetching admin data: {err}");
        flash('Error fetching results from database.', 'error')
    except Exception as e:
         print(f"Unexpected error fetching admin data: {e}\n{traceback.format_exc()}");
         flash('An unexpected error occurred while fetching results.', 'error')

    # Pass fetched data (even if empty) to the template
    return render_template('admin.html', results=results_data, familiarity_ratings=familiarity_data)

@app.route('/admin/download_raw_annotations')
# @login_required
def download_raw_annotations():
    """Downloads the raw annotation data (results_cross table) as CSV."""
    # AUTH CHECK HERE
    print("Raw Annotations download request.")
    db_conn_raw = None # Use separate connection for safety with pandas
    try:
        # Establish a new connection for the download process
        db_conn_raw = mysql.connector.connect(
            host=app.config['MYSQL_HOST'], user=app.config['MYSQL_USER'],
            password=app.config['MYSQL_PASSWORD'], database=app.config['MYSQL_DB'],
            port=app.config['MYSQL_PORT']
        )
        if not db_conn_raw.is_connected(): raise MySQLError("Raw Download: Failed to establish DB connection.")

        # Use pandas to read data directly into a DataFrame
        query = f"SELECT * FROM {RESULTS_TABLE_CROSS} ORDER BY timestamp DESC, id DESC"
        df = pd.read_sql_query(query, db_conn_raw)

        if df.empty:
            flash(f"Annotations table ('{RESULTS_TABLE_CROSS}') is currently empty. No data to download.", "warning")
            return redirect(url_for('admin_view'))

        # Create an in-memory buffer for the CSV data
        buffer = io.BytesIO()
        df.to_csv(buffer, index=False, encoding='utf-8-sig') # Use utf-8-sig for Excel compatibility
        buffer.seek(0) # Rewind buffer to the beginning

        print(f"Prepared CSV download for {len(df)} raw annotations.")
        # Send the buffer as a file download
        return send_file(
            buffer,
            mimetype='text/csv',
            download_name='raw_cross_annotations.csv',
            as_attachment=True
        )
    except (MySQLError, pd.errors.DatabaseError) as db_pd_err:
        print(f"DB/Pandas Error during Raw Annotations download: {db_pd_err}\n{traceback.format_exc()}");
        flash(f"Error fetching raw annotations from database: {db_pd_err}", "error")
        return redirect(url_for('admin_view'))
    except Exception as e:
        print(f"Unexpected Error during Raw Annotations download:\n{traceback.format_exc()}");
        flash(f"An unexpected error occurred preparing the raw annotations download: {e}", "error")
        return redirect(url_for('admin_view'))
    finally:
        # Ensure the separate connection is closed
        if db_conn_raw and db_conn_raw.is_connected():
            db_conn_raw.close()
            print("Raw download DB connection closed.")

@app.route('/admin/download_familiarity')
# @login_required
def download_familiarity_ratings():
    """Downloads the familiarity ratings (familiarity_ratings table) as CSV."""
    # AUTH CHECK HERE
    print("Familiarity Ratings download request.")
    db_conn_fam = None # Use separate connection
    try:
        db_conn_fam = mysql.connector.connect(
            host=app.config['MYSQL_HOST'], user=app.config['MYSQL_USER'],
            password=app.config['MYSQL_PASSWORD'], database=app.config['MYSQL_DB'],
            port=app.config['MYSQL_PORT']
        )
        if not db_conn_fam.is_connected(): raise MySQLError("Familiarity Download: Failed to establish DB connection.")

        query = f"SELECT * FROM {FAMILIARITY_TABLE} ORDER BY timestamp DESC, id DESC"
        df = pd.read_sql_query(query, db_conn_fam)

        if df.empty:
            flash(f"Familiarity ratings table ('{FAMILIARITY_TABLE}') is currently empty. No data to download.", "warning")
            return redirect(url_for('admin_view'))

        buffer = io.BytesIO()
        df.to_csv(buffer, index=False, encoding='utf-8-sig')
        buffer.seek(0)

        print(f"Prepared CSV download for {len(df)} familiarity ratings.")
        return send_file(
            buffer,
            mimetype='text/csv',
            download_name='familiarity_ratings.csv',
            as_attachment=True
        )
    except (MySQLError, pd.errors.DatabaseError) as db_pd_err:
        print(f"DB/Pandas Error during Familiarity Ratings download: {db_pd_err}\n{traceback.format_exc()}");
        flash(f"Error fetching familiarity ratings from database: {db_pd_err}", "error")
        return redirect(url_for('admin_view'))
    except Exception as e:
        print(f"Unexpected Error during Familiarity Ratings download:\n{traceback.format_exc()}");
        flash(f"An unexpected error occurred preparing the familiarity ratings download: {e}", "error")
        return redirect(url_for('admin_view'))
    finally:
        if db_conn_fam and db_conn_fam.is_connected():
            db_conn_fam.close()
            print("Familiarity download DB connection closed.")


# --- Helper Functions for Processed Data ---
def calculate_mean_offensiveness(series):
    """Calculates mean of non-negative offensiveness ratings (0-5). Filters out -1."""
    valid_ratings = series[series >= 0] # Filter out the default -1 value
    return valid_ratings.mean() if not valid_ratings.empty else np.nan

def calculate_mean_familiarity(series):
    """Calculates mean of familiarity ratings (assuming 0-5)."""
    # Ensure ratings are numeric and filter invalid entries if necessary, though DB should handle this.
    valid_ratings = pd.to_numeric(series, errors='coerce').dropna()
    valid_ratings = valid_ratings[valid_ratings >= 0] # Ensure non-negative if needed
    return valid_ratings.mean() if not valid_ratings.empty else np.nan

@app.route('/admin/download_processed_data')
# @login_required
def download_processed_data():
    """Generates and downloads aggregated/processed stereotype data dynamically."""
    # AUTH CHECK HERE
    print("Processed Data download request.")
    db_conn_proc = None
    base_dir = os.path.dirname(os.path.abspath(__file__))
    stereotypes_path = os.path.join(base_dir, CSV_FILE_PATH)

    try:
        # --- Step 1: Load Base Data ---
        # Load Stereotype Definitions (including subsets)
        if not os.path.exists(stereotypes_path):
            flash(f"Stereotypes definition file missing at expected location: {stereotypes_path}", "error")
            return redirect(url_for('admin_view'))
        try:
            stereotypes_df = pd.read_csv(stereotypes_path, encoding='utf-8-sig')
            # Ensure essential columns exist in definitions
            required_def_cols = ['State', 'Category', 'Superset', 'Subsets']
            if not all(col in stereotypes_df.columns for col in required_def_cols):
                 missing_defs = [c for c in required_def_cols if c not in stereotypes_df.columns]
                 raise ValueError(f"Definitions CSV missing columns: {missing_defs}")

            stereotypes_df['Subsets_List'] = stereotypes_df['Subsets'].fillna('').astype(str).apply(
                lambda x: sorted([s.strip() for s in x.split(',') if s.strip()])
            )
            # Create a lookup dictionary: (State, Category, Superset) -> [Subset1, Subset2]
            subset_lookup = stereotypes_df.set_index(['State', 'Category', 'Superset'])['Subsets_List'].to_dict()
            print(f"Loaded {len(stereotypes_df)} stereotype definitions for processing.")
        except Exception as csv_err:
            print(f"Error loading or processing definitions CSV '{stereotypes_path}': {csv_err}")
            flash(f"Error reading stereotype definitions file: {csv_err}", "error")
            return redirect(url_for('admin_view'))


        # Connect to DB and Load Annotation Results & Familiarity Ratings
        print("Connecting to database for processed data...")
        db_conn_proc = mysql.connector.connect(
            host=app.config['MYSQL_HOST'], user=app.config['MYSQL_USER'],
            password=app.config['MYSQL_PASSWORD'], database=app.config['MYSQL_DB'],
            port=app.config['MYSQL_PORT']
        )
        print("Loading data from database tables...")
        results_df = pd.read_sql_query(f"SELECT * FROM {RESULTS_TABLE_CROSS}", db_conn_proc)
        familiarity_df = pd.read_sql_query(f"SELECT * FROM {FAMILIARITY_TABLE}", db_conn_proc)
        print(f"Loaded {len(results_df)} annotations and {len(familiarity_df)} familiarity ratings for processing.")

        # Handle empty data cases early
        if results_df.empty:
            flash("Annotation results table is empty. Cannot generate processed data.", "warning")
            return redirect(url_for('admin_view'))

        # --- Step 2: Calculate Average Familiarity per Target State ---
        print("Calculating average familiarity per target state...")
        avg_familiarity = pd.DataFrame(columns=['Stereotype_State', 'Avg_Familiarity_Rating']) # Default empty
        if not familiarity_df.empty:
             # Ensure rating is numeric before grouping
             familiarity_df['familiarity_rating'] = pd.to_numeric(familiarity_df['familiarity_rating'], errors='coerce')
             familiarity_df.dropna(subset=['familiarity_rating'], inplace=True) # Drop rows where conversion failed

             if not familiarity_df.empty:
                 familiarity_grouped = familiarity_df.groupby('target_state')
                 avg_familiarity = familiarity_grouped.agg(
                     # Use the helper function for calculation
                     Avg_Familiarity_Rating=('familiarity_rating', calculate_mean_familiarity)
                 ).reset_index()
                 # Rename column for merging consistency
                 avg_familiarity = avg_familiarity.rename(columns={'target_state': 'Stereotype_State'})
                 print(f"Calculated average familiarity for {len(avg_familiarity)} states.")
             else:
                  print("Warning: Familiarity ratings table contained no valid numeric ratings after cleaning.")
        else:
             print("Warning: Familiarity ratings table is empty. Avg_Familiarity_Rating will be NaN.")


        # --- Step 3: Expand Annotations (Apply Superset annotation to Subsets) ---
        print("Expanding annotations to include subsets...")
        # Use the 'target_state' column from results_df as the state the stereotype is ABOUT
        results_df['Stereotype_State'] = results_df['target_state'] # Define the key state column for clarity

        expanded_rows = []
        for index, row in results_df.iterrows():
            # Ensure required fields are present in the row
            stereotype_state = row.get('Stereotype_State')
            category = row.get('category')
            superset = row.get('attribute_superset')
            annotation = row.get('annotation')
            rating = row.get('offensiveness_rating', -1) # Default to -1 if missing

            if not all([stereotype_state, category, superset, annotation]):
                print(f"Warning: Skipping expansion for result row {row.get('id', index)} due to missing key fields.")
                continue

            # Add Superset row itself
            expanded_rows.append({
                'Stereotype_State': stereotype_state, 'Category': category, 'Attribute': superset,
                'annotation': annotation, 'offensiveness_rating': rating
            })
            # Add corresponding Subset rows using the lookup
            lookup_key = (stereotype_state, category, superset)
            subsets_list = subset_lookup.get(lookup_key, [])
            for subset in subsets_list:
                expanded_rows.append({
                    'Stereotype_State': stereotype_state, 'Category': category, 'Attribute': subset,
                    'annotation': annotation, 'offensiveness_rating': rating
                })

        if not expanded_rows:
            flash("No annotations could be expanded (check data consistency or definitions). Cannot process.", "error")
            return redirect(url_for('admin_view'))

        expanded_df = pd.DataFrame(expanded_rows)
        print(f"Created {len(expanded_df)} expanded annotation rows (including originals).")

        # --- Step 4: Aggregate Annotation Data (Votes & Avg Offensiveness) ---
        print("Aggregating expanded annotation results...")
        # Ensure rating is numeric before aggregation
        expanded_df['offensiveness_rating'] = pd.to_numeric(expanded_df['offensiveness_rating'], errors='coerce')
        # Do not drop rows here, let calculate_mean_offensiveness handle NaNs/invalid (-1)

        grouped = expanded_df.groupby(['Stereotype_State', 'Category', 'Attribute'])
        aggregated_df = grouped.agg(
            Stereotype_Votes=('annotation', lambda x: (x == 'Stereotype').sum()),
            Not_Stereotype_Votes=('annotation', lambda x: (x == 'Not a Stereotype').sum()),
            Not_Sure_Votes=('annotation', lambda x: (x == 'Not sure').sum()),
            # Use helper which handles NaN and filters out -1
            Average_Offensiveness=('offensiveness_rating', calculate_mean_offensiveness)
        ).reset_index()
        aggregated_df['Average_Offensiveness'] = aggregated_df['Average_Offensiveness'].round(2) # Round after calculation
        print(f"Annotation aggregation complete. Result has {len(aggregated_df)} rows.")

        # --- Step 5: Merge Average Familiarity with Aggregated Annotations ---
        print("Merging average familiarity ratings with aggregated results...")
        # Use a left merge to keep all aggregated stereotype rows, adding familiarity where it matches
        final_df = pd.merge(
            aggregated_df,
            avg_familiarity[['Stereotype_State', 'Avg_Familiarity_Rating']], # Select only needed columns
            on='Stereotype_State',
            how='left' # Keep all rows from aggregated_df
        )
        final_df['Avg_Familiarity_Rating'] = final_df['Avg_Familiarity_Rating'].round(2) # Round after merge
        print(f"Merging complete. Final result has {len(final_df)} rows.")

        # Reorder columns for better readability in the final CSV
        final_columns_ordered = [
            'Stereotype_State', 'Category', 'Attribute',
            'Stereotype_Votes', 'Not_Stereotype_Votes', 'Not_Sure_Votes',
            'Avg_Familiarity_Rating', 'Average_Offensiveness'
        ]
        # Filter list to only include columns that actually exist in the DataFrame
        # (Handles case where familiarity might be missing)
        final_columns_ordered = [col for col in final_columns_ordered if col in final_df.columns]
        final_df = final_df[final_columns_ordered]


        # --- Step 6: Generate and Send CSV File ---
        print("Generating CSV file...")
        buffer = io.BytesIO()
        final_df.to_csv(buffer, index=False, encoding='utf-8-sig') # utf-8-sig for Excel
        buffer.seek(0)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        download_filename = f'processed_cross_stereotypes_{timestamp}.csv'
        print(f"Prepared processed data download: {download_filename}")

        return send_file(
            buffer,
            mimetype='text/csv',
            download_name=download_filename,
            as_attachment=True
        )

    except (MySQLError, pd.errors.DatabaseError) as db_pd_err:
        print(f"DB/Pandas Error during Processed Data download: {db_pd_err}\n{traceback.format_exc()}");
        flash(f"Error processing data from database: {db_pd_err}", "error")
        return redirect(url_for('admin_view'))
    except FileNotFoundError as fnf_err:
        print(f"File Not Found Error: {fnf_err}\n{traceback.format_exc()}");
        flash(f"Required data file missing: {fnf_err}", "error")
        return redirect(url_for('admin_view'))
    except KeyError as key_err:
         print(f"Key Error during processing (likely missing column in DB/CSV): {key_err}\n{traceback.format_exc()}");
         flash(f"Data processing error (missing column: {key_err}). Check DB table structures and definitions CSV.", "error")
         return redirect(url_for('admin_view'))
    except ValueError as val_err: # Catch potential errors from data processing steps
         print(f"Value Error during processing: {val_err}\n{traceback.format_exc()}");
         flash(f"Data processing error: {val_err}. Check data consistency.", "error")
         return redirect(url_for('admin_view'))
    except Exception as e:
        print(f"Unexpected Error during Processed Data download:\n{traceback.format_exc()}");
        flash(f"An unexpected error occurred processing the data: {e}", "error")
        return redirect(url_for('admin_view'))
    finally:
        # Ensure the processing database connection is closed
        if db_conn_proc and db_conn_proc.is_connected():
            db_conn_proc.close()
            print("Processed data download DB connection closed.")


# --- Main Execution ---
if __name__ == '__main__':
    print(f"----- Stereotype Cross-State Quiz App Starting (User Selects {NUM_STATES_TO_SELECT} States) -----")
    print(f"Attempting to initialize database '{MYSQL_DB}'...")
    try:
        # Use app context for init_db as it might access app config or g
        with app.app_context():
            init_db()
        print(f"Database initialization completed for '{MYSQL_DB}'.")
    except Exception as init_err:
        print(f"FATAL: Database initialization failed: {init_err}")
        print("--- APPLICATION HALTED ---")
        # Exit if DB init fails, as the app likely can't run
        import sys
        sys.exit(1) # Use a non-zero exit code to indicate failure

    print(f"Stereotype data path: {os.path.join(os.path.dirname(__file__), CSV_FILE_PATH)}")
    print(f"Schema file path: {os.path.join(os.path.dirname(__file__), SCHEMA_FILE)}")
    print("Starting Flask application...")
    # Consider setting debug=False for production deployment
    # Use host='0.0.0.0' to make it accessible on your network
    app.run(debug=True, host='0.0.0.0', port=5001)
    print("----- Stereotype Cross-State Quiz App Stopped -----")