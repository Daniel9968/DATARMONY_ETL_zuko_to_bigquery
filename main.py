import os
import json
import smtplib
import requests
import csv
import pandas as pd
from flask import Flask
from urllib.parse import urlencode
from google.cloud import bigquery
from datetime import datetime, timedelta
from google.oauth2 import service_account
from dotenv import load_dotenv

app = Flask(__name__)

def send_email(subject, body):
    """
    Sends an email with the given subject and body to the specified receiver.

    Args:
        subject (str): The subject of the email.
        body (str): The body content of the email.
    """

    sender = "notifier.datarmony@gmail.com"
    sender_pass = "wjcgvgnckisovaif"
    email_receiver = receiver.split(",") if receiver is not None else sender
    with smtplib.SMTP("smtp.gmail.com", 587) as smtp:
        smtp.ehlo()
        smtp.starttls()
        smtp.ehlo()
        smtp.login(sender, sender_pass)
        msg = f'Subject: {subject}\n\n{body}'
        smtp.sendmail(sender, email_receiver, msg=msg)

def delete_form_files(forms):
    """
    Deletes files from the './tmp' directory based on form names provided.

    Args:
        forms (list): A list of form names to delete corresponding CSV files.
    """

    for form in forms:
        form_name = form[0][0]
        file_path = f"./tmp/zuko_sessions_{form_name}.csv"
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"Deleted: {file_path}")

def flatten_session(session):
    """
    Flattens a session by converting nested fields such as attributes, last_touched_field, fields, and events 
    into individual key-value pairs or counts.

    Args:
        session (dict): The session data to be flattened.

    Returns:
        dict: A flattened version of the session with key-value pairs.
    """

    flat = session.copy()

    # Flatten attributes
    attributes = flat.pop("attributes", {})
    for k, v in attributes.items():
        flat[f"attributes_{k}"] = v

    # Flatten last_touched_field
    last_field = flat.pop("last_touched_field", {})
    for k, v in last_field.items():
        flat[f"last_touched_field_{k}"] = v

    # Flatten fields (list of dicts) -> convert to string or count
    fields = flat.pop("fields", [])
    flat["fields_count"] = len(fields)
    flat["fields_json"] = json.dumps(fields)

    # Flatten events (list of dicts) -> convert to string or count
    events = flat.pop("events", [])
    flat["events_count"] = len(events)
    flat["events_json"] = json.dumps(events)

    return flat

def fetch_sessions_for_form(form_name, form_uuid, days_back):
    """
    Fetches sessions for a specific form from the Zuko API within a specified time range.

    Args:
        form_name (str): The name of the form.
        form_uuid (str): The UUID of the form.
        days_back (int): The number of days back to fetch sessions for.

    Returns:
        list: A list of flattened session data.
    """

    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=int(days_back))

    params = {
        'form_uuid': form_uuid,
        'time[from]': start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        'time[to]': end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        'includes': 'fields,events'
    }

    ZUKO_SESSIONS_API_URL = 'https://egress.api.zuko.io/sessions'

    def fetch_batch(params):
        """
        Fetches a batch of sessions from the Zuko API using the provided parameters.

        Args:
            params (dict): The query parameters to send with the API request.

        Returns:
            tuple: The next page ID and the list of sessions.
        """

        response = requests.get(
            ZUKO_SESSIONS_API_URL + "?" + urlencode(params),
            headers={'X-Api-Key': ZUKO_API_KEY}
        )
        response.raise_for_status()
        data = response.json()
        return data.get('next_page_id'), data.get('sessions', [])

    sessions = []
    next_page_id = True
    while next_page_id:
        next_page_id, batch = fetch_batch(params)
        sessions.extend(batch)
        if next_page_id:
            params['next_page_id'] = next_page_id

    print(f"[{form_name}] Fetched {len(sessions)} session(s)")

    flat_sessions = [flatten_session(s) for s in sessions]

    if flat_sessions:
        raw_keys = {key for session in flat_sessions for key in session}
        key_map = {key: key.replace(" ", "_").replace("-", "_") for key in raw_keys}
        keys = sorted(key_map.values())

        for session in flat_sessions:
            for original_key, new_key in key_map.items():
                if original_key in session:
                    session[new_key] = session.pop(original_key)

        # Convert all values to strings before writing the CSV. This ensures consistency in the exported file, since CSV does not enforce data types
        # Converting everything to string avoids type-related issues and makes the file easier to parse
        flat_sessions_str = [
            {k: str(v) if v is not None else "" for k, v in session.items()}
            for session in flat_sessions
        ]

        file_path = f"tmp/zuko_sessions_{form_name}.csv"
        with open(file_path, "w", newline='', encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=keys,
                quoting=csv.QUOTE_ALL
            )
            writer.writeheader()
            writer.writerows(flat_sessions_str)
        print(f"[{form_name}] CSV file saved: {file_path}")
    else:
        print(f"[{form_name}] No sessions to write.")


def csv_to_bq(form_name):
    """
    Loads the CSV file corresponding to a form into BigQuery.

    Args:
        form_name (str): The name of the form associated with the CSV file.
    """

    # Check if the CSV file exists
    csv_path = f"./tmp/zuko_sessions_{form_name}.csv"
    if not os.path.exists(csv_path):
        print(f"[{form_name}] CSV not found: {csv_path}")
        return

    print(f"[{form_name}] Processing upload to BigQuery...")

    scopes = ["https://www.googleapis.com/auth/bigquery"]

    credentials = service_account.Credentials.from_service_account_info(
        json.loads(google_credentials_json), scopes=scopes)
    project = credentials.project_id

    client = bigquery.Client(credentials=credentials, project=project)

    dataset_id = "Zuko_data"
    table_id = f"{project}.{dataset_id}.{form_name}"

    df_csv = pd.read_csv(csv_path)

    try:
        client.get_table(table_id)
        table_exists = True
        print(f"[{form_name}] Table found in BQ: {table_id}")
    except Exception:
        table_exists = False
        print(f"[{form_name}] Table not found, it will be created: {table_id}")

    if not table_exists:
        standard_schema = [
            bigquery.SchemaField("attributes_Operating_System", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_Visitor_Type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_autofillTriggered", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("attributes_browserFamily", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_deviceType", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_trafficMedium", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("completed", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("completed_at", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("duration", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("events_count", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("events_json", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("fields_count", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("fields_json", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("form_uuid", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("last_touched_field_html_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("last_touched_field_html_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("last_touched_field_html_tag_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("last_touched_field_html_type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("last_touched_field_label", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("started", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("started_at", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("time", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("total_field_returns", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("viewed", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("viewed_at", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("visitor_id", "STRING", mode="NULLABLE"),
        ]

        schema_with_checkbox_fields = [
            bigquery.SchemaField("attributes_Operating_System", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_Visitor_Type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_autofillTriggered", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("attributes_browserFamily", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_editorial_crossbooks", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_editorial_ediciones_martinez_roca", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_editorial_editorial_alienta", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_editorial_editorial_ariel", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_editorial_editorial_austral", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_editorial_editorial_booket", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_editorial_editorial_critica", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_editorial_editorial_destino", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_editorial_editorial_deusto", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_editorial_editorial_espasa", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_editorial_editorial_minotauro", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_editorial_editorial_peninsula", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_editorial_editorial_planeta", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_editorial_editorial_planeta_comic", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_editorial_editorial_seix_barral", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_editorial_editorial_tusquets", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_editorial_esencia", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_editorial_temas_de_hoy", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_tematica_actualidad", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_tematica_arte", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_tematica_autoayuda", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_tematica_bebes", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_tematica_ciencia", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_tematica_ciencia_ficcion", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_tematica_ciencias_humanas_y_sociales", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_tematica_cocina", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_tematica_comic_y_manga", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_tematica_economia", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_tematica_empresa", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_tematica_esoterismo", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_tematica_estilo_de_vida", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_tematica_fantasia", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_tematica_filosofia", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_tematica_historia", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_tematica_humor", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_tematica_infantil", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_tematica_juvenil", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_tematica_novela_contemporanea", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_tematica_novela_erotica", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_tematica_novela_historica", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_tematica_novela_literaria", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_tematica_novela_negra", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_tematica_novela_romantica", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_tematica_ocio_y_entretenimiento", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_tematica_para_padres", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_tematica_poesia", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_tematica_psicologia", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_tematica_religion", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_tematica_salud", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_tematica_teatro", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_tematica_terror", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_tematica_viajes", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_checkbox_todas_tematicas", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_deviceType", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributes_trafficMedium", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("completed", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("completed_at", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("duration", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("events_count", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("events_json", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("fields_count", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("fields_json", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("form_uuid", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("last_touched_field_html_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("last_touched_field_html_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("last_touched_field_html_tag_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("last_touched_field_html_type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("started", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("started_at", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("time", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("total_field_returns", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("viewed", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("viewed_at", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("visitor_id", "STRING", mode="NULLABLE"),
        ]

        # Read the CSV headers for inspection
        with open(csv_path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames

        # Choose the schema depending on whether any field contains 'attributes_checkbox'
        if any('attributes_checkbox' in h for h in headers):
            schema = schema_with_checkbox_fields
        else:
            schema = standard_schema

        # Create the table in BigQuery and upload the CSV data
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)
        print(f"[{form_name}] Table created: {table_id}")

        job = client.load_table_from_dataframe(df_csv, table_id)
        job.result()
        print(f"[{form_name}] Loaded {len(df_csv)} records into the new table")
    else:
        # Update the table
        query = f"SELECT id FROM `{table_id}`"
        existing_ids = set(row.id for row in client.query(query).result())

        df_new = df_csv[~df_csv["id"].isin(existing_ids)]

        if df_new.empty:
            print(f"[{form_name}] No new records to upload.")
        else:
            job = client.load_table_from_dataframe(df_new, table_id)
            job.result()
            print(f"[{form_name}] Loaded {len(df_new)} new records to BQ")



def main():
    forms = [
        [['DeAgostini_ES'], ['94########################']],
        [['PASA_Web_step_2'], ['e2########################']],
        [['PASA_Web_step_3'], ['72########################']],
        [['DeAgostini_FR'], ['1f########################']],
        [['DeAgostini_BE'], ['89########################']],
        [['ALTAYA_Web_step_3'], ['64########################']],
        [['FANHOME_UK'], ['c8########################']],
        [['FANHOME_DE'], ['67########################']],
        [['FANHOME_US'], ['72########################']],
        [['DeAgostini_UK'], ['fd########################']],
        [['DeAgostini_DE'], ['e8########################']],
        [['DeAgostini_GR'], ['5c########################']],
        [['DeAgostini_IT'], ['44########################']],
        [['DeAgostini_SK'], ['f5########################']],
        [['DeAgostini_US'], ['6a########################']],
        [['CDL'], ['3c########################']],
        [['CDL_New_Checkout'], ['37########################']],
        [['CDL_New_Checkout_Final'], ['3b0########################']],
        [['CDL_New_Checkout_Definitivo'], ['5d########################']],
        [['saldoCDL'], ['eb########################']],
        [['Registro'], ['4a########################']],
        [['alta_socio'], ['8a########################']],
        [['form_lateral_socio'], ['ae########################']],
        [['CDL_mis_datos'], ['1e########################']],
        [['Checkout_test'], ['77########################']],
        [['SI_ESLSCA'], ['6c########################']],
        [['RBS_Form_details'], ['85########################']],
        [['ESPACIO_MISTERIO_REGISTRO_BASICO_and_CLUB_MISTERIO'], ['14########################']],
        [['Gastronosfera'], ['1a########################']],
        [['TanTanfan'], ['47########################']],
        [['PDL_ES_FORMULARIO_DE_REGISTRO'], ['5d########################']],
        [['PDL_ES_ENCUESTA_DE_PREFERENCIAS'], ['c2########################']],
        [['ARTIKA_BOOKING_FORM'], ['38########################']],
        [['ARTIKA_INFORMATION_FORM'], ['75########################']],
        [['EAEPROGRAMAS_FORM_SI_GENERICO'], ['92########################']],
        [['EAEPROGRAMAS_FORM_SA'], ['56########################']],
        [['EAEPROGRAMAS_FORM_SI_FICHA'], ['fa########################']],
        [['Ostelea'], ['93########################']],
        [['EAE_Programas_Formacion_Landing'], ['ce########################']],
        [['Ostelea_FICHA'], ['c7########################']],
        [['EAE_Programas_Formacion_Ficha'], ['ab########################']],
        [['ARTIKA_Los_sueños_de_Frida_Kahlo'], ['28########################']],
        [['ARTIKA_Las_mujeres_de_Botero'], ['c3########################']],
        [['ARTIKA_Jaume_Plensa_61'], ['e########################']],
        [['ARTIKA_Bodas_de_sangre'], ['f1########################']],
        [['ARTIKA_Garagatos_bis'], ['d2########################']],
        [['Artika_galleria_México'], ['10########################']],
        [['Artika_Mexico_landing'], ['c0########################']],
        [['EAE_FORMACIÓN_FORM_CONVERSACIONAL_TEST'], ['bf########################']]
    ]

    delete_form_files(forms)

    for form in forms:
        form_name, form_uuid = form[0][0], form[1][0]
        try:
            fetch_sessions_for_form(form_name, form_uuid, days_back)
            csv_to_bq(form_name)
            delete_form_files(forms)
        except Exception as e:
            print(f"Error processing {form_name}: {e}")
            send_email(f"Error en el proceso de carga de datos de Zuko a BQ", f"[planeta-de-agostini] Error en el formulario {form_name}: {e}")

    delete_form_files(forms)


if __name__ == "__main__":
    try:
        print("--- START ---")
        load_dotenv()
        days_back = os.environ["DAYS_BACK"]
        receiver = os.environ["RECEIVER"]
        ZUKO_API_KEY = os.environ["ZUKO_API_KEY"]
        google_credentials_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        main()
        print("--- FINISH ---")
    except Exception as e:
        print(f"Error durente el proceso. Error: {e}")
        send_email("Error en el proceso de carga de datos de Zuko a BQ",f"[planeta-de-agostini] Error: {e}")
