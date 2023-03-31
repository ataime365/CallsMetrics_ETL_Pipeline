import json, boto3, requests
from sqlalchemy import create_engine, Table, Column, Integer, String, Boolean, MetaData, UniqueConstraint
from sqlalchemy.sql import text
import pandas as pd
import os

secret_name = 'prod_postgres_entries_user'
region_name ='us-east-1'
session = boto3.session.Session()
client = session.client(service_name='secretsmanager',region_name=region_name)
get_secret_value_response = client.get_secret_value(SecretId=secret_name)
creds = json.loads(get_secret_value_response['SecretString'])

username = creds['username']
password = creds['password']
host = creds['host']
account_id = os.environ['account_id']
Basic_Auth = os.environ['Basic_Auth']

# Create a metadata object
metadata = MetaData()

# Define the table schema #I already created the table using sql CREATE TABLE

headers = {'Authorization': f'{Basic_Auth}',
            'Content-Type': 'application/json'}

def send_requests(account_id):
    response = requests.get(f'https://api.calltrackingmetrics.com/api/v1/accounts/{account_id}/calls', headers=headers)
    data = json.loads(response.text)
    data = data.get('calls') #data is a list of dictionaries
    return data

def transform_data(data):
    df = pd.DataFrame(data)

    df['call_path_route_name'] = df['call_path'].apply(lambda x: x[0]['route_name'])
    df['call_path_route_id'] = df['call_path'].apply(lambda x: x[0]['route_id'])
    df['call_path_route_type'] = df['call_path'].apply(lambda x: x[0]['route_type'])
    df['call_path_started_at'] = df['call_path'].apply(lambda x: x[0]['started_at'])

    df['inbound_rate_center_country'] = df['inbound_rate_center'].apply(lambda x: x['country'])
    df['inbound_rate_center_prefix'] = df['inbound_rate_center'].apply(lambda x: x['prefix'])
    df['inbound_rate_center_tollfree'] = df['inbound_rate_center'].apply(lambda x: x['tollfree'])
    df['ga_cid'] = df['ga'].apply(lambda x: x['cid'])

    # Convert called_at, billed_at, call_path_started_at,  column to TIMESTAMP WITH TIME ZONE
    df['called_at'] = pd.to_datetime(df['called_at'], utc=True)
    df['billed_at'] = pd.to_datetime(df['billed_at'], utc=True)
    df['call_path_started_at'] = pd.to_datetime(df['call_path_started_at'], utc=True)

    # For caller_number_split, transfers, spotted column #List to string #Empty list and Empty jsons included
    df['caller_number_split'] = df['caller_number_split'].astype(str)
    df['transfers'] = df['transfers'].astype(str)
    df['spotted'] = df['spotted'].astype(str)
    df['salesforce'] = df['salesforce'].astype(str)
    df['callbacks'] = df['callbacks'].astype(str)
    df['emails'] = df['emails'].astype(str)
    df['tag_list'] = df['tag_list'].astype(str)
    df['agent'] = df['agent'].astype(str)
    df['legs'] = df['legs'].astype(str)
    df.drop(columns=['call_path', 'inbound_rate_center', 'ga'], inplace=True)

    # Columns has to be ordered in the same way the sql table was created
    df = df[['id','sid', 'account_id', 'name', 'cnam', 'search', 'referrer', 'location', 'source', 
            'source_id', 'source_sid', 'tgid', 'likelihood', 'duration', 'direction', 'talk_time', 
            'ring_time', 'hold_time', 'wait_time', 'parent_id', 'email', 'street', 'city', 'state', 
            'country', 'postal_code', 'called_at', 'unix_time', 'tracking_number_id', 
            'tracking_number_sid', 'tracking_number', 'tracking_label', 'dial_status', 
            'is_new_caller', 'indexed_at', 'inbound_rate_center_country', 
            'inbound_rate_center_prefix', 'inbound_rate_center_tollfree', 'billed_amount', 
            'billed_at', 'caller_number_split', 'contact_number', 'excluded', 'redacted', 
            'tracking_number_format', 'caller_number_format', 'alternative_number', 
            'caller_number_complete', 'caller_number_bare', 'tracking_number_bare', 'caller_number', 
            'visitor', 'call_path_route_name', 'call_path_route_id', 'call_path_route_type', 'call_path_started_at', 
            'left_talk_time', 'right_talk_time', 'transfers', 'call_status', 'status', 'spotted', 'salesforce', 'audio', 
            'callbacks', 'emails', 'day', 'month', 'hour', 'ga_cid', 'tag_list', 'notes', 'latitude', 'longitude', 'extended_lookup_on', 'agent_id', 'agent', 'legs']]

    dict_list = df.to_dict('records')
    values = dict_list
    return values

def upsert_data_into_database(values):
    # Create an SQLAlchemy engine to connect to the database
    engine = create_engine(f'postgresql://{username}:{password}@{host}/benson_db')

    conn = engine.connect()

    # Create an SQLAlchemy text object representing the insert/update statement
    stmt = text("""
        INSERT INTO benson_schema.calls (
            id, sid, account_id, name, cnam, search, referrer, location,
            source, source_id, source_sid, tgid, likelihood, duration,
            direction, talk_time, ring_time, hold_time, wait_time, parent_id,
            email, street, city, state, country, postal_code, called_at,
            unix_time, tracking_number_id, tracking_number_sid, tracking_number,
            tracking_label, dial_status, is_new_caller, indexed_at,
            inbound_rate_center_country, inbound_rate_center_prefix,
            inbound_rate_center_tollfree, billed_amount, billed_at,
            caller_number_split, contact_number, excluded, redacted,
            tracking_number_format, caller_number_format, alternative_number,
            caller_number_complete, caller_number_bare, tracking_number_bare,
            caller_number, visitor, call_path_route_name, call_path_route_id,
            call_path_route_type, call_path_started_at, left_talk_time,
            right_talk_time, transfers, call_status, status, spotted, salesforce,
            audio, callbacks, emails, day, month, hour, ga_cid, tag_list, notes,
            latitude, longitude, extended_lookup_on, agent_id, agent, legs
        ) VALUES (
            :id, :sid, :account_id, :name, :cnam, :search, :referrer, :location,
            :source, :source_id, :source_sid, :tgid, :likelihood, :duration,
            :direction, :talk_time, :ring_time, :hold_time, :wait_time, :parent_id,
            :email, :street, :city, :state, :country, :postal_code, :called_at,
            :unix_time, :tracking_number_id, :tracking_number_sid, :tracking_number,
            :tracking_label, :dial_status, :is_new_caller, :indexed_at,
            :inbound_rate_center_country, :inbound_rate_center_prefix,
            :inbound_rate_center_tollfree, :billed_amount, :billed_at,
            :caller_number_split, :contact_number, :excluded, :redacted,
            :tracking_number_format, :caller_number_format, :alternative_number,
            :caller_number_complete, :caller_number_bare, :tracking_number_bare,
            :caller_number, :visitor, :call_path_route_name, :call_path_route_id,
            :call_path_route_type, :call_path_started_at, :left_talk_time,
            :right_talk_time, :transfers, :call_status, :status, :spotted, :salesforce,
            :audio, :callbacks, :emails, :day, :month, :hour, :ga_cid, :tag_list, :notes,
            :latitude, :longitude, :extended_lookup_on, :agent_id, :agent, :legs
        ) ON CONFLICT (id) DO UPDATE
        SET id = EXCLUDED.id, sid = EXCLUDED.sid,
            account_id = EXCLUDED.account_id, name = EXCLUDED.name,
            cnam = EXCLUDED.cnam, search = EXCLUDED.search,
            referrer = EXCLUDED.referrer, location = EXCLUDED.location,
            source = EXCLUDED.source, source_id = EXCLUDED.source_id,
            source_sid = EXCLUDED.source_sid, tgid = EXCLUDED.tgid,
            likelihood = EXCLUDED.likelihood, duration = EXCLUDED.duration,
                    direction = EXCLUDED.direction, talk_time = EXCLUDED.talk_time,
            ring_time = EXCLUDED.ring_time, hold_time = EXCLUDED.hold_time,
            wait_time = EXCLUDED.wait_time, parent_id = EXCLUDED.parent_id,
            email = EXCLUDED.email, street = EXCLUDED.street, city = EXCLUDED.city,
            state = EXCLUDED.state, country = EXCLUDED.country,
            postal_code = EXCLUDED.postal_code, called_at = EXCLUDED.called_at,
            unix_time = EXCLUDED.unix_time,
            tracking_number_id = EXCLUDED.tracking_number_id,
            tracking_number_sid = EXCLUDED.tracking_number_sid,
            tracking_number = EXCLUDED.tracking_number,
            tracking_label = EXCLUDED.tracking_label,
            dial_status = EXCLUDED.dial_status,
            is_new_caller = EXCLUDED.is_new_caller,
            indexed_at = EXCLUDED.indexed_at,
            inbound_rate_center_country = EXCLUDED.inbound_rate_center_country,
            inbound_rate_center_prefix = EXCLUDED.inbound_rate_center_prefix,
            inbound_rate_center_tollfree = EXCLUDED.inbound_rate_center_tollfree,
            billed_amount = EXCLUDED.billed_amount, billed_at = EXCLUDED.billed_at,
            caller_number_split = EXCLUDED.caller_number_split,
            contact_number = EXCLUDED.contact_number,
            excluded = EXCLUDED.excluded, redacted = EXCLUDED.redacted,
            tracking_number_format = EXCLUDED.tracking_number_format,
            caller_number_format = EXCLUDED.caller_number_format,
            alternative_number = EXCLUDED.alternative_number,
            caller_number_complete = EXCLUDED.caller_number_complete,
            caller_number_bare = EXCLUDED.caller_number_bare,
            tracking_number_bare = EXCLUDED.tracking_number_bare,
            caller_number = EXCLUDED.caller_number,
            visitor = EXCLUDED.visitor,
            call_path_route_name = EXCLUDED.call_path_route_name,
            call_path_route_id = EXCLUDED.call_path_route_id,
            call_path_route_type = EXCLUDED.call_path_route_type,
            call_path_started_at = EXCLUDED.call_path_started_at,
            left_talk_time = EXCLUDED.left_talk_time,
            right_talk_time = EXCLUDED.right_talk_time,
            transfers = EXCLUDED.transfers, call_status = EXCLUDED.call_status,
            status = EXCLUDED.status, spotted = EXCLUDED.spotted,
            salesforce = EXCLUDED.salesforce, audio = EXCLUDED.audio,
            callbacks = EXCLUDED.callbacks, emails = EXCLUDED.emails,
            day = EXCLUDED.day, month = EXCLUDED.month, hour = EXCLUDED.hour,
            ga_cid = EXCLUDED.ga_cid, tag_list = EXCLUDED.tag_list,
            notes = EXCLUDED.notes, latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            extended_lookup_on = EXCLUDED.extended_lookup_on,
            agent_id = EXCLUDED.agent_id, agent = EXCLUDED.agent, legs = EXCLUDED.legs;
    """)

    # Execute the insert/update statement with the values
    conn.execute(stmt, values)

    # Close the connection
    conn.close()


def lambda_handler(event, context):

    data = send_requests(account_id)
    values = transform_data(data)
    upsert_data_into_database(values)


    return {
        'statusCode': 200,
        'body': json.dumps(f'Values Inserted')
    }




