import logging

from psycopg2.extensions import STATUS_ASYNC
import azure.functions as func
import configparser
import psycopg2
import os
from datetime import datetime
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

def main(msg: func.ServiceBusMessage):

    cfg = configparser.ConfigParser()
    cfg.read(os.path.join(os.path.dirname(__file__), '.config'))
    

    notification_id = int(msg.get_body().decode('utf-8'))
    logging.info('Python ServiceBus queue trigger processed message: %s',notification_id)

    print('working')

    print

    # TODO: Get connection to database
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_URL", cfg.get('POSTGRES', 'POSTGRES_URL')),
        database=os.getenv("POSTGRES_DB", cfg.get('POSTGRES', 'POSTGRES_DB')),
        user=os.getenv("POSTGRES_USER", cfg.get('POSTGRES', 'POSTGRES_USER')),
        password=os.getenv("POSTGRES_PW", cfg.get('POSTGRES', 'POSTGRES_PW')))

    cursor = conn.cursor()

    try:
        # TODO: Get notification message and subject from database using the notification_id
        notification_Query = "select * from notification where id = " + str(notification_id)

        cursor.execute(notification_Query)
        notification_record = cursor.fetchone()

        mail_message = notification_record[2]
        mail_subject = notification_record[5]

        # TODO: Get attendees email and name
        attendee_Query = "select email, first_name from attendee"
        cursor.execute(attendee_Query)
        attendees = cursor.fetchall()

        # TODO: Loop through each attendee and send an email with a personalized subject
        for attendee in attendees:
            print ("Name: " + attendee[1])

            message = Mail(
                from_email='thomas.lettenbauer@outlook.de',
                to_emails='tommi.datenschutz@gmail.com',
                subject=attendee[0] + " " + mail_subject,
                html_content='Dear ' + attendee[1] + ', <br><br><strong>' + mail_message + '</strong>')
            try:
                sg = SendGridAPIClient(os.getenv('SENDGRID_API_KEY', cfg.get('SENDGRID', 'SENDGRID_API_KEY')))
                response = sg.send(message)
                print(response.status_code)
                print(response.body)
                print(response.headers)
            except Exception as e:
                print(e.message)

        # TODO: Update the notification table by setting the completed date and updating the status with the total number of attendees notified
        notification_Update = "update notification set status = 'Notified " + str(len(attendees)) + " attendees' where id = " + str(notification_id)
        cursor.execute(notification_Update)
        conn.commit()
        cursor.close()

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        # TODO: Close connection
        conn.close


