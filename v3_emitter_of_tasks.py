"""
    This program sends a message to a queue on the RabbitMQ server.
    Make tasks harder/longer-running by adding dots at the end of the message.

Original Author: Denise Case
    Date: January 15, 2023

    Functions:
    offer_rabbitmq_admin_site() -> None:
        Offers to open the RabbitMQ Admin website for monitoring.

    send_message(host: str, queue_name: str, message: str) -> None:
        Sends a message to the specified RabbitMQ queue.

    read_tasks_from_csv(file_path: str) -> list[str]:
        Reads tasks from a CSV file and returns them as a list of strings.

    main() -> None:
        Main entry point of the script. Reads tasks from the CSV file and sends them to the RabbitMQ queue.

Usage:
    Run this script directly to send messages to the RabbitMQ queue.
    Ensure RabbitMQ server is running and the tasks.csv file is present in the 

"""
import csv
import pika
import sys
import webbrowser

def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    print()
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        print()

def send_message(host: str, queue_name: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """

    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=queue_name, durable=True)
        # use the channel to publish a message to the queue
        # every message passes through an exchange
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        # print a message to the console for the user
        print(f" [x] Sent {message}")
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()
tasks_csv = "tasks.csv" 

## Reads the CSV file and sends the messages to the queue
def read_tasks_from_csv(tasks_csv: str):
    """
    Reads tasks from a CSV file.

    Parameters:
        file_path (str): the path to the CSV file

    Returns:
        list of str: a list of messages (tasks) read from the CSV file
    """
    tasks = []
    try:
        with open(tasks_csv, mode='r', newline='') as file:
            reader = csv.reader(file)
            for row in reader:
                if row:  # Ensure the row is not empty
                    tasks.append(row[0])  # Assuming each task is in the first column
    except FileNotFoundError as e:
        print(f"Error: {e}")
        sys.exit(1)
    return tasks


# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":  
    # ask the user if they'd like to open the RabbitMQ Admin site
    offer_rabbitmq_admin_site()
    # get the message from the command line
    # if no arguments are provided, use the default message
    # use the join method to convert the list of arguments into a string
    # join by the space character inside the quotes
    tasks = read_tasks_from_csv("tasks.csv")
    # send the message to the queue
    for task in tasks:
        send_message("localhost", "task_queue3", task)