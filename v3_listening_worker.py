"""
This script listens for task messages on a specified RabbitMQ queue and processes each task.
The process runs continuously, making it suitable for a work queue setup where multiple workers share the workload.

You can start multiple instances of this script in different terminals to create as many listening workers as needed,
enabling the distribution of tasks across several workers for parallel processing.

Approach
--------
This implementation follows the Work Queues pattern where one task producer sends tasks to a queue and multiple workers
consume tasks from the queue, sharing the workload.

Terminal Reminders
------------------
- Use Control+C to close a terminal and end a process.
- Use the up arrow to retrieve the last command executed.

Functions:
    listen_for_tasks() -> None:
        Continuously listens for task messages on a named queue and processes them.
    
    callback(ch, method, properties, body) -> None:
        Defines behavior on receiving a message, including simulating work and acknowledging the message.

Usage:
    Run this script directly to start listening for and processing messages from the RabbitMQ queue.
    Ensure the RabbitMQ server is running before starting the script.
"""
import pika
import sys
import os
import time

def listen_for_tasks():
    """ Continuously listen for task messages on a named queue."""
    
    # create a blocking connection to the RabbitMQ server
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    # use the connection to create a communication channel
    ch = connection.channel()

    # define a callback function to be called when a message is received
    def callback(ch, method, properties, body):
        """ Define behavior on getting a message."""

        # decode the binary message body to a string
        print(f" [x] Received {body.decode()}")
        # simulate work by sleeping for the number of dots in the message
        time.sleep(body.count(b"."))
        # when done with task, tell the user
        print(" [x] Done")
        # acknowledge the message was received and processed 
        # (now it can be deleted from the queue)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    # use the channel to declare a durable queue
    # a durable queue will survive a RabbitMQ server restart
    # and help ensure messages are processed in order
    # messages will not be deleted until the consumer acknowledges    
    ch.queue_declare(queue="task_queue3", durable=True)
    print(" [*] Ready for work. To exit press CTRL+C")

    # The QoS level controls the number of messages 
    # that can be in-flight (unacknowledged by the consumer) 
    # at any given time. 
    # Set the prefetch count to one to limit the number of messages 
    # being consumed and processed concurrently.
    # This helps prevent a worker from becoming overwhelmed 
    # and improve the overall system performance.
    # prefetch_count = Per consumer limit of unacknowledged messages      
    ch.basic_qos(prefetch_count=1) 
    
    # configure the channel to listen on a specific queue,  
    # use the callback function named callback,
    # and do not auto-acknowledge the message (let the callback handle it)
    ch.basic_consume(queue="task_queue3", on_message_callback=callback)

    # start consuming messages via the communication channel
    ch.start_consuming()

if __name__ == "__main__":
    try:
        listen_for_tasks()
    except KeyboardInterrupt:
        print("Interrupted")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)

