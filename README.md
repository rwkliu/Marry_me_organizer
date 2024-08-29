# Marry_me_organizer
## Starting the Application
To start the application, type the following to run rabbitmq in a Docker container:
`docker compose up -d --build`

## Publish/Consume Messages
To consume messages from the rabbitmq queues, run `consumer.py`. To publish messages to a queue, run `publisher.py`. To view the messages printed by the consumer container, run `docker logs <container name> --follow`.