#!/bin/bash

echo "Starting script..."

# Set the initial date
start_date="2020-03-29"

# Convert the initial date to seconds since the epoch
start_seconds=$(date -d "$start_date" +%s)

# Add the following line at the beginning of your script
for i in $(seq 1 18); do
  # Calculate the seconds for the current iteration
  current_seconds=$((start_seconds + (i-1) * 24 * 60 * 60))

  # Convert back to date format
  file_date=$(date -d "@$current_seconds" +%Y-%m-%d)
  file_name="$file_date Coronavirus Tweets"

  echo "Checking file: $file_name.csv"

  if [ -f "$file_name.CSV" ]; then
    # Ensure the topic exists or create it if not
    docker exec -i project-kafka-1 /opt/kafka/bin/kafka-topics.sh --create --topic covid_tweets_$i --partitions 1 --replication-factor 1 --bootstrap-server project-kafka-1:9092

    # Sleep for a moment to allow Kafka to initialize (adjust as needed)
    sleep 5

    echo "Uploading data for $file_name to topic covid_tweets_$i"
    cat "$file_name.CSV" | docker exec -i project-kafka-1 /opt/kafka/bin/kafka-console-producer.sh --topic covid_tweets_$i --bootstrap-server project-kafka-1:9092
  else
    echo "File not found: $file_name.csv"
  fi
done

echo "Script completed."
