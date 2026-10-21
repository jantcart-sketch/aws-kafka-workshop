# Pre-work checklist

Complete this before the workshop so connecting doesn't eat into lab time.

1. **Access your Workshop Studio account.** Use the Event Hash / Team Hash provided by the instructor at [catalog.workshops.aws](https://catalog.workshops.aws). You should see temporary AWS credentials and access to your workshop host.
   - *Check:* you can sign in and see your AWS console.

2. **Connect to your workshop host.** From your account's console, open a Systems Manager Session Manager session to the `KafkaClientInstance`.
   - *Check:* you get a terminal on the host.

3. **Clone the workshop repo.** All the material (scripts, labs, deck) lives in a public GitHub repo. Inside the host:
   ```bash
   sudo su - ec2-user
   git clone https://github.com/jantcart-sketch/aws-kafka-workshop.git
   cd aws-kafka-workshop
   ```
   - *Check:* the command finishes with no error and `ls` shows the `labs/`, `scripts/`, `producer/`, `schemas/` folders.

4. **Run the connection check** (copy/paste, run inside `aws-kafka-workshop/scripts/`):
   ```bash
   cd scripts/
   source bootstrap.sh
   /home/ec2-user/kafka/bin/kafka-topics.sh --bootstrap-server $BS --command-config /home/ec2-user/kafka/config/client-iam.properties --list
   ```
   - *Check:* the command returns a list of topics without an authentication or timeout error, and it includes `orders`, `customers`, and `orders-avro`. `bootstrap.sh` creates these three automatically (idempotently, `--if-not-exists`) every time it runs — they don't rely on being pre-created in your account, so even a brand-new/reset account gets them back on the next `source bootstrap.sh`.

5. **Confirm your topics exist**:
   ```bash
   /home/ec2-user/kafka/bin/kafka-topics.sh --bootstrap-server $BS --command-config /home/ec2-user/kafka/config/client-iam.properties \
     --describe --topic orders
   ```
   - *Check:* the command shows the `orders` topic with 3 partitions. Note: on this cluster's Express brokers, MSK always enforces `ReplicationFactor: 3` regardless of what `--replication-factor` you pass when creating a topic — that's expected AWS behavior for Express, not a mistake in the command.

6. **If anything fails**, note the exact error message and who to contact (channel/contact to be filled in by the event organizer).

## Workshop resources

- **Repo (code, scripts, labs):** https://github.com/jantcart-sketch/aws-kafka-workshop
- **Deck (slides, open in your browser):** https://jantcart-sketch.github.io/aws-kafka-workshop/
