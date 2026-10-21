# Pre-work checklist

Complete this before the workshop so connecting doesn't eat into lab time.

1. **Access your Workshop Studio account.** Use the Event Hash / Team Hash provided by the instructor at [catalog.workshops.aws](https://catalog.workshops.aws). You should see temporary AWS credentials and access to your workshop host.
   - *Check:* you can sign in and see your AWS console.

2. **Connect to your workshop host.** From your account's console, open a Systems Manager Session Manager session to the `KafkaClientInstance`.
   - *Check:* you get a terminal on the host.

3. **Run the connection check** (copy/paste, run inside the host):
   ```bash
   sudo su - ec2-user
   source bootstrap.sh
   /home/ec2-user/kafka/bin/kafka-topics.sh --bootstrap-server $BS --command-config /home/ec2-user/kafka/config/client-iam.properties --list
   ```
   - *Check:* the command returns a list of topics (internal `__...` topics at minimum) without an authentication or timeout error.

4. **Confirm your topics exist**:
   ```bash
   /home/ec2-user/kafka/bin/kafka-topics.sh --bootstrap-server $BS --command-config /home/ec2-user/kafka/config/client-iam.properties \
     --describe --topic orders
   ```
   - *Check:* the command shows the `orders` topic with 3 partitions.

5. **If anything fails**, note the exact error message and who to contact (channel/contact to be filled in by the event organizer).
