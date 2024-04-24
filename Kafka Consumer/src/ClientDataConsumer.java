import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.List;

//multiple clients for a single zid:resourcetype combo
public class ClientDataConsumer {

	public static void main(String[] args) {

		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "client-data-consumer");

		String topic = "InternalTopic";
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Collections.singleton(topic));
 
		try {
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
				for (ConsumerRecord<String, String> record : records) {
					JSONObject json = new JSONObject(record.value());
					String zid = json.getString("zid");
					String resourceType = json.getString("resourceType");
					List<String> clientNames = FetchingClientData.fetchClients(zid, resourceType); 
					for (String clientName : clientNames) {
						System.out.println("Output: {" + clientName + " }");
					}
				}
			}
		} catch (Exception ee) {
			System.out.println("Error!" + ee.getMessage());
			ee.printStackTrace();
		} finally {
			consumer.close();
		}
	}
}
