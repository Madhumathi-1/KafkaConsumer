import java.io.BufferedReader;
import java.io.FileReader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONArray;
import org.json.JSONObject;

public class ResourceLevelFilterConsumer {
	
	 String bootstrapServers = "localhost:9092";
	
		public static void main(String[] args) {
			Properties props = new Properties();
			props.put("enable.auto.commit", "true");
			props.put("auto.commit.interval.ms", "100");

			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
			props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");

//          Producer<String, String> producer = new KafkaProducer<>(props);
			KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

			List<String> topics = loadFile("/home/madhumathi/kafka.txt");
			List<String> resourceTypes = loadFile("/home/madhumathi/resource.txt");
			Map<String, List<String>> resourceHierarchy = loadResourceHierarchyFromFile(
					"/home/madhumathi/resourcehierarchy.json");

			consumer.subscribe(topics);
			consumer.listTopics();

			try {
				while (true) {
					ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
					for (ConsumerRecord<String, String> record : records) {
						JSONObject incomingJson = new JSONObject(record.value());
						String resourceTypeValue = incomingJson.getString("resourceType");

						if (resourceTypeValue != null && resourceTypes.contains(resourceTypeValue)) {
							List<String> parents = resourceHierarchy.get(resourceTypeValue);
							if (parents != null) {
								incomingJson.put("ParentResource", parents.get(0));
							}
						} else {
							System.out.println("Updated JSON: " + incomingJson);
						}
						
						String zid = incomingJson.getString("zid");
//						System.out.println();
						InsertDataIntoDb.saveToDatabase(String.valueOf(zid), resourceTypeValue, null, null);
					}
					consumer.commitAsync();
				}
			} catch (Exception ee) {
				System.out.println("Error: " + ee.getMessage());
				ee.printStackTrace();
			} finally {
				consumer.close();
			}
		}

		private static ArrayList<String> loadFile(String filename) {
			ArrayList<String> lines = new ArrayList<>();
			try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
				String line;
				while ((line = br.readLine()) != null) {
					lines.add(line);
				}
			} catch (Exception ee) {
				System.out.println("Error loading file: " + ee.getMessage());
				ee.printStackTrace();
			}
			return lines;
		}

	private static Map<String, List<String>> loadResourceHierarchyFromFile(String filename) {
		Map<String, List<String>> resourceHierarchy = new HashMap<>();
		StringBuilder jsonData = new StringBuilder();

		try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
			String line;
			while ((line = br.readLine()) != null) {
				jsonData.append(line);
			}
			// read the jsonData using JSONArray
			JSONArray jsonArray = new JSONArray(jsonData.toString());
			for (int i = 0; i < jsonArray.length(); i++) {
				JSONObject jsonObject = jsonArray.getJSONObject(i);
				// get the resource name from the json
				String resourceName = jsonObject.getString("ResourceName");

				JSONArray parentArray = jsonObject.getJSONArray("Parent");
				// get the parents as list from the json
				List<String> parents = new ArrayList<>();

				for (int j = 0; j < parentArray.length(); j++) {
					parents.add(parentArray.getString(j));
				}
				resourceHierarchy.put(resourceName, parents);
			}
		} catch (Exception ee) {
			System.out.println("Error! " + ee.getMessage());
			ee.printStackTrace();
		}
		return resourceHierarchy;
	}
}
