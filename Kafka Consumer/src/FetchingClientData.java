import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FetchingClientData {

	private static final Map<String, List<String>> clientMap = new HashMap<>();

	public static List<String> fetchClients(String zid, String resourceType) throws Exception {
		String key = zid + ":" + resourceType;

		List<String> clients = fetchDataFromMap(zid, resourceType);

		if (clients == null) {
			clients = fetchDataFromDB(zid, resourceType);
			if (clients != null) {
				clientMap.put(key, clients);
			}
		}
		return clients;
	}

	private static List<String> fetchDataFromMap(String zid, String resourceType) {
		String key = zid + ":" + resourceType;
		return clientMap.get(key);
	}

	private static List<String> fetchDataFromDB(String zid, String resourceType) throws Exception {
		List<String> clients = new ArrayList<>();
		String clientID = getClientIDFromZID(zid);
		if (clientID != null) {
			clients.add(getClientName(clientID));
		}
		return clients;
	}

	private static String getClientIDFromZID(String zid) throws Exception {

		List<String> columns = new ArrayList<>();
		columns.add("clientID");

		List<String> whereConditions = new ArrayList<>();
		whereConditions.add("zid = " + zid);

		return DbUtils.performSelect(columns, "ZID", whereConditions, 1);
	}

	private static String getClientName(String clientID) throws Exception {
		List<String> columns = new ArrayList<>();
		columns.add("clientName");

		List<String> whereConditions = new ArrayList<>();
		whereConditions.add("clientID = " + clientID);

		String clientName = DbUtils.performSelect(columns, "Client", whereConditions, 3);
		return clientName;
	}

}
