import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//Client Handling 

public class InsertDataIntoDb {

	public static void saveToDatabase(String zid, String resourceType, String clientName, String clientDomain) {
		try {
			int clientID = getClientID("mail");
			String resourceTypeID = getResourceID(resourceType);
			insertIntoZid(zid, resourceTypeID, clientID);

			insertIntoResource(resourceType, String.valueOf(zid));
			System.out.println("Inserted data: { zid: " + zid + ", resourceType: " + resourceType + " }");

		} catch (Exception ee) {
			System.out.println("Error saving data to database: " + ee.getMessage());
			ee.printStackTrace();
		}
	}

	private static int insertIntoZid(String zid, String resourceTypeID, int clientID) throws Exception {
		// zid,resourceTypeID,cleintID
		List<String> zidColumnNames = Arrays.asList("zid", "resourceTypeID", "clientID");
        
		List<String> zidValues = Arrays.asList(zid, resourceTypeID, String.valueOf(clientID));
		
		return DbUtils.performInsert("ZID", zidColumnNames, zidValues);
	}

	private static int insertIntoResource(String resourceName, String zid) throws Exception {
		// resourceID, resourceName, zid
		List<String> resourceColumnNames = new ArrayList<>();
		resourceColumnNames.add("resourceName");
		resourceColumnNames.add("zid");

		List<String> resourceValues = new ArrayList<>();
		resourceValues.add(resourceName);
		resourceValues.add(zid);

		return DbUtils.performInsert("Resource", resourceColumnNames, resourceValues);
	}

	public static int insertIntoClient(String clientName, String clientDomain) {
		try {
			List<String> clientColumnNames = new ArrayList<>();
			clientColumnNames.add("clientName");
			clientColumnNames.add("clientDomain");

			List<String> clientValues = new ArrayList<>();
			clientValues.add(clientName);
			clientValues.add(clientDomain);

			int generatedKey = DbUtils.performInsert("Client", clientColumnNames, clientValues);
			// get generated key
			System.out.println("Generated Client ID: " + generatedKey);
		} catch (Exception e) {
			System.out.println("Error inserting client data: " + e.getMessage());
			e.printStackTrace();
		}
		return 0;
	}

	// Retrieval of ResourceID based on ResourceType
	private static final Map<String, Integer> RESOURCE_TYPE_IDS = new HashMap<>();
	static {
		RESOURCE_TYPE_IDS.put("User", 1);
		RESOURCE_TYPE_IDS.put("Group", 2);
		RESOURCE_TYPE_IDS.put("Account", 3);
	}

	public static String getResourceID(String resourceType) {
		return RESOURCE_TYPE_IDS.getOrDefault(resourceType, -1).toString();//setting a default value -1, if clientName is not present.
	}
	
	// Retrieval of ClientIDs based on ClientNames
	private static final Map<String, Integer> Client_IDS = new HashMap<>();
	static {
		Client_IDS.put("mail", 1);
		Client_IDS.put("cliq", 2);
		Client_IDS.put("sheet", 3);
	}

	public static int getClientID(String clientName) {
		return Client_IDS.getOrDefault(clientName, -1);//setting a default value -1, if clientName is not present.
	}
	
// Client name - Name of the client registered* with RARN - name usually contains service's name.
//	Client-domain - Domain address where notifications are to be sent eg., crm.zoho.com, zvideo-integ.localzoho.com

}
