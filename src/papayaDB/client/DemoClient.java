package papayaDB.client;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import papayaDB.api.query.QueryInterface;

public class DemoClient extends AbstractVerticle{
	public static void main(String[] args) {
		QueryInterface client = QueryInterface.newHttpQueryInterface("localhost", 8080); /*"Genroa", "a58fdaf6dc9ee61c5aa5ee514d9b711ef72e239d8f1c53e1e05631357ffc8ed6f1f21d3f2f4c1d2220f5874b6d6e6d74fca6618a21c145866978052fb215c3be" lapin */
		setSSLWithKeystore("keystore.jks");
		
		String user = "Genroa";
		String hash = "a58fdaf6dc9ee61c5aa5ee514d9b711ef72e239d8f1c53e1e05631357ffc8ed6f1f21d3f2f4c1d2220f5874b6d6e6d74fca6618a21c145866978052fb215c3be";

//		client.createNewDatabase("myDb", user, hash, answer -> {
//			System.out.println("CREATE :"+answer);
//			
//			client.insertNewRecord("myDb", new JsonObject().put("newKey", 3), user, hash, answer2 -> {
//				System.out.println("INSERT: "+answer2);
//				
//				
//				client.getRecords("myDb", new JsonObject().put("bounds", new JsonObject()
//						.put("value", new JsonArray().add(new JsonObject()
//															.put("field", "newKey")
//															.put("min", 3)
//															.put("max", 8)
//						)))
//						.put("limit", new JsonObject()
//										.put("value", 2)), answer3 -> {
//					System.out.println("GET: "+answer3);
//					client.close();
//				});
//			});
//		});
		
		client.getRecords("myDb", new JsonObject().put("bounds", new JsonObject()
				.put("value", new JsonArray().add(new JsonObject()
													.put("field", "newKey")
													.put("min", 3)
													.put("max", 8)
				)))
				.put("limit", new JsonObject()
								.put("value", 2)), answer3 -> {
			System.out.println("GET: "+answer3);
			client.close();
		});
		
//		client.createNewDatabase("myDb", user, hash, answer -> {
//			System.out.println("CREATE :"+answer);
//			
//			client.getRecords("myDb", new JsonObject().put("equals", new JsonObject()
//					.put("value", new JsonArray().add(new JsonObject()
//														.put("field", "newKey")
//														.put("value", "value")
//					)))
//					.put("limit", new JsonObject()
//									.put("value", 2)), answer3 -> {
//				System.out.println("GET: "+answer3);
//				client.close();
//			});
//		});
	}
	
	public static void setSSLWithKeystore(String keystorePath) {
		System.setProperty("javax.net.ssl.keyStorePassword", "papayadb");
		System.setProperty("javax.net.ssl.trustStore", keystorePath);
		System.setProperty("javax.net.ssl.keyStore", keystorePath);
		System.setProperty("javax.net.ssl.trustStoreType", "jks");
	}
}