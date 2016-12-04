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
		long time1 = System.currentTimeMillis();
		client.createNewDatabase("myDb", user, hash, answer -> {
			long time2 = System.currentTimeMillis();
			System.out.println("CREATE :"+answer+" in "+(time2-time1)+"ms");
			long time3 = System.currentTimeMillis();
			client.insertNewRecord("myDb", new JsonObject().put("newKey", 3), user, hash, answer2 -> {
				long time4 = System.currentTimeMillis();
				System.out.println("INSERT: "+answer2+" in "+(time4-time3)+"ms");
				
				long time5 = System.currentTimeMillis();
				client.getRecords("myDb", new JsonObject().put("equals", new JsonObject()
						.put("value", new JsonArray().add(new JsonObject()
															.put("field", "newKey")
															.put("value", 3)
						)))
						.put("limit", new JsonObject()
										.put("value", 2)), answer3 -> {
					long time6 = System.currentTimeMillis();
					System.out.println("GET: "+answer3+" in "+(time6-time5+"ms"));
					long time7 = System.currentTimeMillis();
					client.getRecords("myDb", new JsonObject().put("equals", new JsonObject()
							.put("value", new JsonArray().add(new JsonObject()
																.put("field", "newKey")
																.put("value", 3)
							)))
							.put("limit", new JsonObject()
											.put("value", 2)), answer4 -> {
								long time8 = System.currentTimeMillis();
								System.out.println("GET: "+answer4+" in "+(time8-time7+"ms"));
		
								client.close();
					});
				});
			});
		});
	}
	
	public static void setSSLWithKeystore(String keystorePath) {
		System.setProperty("javax.net.ssl.keyStorePassword", "papayadb");
		System.setProperty("javax.net.ssl.trustStore", keystorePath);
		System.setProperty("javax.net.ssl.keyStore", keystorePath);
		System.setProperty("javax.net.ssl.trustStoreType", "jks");
	}
}