package papayaDB.client;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.rxjava.core.Vertx;

public class DemoClient extends AbstractVerticle{
	public static void main(String[] args) {
		/*QueryInterface client = QueryInterface.newHttpQueryInterface("localhost", 8080);
//		client.processQuery("get/testDb/fields/[name,age]/bounds/[age;0;18]/limit/1/equals/[name=%22/Pierre%22]/order/[age;ASC]", answer -> {
//			System.out.println(answer);
//			client.close();
//		});
		
		client.processQuery("get/testDb/limit/8", answer -> {
			System.out.println(answer);
			client.close();
		});*/
		
		/*HttpResponse response = null;
		try {
			response = HttpRequest
			          .create(new URI("http://localhost:8080/get/testDb/limit/8"))
			          .GET()
			          .response();
		} catch (IOException | InterruptedException | URISyntaxException e) {
			System.out.println("Bad Request : " + e);
			return;
		}
		String responseBody = response.body(HttpResponse.asString());
		System.out.println(responseBody);*/
		setSSLWithKeystore("keystore.jks");
		
		Vertx.vertx().createHttpClient(new HttpClientOptions().setSsl(true).setTrustAll(true)).getNow(8080, "localhost", "/get/testDb/limit/8", resp -> {
		      System.out.println("Got response " + resp.statusCode());
		      resp.bodyHandler(body -> System.out.println("Got data " + body.toString("ISO-8859-1")));
		});
	}
	
	public static void setSSLWithKeystore(String keystorePath) {
		System.setProperty("javax.net.ssl.keyStorePassword", "papayadb");
		System.setProperty("javax.net.ssl.trustStore", keystorePath);
		System.setProperty("javax.net.ssl.keyStore", keystorePath);
		System.setProperty("javax.net.ssl.trustStoreType", "jks");
	}
}