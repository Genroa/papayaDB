package papayaDB.client;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.rxjava.core.Vertx;
import papayaDB.api.query.QueryInterface;

public class DemoClient extends AbstractVerticle{
	public static void main(String[] args) {
		QueryInterface client = QueryInterface.newHttpQueryInterface("localhost", 8080); /*"Genroa", "a58fdaf6dc9ee61c5aa5ee514d9b711ef72e239d8f1c53e1e05631357ffc8ed6f1f21d3f2f4c1d2220f5874b6d6e6d74fca6618a21c145866978052fb215c3be" lapin */
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