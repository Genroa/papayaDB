package papayaDB.client;

import papayaDB.api.query.QueryInterface;

public class DemoClient {
	public static void main(String[] args) {
		QueryInterface client = QueryInterface.newHttpQueryInterface("localhost", 8080, "Genroa", "a58fdaf6dc9ee61c5aa5ee514d9b711ef72e239d8f1c53e1e05631357ffc8ed6f1f21d3f2f4c1d2220f5874b6d6e6d74fca6618a21c145866978052fb215c3be"); // lapin
//		client.processQuery("get/testDb/fields/[name,age]/bounds/[age;0;18]/limit/1/equals/[name=%22/Pierre%22]/order/[age;ASC]", answer -> {
//			System.out.println(answer);
//			client.close();
//		});
		
		client.processQuery("get/testDb/fields/[name,age]/bounds/[age;0;18]/limit/1/equals/[name=%22/Pierre%22]/order/[age;ASC]", answer -> {
			System.out.println(answer);
			client.close();
		});
	}
}