package papayaDB.client;

import papayaDB.api.query.HttpQueryInterface;

public class DemoClient {
	public static void main(String[] args) {
		HttpQueryInterface client = new HttpQueryInterface("localhost", 8080);
//		client.processQuery("get/testDb/fields/[name,age]/bounds/[age;0;18]/limit/1/equals/[name=%22/Pierre%22]/order/[age;ASC]", answer -> {
//			System.out.println(answer);
//			client.close();
//		});
		
		client.processQuery("get/testDb/limit/8", answer -> {
			System.out.println(answer);
			client.close();
		});
	}
}