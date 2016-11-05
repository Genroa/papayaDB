package papayaDB.db;

import java.util.function.Consumer;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.NetSocket;
import papayaDB.api.QueryAnswer;
import papayaDB.api.QueryAnswerStatus;
import papayaDB.api.chainable.AbstractChainableQueryInterface;

public class LocalDataInterface extends AbstractChainableQueryInterface {
	private final NetServer tcpServer;

	public LocalDataInterface(int listeningPort) {
		NetServerOptions options = new NetServerOptions().setPort(listeningPort);
		tcpServer = getVertx().createNetServer(options);
		tcpServer.connectHandler(this::onTcpQuery);
	}

	public void listen() {
		tcpServer.listen();
		System.out.println("Now listening for TCP string queries...");
	}

	@Override
	public void close() {
		tcpServer.close();
		super.close();
	}

	@Override
	public void processQuery(String query, Consumer<QueryAnswer> callback) {
		JsonObject answer = new JsonObject();
		answer.put("status", QueryAnswerStatus.OK.name());
		answer.put("data", new JsonArray().add(query));

		callback.accept(new QueryAnswer(answer));
	}

	public void onTcpQuery(NetSocket socket) {
		System.out.println("New connection!");
		socket.handler(buffer -> {
			String query = buffer.toString();
			System.out.println("Received query: " + buffer.toString());

			processQuery(query, answer -> {
				socket.write(answer.getData().toString());
				socket.close();
			});

		});
	}


	public static void main(String[] args) {
		LocalDataInterface db = new LocalDataInterface(6666);
		db.listen();
	}
}