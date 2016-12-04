package papayaDB.rest;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.JksOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.core.Vertx;
import papayaDB.api.query.QueryInterface;
import papayaDB.api.query.QueryType;

/**
 * Représente une interface web d'accès à une base de données, par l'intermédiaire de routes
 * suivant le REST. 
 *
 */
public class RESTQueryInterface extends AbstractVerticle{
	private final HttpServer listeningServer;
	private final QueryInterface tcpClient;
	private final int listeningPort;
	private final Router router;

	public RESTQueryInterface(String host, int connectionPort, int listeningPort) {
		
		HttpServerOptions options = new HttpServerOptions().setSsl(true).setKeyStoreOptions(
				new JksOptions()
					.setPath("keystore.jks")
					.setPassword("papayadb")
				);
		
		tcpClient = QueryInterface.newTcpQueryInterface(host, connectionPort);
		this.listeningPort = listeningPort;
		
		router = Router.router(getVertx());
		router.post("/createdb/*").handler(x -> this.createNewDatabase(x));
		router.post("/insert/*").handler(x -> this.insertNewRecord(x));
		router.post("/update/*").handler(x -> this.updateRecord(x));
		router.delete("/deletedb/*").handler(x -> this.deleteDatabase(x));
		router.get("/exportall/*").handler(x -> this.exportDatabase(x));
		router.get("/get/*").handler(x -> this.getRecords(x));
		router.delete("/delete/*").handler(x -> this.deleteRecords(x));
		
		listeningServer = Vertx.vertx().createHttpServer(options);
	} 
	
	@Override
	public void start() throws Exception {
		listen();
		super.start();
	}
	
	public void listen() {
		listeningServer.requestHandler(router::accept).listen(listeningPort);
		System.out.println("[REST:listen]Now listening for HTTP REST queries...");
	}
	
	public void close() {
		listeningServer.close();
		tcpClient.close();
	}
	
	public static void main(String[] args) {
		RESTQueryInterface RESTInterface = new RESTQueryInterface("localhost", 6666, 8080);
		RESTInterface.listen();
	}

	public void createNewDatabase(RoutingContext routingContext) {
		HttpServerResponse response = routingContext.response();
		HttpServerRequest request = routingContext.request();
		
		JsonObject json = UrlToQuery.convertToJson(request.path(), QueryType.CREATEDB);
		if(json.containsKey("status")) { //Savoir si une erreur à été détectée pendant le parsing de l'URL
			response.putHeader("content-type", "application/json")
			.end(Json.encodePrettily(json));
			return;
		}
		tcpClient.createNewDatabase(json.getString("db"),
									json.getJsonObject("auth").getString("user"), 
									json.getJsonObject("auth").getString("hash"), 
									answer -> {
										response.putHeader("content-type", "application/json")
										.end(Json.encodePrettily(answer.getData()));
									});
	}

	public void deleteDatabase(RoutingContext routingContext) {
		HttpServerResponse response = routingContext.response();
		HttpServerRequest request = routingContext.request();
		
		JsonObject json = UrlToQuery.convertToJson(request.path(), QueryType.DELETEDB);
		if(json.containsKey("status")) { //Savoir si une erreur à été détectée pendant le parsing de l'URL
			response.putHeader("content-type", "application/json")
			.end(Json.encodePrettily(json));
			return;
		}
		
		tcpClient.deleteDatabase(json.getString("db"),
									json.getJsonObject("auth").getString("user"), 
									json.getJsonObject("auth").getString("hash"), 
									answer -> {
										response.putHeader("content-type", "application/json")
										.end(Json.encodePrettily(answer.getData()));
									});
	}

	public void exportDatabase(RoutingContext routingContext) {
		HttpServerResponse response = routingContext.response();
		HttpServerRequest request = routingContext.request();
		
		JsonObject json = UrlToQuery.convertToJson(request.path(), QueryType.EXPORTALL);
		if(json.containsKey("status")) { //Savoir si une erreur à été détectée pendant le parsing de l'URL
			response.putHeader("content-type", "application/json")
			.end(Json.encodePrettily(json));
			return;
		}
		
		tcpClient.exportDatabase(json.getString("db"),
									answer -> {
										response.putHeader("content-type", "application/json")
										.end(Json.encodePrettily(answer.getData()));
									});
	}

	public void updateRecord(RoutingContext routingContext) {
		HttpServerResponse response = routingContext.response();
		HttpServerRequest request = routingContext.request();
		
		JsonObject json = UrlToQuery.convertToJson(request.path(), QueryType.UPDATE);
		if(json.containsKey("status")) { //Savoir si une erreur à été détectée pendant le parsing de l'URL
			response.putHeader("content-type", "application/json")
			.end(Json.encodePrettily(json));
			return;
		}
		
		request.bodyHandler(buffer -> {
			JsonObject body = buffer.toJsonObject();
			tcpClient.updateRecord(json.getString("db"),
					body.getString("uid"),
					body.getJsonObject("newRecord"),
					json.getJsonObject("auth").getString("user"), 
					json.getJsonObject("auth").getString("hash"), 
					answer -> {
						response.putHeader("content-type", "application/json")
						.end(Json.encodePrettily(answer.getData()));
					});
		});
	}

	public void deleteRecords(RoutingContext routingContext) {
		HttpServerResponse response = routingContext.response();
		HttpServerRequest request = routingContext.request();
		
		JsonObject json = UrlToQuery.convertToJson(request.path(), QueryType.DELETE);
		if(json.containsKey("status")) { //Savoir si une erreur à été détectée pendant le parsing de l'URL
			response.putHeader("content-type", "application/json")
			.end(Json.encodePrettily(json));
			return;
		}
		
		tcpClient.deleteRecords(json.getString("db"),
								json.getJsonObject("parameters"),
								json.getJsonObject("auth").getString("user"), 
								json.getJsonObject("auth").getString("hash"), 
								answer -> {
									response.putHeader("content-type", "application/json")
									.end(Json.encodePrettily(answer.getData()));
								});
	}

	public void insertNewRecord(RoutingContext routingContext) {
		HttpServerResponse response = routingContext.response();
		HttpServerRequest request = routingContext.request();
		
		JsonObject json = UrlToQuery.convertToJson(request.path(), QueryType.INSERT);
		if(json.containsKey("status")) { //Savoir si une erreur à été détectée pendant le parsing de l'URL
			response.putHeader("content-type", "application/json")
			.end(Json.encodePrettily(json));
			return;
		}
		request.bodyHandler(buffer -> {
			JsonObject body = buffer.toJsonObject();
			tcpClient.insertNewRecord(json.getString("db"),
					body,
					json.getJsonObject("auth").getString("user"), 
					json.getJsonObject("auth").getString("hash"), 
					answer -> {
						response.putHeader("content-type", "application/json")
						.end(Json.encodePrettily(answer.getData()));
					});
		});
	}

	public void getRecords(RoutingContext routingContext) {
		HttpServerResponse response = routingContext.response();
		HttpServerRequest request = routingContext.request();
		
		JsonObject json = UrlToQuery.convertToJson(request.path(), QueryType.GET);
		if(json.containsKey("status")) { //Savoir si une erreur à été détectée pendant le parsing de l'URL
			response.putHeader("content-type", "application/json")
			.end(Json.encodePrettily(json));
			return;
		}
		
		tcpClient.getRecords(json.getString("db"),
							json.getJsonObject("parameters"),
							answer -> {
								response.putHeader("content-type", "application/json")
								.end(Json.encodePrettily(answer.getData()));
							});
	}
}
