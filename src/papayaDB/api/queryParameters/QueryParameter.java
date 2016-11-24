package papayaDB.api.queryParameters;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.vertx.core.json.JsonObject;
import papayaDB.api.QueryType;
import papayaDB.db.Record;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public abstract class QueryParameter {
	static final Map<QueryType, Map<String, Class<? extends QueryParameter>>> parameter = new HashMap<>();
	
	static {
		for (QueryType qt: QueryType.values()) {
			parameter.put(qt, new HashMap<String, Class<? extends QueryParameter>>());
		}
		
		Class<?>[] clazz = null;
		try {
			clazz = getClasses("papayaDB.api.queryParameters");
		} catch (ClassNotFoundException | IOException e) {
			
		}
		for (Class<?> c : clazz) {
			if(!c.getName().toString().endsWith("QueryParameter")) {
				System.out.println("QP : " + c.getName());
				Method method = null;
				try {
					method = c.getMethod("registerParameter");
				} catch (NoSuchMethodException | SecurityException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				try {
					method.invoke(null);
				} catch (IllegalAccessException e) {
					throw new AssertionError(e);
				} catch (InvocationTargetException e) {
					Throwable cause = e.getCause(); 
					if(cause instanceof RuntimeException) {
						throw (RuntimeException)cause;
					}
					if(cause instanceof Error) { 
						throw (Error)cause;
					}
					throw new UndeclaredThrowableException(cause);
				}
			}
		}
	}
	
	private static Class<?>[] getClasses(String packageName) throws ClassNotFoundException, IOException {
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		String path = packageName.replace('.', '/');
		Enumeration<URL> resources = classLoader.getResources(path);
		List<Path> dirs = new ArrayList<>();
		while (resources.hasMoreElements()) {
			URL resource = (URL) resources.nextElement();
			
			dirs.add(Paths.get(resource.getPath()));
		}
		ArrayList<Class<?>> classes = new ArrayList<>();
		for (Path directory : dirs) {
			classes.addAll(findClasses(directory, packageName));
		}
		return (Class[]) classes.toArray(new Class[classes.size()]);
	}

    private static List<Class<?>> findClasses(Path directory, String packageName) throws ClassNotFoundException, IOException {
    	/*if (Files.exists(directory)) {
    		return classes;
    	}*/
    	
    	//TODO: voir dans le tp 3 pour faire ça propre
    	Stream<Path> files = Files.list(directory);
    	List<Class<?>> classes = files	.filter(f -> f.getFileName().toString().endsWith(".class"))
    									.map(f -> {
											try {
												return Class.forName(packageName + '.' + f.getFileName().toString().substring(0, f.getFileName().toString().length() - 6));
											} catch (ClassNotFoundException e) {
												return null;
											}
										})
    									.filter(f -> f != null)
    									.collect(Collectors.toList());
    	return classes;
    }
	
	public static void registerParameter() {
		throw new NotImplementedException();
	}
	
	public static JsonObject valueToJson(JsonObject json, String value) {
		throw new NotImplementedException();
	}
	
	public static Stream<JsonObject> processQueryParameters(JsonObject parameters, Stream<Record> elements) {
		throw new NotImplementedException();
	}
	
	static JsonObject getParams(JsonObject json) {
		if(json.containsKey("parameters")) {
			return json.getJsonObject("parameters");
		}
		return new JsonObject();
	}
	
	public static Map<QueryType, Map<String, Class<? extends QueryParameter>>> getParameter() {
		return parameter;
	}
}
