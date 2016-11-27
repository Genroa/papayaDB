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
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.vertx.core.json.JsonObject;
import papayaDB.api.query.QueryType;
import papayaDB.api.query.SyntaxErrorException;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;


/**
 * Classe abstraite représentant les différents paramètres de requete
 */
public abstract class QueryParameter {
	/**
	 * Map des différents paramètres de requete en fonction du type de requete.
	 */
	static final Map<QueryType, Map<String, ? super QueryParameter>> parameter = new HashMap<>();
	/**
	 * Permet de savoir si la classe est déjà chargée ou non.
	 */
	private static boolean isLoaded;
	
	static {
		loadQueryParameter();
	}
	
	/**
	 * Méthode de chargement des différentes classes héritant de cette classe. Une instance est stockée dans la map, permettant 
	 * d'appeller les méthode de ces classes dynamiquement.
	 */
	private static void loadQueryParameter() {
		//Création de la map pour chaque type contenu dans les QueryType
		for (QueryType qt: QueryType.values()) {
			parameter.put(qt, new HashMap<String, QueryParameter>());
		}
		
		Class<?>[] clazz = null;
		try {
			//Récupération des différentes classes du package
			clazz = getClasses("papayaDB.api.queryParameters");
		} catch (ClassNotFoundException | IOException e) {
			
		}
		//pour chaque clesse si elle hérite de celle ci, on invoke sa méthode register pour qu'elle s'ajoute dans la bonne map.
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
		// Confirmation du chargement des différentes classes.
		isLoaded = true;
	}
	
	/**
	 * Récupération des différentes classes d'un package
	 * @param packageName
	 * 				Nom du package dans lequel chercher les classes
	 * @return
	 * 				Retourne un tabeau des différentes classes du package
	 * @throws ClassNotFoundException
	 * @throws IOException
	 */
	private static Class<?>[] getClasses(String packageName) throws ClassNotFoundException, IOException {
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		String path = packageName.replace('.', '/');
		Enumeration<URL> resources = classLoader.getResources(path);
		List<Path> dirs = new ArrayList<>();
		while (resources.hasMoreElements()) {
			URL resource = (URL) resources.nextElement();
			// Fix pour les path Windows
			dirs.add(Paths.get(resource.getPath().replaceFirst("^/(.:/)", "$1")));
		}
		ArrayList<Class<?>> classes = new ArrayList<>();
		for (Path directory : dirs) {
			classes.addAll(findClasses(directory, packageName));
		}
		return (Class[]) classes.toArray(new Class[classes.size()]);
	}

    /**
     * Chargement des différentes classes d'un package
     * @param directory
     * 				Dossier à partir du quel charger les classes
     * @param packageName
     * 				Nom du package
     * @return
     * 				Renvoie une liste des classes trouvées
     * @throws ClassNotFoundException
     * @throws IOException
     */
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
	
	/**
	 * Méthode à réimplémenter pour définir comment la classe s'enregistre.
	 */
	public static void registerParameter() {
		throw new NotImplementedException();
	}
	
	/**
	 * Méthode permettant d'ajouter le la valeur dans le JSON.
	 * Chaque classe qui hérite de {@link QueryParameter} connait la clé à ajouter et la méthode d'ajout.
	 * 
	 * @param json
	 * 				{@link JsonObject} dans lequel ajouter la valeur
	 * @param value
	 * 				Valeur à ajouter
	 * @return
	 * 				Renvoie le {@link JsonObject} modifié. Il revient avec une erreur dans le cas où la classe à rencontré un soucis de traitement.
	 */
	public JsonObject valueToJson(JsonObject json, String value) throws SyntaxErrorException {
		throw new NotImplementedException();
	}
	
	/**
	 * @param parameters
	 * @param elements
	 * @return
	 */
	public Stream<JsonObject> processQueryParameters(JsonObject parameters, Stream<JsonObject> elements) {
		return elements;
	}
	
	/**
	 * Renvoie le {@link JsonObject} associé au mot clé parameters du json donné 
	 * @param json
	 * 				{@link JsonObject} sur lequel chercher le champ parameters
	 * @return
	 * 				Le {@link JsonObject} parameters ou un {@link JsonObject} vide si la clé parameters n'existe pas
	 */
	static JsonObject getJsonParameters(JsonObject json) {
		if(json.containsKey("parameters")) {
			return json.getJsonObject("parameters");
		}
		return new JsonObject();
	}
	
	/**
	 * @param type
	 * @return
	 */
	public static Map<String, ? super QueryParameter> getQueryParametersForType(QueryType type) {
		if(!isLoaded) {
			loadQueryParameter();
		}
		return parameter.get(type);
	}
	
	/**
	 * @param type
	 * @param key
	 * @return
	 */
	public static Optional<QueryParameter> getQueryParameterKey(QueryType type, String key) {
		if(!isLoaded) {
			loadQueryParameter();
		}
//		System.out.println("getQueryParamKey = " + type.toString() + " " + key);
		return Optional	.ofNullable((QueryParameter) getQueryParametersForType(type).get(key));
	}
}
 