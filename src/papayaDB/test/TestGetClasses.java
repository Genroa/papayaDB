package papayaDB.test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestGetClasses {
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
    	Stream<Path> files = Files.list(directory);
    	List<Class<?>> classes = files	.filter(f -> f.getFileName().toString().endsWith(".class"))
    									.map(f -> {
											try {
												return Class.forName(packageName + '.' + f.getFileName().toString().substring(0, f.getFileName().toString().length() - 6));
											} catch (ClassNotFoundException e) {
												return null;
											}
										})
    									.collect(Collectors.toList());
    	return classes;
    }
    
	public static void main(String[] args) throws ClassNotFoundException, IOException {
		
		Class<?>[] clazz = getClasses("papayaDB.api.queryParameters");
		for (Class<?> c : clazz) {
		//	System.out.println(c.getName());
		}
	}
}
