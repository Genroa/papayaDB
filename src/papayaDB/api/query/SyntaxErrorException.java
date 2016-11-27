package papayaDB.api.query;

import java.io.UncheckedIOException;

public class SyntaxErrorException extends RuntimeException {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public SyntaxErrorException(String message) {
		super(message);
	}
}
