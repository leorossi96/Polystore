package it.uniroma3.exeptions;

public class MalformedQueryException extends Exception {

	private static final long serialVersionUID = 1L;
	
    public MalformedQueryException(String message) {
        super(message);
    }

    public MalformedQueryException(Throwable cause) {
        super(cause);
    }

    public MalformedQueryException(String message, Throwable cause) {
        super(message, cause);
    }

}
