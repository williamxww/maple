package com.bow.maple.server;


/**
 */
public class EventDispatchException extends RuntimeException {
    public EventDispatchException() {
        super();
    }


    public EventDispatchException(String message) {
        super(message);
    }
    
    
    public EventDispatchException(Throwable cause) {
        super(cause);
    }
    
    
    public EventDispatchException(String message, Throwable cause) {
        super(message, cause);
    }
}
