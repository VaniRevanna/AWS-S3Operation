package com.practice.exception;

import org.springframework.http.HttpStatus;

import com.amazonaws.services.waf.model.HTTPRequest;


/**
 * Custom Exception to handle S3 operation from S3 with Code
 * @author i508938
 *
 */
public class FileTransferException extends Exception{
	
	 private  int code;


	    public FileTransferException(final String message) {
	        super(message);
	        this.code = HttpStatus.INTERNAL_SERVER_ERROR.value();
	    }

	    public FileTransferException(final String message,final int code) {
	        super(message);
	        this.code = code;
	    }

	    public FileTransferException(final String message,
	                                      final Throwable cause,
	                                      final int code) {
	        super(message, cause);
	        this.code = code;
	    }

	   

	    public int getErrorCode() {
	        return code;
	    }
}
