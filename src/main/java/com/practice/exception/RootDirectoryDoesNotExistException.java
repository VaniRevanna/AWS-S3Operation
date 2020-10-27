package com.practice.exception;

import com.practice.constants.FileTransferErrors;

public class RootDirectoryDoesNotExistException extends Exception {

	private String error_Code;
	
    public RootDirectoryDoesNotExistException() {
        super();
        this.error_Code = FileTransferErrors.ROOT_DIRECTORY_DOES_NOT_EXIST.toString();
    }

    public RootDirectoryDoesNotExistException(final String message) {
        super(message);
        this.error_Code = FileTransferErrors.ROOT_DIRECTORY_DOES_NOT_EXIST.toString();
        		
    }

    public RootDirectoryDoesNotExistException(final String message,
                                                  final Throwable cause) {
        super(message, cause);
        this.error_Code = FileTransferErrors.ROOT_DIRECTORY_DOES_NOT_EXIST.toString();
    }
}
