package com.practice.exception;

import com.practice.constants.FileTransferErrors;
/**
 * 
 * @author i508938
 *
 */
public class InvalidFileNamePatternException extends FileTransferException {

   

    public InvalidFileNamePatternException(final String message) {
        super(message, FileTransferErrors.INVALID_FILE_REGULAR_EXPRESSION.getCode());
    }

    public InvalidFileNamePatternException(final String message,
                                               final Throwable cause) {
        super(message, cause, FileTransferErrors.INVALID_FILE_REGULAR_EXPRESSION.getCode());
    }
}
