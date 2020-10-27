package com.practice.exception;

import com.practice.constants.FileTransferErrors;

public class CannotFetchRemoteFileException extends FileTransferException {

   

    public CannotFetchRemoteFileException(final String message) {

        super(message, FileTransferErrors.FILE_NOT_FOUND.getCode());
    }

    public CannotFetchRemoteFileException(final String message,
                                              final Throwable cause) {
        super(message, cause, FileTransferErrors.FILE_NOT_FOUND.getCode());
    }
}
