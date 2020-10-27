package com.practice.constants;

public enum FileTransferErrors {
	
	 CANNOT_CONNECT(404),
	    FILE_NOT_FOUND(416),
	    INVALID_FILE_NAME(417),
	    INVALID_REMOTE_LOCATION(418),
	    INVALID_FILE_REGULAR_EXPRESSION(419),
	    USER_NAME_NOT_PROPER(420),
	    CANNOT_COPY(421),
	    CANNOT_WRITE_TO_FILE(422),
	    CANNOT_DELETE(423),
	    OPERATION_NOT_COMPLETED(424),
	    ROOT_DIRECTORY_DOES_NOT_EXIST(425),
	    ALREADY_EXISTS(426),
	    INVALID_INPUT(427),
	    NETWORK_ERROR(428),
	    GENERIC_ERROR(429);
	
	private Integer code;
	FileTransferErrors(int code) {
		this.code = code;
	}
	
	public Integer getCode() {
		return code;
	}
	
   
}
