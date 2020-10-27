package com.practice.controller;

import com.newrelic.api.agent.Trace;
import com.practice.constants.ConfigurationConsts;
import com.practice.constants.ContentMimeType;
import com.practice.constants.ErrorConstants;
import com.practice.constants.FileSystemOptionKeys;
import com.practice.constants.FileTransferErrors;
import com.practice.constants.StoreFilePayload;
import com.practice.exception.AWSConnectionException;
import com.practice.exception.FileTransferException;
import com.practice.exception.InvalidFileNamePatternException;
import com.practice.model.CreateDirectoryPayload;
import com.practice.model.DeleteFolderPayLoad;
import com.practice.model.FileObjectProxy;
import com.practice.model.FileOperationResponse;
import com.practice.model.MoveFilePayload;
import com.practice.model.Pair;
import com.practice.s3.service.FileTransferService;
import com.practicecom.practice.utill.Validation;

import ch.qos.logback.classic.Logger;
import jakarta.validation.Valid;

import org.apache.commons.io.IOUtils;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import javax.servlet.http.HttpServletRequest;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Callable;
import static org.springframework.http.HttpStatus.*;
import static com.practice.constants.FileSystemOptionKeys.*;

@CrossOrigin
@RestController
@RequestMapping(value = "/api/sftp")
@EnableAsync
public class SFTPFileOperationController {
   

    private FileTransferService fileTransferService;
    private ConfigurationConsts consts;

    private static Logger logger = (Logger) LoggerFactory.getLogger(SFTPFileOperationController.class);
    @Autowired
    SFTPFileOperationController(final FileTransferService fileTransferService,
                                final ConfigurationConsts		 consts) {
        this.fileTransferService = fileTransferService;
        this.consts = consts;
    }

    @GetMapping(value = "/getTempLocation")
    public String getTempLocation() {
        return consts.getFileOperationTempDir();
    }

    @GetMapping(value = "/release")
    public String release() {
        return "2.2.2";
    }

    @Trace
    @PostMapping(value = "/v1/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<?> uploadV1(@RequestPart("file") MultipartFile multipartFile,
                                      @RequestParam("remoteDir") String remoteDir,
                                      @RequestParam(value = "server", defaultValue = "") String server,
                                      @RequestParam(value = "port", defaultValue = "0") String port,
                                      @RequestHeader(value = "env", defaultValue = "", required = false) String env,
                                      @RequestHeader(value = "x-correlation-id", defaultValue = "", required = false) String correlationId,
                                      @RequestHeader(value = "user", defaultValue = "", required = false) String user,
                                      @RequestHeader(value = "password", defaultValue = "", required = false) char[] pass,
                                      @RequestHeader(value = "entityId", defaultValue = "", required = false) String entityId,
                                      @RequestHeader(value = "connectorName", defaultValue = "", required = false) String connectorName) {
        final String nameofCurrMethod = "uploadV1";
        final String logMessage = String.format("Upload file %s to %s location in env: %s",
                multipartFile.getName(), remoteDir, env);
     //   final Instant start = logger.logStartTime(nameofCurrMethod, logMessage, correlationId, entityId, connectorName);
        final Map<FileSystemOptionKeys, Object> options = new EnumMap<>(FileSystemOptionKeys.class);
        options.put(FileSystemOptionKeys.REMOTE_DIR, remoteDir);
        options.put(FileSystemOptionKeys.REMOTE_DESTINATION, server);
        options.put(FileSystemOptionKeys.PORT, port);
        options.put(FileSystemOptionKeys.USER_NAME, user);
        options.put(FileSystemOptionKeys.PASSWORD, pass);
        options.put(FileSystemOptionKeys.ENVIRONMENT, Validation.sanitizeEnvironment(env));
        ResponseEntity<?> responseEntity;
        logger.info(String.format("multipartFile size: %s", multipartFile.getSize()));
        final String err =
                String.format("Could not upload to %s/%s in env %s", server, remoteDir, env);
        try {
            responseEntity = fileTransferService.uploadFile(multipartFile, options, entityId, connectorName)
                    .map(ResponseEntity::ok)
                    .orElseGet(() -> new ResponseEntity<>(HttpStatus.EXPECTATION_FAILED));
        } catch (AWSConnectionException e) {
            final String exc = String.format("%s. Exception from server %s", err, e.getMessage());
            logger.error(exc, correlationId, entityId, connectorName);
            responseEntity = ResponseEntity.status(e.getStatusCode()).body(e.getMessage());
        } catch (FileTransferException e) {
            responseEntity = new ResponseEntity<>(HttpStatus.EXPECTATION_FAILED);
            logger.error(String.format("uploadV1 Error: %s", e), correlationId, entityId, connectorName);
        }
      //  logger.logEndTime(nameofCurrMethod, logMessage, correlationId, start, entityId, connectorName);
        return responseEntity;
    }

    @Trace
    @GetMapping(value = "/v1/download")
    public ResponseEntity<?> downloadV1(@RequestHeader(value = "x-correlation-id", defaultValue = "", required = false) String correlationId,
                                        @Valid @RequestParam("remoteDir") String remoteDir,
                                        @Valid @RequestParam("fileName") String fileName,
                                        @RequestParam(value = "server", defaultValue = "", required = false) String server,
                                        @RequestParam(value = "port", defaultValue = "0", required = false) Integer port,
                                        @RequestHeader(value = "user", defaultValue = "", required = false) String user,
                                        @RequestHeader(value = "password", defaultValue = "", required = false) char[] pass,
                                        @RequestHeader(value = "env", defaultValue = "", required = false) String env,
                                        @RequestHeader(value = "entityId", defaultValue = "", required = false) String entityId,
                                        @RequestHeader(value = "connectorName", defaultValue = "", required = false) String connectorName) {
        final String nameofCurrMethod = "downloadV1";
        final String logMessage = String.format("Download file %s from %s location", fileName, remoteDir);
     //   final Instant start = logger.logStartTime(nameofCurrMethod, logMessage, correlationId, entityId, connectorName);

        final HttpHeaders headers = new HttpHeaders();
        // "Content-Disposition", String.format("inline; filename=\"%s\"", fileName
        headers.add(HttpHeaders.CONTENT_DISPOSITION, String.format("attachment; filename=\"%s\"", fileName));
        headers.add("Cache-Control", "no-cache, no-store, must-revalidate");
        headers.add("Pragma", "no-cache");
        headers.add("Expires", "0");
        InputStreamResource resource = null;
        final Map<FileSystemOptionKeys, Object> options =
                new EnumMap<>(FileSystemOptionKeys.class);
        options.put(FileSystemOptionKeys.REMOTE_DIR, remoteDir);
        options.put(FileSystemOptionKeys.FILE_NAME, fileName);
        options.put(FileSystemOptionKeys.ENVIRONMENT, Validation.sanitizeEnvironment(env));
        long fileLength = 0;

        final String err =
                String.format("Could not download from %s/%s in env %s", remoteDir, fileName, env);
        ResponseEntity<?> responseEntity;
        try {
            Optional<Pair<FileOperationResponse, String>> result =
                    fileTransferService.downloadFile(fileName, options, entityId, connectorName);
            if (result.isPresent()) {
                resource = new InputStreamResource(new FileInputStream(result.get().getValue()));
                fileLength = result.get().getKey().getUploadSize();
            }
            responseEntity = ResponseEntity.ok()
                    .headers(headers)
                    .contentLength(fileLength)
                    .contentType(MediaType.parseMediaType("application/txt"))
                    .body(resource);
        } catch (AWSConnectionException e) {
            final String exc = String.format("%s. Exception from server %s", err, e.getMessage());
            logger.error(exc, correlationId, entityId, connectorName);
            responseEntity = ResponseEntity.status(e.getStatusCode()).body(e.getMessage());
        } catch (FileNotFoundException | FileTransferException e) {
            logger.error(
                    String.format("%s, Error: %s, filename: %s", ErrorConstants.CANNOT_FETCH_FILE, e, fileName), correlationId, entityId, connectorName);
            responseEntity = ResponseEntity.notFound().build();
        }

      //  logger.logEndTime(nameofCurrMethod, logMessage, correlationId, start, entityId, connectorName);
        return responseEntity;
    }

    @Trace
    @GetMapping(value = "/v1/listFiles")
    public Callable<ResponseEntity<Object>> listFilesV1(
            @RequestHeader(value = "x-correlation-id", defaultValue = "", required = false) String correlationId,
            @RequestHeader(value = "entityId", defaultValue = "", required = false) String entityId,
            @RequestHeader(value = "connectorName", defaultValue = "", required = false) String connectorName,
            @RequestHeader(value = "env", defaultValue = "", required = false) String env,
            @Valid @RequestParam("remoteDir") String remoteDir,
            @RequestParam(value = "server", defaultValue = "", required = false) String server,
            @RequestParam(value = "port", defaultValue = "0", required = false) Integer port,
            @RequestHeader(value = "user", defaultValue = "", required = false) String user,
            @RequestHeader(value = "password", defaultValue = "", required = false) char[] pass,
            @RequestParam(name = "regularExpression", defaultValue = ".*") String regularExpression) {
        final String nameofCurrMethod = "listFilesV1";
        final String logMessage = String.format("Listing files in %s location with regex %s", remoteDir, regularExpression);
      //  final Instant start = logger.logStartTime(nameofCurrMethod, logMessage, correlationId, entityId, connectorName);
        Map<FileSystemOptionKeys, Object> options = new EnumMap<>(FileSystemOptionKeys.class);
        options.put(FileSystemOptionKeys.REMOTE_DIR, remoteDir);
        options.put(FileSystemOptionKeys.REGULAR_EXPRESSION, regularExpression);
        if(correlationId == null || correlationId.isEmpty()) {
            correlationId = String.format("%s:%s", remoteDir, env);
        }
       
        options.put(FileSystemOptionKeys.ENVIRONMENT, Validation.sanitizeEnvironment(env));
        ResponseEntity<Object> response;
        final Optional<List<String>> res;
        final String err =
                String.format("Could not list file %s in env %s", remoteDir, env);
        try {
            res = fileTransferService.listFiles(options, entityId, connectorName);
            if (res.isPresent()) {
                response = ResponseEntity.ok(res.get());
            } else {
                response = ResponseEntity.status(INTERNAL_SERVER_ERROR).body(ErrorConstants.FILE_NOT_FOUND);
                logger.error(ErrorConstants.FILE_NOT_FOUND, correlationId, entityId, connectorName);
            }
        } catch (AWSConnectionException e) {
            final String exc = String.format("%s. Exception from server %s", err, e.getMessage());
            logger.error(exc, correlationId, entityId, connectorName);
            response = ResponseEntity.status(e.getStatusCode()).body(e.getMessage());
        } catch (FileTransferException e) {
            String error = ErrorConstants.GENERIC_ERROR;
            if (e.getErrorCode() == FileTransferErrors.INVALID_FILE_REGULAR_EXPRESSION.getCode()) {
                error = ErrorConstants.WRONG_FILE_NAME_FILTER;
                response = ResponseEntity.status(BAD_REQUEST).body(error + ErrorConstants.DISPLAY_SPACE
                        + e.getMessage());
            } else if (e.getErrorCode() == FileTransferErrors.INVALID_REMOTE_LOCATION.getCode()) {
                error = ErrorConstants.REMOTE_FOLDER_NOT_FOUND;
                response = ResponseEntity.status(BAD_REQUEST).body(error + ErrorConstants.DISPLAY_SPACE
                        + e.getMessage());
            } else if (e.getErrorCode() == FileTransferErrors.CANNOT_CONNECT.getCode()) {
                error = ErrorConstants.CONNECTION_FAILED;
                response = ResponseEntity.status(BAD_REQUEST).body(error + ErrorConstants.DISPLAY_SPACE
                        + e.getMessage());
            } else {
                response = ResponseEntity.status(HttpStatus.EXPECTATION_FAILED).body("Wrong filename filter" +
                        e.getMessage());
            }
            logger.error(String.format("%s, Error: %s", error, e), correlationId, entityId, connectorName);
        } catch (Exception e) {
            final String exc = String.format("IOException in server %s", e.getMessage());
            logger.error(exc, correlationId, entityId, connectorName);
            response = ResponseEntity.status(INTERNAL_SERVER_ERROR)
                    .body(String.format("%s with exception %s", err, e.getMessage()));
        }
  //      logger.logEndTime(nameofCurrMethod, logMessage, correlationId, start, entityId, connectorName);
        final ResponseEntity<Object> finalResponse = response;
        return () -> finalResponse;
    }

    @Trace
    @GetMapping(value = "/v1/getfile")
    public ResponseEntity<Object> getFile(
            @RequestHeader(value = "x-correlation-id", defaultValue = "", required = false) String correlationId,
            @Valid @RequestParam("remoteDir") String remoteDir,
            @Valid @RequestParam("fileName") String fileName,
            @RequestParam(value = "server", defaultValue = "", required = false) String server,
            @RequestParam(value = "port", defaultValue = "0", required = false) Integer port,
            @RequestHeader(value = "user", defaultValue = "", required = false) String user,
            @RequestHeader(value = "password", defaultValue = "", required = false) char[] pass,
            @RequestHeader(value = "env", defaultValue = "", required = false) String env,
            @RequestHeader(value = "entityId", defaultValue = "", required = false) String entityId,
            @RequestHeader(value = "connectorName", defaultValue = "", required = false) String connectorName) {
        final String nameofCurrMethod = "getFile";
        final String logMessage = String.format("Get file %s from %s", fileName, remoteDir);
    //    final Instant start = logger.logStartTime(nameofCurrMethod, logMessage, correlationId, entityId, connectorName);
        Map<FileSystemOptionKeys, Object> options = new EnumMap<>(FileSystemOptionKeys.class);
        options.put(FileSystemOptionKeys.PROTOCOL, "sftp");
        options.put(FileSystemOptionKeys.REMOTE_DIR, remoteDir);
        options.put(FileSystemOptionKeys.FILE_NAME, fileName);
        options.put(FileSystemOptionKeys.ENVIRONMENT, Validation.sanitizeEnvironment(env));
        if(correlationId == null || correlationId.isEmpty()) {
            correlationId = String.format("%s:%s:%s", remoteDir, fileName, env);
        }
      

        ResponseEntity<Object> response;
        final String err =
                String.format("Error reading file %s in folder %s", fileName, remoteDir);
        try {
            final Optional<byte[]> result = fileTransferService.getRemoteFileStream(options, entityId, connectorName);

            if (result.isPresent()) {
                final byte[] resultBytes = result.get();
                logger.info(String.format("%s data length: %d", fileName, resultBytes.length),
                        correlationId, entityId, connectorName);
                response = ResponseEntity.ok(resultBytes);
            } else {
                logger.error(err, correlationId, entityId, connectorName);
                response = ResponseEntity.status(INTERNAL_SERVER_ERROR).body(err);
            }
        } catch (InvalidFileNamePatternException e) {
            final String exc = String.format("%s. Exception from server %s", err, e.getMessage());
            logger.error(exc, correlationId, entityId, connectorName);
            response = ResponseEntity.status(BAD_REQUEST)
                    .body(String.format("%s with exception %s", err, e.getMessage()));
        } catch (AWSConnectionException e) {
            final String exc = String.format("%s. Exception from server %s", err, e.getMessage());
            logger.error(exc, correlationId, entityId, connectorName);
            response = ResponseEntity.status(e.getStatusCode()).body(e.getMessage());
        } catch (FileTransferException e) {
            final String exc = String.format("%s with exception %s", err, e);
            logger.error(exc, correlationId, entityId, connectorName);
            response = ResponseEntity.status(BAD_REQUEST)
                    .body(String.format("%s with exception %s", err, e.getMessage()));
        } catch (Exception e) {
            final String exc = String.format("IOException in server %s", e.getMessage());
            logger.error(exc, correlationId, entityId, connectorName);
            response = ResponseEntity.status(INTERNAL_SERVER_ERROR)
                    .body(String.format("%s with exception %s", err, e.getMessage()));
        }
   //     logger.logEndTime(nameofCurrMethod, logMessage, correlationId, start, entityId, connectorName);
        return response;
    }

    @Trace
    @GetMapping(value = "/v2/getfile", produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public ResponseEntity<StreamingResponseBody> getFileV2(
            @RequestHeader(value = "x-correlation-id", defaultValue = "", required = false) String correlationId,
            @RequestHeader(value = "entityId", defaultValue = "", required = false) String entityId,
            @RequestHeader(value = "connectorName", defaultValue = "", required = false) String connectorName,
            @RequestHeader(value = "env", defaultValue = "", required = false) String env,
            @RequestParam(value = "server", defaultValue = "", required = false) String server,
            @RequestParam(value = "port", defaultValue = "0", required = false) Integer port,
            @RequestHeader(value = "user", defaultValue = "", required = false) String user,
            @RequestHeader(value = "password", defaultValue = "", required = false) char[] pass,
            @Valid @RequestParam("remoteDir") String remoteDir,
            @Valid @RequestParam("fileName") String fileName) {
        final String logMessage = String.format("Get file %s from %s", fileName, remoteDir);
        final String nameofCurrMethod = "getFileV2";
    //    final Instant start = logger.logStartTime(nameofCurrMethod, logMessage, correlationId, entityId, connectorName);

        Map<FileSystemOptionKeys, Object> options = new EnumMap<>(FileSystemOptionKeys.class);
        options.put(FileSystemOptionKeys.PROTOCOL, "sftp");
        options.put(FileSystemOptionKeys.FILE_NAME, fileName);
        options.put(FileSystemOptionKeys.REMOTE_DIR, remoteDir);
        options.put(FileSystemOptionKeys.ENVIRONMENT, Validation.sanitizeEnvironment(env));
        if(correlationId == null || correlationId.isEmpty()) {
            correlationId = String.format("%s:%s:%s", remoteDir, fileName, env);
        }
       

        ResponseEntity<StreamingResponseBody> response;
        final String err =
                String.format("Error reading file %s in folder %s", fileName, remoteDir);
        try {
            final Optional<FileObjectProxy> result =
                    fileTransferService.getRemoteFileStreamV2(options, entityId, connectorName);
            if (result.isPresent()) {
                final FileObjectProxy fileObjectProxy = result.get();
                final String finalCorrelationId = correlationId;
                final StreamingResponseBody streamingResponseBody = outputStream -> {
                    IOUtils.copy(fileObjectProxy.getFileStream(), outputStream);
                    logger.info(String.format("Closing %s", fileName), finalCorrelationId, entityId, connectorName);
                    fileObjectProxy.close();
                    outputStream.flush();
                };
                response = ResponseEntity.ok().body(streamingResponseBody);
            } else {
                final String msg = String.format("Cannot fetch %s ", fileName);
                logger.error(msg, correlationId, entityId, connectorName);
                response = ResponseEntity.status(BAD_REQUEST).body(null);
            }
        } catch (InvalidFileNamePatternException e) {
            final String exc = String.format("%s. Exception from server %s", err, e.getMessage());
            logger.error(exc, correlationId, entityId, connectorName);
            response = ResponseEntity.status(BAD_REQUEST).body(null);
        } catch (AWSConnectionException e) {
            final String exc = String.format("%s. Exception from server %s", err, e.getMessage());
            logger.error(exc, correlationId, entityId, connectorName);
            response = ResponseEntity.status(e.getStatusCode()).body(null);
        } catch (FileTransferException e) {
            final String exc = String.format("%s with exception %s", err, e);
            logger.error(exc, correlationId, entityId, connectorName);
            response = ResponseEntity.status(BAD_REQUEST).body(null);
        } catch (Exception e) {
            final String exc = String.format("Exception in server %s", e.getMessage());
            logger.error(exc, correlationId, entityId, connectorName);
            response = ResponseEntity.status(INTERNAL_SERVER_ERROR).body(null);
        }
     //   logger.logEndTime(nameofCurrMethod, logMessage, correlationId, start, entityId, connectorName);

        return response;
    }

    @Trace
    @PostMapping(value = "/v1/storefile")
    public ResponseEntity<Object> storeFile(
            @RequestHeader(value = "x-correlation-id", defaultValue = "", required = false) String correlationId,
            @RequestHeader(value = "entityId", defaultValue = "", required = false) String entityId,
            @RequestHeader(value = "connectorName", defaultValue = "", required = false) String connectorName,
            @RequestHeader(value = "env", defaultValue = "", required = false) String env,
            @RequestBody StoreFilePayload fileObjectProxy) {
        final String nameofCurrMethod = "storeFile";
        final String logMessage =
                String.format("Store file %s in %s", fileObjectProxy.getFileName(), fileObjectProxy.getRemoteDir());
       // final Instant start = logger.logStartTime(nameofCurrMethod, logMessage, correlationId, entityId, connectorName);

        Map<FileSystemOptionKeys, Object> options = new EnumMap<>(FileSystemOptionKeys.class);
        options.put(FileSystemOptionKeys.PROTOCOL, "sftp");
        options.put(FileSystemOptionKeys.REMOTE_DIR, fileObjectProxy.getRemoteDir());
        options.put(FileSystemOptionKeys.FILE_NAME, fileObjectProxy.getFileName());
        options.put(FileSystemOptionKeys.FILE_OBJECT, fileObjectProxy.getFileContent());
        options.put(FileSystemOptionKeys.CONTENT_MIME_TYPE, ContentMimeType.PLAIN_TEXT);
        if(correlationId == null || correlationId.isEmpty()) {
            correlationId = String.format("%s:%s:%s", fileObjectProxy.getRemoteDir(), fileObjectProxy.getFileName(), env);
        }
       
        options.put(FileSystemOptionKeys.ENVIRONMENT, Validation.sanitizeEnvironment(env));

        ResponseEntity<Object> response;
        final String err =
                String.format("Could not write file %s in %s", fileObjectProxy.getFileName(), fileObjectProxy.getRemoteDir());
        try {
            final Optional<FileOperationResponse> res = fileTransferService.setRemoteFileStream(options, entityId, connectorName);
            if (res.isPresent()) {
                response = ResponseEntity.ok(res.get());
            } else {
                logger.error(err, correlationId, entityId, connectorName);
                response = ResponseEntity.status(INTERNAL_SERVER_ERROR).body("Could not write file to SFTP");
            }
        } catch (InvalidFileNamePatternException e) {
            final String exc = String.format("%s. Exception from server %s", err, e.getMessage());
            logger.error(exc, correlationId, entityId, connectorName);
            response = ResponseEntity.status(BAD_REQUEST)
                    .body(String.format("%s with exception %s", err, e.getMessage()));
        } catch (AWSConnectionException e) {
            final String exc = String.format("%s. Exception from server %s", err, e.getMessage());
            logger.error(exc, correlationId, entityId, connectorName);
            response = ResponseEntity.status(e.getStatusCode()).body(e.getMessage());
        } catch (FileTransferException e) {
            final String exc = String.format("%s with exception %s", err, e);
            logger.error(exc, correlationId, entityId, connectorName);
            response = ResponseEntity.status(BAD_REQUEST)
                    .body(String.format("%s with exception %s", err, e.getMessage()));
        } catch (Exception e) {
            final String exc = String.format("IOException in server %s", e.getMessage());
            logger.error(exc, correlationId, entityId, connectorName);
            response = ResponseEntity.status(INTERNAL_SERVER_ERROR)
                    .body(String.format("%s with exception %s", err, e.getMessage()));
        }
       // logger.logEndTime(nameofCurrMethod, logMessage, correlationId, start, entityId, connectorName);
        return response;
    }

    @Trace
    @PostMapping(value = "/v2/storefile")
    public ResponseEntity<Object> storeFileV2(@RequestParam(value = "fileName") String fileName,
                                              @RequestParam(value = "remoteDir") String remoteDir,
                                              @RequestParam(value = "server", defaultValue = "", required = false) String server,
                                              @RequestParam(value = "port", defaultValue = "0", required = false) Integer port,
                                              @RequestHeader(value = "user", defaultValue = "", required = false) String user,
                                              @RequestHeader(value = "password", defaultValue = "", required = false) char[] pass,
                                              @RequestHeader(value = "env", defaultValue = "", required = false) String env,
                                              @RequestHeader(value = "x-correlation-id", defaultValue = "", required = false) String correlationId,
                                              @RequestHeader(value = "entityId", defaultValue = "", required = false) String entityId,
                                              @RequestHeader(value = "connectorName", defaultValue = "", required = false) String connectorName,
                                              HttpServletRequest request) {
        final String nameofCurrMethod = "storeFile";
        final String logMessage = String.format("Store file %s in %s", fileName, remoteDir);
       // final Instant start = logger.logStartTime(nameofCurrMethod, logMessage, correlationId, entityId, connectorName);

        final Map<FileSystemOptionKeys, Object> options = new EnumMap<>(FileSystemOptionKeys.class);
        options.put(PROTOCOL, "sftp");
        options.put(REMOTE_DIR, remoteDir);
        options.put(FILE_NAME, fileName);
        options.put(REMOTE_DESTINATION, server);
        options.put(PORT, port);
        options.put(USER_NAME, user);
        options.put(PASSWORD, pass);
        options.put(CONTENT_MIME_TYPE, ContentMimeType.FILE_INPUT_STREAM);
        if(correlationId == null || correlationId.isEmpty()) {
            correlationId = String.format("%s:%s:%s", remoteDir, fileName, env);
        }
       
        options.put(ENVIRONMENT, Validation.sanitizeEnvironment(env));

        ResponseEntity<Object> response;
        final String err =
                String.format("%s file %s in %s", FileTransferErrors.CANNOT_WRITE_TO_FILE, fileName, remoteDir);
        try {
            final InputStream inputStream = request.getInputStream();
            options.put(FILE_OBJECT, inputStream);
            final Optional<FileOperationResponse> res = fileTransferService.setRemoteFileStreamV2(options, entityId, connectorName);
            if (res.isPresent()) {
                response = ResponseEntity.ok(res.get());
            } else {
                response = ResponseEntity.status(INTERNAL_SERVER_ERROR).body(FileTransferErrors.CANNOT_WRITE_TO_FILE);
                logger.error(err, correlationId, entityId, connectorName);
            }
        } catch (InvalidFileNamePatternException e) {
            final String exc = String.format("%s. Exception from server %s", err, e.getMessage());
            logger.error(exc, correlationId, entityId, connectorName);
            response = ResponseEntity.status(BAD_REQUEST)
                    .body(String.format("%s with exception %s", err, e.getMessage()));
        } catch (AWSConnectionException e) {
            final String exc = String.format("%s. Exception from server %s", err, e.getMessage());
            logger.error(exc, correlationId, entityId, connectorName);
            response = ResponseEntity.status(e.getStatusCode()).body(e.getMessage());
        } catch (FileTransferException e) {
            final String exc = String.format("%s with exception %s", err, e);
            logger.error(exc, correlationId, entityId, connectorName);
            response = ResponseEntity.status(BAD_REQUEST)
                    .body(String.format("%s with exception %s", err, e.getMessage()));
        } catch (IOException e) {
            final String exc = String.format("IOException in server %s", e.getMessage());
            logger.error(exc, correlationId, entityId, connectorName);
            response = ResponseEntity.status(INTERNAL_SERVER_ERROR)
                    .body(String.format("%s with exception %s", err, e.getMessage()));
        }

       // logger.logEndTime(nameofCurrMethod, logMessage, correlationId, start, entityId, connectorName);
        return response;
    }

    @Trace
    @PostMapping(value = "/v1/mkdir")
    public ResponseEntity<Object> createDirectoryV1(
            @RequestHeader(value = "x-correlation-id", defaultValue = "", required = false) String correlationId,
            @RequestHeader(value = "entityId", defaultValue = "", required = false) String entityId,
            @RequestHeader(value = "connectorName", defaultValue = "", required = false) String connectorName,
            @RequestHeader(value = "env", defaultValue = "", required = false) String env,
            @RequestBody CreateDirectoryPayload createDirectoryPayload) {
        final String nameofCurrMethod = "createDirectoryV1";
        final String logMessage = String.format("Create file %s in %s",
                createDirectoryPayload.getCreatePath(), createDirectoryPayload.getRootDirectory());
       // final Instant start = logger.logStartTime(nameofCurrMethod, logMessage, correlationId, entityId, connectorName);
        final Map<FileSystemOptionKeys, Object> options = new EnumMap<>(FileSystemOptionKeys.class);
        options.put(ENVIRONMENT, Validation.sanitizeEnvironment(env));
        if(correlationId == null || correlationId.isEmpty()) {
            correlationId = String.format("%s:%s:%s",
                    createDirectoryPayload.getCreatePath(), createDirectoryPayload.getRootDirectory(), env);
        }
      
        ResponseEntity<Object> response;
        final String err =
                String.format("Error copying directory %s in %s",
                        createDirectoryPayload.getCreatePath(), createDirectoryPayload.getRootDirectory());
        try {
            response = ResponseEntity.ok(fileTransferService.createDirectory(createDirectoryPayload, options, entityId, connectorName));
        } catch (AWSConnectionException e) {
            final String exc = String.format("%s. Exception from server %s", err, e.getMessage());
            logger.error(exc, correlationId, entityId, connectorName);
            response = ResponseEntity.status(e.getStatusCode()).body(e.getMessage());
        } catch (FileTransferException e) {
            String statusMessage = "";
            final int erroCode = e.getErrorCode();
            if (erroCode == FileTransferErrors.CANNOT_CONNECT.getCode()) {
                statusMessage = "Cannot connect to the server. ";
            } else if (erroCode == FileTransferErrors.ROOT_DIRECTORY_DOES_NOT_EXIST.getCode()) {
                statusMessage = "No root directory: " + createDirectoryPayload.getRootDirectory();
            } else if (erroCode == FileTransferErrors.ALREADY_EXISTS.getCode()) {
                statusMessage = "Directory already exists: " + createDirectoryPayload.getRootDirectory();
            }
            logger.error(statusMessage, correlationId, entityId, connectorName);
            response = ResponseEntity.status(BAD_REQUEST).body(
                    statusMessage.isEmpty() ? e.getMessage() : statusMessage);
        }
     //   logger.logEndTime(nameofCurrMethod, logMessage, correlationId, start, entityId, connectorName);
        return response;
    }

    @Trace
    @PostMapping(value = "/v1/move")
    public ResponseEntity<Object> moveFileV1(
            @RequestHeader(value = "x-correlation-id", defaultValue = "", required = false) String correlationId,
            @RequestHeader(value = "entityId", defaultValue = "", required = false) String entityId,
            @RequestHeader(value = "connectorName", defaultValue = "", required = false) String connectorName,
            @RequestHeader(value = "env", defaultValue = "", required = false) String env,
            @RequestBody MoveFilePayload moveFilePayload) {
        final String nameofCurrMethod = "moveFileV1";
        final String logMessage = String.format("Move from %s to %s",
                moveFilePayload.getFromLocation(), moveFilePayload.getToRootDirectory());
       // final Instant start = logger.logStartTime(nameofCurrMethod, logMessage, correlationId, entityId, connectorName);
        final String fromLocation = moveFilePayload.fromLocation;
        final String toLocation = moveFilePayload.toLocationName;
        final Map<FileSystemOptionKeys, Object> options = new EnumMap<>(FileSystemOptionKeys.class);
        options.put(ENVIRONMENT, Validation.sanitizeEnvironment(env));
        if(correlationId == null || correlationId.isEmpty()) {
            correlationId = String.format("%s:%s:%s", fromLocation, toLocation, env);
        }
       
        ResponseEntity<Object> response;
        final String err =
                String.format("Error moving file from %s to %s",fromLocation, toLocation );
        try {
            final Optional<String> status = fileTransferService.moveFilesV1(moveFilePayload, options, entityId, connectorName);
            response = status.<ResponseEntity<Object>>map(ResponseEntity::ok)
                    .orElseGet(() -> ResponseEntity.status(INTERNAL_SERVER_ERROR).body("Could not move"));
        } catch (AWSConnectionException e) {
            final String exc = String.format("%s. Exception from server %s", err, e.getMessage());
            logger.error(exc, correlationId, entityId, connectorName);
            response = ResponseEntity.status(e.getStatusCode()).body(e.getMessage());
        } catch (FileTransferException e) {
            if (e.getErrorCode() == FileTransferErrors.ROOT_DIRECTORY_DOES_NOT_EXIST.getCode()) {
                response = ResponseEntity.status(NOT_FOUND).body(e.getMessage());
            } else if (e.getErrorCode() == FileTransferErrors.CANNOT_WRITE_TO_FILE.getCode()) {
                response = ResponseEntity.status(INSUFFICIENT_STORAGE).body(e.getMessage());
            } else if (e.getErrorCode() == FileTransferErrors.ALREADY_EXISTS.getCode()) {
                response = ResponseEntity.status(CONFLICT).body(e.getMessage());
            } else {
                response = ResponseEntity.status(BAD_REQUEST).body(e.getMessage());
            }
            final String exc = String.format("%s. Exception from server %s", err, e.getMessage());
            logger.error(exc, correlationId, entityId, connectorName);
        }
       // logger.logEndTime(nameofCurrMethod, logMessage, correlationId, start, entityId, connectorName);
        return response;
    }

    @Trace
    @PostMapping(value = "/v1/copy")
    public ResponseEntity<Object> copyFileV1(
            @RequestHeader(value = "x-correlation-id", defaultValue = "", required = false) String correlationId,
            @RequestHeader(value = "entityId", defaultValue = "", required = false) String entityId,
            @RequestHeader(value = "connectorName", defaultValue = "", required = false) String connectorName,
            @RequestHeader(value = "env", defaultValue = "", required = false) String env,
            @RequestBody MoveFilePayload copyFileUpload) {
        final String nameofCurrMethod = "copyFileV1";
        final String logMessage = String.format("Copy from %s to %s",
        		copyFileUpload.getFromLocation(), copyFileUpload.getToRootDirectory());
      //  final Instant start = logger.logStartTime(nameofCurrMethod, logMessage, correlationId, entityId, connectorName);

        final String fromLocation = copyFileUpload.fromLocation;
        final String toLocation = copyFileUpload.toLocationName;
        final Map<FileSystemOptionKeys, Object> options = new EnumMap<>(FileSystemOptionKeys.class);
        if(correlationId == null || correlationId.isEmpty()) {
            correlationId = String.format("%s:%s:%s", fromLocation, toLocation, env);
        }

        options.put(ENVIRONMENT, Validation.sanitizeEnvironment(env));
        ResponseEntity<Object> response;
        final String err =
                String.format("Error copying file from %s to %s",fromLocation, toLocation );
        try {
            final Optional<String> status = fileTransferService.copyFilesV1(copyFileUpload, options, entityId, connectorName);
            response = status.<ResponseEntity<Object>>map(ResponseEntity::ok)
                    .orElseGet(() -> ResponseEntity.status(INTERNAL_SERVER_ERROR).body("Could not copy"));
        } catch (AWSConnectionException e) {
            final String exc = String.format("%s. Exception from server %s", err, e.getMessage());
            logger.error(exc, correlationId, entityId, connectorName);
            response = ResponseEntity.status(e.getStatusCode()).body(e.getMessage());
        } catch (FileTransferException e) {
            if (e.getErrorCode() == FileTransferErrors.ROOT_DIRECTORY_DOES_NOT_EXIST.getCode()) {
                response = ResponseEntity.status(HttpStatus.NOT_FOUND).body(e.getMessage());
            } else if (e.getErrorCode() == FileTransferErrors.CANNOT_WRITE_TO_FILE.getCode()) {
                response = ResponseEntity.status(HttpStatus.INSUFFICIENT_STORAGE).body(e.getMessage());
            } else if (e.getErrorCode() == FileTransferErrors.ALREADY_EXISTS.getCode()) {
                response = ResponseEntity.status(CONFLICT).body(e.getMessage());
            } else {
                response = ResponseEntity.status(BAD_REQUEST).body(e.getMessage());
            }
            logger.error(String.format("copyFileV1 exception: %s", e), correlationId, entityId, connectorName);
        }
      //  logger.logEndTime(nameofCurrMethod, logMessage, correlationId, start, entityId, connectorName);
        return response;
    }

    @Trace
    @PostMapping(value = "/v2/move")
    public ResponseEntity<Object> moveFileV2(
            @RequestHeader(value = "x-correlation-id", defaultValue = "", required = false) String correlationId,
            @RequestHeader(value = "entityId", defaultValue = "", required = false) String entityId,
            @RequestHeader(value = "connectorName", defaultValue = "", required = false) String connectorName,
            @RequestHeader(value = "env", defaultValue = "", required = false) String env,
            @RequestBody MoveFilePayload moveFilePayload) {
        final String nameofCurrMethod = "moveFileV2";
        final String logMessage = String.format("Move from %s to %s",
                moveFilePayload.getFromLocation(), moveFilePayload.getToRootDirectory());
       // final Instant start = logger.logStartTime(nameofCurrMethod, logMessage, correlationId, entityId, connectorName);

        ResponseEntity<Object> response;
        final Map<FileSystemOptionKeys, Object> options = new EnumMap<>(FileSystemOptionKeys.class);
        final String fromLocation = moveFilePayload.fromLocation;
        final String toLocation = moveFilePayload.toLocationName;
        if(correlationId == null || correlationId.isEmpty()) {
            correlationId = String.format("%s:%s:%s", fromLocation, toLocation, env);
        }
     
        options.put(ENVIRONMENT, Validation.sanitizeEnvironment(env));
        final String err =
                String.format("Error moving file from %s to %s",fromLocation, toLocation );
        try {
            final Optional<String> status =
                    fileTransferService.moveFilesV2(moveFilePayload, options, entityId, connectorName);
            response = status.<ResponseEntity<Object>>map(ResponseEntity::ok)
                    .orElseGet(() -> ResponseEntity.status(INTERNAL_SERVER_ERROR).body("Could not move"));
        } catch (AWSConnectionException e) {
            final String exc = String.format("%s. Exception from server %s", err, e.getMessage());
            logger.error(exc, correlationId, entityId, connectorName);
            response = ResponseEntity.status(e.getStatusCode()).body(e.getMessage());
        } catch (FileTransferException e) {
            if (e.getErrorCode() == FileTransferErrors.ROOT_DIRECTORY_DOES_NOT_EXIST.getCode()) {
                response = ResponseEntity.status(HttpStatus.NOT_FOUND).body(e.getMessage());
            } else if (e.getErrorCode() == FileTransferErrors.ALREADY_EXISTS.getCode()) {
                response = ResponseEntity.status(CONFLICT).body(e.getMessage());
            } else if (e.getErrorCode() == FileTransferErrors.CANNOT_WRITE_TO_FILE.getCode()) {
                response = ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(e.getMessage());
            } else {
                response = ResponseEntity.status(BAD_REQUEST).body(e.getMessage());
            }
            final String exc = String.format("%s. Exception from server %s", err, e.getMessage());
            logger.error(exc, correlationId, entityId, connectorName);
        }
     //   logger.logEndTime(nameofCurrMethod, logMessage, correlationId, start, entityId, connectorName);
        return response;
    }

    @Trace
    @PostMapping(value = "/v1/delete")
    public ResponseEntity<Object> deleteFolderV1(
            @RequestHeader(value = "x-correlation-id", defaultValue = "", required = false) String correlationId,
            @RequestHeader(value = "entityId", defaultValue = "", required = false) String entityId,
            @RequestHeader(value = "connectorName", defaultValue = "", required = false) String connectorName,
            @RequestHeader(value = "env", defaultValue = "", required = false) String env,
            @RequestBody DeleteFolderPayLoad deleteFolderPayLoad) {
        final String nameofCurrMethod = "deleteFolderV1";
        final String logMessage = String.format("Delete %s",
                deleteFolderPayLoad.getDirectorName());
       // final Instant start = logger.logStartTime(nameofCurrMethod, logMessage, correlationId, entityId, connectorName);
        ResponseEntity<Object> response;
        final Map<FileSystemOptionKeys, Object> options = new EnumMap<>(FileSystemOptionKeys.class);
        options.put(CORRELATION_ID, correlationId);
        options.put(ENVIRONMENT, Validation.sanitizeEnvironment(env));
        final String err =
                String.format("Error deleting file %s in env %s",deleteFolderPayLoad.getDirectorName(), env);
        try {
            response = ResponseEntity.ok(fileTransferService.removeDirectory(deleteFolderPayLoad, options, entityId, connectorName));
        } catch (AWSConnectionException e) {
            final String exc = String.format("%s. Exception from server %s", err, e.getMessage());
            logger.error(exc, correlationId, entityId, connectorName);
            response = ResponseEntity.status(e.getStatusCode()).body(e.getMessage());
        } catch (FileTransferException e) {
            if (e.getErrorCode() == FileTransferErrors.ROOT_DIRECTORY_DOES_NOT_EXIST.getCode()) {
                response = ResponseEntity.status(HttpStatus.NOT_FOUND).body(e.getMessage());
            } else if(e.getErrorCode() == FileTransferErrors.FILE_NOT_FOUND.getCode()) {
                response = ResponseEntity.status(HttpStatus.NOT_FOUND).body(e.getMessage());
            } else if (e.getErrorCode() == FileTransferErrors.GENERIC_ERROR.getCode()) {
                response = ResponseEntity.status(HttpStatus.EXPECTATION_FAILED).body(e.getMessage());
            } else {
                response = ResponseEntity.status(BAD_REQUEST).body(e.getMessage());
            }
            logger.error(String.format("deleteFolderV1 exception: %s", e), correlationId, entityId, connectorName);
        }
       // logger.logEndTime(nameofCurrMethod, logMessage, correlationId, start, entityId, connectorName);
        return response;
    }

    @Trace
    @GetMapping(value = "/testFileOperations")
    public ResponseEntity<String> testFileOperations(@RequestHeader(value = "env", defaultValue = "", required = false) String env) {
        final String res = fileTransferService.testFileOperations(Validation.sanitizeEnvironment(env));
        return ResponseEntity.ok(res);
    }
}
