package com.practice.s3.service;

import com.amazonaws.*;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.http.timers.client.ClientExecutionTimeoutException;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.transfer.model.UploadResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.newrelic.api.agent.Trace;
import com.practice.constants.ConfigurationConsts;
import com.practice.constants.FileSystemOptionKeys;
import com.practice.exception.AWSConnectionException;
import com.practice.exception.CannotFetchRemoteFileException;
import com.practice.exception.FileTransferException;
import com.practice.exception.RemoteFolderNameWrongException;
import com.practice.exception.RootDirectoryDoesNotExistException;
import com.practice.model.CreateDirectoryPayload;
import com.practice.model.DeleteFolderPayLoad;
import com.practice.model.FileObjectProxy;
import com.practice.model.FileOperationResponse;
import com.practice.model.MoveFilePayload;
import com.practicecom.practice.utill.TimeIt;
import com.practicecom.practice.utill.Validation;

import org.apache.commons.io.FileUtils;
import org.apache.http.Consts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

import static com.amazonaws.retry.PredefinedRetryPolicies.DEFAULT_BACKOFF_STRATEGY;
import static java.nio.file.Files.createTempFile;
import static org.springframework.http.HttpStatus.*;
import static com.practice.constants.ConfigurationConsts.*;
import static com.amazonaws.retry.PredefinedRetryPolicies.DEFAULT_BACKOFF_STRATEGY;
import static com.amazonaws.retry.PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION;
import static com.practice.constants.FileSystemOptionKeys.*;
import static com.practice.constants.ContentMimeType.*;
import static com.practicecom.practice.utill.FileSystemUtils.readFileToString;
import static com.practicecom.practice.utill.FileSystemUtils.writeContentsToFile;
/**
 * {@link S3OperationsManager} AWS S3 operations manager. This class does all the operations needed for AWS S3
 * It creates the AWS access token and then uses it to operate on the S3 bucket. This class is tightly coupled with
 * a particular bucket. To operate on a different bucket a new instance of the class needs to be created.
 */
public class S3OperationsManager {
    private static final String CLASS_NAME = S3OperationsManager.class.getSimpleName();
   
    /**
     * Used for pretty printing objects as JSON in {@link #prettyPrintJson(String, Object)}
     */
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final String SEPERATOR = "/";
    private static final char SEPERATOR_CHAR = '/';

    /**
     * The S3 client used to operate on the S3 buckets. This is the default client.
     */
    private final AmazonS3 s3ClientDefault;
    private final TransferManager s3TransferManagerDefault;

    /**
     * The S3 client used to operate on the S3 buckets. This client points to QA
     */
    private final AmazonS3 s3ClientQa;
    private final TransferManager s3TransferManagerQa;

    /**
     * The S3 client used to operate on the S3 buckets. This client points to Production
     */
    private final AmazonS3 s3ClientProduction;
    private final TransferManager s3TransferManagerProduction;

    private final ConfigurationConsts consts;
    
    private final ExceptionHandler exceptionHandler;

    /**
     * Constructs a new S3OperationsManager, which creates a temporary token based S3 client
     * This uses arn to assume a role and operate on behalf of that role.
     *
     * @param consts {@link Consts} This contains all the values fetched from the config server
     */
    public S3OperationsManager(final ConfigurationConsts consts, final ExceptionHandler exceptionHandler) {
        this.consts = consts;
        this.exceptionHandler = exceptionHandler;

        s3ClientDefault = createS3Client(DEFAULT);
        s3TransferManagerDefault = createS3TransferManager(DEFAULT);

        s3ClientQa = createS3Client(QA);
        s3TransferManagerQa = createS3TransferManager(QA);

        s3ClientProduction = createS3Client(PRODUCTION);
        s3TransferManagerProduction = createS3TransferManager(PRODUCTION);
    }

    /***
     *
     * @param env environment For which to return the AWS client
     * @return {@link AmazonS3} depending on the environment
     */
    @Trace
    private AmazonS3 getAwsClient(final String env) {
        AmazonS3 awsClient;
        switch (env.trim().toUpperCase()) {
            case QA:
                awsClient = s3ClientQa;
                break;
            case PRODUCTION:
                awsClient = s3ClientProduction;
                break;
            default:
                awsClient = s3ClientDefault;
                break;
        }
        return awsClient;
    }

    @Trace
    private TransferManager getAwsTransferManager(final String env) {
        TransferManager transferManager;
        switch (env.trim().toUpperCase()) {
            case QA:
                transferManager = s3TransferManagerQa;
                break;
            case PRODUCTION:
                transferManager = s3TransferManagerProduction;
                break;
            default:
                transferManager = s3TransferManagerDefault;
                break;
        }
        return transferManager;
    }

    /**
     *
     * @param env Environment for which the manager should be created
     * @return {@link TransferManager}
     */
    private TransferManager createS3TransferManager(final String env) {
        return TransferManagerBuilder.standard()
                .withS3Client(getAwsClient(env))
                .withDisableParallelDownloads(false)
                .withMinimumUploadPartSize(consts.getAwsS3MinimumPartsize())
                .withMultipartCopyPartSize(consts.getAwsS3MinimumPartsize())
                .withExecutorFactory(() -> Executors.newFixedThreadPool(consts.getAwsS3MaxUploadthreads()))
                .build();
    }

    /**
     * Constructs a new {@link AmazonS3} object. This object is inorder used to do operations on S3
     *
     * @param env The environment for the client. It can be either Qa or production. If this value is empty or null then
     *            the default client is created.
     * @return {@link AmazonS3}
     */
    private AmazonS3 createS3Client(final String env) {
        final String methodName = " | createS3Client | ";
        TimeIt timeIt = new TimeIt(methodName, "", "", "");
        AmazonS3 amazonS3 = null;
        try {
            timeIt.start();
            System.setProperty("java.net.useSystemProxies", "true");
            final RetryPolicy retryPolicy = new RetryPolicy(DEFAULT_RETRY_CONDITION,
                    DEFAULT_BACKOFF_STRATEGY,
                    consts.getAwsMaxRetries(),
                    true);

            final ClientConfiguration clientConfiguration = new ClientConfiguration()
//                    .withRequestTimeout(consts.getAwsS3ClientRequestTimeout())
                    .withConnectionTimeout(consts.getAwsS3ClientConnectionTimeout())
                    .withSocketTimeout(consts.getAwsS3ClientSocketTimeout())
                    .withClientExecutionTimeout(consts.getAwsS3ClientExecutionTimeout())
                    .withThrottledRetries(true)
                    .withMaxErrorRetry(consts.getAwsMaxRetries())
                    .withRetryPolicy(retryPolicy)
                    .withMaxConsecutiveRetriesBeforeThrottling(consts.getMaxConsecutiveRetriesBeforeThrottling())
//                    .withMaxConsecutiveRetriesBeforeThrottling(200)
                    .withConnectionTTL(consts.getAwsS3ClientConnectionTtl());

            if (consts.getConcurProxyHost() != null && !consts.getConcurProxyHost().isEmpty()) {
                clientConfiguration.setProxyHost(consts.getConcurProxyHost().trim());
                clientConfiguration.setProxyPort(consts.getConcurProxyPort());
                this.getlogger().info("getS3Client: Setting proxy host: %s, port: %s",
                        consts.getConcurProxyHost(), consts.getConcurProxyPort());
            } else {
                this.getlogger().info("getS3Client: No proxy set for S3 Client");
            }

            if (consts.getAwsNonProxyHosts() != null && !consts.getAwsNonProxyHosts().isEmpty()) {
                clientConfiguration.setNonProxyHosts(consts.getAwsNonProxyHosts().trim());
                this.getlogger().info("getS3Client: Setting non-proxy hosts: %s", consts.getAwsNonProxyHosts());
            } else {
                this.getlogger().info("getS3Client: No non-proxy set for S3 Client");
            }

            final AWSCredentials basicAWSCredentials = new BasicAWSCredentials(consts.getKid(env), consts.getAkey(env));
            amazonS3 = AmazonS3ClientBuilder.standard()
                    .withCredentials(new STSRefreshCredentialsProvider(basicAWSCredentials, consts.getAwsArn(env), clientConfiguration))
                    .withRegion(consts.getAwsBucketRegion(env))
                    .withClientConfiguration(clientConfiguration)
                    .build();
            this.getlogger().info("getS3Client: S3 client built");
        } catch (Exception e) {
            this.getlogger().error("getS3Client: Exception: %s", e);
        }
        timeIt.stop();
        return amazonS3;
    }

    /**
     * Normalize the file name so that it adheres to S3 standard.
     *
     * @param inputFileName The input file name that we need to normalize
     * @return The normalized file name
     */
    @Trace
    public String normalizeFileName(final String inputFileName) {
        final Path path = Paths.get(inputFileName).normalize();
        String result = path.toString();
        if (path.isAbsolute()) {
            result = result.substring(1);
        }
        this.getlogger().info("normalizeFileName: From %s to %s", path, result);
        return result;
    }

    /**
     * Join 2 path names and normalize them to adhere to S3
     *
     * @param path1 First path
     * @param path2 Second path
     * @return The normalized path
     */
    @Trace
    private String joinPaths(final String path1, final String path2) {
        this.getlogger().info("joinPaths: Join %s and %s", path1, path2);
        return normalizeFileName(path1 + SEPERATOR + path2);
    }

    /**
     * Download the file storeed in S3 as multipart file
     *
     * @param fullFileName       The full file name to download
     * @param localDirectoryName The local directory where to download the file and then send
     * @param optionsObjectMap   The various options we need to pass to this function. We will need the following
     *                           parameters like userDirectory, localfileName, correlation id.
     *                           Uses {@link #normalizeFileName(String)} to normalize the file name.
     *                           Uses {@link TransferManager} to transfer the file. This needs the {@link AmazonS3}
     * @param entityId           The Entity id to log
     * @param connectorName      The connector name to log
     * @return Returns the path to the downloaded file
     * @throws GISTFileTransferExceptions
     */
    @Trace
    public Optional<Path> downloadFile(final String fullFileName,
                                       final String localDirectoryName,
                                       final Map<FileSystemOptionKeys, Object> optionsObjectMap,
                                       String entityId, String connectorName) throws FileTransferException {
        final String methodName = " | downloadFile | ";
        final String userDirectory = normalizeFileName((String) optionsObjectMap.get(REMOTE_DIR));
        final String locaFileName = (String) optionsObjectMap.get(FILE_NAME);
        final String correlationId = (String) optionsObjectMap.getOrDefault(CORRELATION_ID, "");
        final String env = (String) optionsObjectMap.getOrDefault(ENVIRONMENT, "");
        final Path locaFilePath = Paths.get(localDirectoryName + SEPERATOR + locaFileName);
        final File localFile = locaFilePath.toFile();
        localFile.deleteOnExit();
        final String remoteFileName = String.format("%s%s%s", userDirectory, SEPERATOR, locaFileName);
        this.getlogger().info(String.format("downloadFile Root Path: %s, fullFileName: %s, remoteFileName: %s",
                locaFilePath.toString(), fullFileName, remoteFileName), correlationId, entityId, connectorName);
        final TimeIt timeIt = new TimeIt( methodName, correlationId, "", "");
        timeIt.start();
        Optional<Path> pathOptional = Optional.empty();
        try {
            final long partSize = 10 * 1024 * 1024;
            final TransferManager tm = TransferManagerBuilder.standard()
                    .withS3Client(getAwsClient(env))
                    .withDisableParallelDownloads(false)
                    .withMinimumUploadPartSize(partSize)
                    .withExecutorFactory(() -> Executors.newFixedThreadPool(consts.getAwsS3MaxUploadthreads()))
                    .build();
            final Download download =
                    tm.download(consts.getAwsBucketName(env), remoteFileName, localFile);
            download.waitForCompletion();
            pathOptional = Optional.of(localFile.toPath());
            this.getlogger().info(String.format("downloadFile: fullFileName: %s, size: %s", localFile, localFile.length()), correlationId, entityId, connectorName);
        } catch (ClientExecutionTimeoutException e) {
            final String msg = String.format("%s %s: ClientExecutionTimeoutException %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
          
        } catch (AmazonS3Exception e) {
            final String msg = String.format("%s %s: AmazonS3Exception %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
           
        } catch (AmazonServiceException e) {
            final String msg = String.format("%s %s: AmazonServiceException %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
           
        } catch (SdkClientException e) {
            final String msg = String.format("%s %s: SdkClientException %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
           
        } catch (AmazonClientException e) {
            final String msg = String.format("%s %s: AmazonClientException %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
           
        } catch (HttpClientErrorException e) {
            final String msg = String.format("%s %s: HttpClientErrorException %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
           
        } catch (ResourceAccessException e) {
            final String msg = String.format("%s %s: ResourceAccessException %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
           
        } catch (Exception e) {
            final String msg = String.format("%s %s: Exception %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
          
        }

        if (!pathOptional.isPresent() && localFile.exists()) {
            try {
                Files.delete(localFile.toPath());
            } catch (IOException e) {
                this.getlogger().error("downloadFile: Could not cleanup local file. " + e, correlationId, entityId, connectorName);
            }
        } else {
            this.getlogger().warn("downloadFile: %s or %s does not exist", pathOptional, localFile);
        }
        timeIt.stop();
        return pathOptional;
    }

    /**
     * Upload a file to S3 as a multipart file. This uses {@link TransferManager} and {@link AmazonS3} to
     * upload the file
     *
     * @param multipartFile    The file to upload as multipart file
     * @param optionsObjectMap The various options needed for ths function to work.
     * @param entityId         Entity id to log
     * @param connectorName    Connector name to log if needed
     * @return Returns the respose of the upload operation if upload happened
     * @throws GISTFileTransferExceptions
     */
    public Optional<FileOperationResponse> uploadFile(final MultipartFile multipartFile,
                                                      final Map<FileSystemOptionKeys, Object> optionsObjectMap,
                                                      String entityId, String connectorName)
            throws FileTransferException {
        final String methodName = " | uploadFile | ";
        FileOperationResponse fileOperationResponse = null;
        final String env = normalizeFileName((String) optionsObjectMap.get(ENVIRONMENT));
        final TransferManager tm = TransferManagerBuilder.standard()
                .withDisableParallelDownloads(false)
                .withExecutorFactory(() -> Executors.newFixedThreadPool(consts.getAwsS3MaxUploadthreads()))
                .withS3Client(getAwsClient(env))
                .build();
        final String correlationId = (String) optionsObjectMap.getOrDefault(CORRELATION_ID, "");
        final String userDirectory = normalizeFileName((String) optionsObjectMap.get(REMOTE_DIR));
        final TimeIt timeIt = new TimeIt( methodName, correlationId, "", "");
        timeIt.start();
        final String fullFileName = joinPaths(userDirectory, multipartFile.getOriginalFilename());
        try {
            final ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setContentLength(multipartFile.getSize());
            objectMetadata.setContentType(multipartFile.getContentType());
            final Upload upload =
                    tm.upload(consts.getAwsBucketName(env), fullFileName, multipartFile.getInputStream(), objectMetadata);
            final UploadResult uploadResult = upload.waitForUploadResult();
            if (upload.isDone()) {
                multipartFile.getInputStream().close();
            }

            fileOperationResponse = new FileOperationResponse(uploadResult.getKey(), multipartFile.getSize());
        } catch (ClientExecutionTimeoutException e) {
            final String msg = String.format("%s %s: ClientExecutionTimeoutException %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
         
        } catch (AmazonS3Exception e) {
            final String msg = String.format("%s %s: AmazonS3Exception %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
           
        } catch (AmazonServiceException e) {
            final String msg = String.format("%s %s: AmazonServiceException %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
         
        } catch (SdkClientException e) {
            final String msg = String.format("%s %s: SdkClientException %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
          
        } catch (AmazonClientException e) {
            final String msg = String.format("%s %s: AmazonClientException %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
           
        } catch (HttpClientErrorException e) {
            final String msg = String.format("%s %s: HttpClientErrorException %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
          
        } catch (ResourceAccessException e) {
            final String msg = String.format("%s %s: ResourceAccessException %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
           
        } catch (Exception e) {
            final String msg = String.format("%s %s: Exception %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
          
        }
        timeIt.stop();
        return Optional.ofNullable(fileOperationResponse);
    }

    /**
     * List all the buckets
     *
     * @return The list of buckets. Some times the S3 client does not have access to list all the buckets. At this time
     * This method may fail.
     * @throws GISTFileTransferExceptions
     */
    public Optional<Set<String>> listBuckets(final String env) throws FileTransferException {
        this.getlogger().info("listBuckets Listing buckets");
        final Set<String> bucketList = new HashSet<>();
        final AmazonS3 amazonS3 = getAwsClient(env);
        for (final Bucket bucket : amazonS3.listBuckets()) {
            bucketList.add(bucket.getName());
            this.getlogger().info(" - " + bucket.getName());
        }
        return Optional.of(bucketList);
    }

    /**
     * List the files in the particular folder in S3. This takes a regular expression also to apply on the listed files
     * and filter.
     *
     * @param optionsObjectMap Different options needed. This has the remote directory to list files from, the regular
     *                         expression to filter the listed files
     * @param includeFolder    If this is true then display folders too, else only files are gathered
     * @param entityId         The entity id to log
     * @param connectorName    The connector name to log
     * @return List of files
     * @throws GISTFileTransferExceptions
     */
    @Trace
    public Optional<List<String>> listFilesInFolder(final Map<FileSystemOptionKeys, Object> optionsObjectMap,
                                                    final boolean includeFolder,
                                                    String entityId,
                                                    String connectorName)
            throws FileTransferException {
        final String methodName = " | listFilesInFolder | ";
        final String remoteDir = (String) optionsObjectMap.get(REMOTE_DIR);
        final String env = (String) optionsObjectMap.get(ENVIRONMENT);
        final String regularExpression = (String) optionsObjectMap.get(REGULAR_EXPRESSION);
        final String correlationId = (String) optionsObjectMap.getOrDefault(CORRELATION_ID, "");
        final String bname = consts.getAwsBucketName(env);
        final String startMsg = String.format("%s %s Start with remote dir %s, env %s", CLASS_NAME, methodName, remoteDir, env);
        this.getlogger().info(startMsg, correlationId, entityId, connectorName);
        final TimeIt timeIt = new TimeIt(methodName, correlationId, entityId, connectorName);
        timeIt.start();
        final List<String> fileList = new ArrayList<>();
        final AmazonS3 amazonS3 = getAwsClient(env);
        try {
            if (amazonS3 == null) {
                final String msg =
                        String.format("Backend connection not valid. Cannot list files in %s for %s",
                                bname, connectorName);
                this.getlogger().error(String.format("%s %s %s", CLASS_NAME, methodName, msg), correlationId, entityId, connectorName);
                throw new FileTransferException(msg);
            } else {
                final String msg = String.format("%s %s Proceeding with listing files for remotedir %s and env %s",
                        CLASS_NAME, methodName, remoteDir, env);
                this.getlogger().info(msg, correlationId, entityId, connectorName);
            }
            if (amazonS3.doesBucketExistV2(bname)) {
                this.getlogger().info("listFilesInFolder ListObjectsV2Result Bucket exists. Proceeding with listing files",
                        correlationId, entityId, connectorName);
                final ListObjectsV2Result result = amazonS3.listObjectsV2(bname, remoteDir);
                this.getlogger().info(
                        "listFilesInFolder ListObjectsV2Result prefix: " + result.getPrefix() + " ; common prefix: " +
                                result.getCommonPrefixes().toString() + " ;getStartAfter: " + result.getStartAfter(),
                        correlationId, entityId, connectorName);
                final List<S3ObjectSummary> objects = result.getObjectSummaries();
                final Pattern filePattern = Pattern.compile(regularExpression);
                this.getlogger().info(String.format("%s %s Found %d objects", CLASS_NAME, methodName, objects.size()),
                        correlationId, entityId, connectorName);
                for (final S3ObjectSummary os : objects) {
                    final String objName = normalizeFileName(os.getKey());
                    this.getlogger().info(String.format("* %s, Size: %s, ETag: %s", objName, os.getSize(), os.getETag()),
                            correlationId, entityId, connectorName);
                    if (!objName.equalsIgnoreCase("") && (includeFolder || os.getSize() > 0)) {
                        // Add the file object only if the name matches the pattern
                        final String[] fileNameElements = objName.split(SEPERATOR);
                        if (filePattern.matcher(fileNameElements[fileNameElements.length - 1]).matches()) {
                            fileList.add(objName);
                        }
                    }
                }
            } else {
                final String msg = String.format("Root directory %s does not exist in bucket %s",
                        remoteDir, bname);
                this.getlogger().error(String.format("%s %s %s", CLASS_NAME, methodName, msg), correlationId, entityId, connectorName);
                throw new RootDirectoryDoesNotExistException(msg);
            }
        } catch (ClientExecutionTimeoutException e) {
            final String msg = String.format("%s %s: ClientExecutionTimeoutException %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
          
        } catch (AmazonS3Exception e) {
            final String msg = String.format("%s %s: AmazonS3Exception %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
           
        } catch (AmazonServiceException e) {
            final String msg = String.format("%s %s: AmazonServiceException %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
         
        } catch (SdkClientException e) {
            final String msg = String.format("%s %s: SdkClientException %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
          
        } catch (AmazonClientException e) {
            final String msg = String.format("%s %s: AmazonClientException %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
           
        } catch (HttpClientErrorException e) {
            final String msg = String.format("%s %s: HttpClientErrorException %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
          
        } catch (ResourceAccessException e) {
            final String msg = String.format("%s %s: ResourceAccessException %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
         
        } catch (Exception e) {
            final String msg = String.format("%s %s: Exception %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
       
        }

        final String endMsg = String.format("%s %s End with remote dir %s, env %s", CLASS_NAME, methodName, remoteDir, env);
        timeIt.stop();
        this.getlogger().info(endMsg, correlationId, entityId, connectorName);
        return Optional.of(fileList);
    }

    /**
     * Create the remote directory on S3.
     *
     * @param createDirectoryPayload The {@link CreateDirectoryPayload} for capturing the payload for creating the
     *                               directory
     * @param optionsObjectMap       Various options that will be needed by this method
     * @param entityId               Entity id to log
     * @param connectorName          Connector name to log
     * @return IF success the created directory path
     * @throws GISTFileTransferExceptions
     */
    @Trace
    public Optional<String> createdDir(final CreateDirectoryPayload createDirectoryPayload,
                                       final Map<FileSystemOptionKeys, Object> optionsObjectMap,
                                       final String entityId,
                                       final String connectorName) throws FileTransferException {
        final String methodName = " | createdDir | ";
        final String correlationId = (String) optionsObjectMap.getOrDefault(CORRELATION_ID, "");
        final String env = (String) optionsObjectMap.getOrDefault(ENVIRONMENT, "");
        final String startMsg =
                String.format("%s %s Start create of folder %s and path %s in env %s",
                        CLASS_NAME, methodName, createDirectoryPayload.getRootDirectory(),
                        createDirectoryPayload.getCreatePath(), env);
        this.getlogger().info(startMsg, correlationId, entityId, connectorName);
        final TimeIt timeIt = new TimeIt( methodName, correlationId, entityId, connectorName);
        timeIt.start();
        // create meta-data for your folder and set content-length to 0
        final ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(0);

        // create empty content
        final InputStream emptyContent = new ByteArrayInputStream(new byte[0]);

        // create a PutObjectRequest passing the folder name suffixed by /
        String remoteDir = createDirectoryPayload.getRootDirectory();
        if (remoteDir.length() > 0 && remoteDir.charAt(0) == SEPERATOR_CHAR) {
            remoteDir = remoteDir.substring(1);
        }

        String createPath = createDirectoryPayload.getCreatePath();
        if (createPath.length() > 0 && createPath.charAt(0) == SEPERATOR_CHAR) {
            createPath = createPath.substring(1);
        }

        if (createPath.length() > 0 && createPath.charAt(createPath.length() - 1) == SEPERATOR_CHAR) {
            createPath = createPath.substring(0, createPath.length() - 1);
        }

        if (remoteDir.isEmpty() && createPath.isEmpty()) {
            this.getlogger().error("createdDir: Remote directory %s or createpath %s null",
                    remoteDir, createPath);
            throw new RemoteFolderNameWrongException("emty directory");
        }

        PutObjectRequest putObjectRequest = null;
        try {
            final String bname = consts.getAwsBucketName(env);
            putObjectRequest = new PutObjectRequest(bname,
                    remoteDir + SEPERATOR + createPath + SEPERATOR, emptyContent, metadata);
            getAwsClient(env).putObject(putObjectRequest);
            final String msg = String.format("%s %s Response %s ", CLASS_NAME, methodName, putObjectRequest.getKey());
            this.getlogger().info(msg, correlationId, entityId, connectorName);
        } catch (ClientExecutionTimeoutException e) {
            final String msg = String.format("%s %s: ClientExecutionTimeoutException %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
         
        } catch (AmazonS3Exception e) {
            final String msg = String.format("%s %s: AmazonS3Exception %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
           
        } catch (AmazonServiceException e) {
            final String msg = String.format("%s %s: AmazonServiceException %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
         
        } catch (SdkClientException e) {
            final String msg = String.format("%s %s: SdkClientException %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
          
        } catch (AmazonClientException e) {
            final String msg = String.format("%s %s: AmazonClientException %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
          
        } catch (HttpClientErrorException e) {
            final String msg = String.format("%s %s: HttpClientErrorException %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
           
        } catch (ResourceAccessException e) {
            final String msg = String.format("%s %s: ResourceAccessException %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
        
        } catch (Exception e) {
            final String msg = String.format("%s %s: Exception %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
           
        }
        final String endMsg =
                String.format("%s %s Start create of folder %s and path %s in env %s",
                        CLASS_NAME, methodName, createDirectoryPayload.getRootDirectory(),
                        createDirectoryPayload.getCreatePath(), env);
        timeIt.stop();
        this.getlogger().info(endMsg, correlationId, entityId, connectorName);
        return putObjectRequest != null ? Optional.of(putObjectRequest.getKey()) : Optional.of(FAILED);
    }

    /**
     * Remove the directory on remote S3 if it exists
     *
     * @param deleteFolderPayLoad The payload {@link DeleteFolderPayLoad} which has the specifications
     * @param optionsObjectMap    {@link Map<FileSystemOptionKeys, Object>} The various options that is needed.
     *                            They are Remote directory, regular expression,
     *                            environment based on which the bucket is choosen.
     * @param entityId            Entity Id if present
     * @param connectorName       Connector name if present
     * @return Status of the operation
     * @throws GISTFileTransferExceptions If there are any problems then an exception is thrown.
     */
    @Trace
    public Optional<String> removeDir(final DeleteFolderPayLoad deleteFolderPayLoad,
                                      final Map<FileSystemOptionKeys, Object> optionsObjectMap,
                                      String entityId,
                                      String connectorName) throws FileTransferException {
        final String methodName = " | removeDir | ";
        Optional<String> result = Optional.empty();
        final String correlationId = (String)optionsObjectMap.getOrDefault(CORRELATION_ID, "");
        optionsObjectMap.put(REMOTE_DIR, deleteFolderPayLoad.getDirectorName());
        optionsObjectMap.put(REGULAR_EXPRESSION, deleteFolderPayLoad.getRegularExpressionString());
        final String env = (String) optionsObjectMap.getOrDefault(ENVIRONMENT, "");
        final String startMsg =
                String.format("%s %s Start delete of folder %s in env %s",
                        CLASS_NAME, methodName, deleteFolderPayLoad.getDirectorName(), env);
        this.getlogger().info(startMsg, correlationId, entityId, connectorName);
        final TimeIt timeIt = new TimeIt( methodName, correlationId, entityId, connectorName);
        timeIt.start();
        final Optional<List<String>> fileListResult =
                listFilesInFolder(optionsObjectMap, true, entityId, connectorName);
        if (fileListResult.isPresent()) {
            prettyPrintJson("listFilesInFolder: %s", fileListResult.get());
            final List<String> fileListToDelete = fileListResult.get();
            if (!fileListToDelete.isEmpty()) {
                final String bname = consts.getAwsBucketName(env);
                final AmazonS3 awsS3Client = getAwsClient(env);
                try {
                    // Check to ensure that the bucket is versioning-enabled.
                    final String bucketVersionStatus =
                            getAwsClient(env).getBucketVersioningConfiguration(bname).getStatus();
                    String msg = String.format("%s %s Bucket %s, version info: %s in env: %s", CLASS_NAME, methodName,
                            bname, bucketVersionStatus, env);
                    this.getlogger().info(msg, correlationId, entityId, connectorName);

                    DeleteObjectsRequest multiObjectDeleteRequest;
                    /*
                    / Following reasoning is used:
                    / 1. If Versioning is not enabled in bucket then regular deletion is used in bucket
                    / 2. If versioning is enabled but the key deleteAllVersions is false then also regular deletion is used,
                    /    older versions are retained.
                    / 3. IF versioning is enabled but retainOlderFileVersions is true delete all versions
                    */
                    if(!bucketVersionStatus.equals(BucketVersioningConfiguration.ENABLED)) {
                        // Versioning not enabled
                        multiObjectDeleteRequest = new DeleteObjectsRequest(bname)
                                .withQuiet(false)
                                .withKeys(fileListToDelete.toArray(new String[0]));
                    } else {
                        // Versioning is enabled
                        final List<DeleteObjectsRequest.KeyVersion> keys = new ArrayList<>();
                        if(consts.getRetainOlderFileVersions()) {
                            // retain older versions
                            fileListToDelete
                                    .parallelStream()
                                    .forEachOrdered(k -> keys.add(new DeleteObjectsRequest.KeyVersion(k)));
                        } else {
                            // Delete all versions
                            fileListToDelete
                                    .parallelStream()
                                    .map(k -> findAllVersionsToDelete(k, env, awsS3Client, entityId, connectorName, ""))
                                    .forEach(keys::addAll);
                        }
                        multiObjectDeleteRequest =
                                new DeleteObjectsRequest(bname)
                                        .withKeys(keys)
                                        .withQuiet(false);
                    }
                    // Delete objects and Verify that the objects were deleted successfully.
                    final DeleteObjectsResult delObjRes = awsS3Client.deleteObjects(multiObjectDeleteRequest);
                    final int deletedFiles = delObjRes.getDeletedObjects().size();

                    result = Optional.of("Number of files deleted: " + deletedFiles);

                    msg = String.format("%s %s Number of files to delete: %s, files deleted: %s", CLASS_NAME, methodName,
                            deletedFiles, fileListToDelete);
                    this.getlogger().info(msg, correlationId, entityId, connectorName);
                } catch (ClientExecutionTimeoutException e) {
                    final String msg = String.format("%s %s: ClientExecutionTimeoutException %s", CLASS_NAME, methodName, e);
                    this.getlogger().error(msg, correlationId, entityId, connectorName);
                 
                } catch (AmazonS3Exception e) {
                    final String msg = String.format("%s %s: AmazonS3Exception %s", CLASS_NAME, methodName, e);
                    this.getlogger().error(msg, correlationId, entityId, connectorName);
                   
                } catch (AmazonServiceException e) {
                    final String msg = String.format("%s %s: AmazonServiceException %s", CLASS_NAME, methodName, e);
                    this.getlogger().error(msg, correlationId, entityId, connectorName);
                   
                } catch (SdkClientException e) {
                    final String msg = String.format("%s %s: SdkClientException %s", CLASS_NAME, methodName, e);
                    this.getlogger().error(msg, correlationId, entityId, connectorName);
                    
                } catch (AmazonClientException e) {
                    final String msg = String.format("%s %s: AmazonClientException %s", CLASS_NAME, methodName, e);
                    this.getlogger().error(msg, correlationId, entityId, connectorName);
                  
                } catch (HttpClientErrorException e) {
                    final String msg = String.format("%s %s: HttpClientErrorException %s", CLASS_NAME, methodName, e);
                    this.getlogger().error(msg, correlationId, entityId, connectorName);
                    
                } catch (ResourceAccessException e) {
                    final String msg = String.format("%s %s: ResourceAccessException %s", CLASS_NAME, methodName, e);
                    this.getlogger().error(msg, correlationId, entityId, connectorName);
                  
                } catch (Exception e) {
                    final String msg = String.format("%s %s: Exception %s", CLASS_NAME, methodName, e);
                    this.getlogger().error(msg, correlationId, entityId, connectorName);
                   
                }
            } else {
                this.getlogger().error("removeDir: File list to delete empty");
                throw new FileTransferException("Folder not found");
            }
        } else {
            this.getlogger().error("removeDir: Could not retrieve the list of files to delete.");
            throw new FileTransferException("Could not retrieve the list of files to delete.");
        }

        final String endMsg =
                String.format("%s %s End delete of folder %s in env %s",
                        CLASS_NAME, methodName, deleteFolderPayLoad.getDirectorName(), env);
        timeIt.stop();
        this.getlogger().info(endMsg, correlationId, entityId, connectorName);
        return result;
    }

    private Set<DeleteObjectsRequest.KeyVersion> findAllVersionsToDelete(final String fileName, final String env,
                                                                          final AmazonS3 awsS3Client,
                                                                          final String entityId,
                                                                          final String connectorName,
                                                                          final String correlationId) {
        final Set<DeleteObjectsRequest.KeyVersion> result = new HashSet<>();
        try {
            Objects.requireNonNull(findAllVersionsOfAFile(fileName, env, awsS3Client, entityId, connectorName, correlationId))
                    .parallelStream()
                    .forEach(v -> result.add(new DeleteObjectsRequest.KeyVersion(fileName, v)));
        } catch (FileTransferException e) {
            this.getlogger().error("findAllVersionsToDelete AmazonS3Exception: " + e, correlationId, entityId, connectorName);
        }
        return result;
    }

    /**
     * This method finds all the versions of a file and sends. This method assumes that the file name is proper.
     * This method will also find the delete marker with the other versions.
     * @param fileName File name to find
     * @param env Environment
     * @param entityId Used for logging
     * @param connectorName Used for logging.
     * @param awsS3Client S3 client. This service assumes this is a valid entity
     * @param correlationId Correlation id
     * @return {@link Set<String>} of all the file versions.
     */
    private Set<String> findAllVersionsOfAFile(final String fileName, final String env,
                                                    final AmazonS3 awsS3Client, String entityId, String connectorName,
                                                    final String correlationId) throws FileTransferException {
        final String methodName = " | findAllVersionsOfAFile | ";
        final String FIND_VERSION_ERROR = "Cannot set remote file.";
        final Set<String> result = new HashSet<>();
        final TimeIt timeIt = new TimeIt(methodName, correlationId, entityId, connectorName);
        timeIt.start();
        try {
            final String bname = consts.getAwsBucketName(env);
            // Retrieve the list of versions. If the bucket contains more versions
            // than the specified maximum number of results, Amazon S3 returns
            // one page of results per request.
            final ListVersionsRequest ListVersionRequest = new ListVersionsRequest()
                    .withBucketName(bname)
                    .withPrefix(fileName);
            final VersionListing versionListing = awsS3Client.listVersions(ListVersionRequest);
            final List<S3VersionSummary> objectSummaries = versionListing.getVersionSummaries();
            objectSummaries.parallelStream().forEach(e -> result.add(e.getVersionId()));
        } catch (AmazonS3Exception e) {
            this.getlogger().error("findAllVersionsOfAFile AmazonS3Exception: " + e, correlationId, entityId, connectorName);
            throw new CannotFetchRemoteFileException(FIND_VERSION_ERROR);
        } catch (AmazonServiceException e) {
            this.getlogger().error("findAllVersionsOfAFile AmazonServiceException: " + e, correlationId, entityId, connectorName);
            throw new CannotFetchRemoteFileException(FIND_VERSION_ERROR);
        } catch (SdkClientException e) {
            this.getlogger().error("findAllVersionsOfAFile SdkClientException: " + e, correlationId, entityId, connectorName);
            throw new CannotFetchRemoteFileException(FIND_VERSION_ERROR);
        } catch (Exception e) {
            this.getlogger().error("findAllVersionsOfAFile Exception: " + e, correlationId, entityId, connectorName);
            throw new CannotFetchRemoteFileException(FIND_VERSION_ERROR);
        }
        timeIt.stop();
        return result;
    }

    @Trace
    public Optional<FileObjectProxy> getFileInputStream(
            final Map<FileSystemOptionKeys, Object> optionsObjectMap,
            String entityId,
            String connectorName) throws FileTransferException {
        final String methodName = " | getFileInputStream | ";
        final String correlationId = (String) optionsObjectMap.getOrDefault(CORRELATION_ID, "");
        final String remoteDir = normalizeFileName((String) optionsObjectMap.get(REMOTE_DIR));
        final String fileName = (String) optionsObjectMap.get(FileSystemOptionKeys.FILE_NAME);
        final String env = (String) optionsObjectMap.getOrDefault(ENVIRONMENT, "");
        final String fullFileName = joinPaths(remoteDir, fileName);
        final String bname = consts.getAwsBucketName(env);
        final String startMsg =
                String.format("%s %s Start with file name %s, remote dir %s, env %s",
                        CLASS_NAME, methodName,
                        fileName, remoteDir, env);
        this.getlogger().info(startMsg, correlationId, entityId, connectorName);
        final TimeIt timeIt = new TimeIt(methodName, correlationId, entityId, connectorName);
        timeIt.start();
        Download download;
        File tempFile = null;
        FileObjectProxy fileObjectProxy = null;
        boolean doDeleteFile = true;
        try {
            InputStream inputStream;
            Path locaFilePath = Paths.get(consts.getFileOperationTempDir(), remoteDir);
            locaFilePath = Files.createDirectories(locaFilePath);
            final Long timestamp = Instant.now().toEpochMilli();
            final Path path = createTempFile(locaFilePath, consts.getAwsTempDownloadfilePrefix(),
                    timestamp.toString() + ".download");
            Files.deleteIfExists(path);
            tempFile = Files.createFile(path).toFile();
            final TransferManager transferManager = getAwsTransferManager(env);
            download = transferManager.download(bname, fullFileName, tempFile);
            if(download != null) {
                download.waitForCompletion();
                final long downloadedFileSize = tempFile.length();
                if(downloadedFileSize > consts.getAwsS3MaxFileSizeToTransferInMemory()) {
                    doDeleteFile = false;
                    inputStream = FileUtils.openInputStream(tempFile);
                    fileObjectProxy = new FileObjectProxy(inputStream, null, true);
                } else {
                    final String content = readFileToString(tempFile.toPath());
                    inputStream = new ByteArrayInputStream(content.getBytes());
                    fileObjectProxy = new FileObjectProxy(inputStream, null);
                }
                final String msg =
                        String.format("%s %s Successfully downloaded file %s in for env %s and completion description %s and content length %s",
                        CLASS_NAME, methodName, fullFileName, env, download.getDescription(),
                        download.getProgress().getBytesTransferred());
                timeIt.setMessage(msg);
                this.getlogger().info(msg, correlationId, entityId, connectorName);
            } else {
                final String msg = String.format("Unknown error in downloading file %s in for env %s", fullFileName, env);
                this.getlogger().error(String.format("%s %s %s", CLASS_NAME, methodName, msg), correlationId, entityId, connectorName);
                //throw new AWSConnectionException(INTERNAL_SERVER_ERROR.value(), msg);
            }
        } catch (ClientExecutionTimeoutException e) {
            final String msg = String.format("%s %s: ClientExecutionTimeoutException %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
           
            final String msg1 = String.format("%s %s: AmazonS3Exception %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg1, correlationId, entityId, connectorName);
           
        } catch (AmazonServiceException e) {
            final String msg = String.format("%s %s: AmazonServiceException %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
          
        } catch (SdkClientException e) {
            final String msg = String.format("%s %s: SdkClientException %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
          
        } catch (AmazonClientException e) {
            final String msg = String.format("%s %s: AmazonClientException %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
           
        } catch (HttpClientErrorException e) {
            final String msg = String.format("%s %s: HttpClientErrorException %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
        
        } catch (ResourceAccessException e) {
            final String msg = String.format("%s %s: ResourceAccessException %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
        
        } catch (IOException e) {
            final String msg = String.format("%s %s: IOException %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
           
        } catch (Exception e) {
            final String msg = String.format("%s %s: Exception %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
          
        } finally {
            if(doDeleteFile && tempFile != null) {
                tempFile.delete();
            }
        }

        Optional<FileObjectProxy> fileObjectProxyOptional;
        if(fileObjectProxy != null) {
            fileObjectProxyOptional = Optional.of(fileObjectProxy);
        } else {
            fileObjectProxyOptional = Optional.empty();
        }
        final String endMsg =
                String.format("%s %s End with file name %s, remote dir %s, env %s",
                        CLASS_NAME, methodName,
                        fileName, remoteDir, env);
        timeIt.stop();
        this.getlogger().info(endMsg, correlationId, entityId, connectorName);
        return fileObjectProxyOptional;
    }

    @Trace
    public Optional<FileOperationResponse> setFileStream(
            final Map<FileSystemOptionKeys, Object> optionsObjectMap,
            String entityId,
            String connectorName) throws FileTransferException {
        final String methodName = " | setFileStream | ";
        final String correlationId = (String) optionsObjectMap.getOrDefault(CORRELATION_ID, "");
        final String remoteDir = normalizeFileName((String) optionsObjectMap.get(REMOTE_DIR));
        final String fileName = (String) optionsObjectMap.get(FILE_NAME);
        final String mimeType = (String) optionsObjectMap.get(CONTENT_MIME_TYPE);
        final String env = (String) optionsObjectMap.getOrDefault(ENVIRONMENT, "");
        final String fullFileName = joinPaths(remoteDir, fileName);
        final String startMsg =
                String.format("%s %s Start with file name %s, remote dir %s, env %s and mime %s",
                        CLASS_NAME, methodName,
                        fileName, remoteDir, env, mimeType);
        this.getlogger().info(startMsg, correlationId, entityId, connectorName);
        final TimeIt timeIt = new TimeIt(methodName, correlationId, entityId, connectorName);
        timeIt.start();
        Upload upload;
        FileOperationResponse fileOperationResponse = null;
        File tempFile = null;
        try {
            String msg = String.format("%s %s Setting remote file %s", CLASS_NAME, methodName, fullFileName);
            this.getlogger().info(msg, correlationId, entityId, connectorName);
            final TransferManager transferManager = getAwsTransferManager(env);
            final String bname = consts.getAwsBucketName(env);
            final ObjectMetadata objectMetadata = new ObjectMetadata();
            Path locaFilePath = Paths.get(consts.getFileOperationTempDir(), remoteDir);
            locaFilePath = Files.createDirectories(locaFilePath);
            final Long timestamp = Instant.now().toEpochMilli();
            final Path path = createTempFile(locaFilePath, consts.getAwsTempDownloadfilePrefix(),
                    timestamp.toString() + ".upload");
            Files.deleteIfExists(path);
            tempFile = Files.createFile(path).toFile();
            if (mimeType.equalsIgnoreCase(FILE_INPUT_STREAM)) {
                final InputStream inputStream = (InputStream) optionsObjectMap.get(FILE_OBJECT);
//                upload = transferManager.upload(bname, fullFileName, inputStream, objectMetadata);
                FileUtils.copyInputStreamToFile(inputStream, tempFile);
            } else if (mimeType.equalsIgnoreCase(PLAIN_TEXT)) {
                final String content = (String) optionsObjectMap.get(FILE_OBJECT);
//                final InputStream targetStream = new ByteArrayInputStream(content.getBytes());
//                upload = transferManager.upload(bname, fullFileName, targetStream, objectMetadata);
                writeContentsToFile(tempFile.toPath(), content);
            } else {
                msg = String.format("%s %s : Mime type %s not defined. ", CLASS_NAME, methodName, mimeType);
                this.getlogger().info(msg, correlationId, entityId, connectorName);
                throw new FileTransferException(String.format("Mime Type %s not defined", mimeType));
            }
            msg = String.format("%s %s Temp file %s generated with size %s for multipart split",
                    CLASS_NAME, methodName, tempFile.toString(), tempFile.length());
            this.getlogger().info(msg, correlationId, entityId, connectorName);
            upload = transferManager.upload(bname, fullFileName, tempFile);
            if(upload != null) {
                upload.waitForCompletion();
                msg = String.format("%s %s Successfully stored file %s in for env %s and mime %s and" +
                                " completion description %s with size %s",
                        CLASS_NAME, methodName, fullFileName, env, mimeType, upload.getDescription(),
                        upload.getProgress().getBytesTransferred());
                timeIt.setMessage(msg);
                this.getlogger().info(msg, correlationId, entityId, connectorName);
                fileOperationResponse =
                        new FileOperationResponse(fullFileName, upload.getProgress().getBytesTransferred());
            } else {
                msg = String.format("Unknown error in storing file %s in for env %s and mime %s",
                        fullFileName, env, mimeType);
                this.getlogger().error(String.format("%s %s %s", CLASS_NAME, methodName, msg), correlationId, entityId, connectorName);
                throw new AWSConnectionException(INTERNAL_SERVER_ERROR.value(), msg);
            }
        } catch (ClientExecutionTimeoutException e) {
            final String msg = String.format("%s %s: ClientExecutionTimeoutException %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
           
        } catch (AmazonS3Exception e) {
            final String msg = String.format("%s %s: AmazonS3Exception %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
           
        } catch (AmazonServiceException e) {
            final String msg = String.format("%s %s: AmazonServiceException %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
         
        } catch (SdkClientException e) {
            final String msg = String.format("%s %s: SdkClientException %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
          
        } catch (AmazonClientException e) {
            final String msg = String.format("%s %s: AmazonClientException %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
            
        } catch (HttpClientErrorException e) {
            final String msg = String.format("%s %s: HttpClientErrorException %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
          
        } catch (ResourceAccessException e) {
            final String msg = String.format("%s %s: ResourceAccessException %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
         
        } catch (IOException e) {
            final String msg = String.format("%s %s: IOException %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
           
        } catch (Exception e) {
            final String msg = String.format("%s %s: Exception %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
          
        } finally {
            if(tempFile != null) {
                try {
                    Files.deleteIfExists(tempFile.toPath());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        final String endMsg =
                String.format("%s %s End with file name %s, remote dir %s, env %s and mime %s",
                        CLASS_NAME, methodName,
                        fileName, remoteDir, env, mimeType);
        timeIt.stop();
        this.getlogger().info(endMsg, correlationId, entityId, connectorName);
        return Optional.ofNullable(fileOperationResponse);
    }

    /**
     * Move file from one location to other. The way this works is as follows:
     * 1. Copy the file fromLocation->toLocation
     * 2. Delete the file fromLocation
     * @param fileName File name to move
     * @param fromLocation The folder location of the file
     * @param toLocation The folder location where we need to move
     * @param env The environment. It can be either QA or PRODUCTION
     * @param doDeleteOriginal If {@code true} delete original file else do not delete
     * @return true if the operation is a success
     */
    private boolean moveFileAndDeleteOriginal(final String fileName,
                                              final String fromLocation,
                                              final String toLocation,
                                              final String env,
                                              final boolean doDeleteOriginal,
                                              final String entityId,
                                              final String connectorName,
                                              final String correlationId) throws AWSConnectionException {
        boolean result = false;
        final String methodName = " | moveFileAndDeleteOriginal | ";
        final String startMsg =
                String.format("%s %s Start move files from %s to %s in env %s",
                        CLASS_NAME, methodName,
                        fromLocation, toLocation, env);
        this.getlogger().info(startMsg, correlationId, entityId, connectorName);
        final TimeIt timeIt = new TimeIt( methodName, correlationId, entityId, connectorName);
        timeIt.start();
        if (fileName.equalsIgnoreCase(toLocation)) {
            final String msg = String.format("%s %s To location and from location same. Doing nothing for file: %s",
                    CLASS_NAME, methodName, fileName);
            this.getlogger().warn(msg, correlationId, entityId, connectorName);
        } else {
            try {
                final AmazonS3 amazonS3 = getAwsClient(env);
                final String bname = consts.getAwsBucketName(env);
                final String moveFileName =
                        joinPaths(toLocation, fileName.substring(fromLocation.length()));
                String msg = String.format("%s %s org fileName: %s, fromLocation: %s, toLocation: %s, moveFileName: %s",
                        CLASS_NAME, methodName, fileName, fromLocation, toLocation, moveFileName);
                this.getlogger().info(msg, correlationId, entityId, connectorName);
                final CopyObjectResult copyObjectResult =
                        amazonS3.copyObject(bname, fileName, bname, moveFileName);
                msg = String.format("%s %s Copied object. Last modified date: %s, result: %s", CLASS_NAME, methodName,
                        copyObjectResult.getLastModifiedDate(), copyObjectResult);
                this.getlogger().info(msg, correlationId, entityId, connectorName);
                if(doDeleteOriginal) {
                    final DeleteFolderPayLoad deleteFolderPayLoad = new DeleteFolderPayLoad();
                    deleteFolderPayLoad.setDirectorName(fileName);
                    deleteFolderPayLoad.setRegularExpressionString(".*");
                    final Map<FileSystemOptionKeys, Object> optionsObjectMap = new HashMap<>();
                    optionsObjectMap.put(ENVIRONMENT, env);
                    optionsObjectMap.put(CORRELATION_ID, correlationId);
                    final Optional<String> removeRes = removeDir(deleteFolderPayLoad, optionsObjectMap, entityId, connectorName);
                    if(!removeRes.isPresent()) {
                        msg = String.format("%s %s Could not remove files: %s in env: %s", CLASS_NAME, methodName,
                                moveFileName, env);;
                        this.getlogger().error(msg, correlationId, entityId, connectorName);
                        throw new FileTransferException("Cannot delete files");
                    }
                } else {
                    msg = String.format("%s %s Just copying the file. Not deleting it.", CLASS_NAME, methodName);
                    this.getlogger().info(msg, correlationId, entityId, connectorName);
                }
                result = true;
            } catch (ClientExecutionTimeoutException e) {
                final String msg = String.format("%s %s: ClientExecutionTimeoutException %s", CLASS_NAME, methodName, e);
                this.getlogger().error(msg, correlationId, entityId, connectorName);
               
            } catch (AmazonS3Exception e) {
                final String msg = String.format("%s %s: AmazonS3Exception %s", CLASS_NAME, methodName, e);
                this.getlogger().error(msg, correlationId, entityId, connectorName);
            
            } catch (AmazonServiceException e) {
                final String msg = String.format("%s %s: AmazonServiceException %s", CLASS_NAME, methodName, e);
                this.getlogger().error(msg, correlationId, entityId, connectorName);
               
            } catch (SdkClientException e) {
                final String msg = String.format("%s %s: SdkClientException %s", CLASS_NAME, methodName, e);
                this.getlogger().error(msg, correlationId, entityId, connectorName);
                
            } catch (AmazonClientException e) {
                final String msg = String.format("%s %s: AmazonClientException %s", CLASS_NAME, methodName, e);
                this.getlogger().error(msg, correlationId, entityId, connectorName);
               
            } catch (HttpClientErrorException e) {
                final String msg = String.format("%s %s: HttpClientErrorException %s", CLASS_NAME, methodName, e);
                this.getlogger().error(msg, correlationId, entityId, connectorName);
                
            } catch (ResourceAccessException e) {
                final String msg = String.format("%s %s: ResourceAccessException %s", CLASS_NAME, methodName, e);
                this.getlogger().error(msg, correlationId, entityId, connectorName);
               
            } catch (Exception e) {
                final String msg = String.format("%s %s: Exception %s", CLASS_NAME, methodName, e);
                this.getlogger().error(msg, correlationId, entityId, connectorName);
               
            }
        }
        final String endMsg =
                String.format("%s %s End move files from %s to %s in env %s",
                        CLASS_NAME, methodName,
                        fromLocation, toLocation, env);
        timeIt.stop();
        this.getlogger().info(endMsg, correlationId, entityId, connectorName);
        return result;
    }

    /**
     * Move file from one location to other. The way this works is as follows:
     * 1. Copy the file fromLocation->toLocation
     * 2. Delete the file fromLocation
     * @param moveFilePayload The payload for the movefile operation
     * @param optionsObjectMap Any additional options
     * @param entityId Entity id if available
     * @param connectorName Connector name if available
     * @param doDeleteOriginal Delete the original file
     * @return The status of the operation
     * @throws GISTFileTransferExceptions In case of any error there is an exception thrown
     */
    @Trace
    public Optional<String> moveFile(final MoveFilePayload moveFilePayload,
                                     final Map<FileSystemOptionKeys, Object> optionsObjectMap,
                                     final String entityId,
                                     final String connectorName,
                                     final boolean doDeleteOriginal) throws FileTransferException {
        final String methodName = " | moveFile | ";
        final String correlationId =
                (String) optionsObjectMap.getOrDefault(CORRELATION_ID, "");
        final String env = (String) optionsObjectMap.getOrDefault(ENVIRONMENT, "");
        prettyPrintJson("moveFilePayload", moveFilePayload);
        final String fromLocation = normalizeFileName(moveFilePayload.fromLocation);
        final String toLocation = joinPaths(moveFilePayload.toRootDirectory, moveFilePayload.toLocationName);
        prettyPrintJson("moveFilePayload", moveFilePayload);
        this.getlogger().info(String.format("fromLocation: %s, toLocation: %s ", fromLocation, toLocation), correlationId,
                entityId, connectorName);

        final String regex = ".*";
        optionsObjectMap.put(REMOTE_DIR, fromLocation);
        optionsObjectMap.put(REGULAR_EXPRESSION, regex);
        final String startMsg =
                String.format("%s %s Start move files from %s to %s with regular expression %s in env %s",
                        CLASS_NAME, methodName,
                        fromLocation, toLocation, regex, env);
        this.getlogger().info(startMsg, correlationId, entityId, connectorName);
        final TimeIt timeIt = new TimeIt( methodName, correlationId, entityId, connectorName);
        timeIt.start();
        final Optional<List<String>> listOptional =
                listFilesInFolder(optionsObjectMap, false, entityId, connectorName);
        if (!listOptional.isPresent()) {
            throw new CannotFetchRemoteFileException("Cannot fetch the files");
        }

        final Set<String> undoList = new HashSet<>();
        try {
            final List<String> filesList = listOptional.get();
            if (!filesList.isEmpty()) {
                for(final String fileName : filesList) {
                    moveFileAndDeleteOriginal(
                            fileName, fromLocation, toLocation, env, doDeleteOriginal,
                            entityId, connectorName, correlationId);
                    undoList.add(fileName);
                }
            }
        } catch (AWSConnectionException e) {
            final String msg = String.format("%s %s: GISTAWSConnectionException %s", CLASS_NAME, methodName, e);
            this.getlogger().error(msg, correlationId, entityId, connectorName);
            // There was an exception and not all files were moved. So undo the moved files
            for(final String f : undoList) {
                final DeleteFolderPayLoad deleteFolderPayLoad = new DeleteFolderPayLoad();
                deleteFolderPayLoad.setDirectorName(f);
                deleteFolderPayLoad.setRegularExpressionString(".*");
                final Map<FileSystemOptionKeys, Object> omap = new HashMap<>();
                omap.put(ENVIRONMENT, env);
                omap.put(CORRELATION_ID, correlationId);
                final Optional<String> removeRes = removeDir(deleteFolderPayLoad, omap, entityId, connectorName);
            }
            throw e;
        }

        final String endMsg =
                String.format("%s %s End move files from %s to %s with regular expression %s in env %s",
                        CLASS_NAME, methodName,
                        fromLocation, toLocation, regex, env);
        timeIt.stop();
        this.getlogger().info(endMsg, correlationId, entityId, connectorName);
        return Optional.of(String.format("Moved from %s to %s", fromLocation, toLocation));
    }

    @Trace
    public String testS3Access(final String env) {
        String result;
        final AmazonS3 amazonS3 = getAwsClient(env);
        final String bname = consts.getAwsBucketName(env);
        if (amazonS3 == null) {
            result = "S3 client is null";
        } else {
            result = "S3 client is valid. Doing operations. ";
            try {
                result = String.format("%s. Does bucket %s exists %s", result, bname, amazonS3.doesBucketExistV2(bname));
                result = result + " Bucket exists";
            } catch (Exception e) {
                result = String.format("%s Checking bucket existance failed with exception: %s", result, e);
            }
            try {
                final ListObjectsV2Result res = amazonS3.listObjectsV2(bname, "gistCoreArtefacts");
                final List<S3ObjectSummary> objects = res.getObjectSummaries();
                result = String.format("%s. Listing objects %s", result, objects);
            } catch (Exception e) {
                result = String.format("%s Checking bucket listing failed with exception: %s", result, e);
            }
        }
        result = String.format("%s: {%s}", env, result);
        return result;
    }

    private void prettyPrintJson(final String str, final Object json) {
        if(Validation.sanitizeEnvironment(consts.getSpringCloudConfigLable()).isEmpty()) {
            try {
                this.getlogger().info(String.format("%s%s: %n %s%s",
                        "%n ====Start=== %n",
                        str,
                        mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json),
                        "%n ====End=== %n"));
            } catch (JsonProcessingException e) {
                this.getlogger().error(String.format("prettyPrintJson Error: %s", e));
            }
        }
    }
    
    
    /**
	 * Initializing the logger
	 * 
	 * @return
	 */
	private Logger getlogger() {
		return LoggerFactory.getLogger(S3OperationsManager.class);
	}
}
