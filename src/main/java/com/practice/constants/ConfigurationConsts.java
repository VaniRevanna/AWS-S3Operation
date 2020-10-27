package com.practice.constants;

import com.amazonaws.retry.RetryPolicy;
import com.practicecom.practice.utill.Validation;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.stereotype.Component;

@Component
@RefreshScope
public class ConfigurationConsts {
    public static final String DEFAULT = "DEFAULT";
    public static final String QA = "QA";
    public static final String PRODUCTION = "PRODUCTION";
    public static final String PROD = "PROD";
    public static final String DEV = "DEV";
    public static final String DEVELOPMENT = "DEVELOPMENT";
    public static final String SUCCESS = "SUCCESS";
    public static final String FAILED = "FAILED";

    // Default parameters
    @Value("${aws.accesskey.id.default}")
    private String kidDefault;

    @Value("${aws.secretekey.default}")
    private String akeyDefault;

    @Value("${aws.bucket.name.default}")
    private String awsBucketNameDefault;

    @Value("${aws.bucket.arn.default}")
    private String awsArnDefault;

    @Value("${aws.bucket.region.default}")
    private String awsBucketRegionDefault;

    @Value("${spring.cloud.config.label}")
    private String springCloudConfigLable;

    // Qa Environment
    @Value("${aws.accesskey.id.qa}")
    private String kidQa;

    @Value("${aws.secretekey.qa}")
    private String akeyQa;

    @Value("${aws.bucket.name.qa}")
    private String awsBucketNameQa;

    @Value("${aws.bucket.arn.qa}")
    private String awsArnQa;

    @Value("${aws.bucket.region.qa}")
    private String awsBucketRegionQa;

    // Prod Environment
    @Value("${aws.accesskey.id.prod}")
    private String kidProd;

    @Value("${aws.secretekey.prod}")
    private String akeyProd;

    @Value("${aws.bucket.name.prod}")
    private String awsBucketNameProd;

    @Value("${aws.bucket.arn.prod}")
    private String awsArnProd;

    @Value("${aws.bucket.region.prod}")
    private String awsBucketRegionProd;

    /**
     * Time out settings for AWS S3
     */
    @Value("${gist.aws.s3.client.request.timeout}")
    private int awsS3ClientRequestTimeout;

    @Value("${gist.aws.s3.client.connection.timeout}")
    private int awsS3ClientConnectionTimeout;

    @Value("${gist.aws.s3.client.socket.timeout}")
    private int awsS3ClientSocketTimeout;

    @Value("${gist.aws.s3.client.execution.timeout}")
    private int awsS3ClientExecutionTimeout;

    @Value("${gist.aws.s3.client.connection.ttl}")
    private int awsS3ClientConnectionTtl;

    /**
     * if {@code true} then it will retain the older versions of the file in S3 else it will not retain older versions.
     */
    @Value("${retail.older.file.versions}")
    private boolean retainOlderFileVersions;

    /**
     * Proxy host
     */
    @Value("${concur.proxy.host}")
    private String concurProxyHost;

    /**
     * Proxy port
     */
    @Value("${concur.proxy.port}")
    private int concurProxyPort;

    /**
     * No-proxy hosts
     */
    @Value("${aws.nonproxy.hosts}")
    private String awsNonProxyHosts;

    @Value("${gist.aws.max.retries}")
    private int awsMaxRetries;

    @Value("${gist.async.core.pool.size}")
    private int asyncCorePoolSize;

    @Value("${gist.async.max.pool.size}")
    private int asyncMaxPoolSize;

    @Value("${gist.async.queue.capacity}")
    private int asyncQueueCapacity;

    @Value("${gist.async.thread.name.prefix}")
    private String asyncThreadNamePrefix;

    @Value("${gist.async.default.timeout}")
    private int asyncDefaultTimeout;

    @Value("${gist.aws.s3.max.upload.threads}")
    private int awsS3MaxUploadthreads;

    /**
     * Set the maximum number of consecutive failed retries that the client will permit before
     * throttling all subsequent retries of failed requests.
     * <p>
     * Note: This does not guarantee that each failed request will be retried up to this many times.
     * Depending on the configured {@link RetryPolicy} and the number of past failed and successful
     * requests, the actual number of retries attempted may be less.
     * <p>
     */
    @Value("${gist.aws.max.maxConsecutiveRetriesBeforeThrottling}")
    private int maxConsecutiveRetriesBeforeThrottling;

    @Value("${gist.aws.s3.minimum.partSize}")
    private long awsS3MinimumPartsize;

    @Value("${file.operation-dir}")
    private String fileOperationTempDir;

    @Value("${gist.aws.exceptions.503}")
    private String[] gistAWSExceptions503;

    @Value("${gist.aws.exceptions.504}")
    private String[] gistAWSExceptions504;

    @Value("${gist.aws.temp.downloadfile.prefix}")
    private String awsTempDownloadfilePrefix;

    @Value("${gist.aws.temp.downloadfile.subfix}")
    private String awsTempDownloadfileSubfix;

    @Value("${gist.aws.s3.maxFileSizeToTransferInMemory}")
    private long awsS3MaxFileSizeToTransferInMemory;

    @Value("${gist.temp.cleanup.duration.schedule}")
    private long tempCleanUpDurationSchedule;

    @Value("${gist.temp.cleanup.duration.threshold}")
    private long tempCleanUpDurationThreshold;

    public String getKid(final String env) {
        String kid;
        switch (env.trim().toUpperCase()) {
            case QA:
                kid = kidQa;
                break;
            case PRODUCTION:
                kid = kidProd;
                break;
            default:
                kid = kidDefault;
        }
        return Validation.sanitize(kid);
    }

    public String getAkey(final String env) {
        String akey;
        switch (env.trim().toUpperCase()) {
            case QA:
                akey = akeyQa;
                break;
            case PRODUCTION:
                akey = akeyProd;
                break;
            default:
                akey = akeyDefault;
        }
        return Validation.sanitize(akey);
    }

    public String getAwsBucketName(final String env) {
        String awsBucketName;
        switch (env.trim().toUpperCase()) {
            case QA:
                awsBucketName = awsBucketNameQa;
                break;
            case PRODUCTION:
                awsBucketName = awsBucketNameProd;
                break;
            default:
                awsBucketName = awsBucketNameDefault;
        }
        return Validation.sanitize(awsBucketName);
    }

    public String getAwsArn(final String env) {
        String awsArn;
        switch (env.trim().toUpperCase()) {
            case QA:
                awsArn = awsArnQa;
                break;
            case PRODUCTION:
                awsArn = awsArnProd;
                break;
            default:
                awsArn = awsArnDefault;
        }
        return Validation.sanitize(awsArn);
    }

    public String getAwsBucketRegion(final String env) {
        String awsBucketRegion;
        switch (env.trim().toUpperCase()) {
            case QA:
                awsBucketRegion = awsBucketRegionQa;
                break;
            case PRODUCTION:
                awsBucketRegion = awsBucketRegionProd;
                break;
            default:
                awsBucketRegion = awsBucketRegionDefault;
        }
        return Validation.sanitize(awsBucketRegion);
    }

    public String getConcurProxyHost() {
        return concurProxyHost;
    }

    public int getConcurProxyPort() {
        return concurProxyPort;
    }

    public String getAwsNonProxyHosts() {
        return awsNonProxyHosts;
    }

    public String getSpringCloudConfigLable() { return springCloudConfigLable; }

    public boolean getRetainOlderFileVersions() { return retainOlderFileVersions; }

    public int getAwsMaxRetries() {
        return awsMaxRetries;
    }

    public int getMaxConsecutiveRetriesBeforeThrottling() {
        return maxConsecutiveRetriesBeforeThrottling;
    }

    public int getAsyncCorePoolSize() {
        return asyncCorePoolSize;
    }

    public int getAsyncMaxPoolSize() {
        return asyncMaxPoolSize;
    }

    public int getAsyncQueueCapacity() {
        return asyncQueueCapacity;
    }

    public String getAsyncThreadNamePrefix() {
        return asyncThreadNamePrefix;
    }

    public int getAsyncDefaultTimeout() {
        return asyncDefaultTimeout;
    }

    public int getAwsS3ClientRequestTimeout() {
        return awsS3ClientRequestTimeout;
    }

    public int getAwsS3ClientConnectionTimeout() {
        return awsS3ClientConnectionTimeout;
    }

    public int getAwsS3ClientSocketTimeout() {
        return awsS3ClientSocketTimeout;
    }

    public int getAwsS3ClientExecutionTimeout() {
        return awsS3ClientExecutionTimeout;
    }

    public int getAwsS3ClientConnectionTtl() {
        return awsS3ClientConnectionTtl;
    }

    public int getAwsS3MaxUploadthreads() {
        return awsS3MaxUploadthreads;
    }

    public long getAwsS3MinimumPartsize() {
        return awsS3MinimumPartsize;
    }

    public String getFileOperationTempDir() {
        return fileOperationTempDir;
    }

    public String[] getGistAWSExceptions503() {
        return gistAWSExceptions503;
    }

    public String[] getGistAWSExceptions504() {
        return gistAWSExceptions504;
    }

    public String getAwsTempDownloadfilePrefix() {
        return awsTempDownloadfilePrefix;
    }

    public String getAwsTempDownloadfileSubfix() {
        return awsTempDownloadfileSubfix;
    }

    public long getAwsS3MaxFileSizeToTransferInMemory() {
        return awsS3MaxFileSizeToTransferInMemory;
    }

    public long getTempCleanUpDurationSchedule() {
        return tempCleanUpDurationSchedule;
    }

    public long getTempCleanUpDurationThreshold() {
        return tempCleanUpDurationThreshold;
    }
}
