package com.practice.s3.service;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.practice.exception.FileTransferException;
import com.practicecom.practice.utill.TimeIt;

import ch.qos.logback.classic.Logger;

import org.slf4j.LoggerFactory;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResourceAccessException;

import java.io.Closeable;
import java.util.Date;

/**
 * AWSCredentialsProvider implementation that uses the AWS Security Token
 * Service to assume a Role and create temporary, short-lived sessions to use
 * for authentication.
 */
public class STSRefreshCredentialsProvider implements AWSCredentialsProvider, Closeable {
    private static final String CLASS_NAME = STSRefreshCredentialsProvider.class.getSimpleName();
   
    /**
	 * Initializing the logger
	 * 
	 * @return
	 */
	private Logger getlogger() {
		return (Logger) LoggerFactory.getLogger(STSRefreshCredentialsProvider.class);
	}
    /**
     * Default duration for started sessions.
     */
    public static final int DEFAULT_DURATION_SECONDS = 900;

    /**
     * Time before expiry within which credentials will be renewed.
     */
    private static final int EXPIRY_TIME_MILLIS = 60 * 1000;

    /**
     * The client for starting STS sessions.
     */
    private final AWSSecurityTokenService securityTokenService;

    /**
     * The current session credentials.
     */
    private AWSSessionCredentials sessionCredentials;

    /**
     * The expiration time for the current session credentials.
     */
    private Date sessionCredentialsExpiration;

    /**
     * The arn of the role to be assumed.
     */
    private String roleArn;

    /**
     * Constructs a new GistSTSRefreshCredentialsProvider, which makes a
     * request to the AWS Security Token Service (STS), uses the provided
     * {@link #roleArn} to assume a role and then request short lived session
     * credentials, which will then be returned by this class's
     * {@link #getCredentials()} method.
     *
     * @param roleArn The AWS ARN of the Role to be assumed.
     */
    public STSRefreshCredentialsProvider(String roleArn) {
        this.roleArn = roleArn;
        securityTokenService = new AWSSecurityTokenServiceClient();
    }

    /**
     * Constructs a new GistSTSRefreshCredentialsProvider, which makes a
     * request to the AWS Security Token Service (STS), uses the provided
     * {@link #roleArn} to assume a role and then request short lived session
     * credentials, which will then be returned by this class's
     * {@link #getCredentials()} method.
     *
     * @param roleArn             The AWS ARN of the Role to be assumed.
     * @param clientConfiguration The AWS ClientConfiguration to use when making AWS API requests.
     */
    public STSRefreshCredentialsProvider(String roleArn, ClientConfiguration clientConfiguration) {
        this.roleArn = roleArn;
        securityTokenService = new AWSSecurityTokenServiceClient(clientConfiguration);
    }

    /**
     * Constructs a new GistSTSRefreshCredentialsProvider, which will use
     * the specified long lived AWS credentials to make a request to the AWS
     * Security Token Service (STS), uses the provided {@link #roleArn} to
     * assume a role and then request short lived session credentials, which
     * will then be returned by this class's {@link #getCredentials()} method.
     *
     * @param longLivedCredentials The main AWS credentials for a user's account.
     * @param roleArn              The AWS ARN of the Role to be assumed.
     */
    public STSRefreshCredentialsProvider(AWSCredentials longLivedCredentials,
                                             String roleArn) {
        this(longLivedCredentials, roleArn, getConfiguration());
    }

    /**
     * Constructs a new GistSTSRefreshCredentialsProvider, which will use
     * the specified long lived AWS credentials to make a request to the AWS
     * Security Token Service (STS), uses the provided {@link #roleArn} to
     * assume a role and then request short lived session credentials, which
     * will then be returned by this class's {@link #getCredentials()} method.
     *
     * @param longLivedCredentials The main AWS credentials for a user's account.
     * @param roleArn              The AWS ARN of the Role to be assumed.
     * @param clientConfiguration  Client configuration connection parameters.
     */
    public STSRefreshCredentialsProvider(AWSCredentials longLivedCredentials,
                                             String roleArn,
                                             ClientConfiguration clientConfiguration) {
        this.roleArn = roleArn;
        securityTokenService = new AWSSecurityTokenServiceClient(longLivedCredentials, clientConfiguration);
    }

    /**
     * Constructs a new GistSTSRefreshCredentialsProvider, which will use
     * the specified credentials provider (which vends long lived AWS
     * credentials) to make a request to the AWS Security Token Service (STS),
     * usess the provided {@link #roleArn} to assume a role and then request
     * short lived session credentials, which will then be returned by this
     * class's {@link #getCredentials()} method.
     *
     * @param longLivedCredentialsProvider Credentials provider for the main AWS credentials for a user's
     *                                     account.
     * @param roleArn                      The AWS ARN of the Role to be assumed.
     */
    public STSRefreshCredentialsProvider(AWSCredentialsProvider longLivedCredentialsProvider,
                                             String roleArn) {
        this.roleArn = roleArn;
        securityTokenService = new AWSSecurityTokenServiceClient(longLivedCredentialsProvider);
    }

    /**
     * Constructs a new STSAssumeRoleSessionCredentialsProvider, which will use
     * the specified credentials provider (which vends long lived AWS
     * credentials) to make a request to the AWS Security Token Service (STS),
     * uses the provided {@link #roleArn} to assume a role and then request
     * short lived session credentials, which will then be returned by this
     * class's {@link #getCredentials()} method.
     *
     * @param longLivedCredentialsProvider Credentials provider for the main AWS credentials for a user's
     *                                     account.
     * @param roleArn                      The AWS ARN of the Role to be assumed.
     * @param clientConfiguration          Client configuration connection parameters.
     */
    public STSRefreshCredentialsProvider(AWSCredentialsProvider longLivedCredentialsProvider,
                                             String roleArn,
                                             ClientConfiguration clientConfiguration) {
        this.roleArn = roleArn;
        securityTokenService = new AWSSecurityTokenServiceClient(longLivedCredentialsProvider, clientConfiguration);
    }

    private static ClientConfiguration getConfiguration() {
        return new ClientConfiguration();
    }

    /**
     * Get the temporary credentials at any point of time. This function will check the validity of the credential and if this
     * credential is not valid will create another set of temporary credentials.
     * @return {@link AWSCredentials} The temporary credentials that are created.
     */
    @Override
    public AWSCredentials getCredentials() {
        final String methodName = " | getCredentials | ";
        this.getlogger().info("%s %s Start", CLASS_NAME, methodName);
        if (needsNewSession()) {
            refresh();
        } else {
            this.getlogger().info("%s %s Return existing credential", CLASS_NAME, methodName);
        }
        this.getlogger().info("%s %s End", CLASS_NAME, methodName);
        return sessionCredentials;
    }

    /**
     * Refresh the session. This method forcefully starts the session again.
     */
    @Override
    public void refresh() {
        final String methodName = " | refresh | ";
        this.getlogger().info("%s %s Start", CLASS_NAME, methodName);
        final TimeIt timeIt = new TimeIt( methodName, "", "", "");
        timeIt.start();
        try {
            startSession();
        } catch (FileTransferException e) {
            this.getlogger().error("%s %s Cannot refresh S3 token with exception %s and code %s",
                    CLASS_NAME, methodName, e.getMessage(), e.getErrorCode());
        }
        timeIt.stop();
        this.getlogger().info("%s %s End", CLASS_NAME, methodName);
    }

    /**
     * Starts a new session by sending a request to the AWS Security Token
     * Service (STS) to assume a Role using the long lived AWS credentials. This
     * class then vends the short lived session credentials for the assumed Role
     * sent back from STS.
     */
    private void startSession() throws FileTransferException {
        final String methodName = " | startSession | ";
        this.getlogger().info("%s %s Start", CLASS_NAME, methodName);
        try {
            final AssumeRoleResult assumeRoleResult = securityTokenService
                    .assumeRole(new AssumeRoleRequest()
                            .withRoleArn(roleArn)
                            .withDurationSeconds(DEFAULT_DURATION_SECONDS)
                            .withRoleSessionName(CLASS_NAME));
            final Credentials stsCredentials = assumeRoleResult.getCredentials();
            sessionCredentials = new BasicSessionCredentials(stsCredentials.getAccessKeyId(),
                    stsCredentials.getSecretAccessKey(),
                    stsCredentials.getSessionToken());
            sessionCredentialsExpiration = stsCredentials.getExpiration();
            this.getlogger().info("%s %s Starting a new temporary session with expiration: %s",
                    CLASS_NAME, methodName, sessionCredentialsExpiration);
        } catch (AmazonS3Exception e) {
            final String msg = String.format("GistSTSRefreshCredentialsProvider: AmazonS3Exception: %s", e);
            this.getlogger().error(msg);
            throw new FileTransferException(msg);
        } catch (AmazonServiceException e) {
            final String msg = String.format("GistSTSRefreshCredentialsProvider: AmazonServiceException: %s", e);
            this.getlogger().error(msg);
            throw new FileTransferException(msg);
        } catch (SdkClientException e) {
            final String msg = String.format("GistSTSRefreshCredentialsProvider: SdkClientException: %s", e);
            this.getlogger().error(msg);
            throw new FileTransferException(msg);
        } catch (HttpClientErrorException e) {
            final String msg = String.format("GistSTSRefreshCredentialsProvider: HttpClientErrorException: %s", e);
            this.getlogger().error(msg);
            throw new FileTransferException(msg);
        } catch (ResourceAccessException e) {
            final String msg = String.format("GistSTSRefreshCredentialsProvider: ResourceAccessException: %s", e);
            this.getlogger().error(msg);
            throw new FileTransferException(msg);
        }  catch (Exception e) {
            this.getlogger().error("GistSTSRefreshCredentialsProvider: startSession: Could not start a new session Exception=%s",
                    e);
            throw new FileTransferException(e.getMessage());
        }
        this.getlogger().info("%s %s End", CLASS_NAME, methodName);
    }

    /**
     * Returns true if a new STS session needs to be started. A new STS session
     * is needed when no session has been started yet, or if the last session is
     * within {@link #EXPIRY_TIME_MILLIS} seconds of expiring.
     *
     * @return True if a new STS session needs to be started.
     */
    private boolean needsNewSession() {
        boolean result = false;
        final String methodName = " | needsNewSession | ";
        this.getlogger().info("%s %s sessionCredentialsExpiration: %s", CLASS_NAME, methodName, sessionCredentialsExpiration);
        if (sessionCredentials == null) {
            this.getlogger().info("%s %s No existing sessions found. Need new session", CLASS_NAME, methodName);
            result = true;
        } else {
            final long timeRemaining = sessionCredentialsExpiration.getTime() - System.currentTimeMillis();
            this.getlogger().info("%s %s timeRemaining: %s, EXPIRY_TIME_MILLIS: %s", CLASS_NAME, methodName, timeRemaining, EXPIRY_TIME_MILLIS);
            if (timeRemaining < EXPIRY_TIME_MILLIS) {
                this.getlogger().info("%s %s Time expired", CLASS_NAME, methodName);
                result = true;
            } else {
                this.getlogger().info("%s %s. Time still valid", CLASS_NAME, methodName);
            }
        }

        this.getlogger().info("%s %s Result: %s", CLASS_NAME, methodName, result);
        return result;
    }

    @Override
    public void close(){
        final String methodName = " | close | ";
        this.getlogger().info("%s %s Closing the token connection.", CLASS_NAME, methodName);
    }
}
