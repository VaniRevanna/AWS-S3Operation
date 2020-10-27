package com.practicecom.practice.utill;

import static net.logstash.logback.encoder.org.apache.commons.lang.StringEscapeUtils.escapeHtml;
import static net.logstash.logback.encoder.org.apache.commons.lang.StringEscapeUtils.escapeSql;

import com.practice.constants.ConfigurationConsts;
import com.practice.exception.FileTransferException;


/**
 * input field validation for SQL injection
 * @author i508938
 *
 */
public class Validation {
    public static String sanitize(final String s) {
        if (s == null) {
            return "";
        } else {
            return escapeSql(escapeHtml(s.trim()));
        }
    }
    public static void checkAWSClientFactoryRegionConfig(String region) throws FileTransferException {
        if (region.isEmpty()) {
            throw new FileTransferException("Enter a valid AWS region");
        }
    }
    public static String sanitizeEnvironment(final String env) {
        String result;
        if(env == null ||  env.isEmpty()) {
            result = "";
        } else {
            switch(env.trim().toUpperCase()) {
                case ConfigurationConsts.PRODUCTION:
                case ConfigurationConsts.PROD:
                    result = ConfigurationConsts.PRODUCTION;
                    break;
                case ConfigurationConsts.QA:
                    result = ConfigurationConsts.QA;
                    break;
                case ConfigurationConsts.DEV:
                case ConfigurationConsts.DEVELOPMENT:
                default:
                    result = "";
            }
        }

        return result;
    }
}
