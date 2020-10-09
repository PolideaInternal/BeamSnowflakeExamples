package util;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;

public class AwsOptionsParser {

    private static final String AWS_S3_PREFIX = "s3";

    public static void format(AwsOptions options) {
        if (options.getStagingBucketName().get().toLowerCase().startsWith(AWS_S3_PREFIX)) {
            options.setAwsCredentialsProvider(
                    new AWSStaticCredentialsProvider(
                            new BasicAWSCredentials(options.getAwsAccessKey(), options.getAwsSecretKey())));
        }
    }

}