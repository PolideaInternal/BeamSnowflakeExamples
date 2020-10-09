package util;

import org.apache.beam.sdk.io.aws.options.S3Options;
import org.apache.beam.sdk.io.snowflake.SnowflakePipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

public interface AwsOptions extends SnowflakePipelineOptions, S3Options {

    @Description("AWS Access Key")
    @Default.String("access_key")
    String getAwsAccessKey();

    void setAwsAccessKey(String awsAccessKey);

    @Description("AWS secret key")
    @Default.String("secret_key")
    String getAwsSecretKey();

    void setAwsSecretKey(String awsSecretKey);
}