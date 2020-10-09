package batching;

import org.apache.beam.sdk.io.snowflake.SnowflakePipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.io.aws.options.S3Options;

/**
 * Supported PipelineOptions used in provided examples.
 */
public interface SnowflakeWordCountOptions extends SnowflakePipelineOptions, S3Options {

    @Description("Path of the file to read from")
    @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
    String getInputFile();

    void setInputFile(String value);

    @Description("Path of the file to write to")
    @Required
    String getOutput();

    void setOutput(String value);

    @Description("AWS Access Key")
    @Required
    String getAwsAccessKey();

    void setAwsAccessKey(String awsAccessKey);

    @Description("AWS secret key")
    @Required
    String getAwsSecretKey();

    void setAwsSecretKey(String awsSecretKey);
}