package batching;

import org.apache.beam.sdk.io.snowflake.SnowflakePipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.io.aws.options.S3Options;
import util.AwsOptions;

/**
 * Supported PipelineOptions used in provided examples.
 */
public interface SnowflakeWordCountOptions extends SnowflakePipelineOptions, AwsOptions, S3Options {

    @Description("Path of the file to read from")
    @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
    String getInputFile();

    void setInputFile(String value);

    @Description("Path of the file to write to")
    @Required
    String getOutput();

    void setOutput(String value);
}