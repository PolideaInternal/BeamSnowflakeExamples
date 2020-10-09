package streaming;

import org.apache.beam.sdk.io.aws.options.S3Options;
import org.apache.beam.sdk.io.snowflake.SnowflakePipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation.Required;
import util.AwsOptions;

public interface TaxiRidesOptions extends SnowflakePipelineOptions, AwsOptions, S3Options {
}