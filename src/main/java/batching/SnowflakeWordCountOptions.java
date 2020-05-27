package batching;

import org.apache.beam.sdk.io.snowflake.SnowflakePipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation.Required;

/**
 * Supported PipelineOptions used in provided examples.
 */
public interface SnowflakeWordCountOptions extends SnowflakePipelineOptions {

    @Description("Path of the file to read from")
    @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
    String getInputFile();

    void setInputFile(String value);

    @Description("Path of the file to write to")
    @Required
    String getOutput();

    void setOutput(String value);

    @Description("Snowflake table name")
    @Default.String("WORD_COUNT")
    String getTableName();

    void setTableName(String tableName);
}