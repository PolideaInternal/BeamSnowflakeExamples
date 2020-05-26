package batching;

import org.apache.beam.sdk.io.snowflake.SnowflakePipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation.Required;

/**
 * Options supported by {@link SnowflakeWordCount}.
 * <p>Inherits standard configuration options.
 */
public interface SnowflakeWordCountOptions extends SnowflakePipelineOptions {

    /**
     * By default, this example reads from a public dataset containing the text of King Lear. Set
     * this option to choose a different input file or glob.
     */
    @Description("Path of the file to read from")
    @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
    String getInputFile();

    void setInputFile(String value);


    /** Set this required option to specify where to write the output. */
    @Description("Path of the file to write to")
    @Required
    String getOutput();

    void setOutput(String value);

    /**
     * TODO
     */
    @Description("TODO")
    @Default.String("WORD_COUNT")
    String getTableName();

    void setTableName(String tableName);
}