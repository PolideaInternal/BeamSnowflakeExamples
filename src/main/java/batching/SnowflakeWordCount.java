package batching;

import common.SnowflakeWordCountOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.snowflake.Location;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.io.snowflake.credentials.SnowflakeCredentialsFactory;
import org.apache.beam.sdk.io.snowflake.data.SFColumn;
import org.apache.beam.sdk.io.snowflake.data.SFTableSchema;
import org.apache.beam.sdk.io.snowflake.data.numeric.SFNumber;
import org.apache.beam.sdk.io.snowflake.data.text.SFString;
import org.apache.beam.sdk.io.snowflake.enums.CreateDisposition;
import org.apache.beam.sdk.io.snowflake.enums.WriteDisposition;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/**
 * An example that contains batch writing and reading from Snowflake. Inspired by Apache Beam/WordCount-example(https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/WordCount.java)
 *
 * An example consists of two piplines:
 * a) Writing into Snowflake
 * b) Reading from Snowflake
 *
 * The flow of writing into Snowflake is following:
 * 1. Reading provided files
 * 2. Counting words
 * 3. Writing counts into Snowflake
 *
 * The flow of reading from Snowflake is following:
 * 1. Reading counts from Snowflake
 * 2. Writing counts into output
 *
 * TODO
 *
 * ./gradlew run --args=" /
 *              --inputFile=gs://apache-beam-samples/shakespeare/* /
 *              --output=gs://beam-snowflake-test/counts  /
 *              --serverName=<SNOWFLAKE SERVER NAME>  /
 *              --username=<SNOWFLAKE USERNAME>  /
 *              --password=<SNOWFLAKE PASSWORD> /
 *              --database=<SNOWFLAKE DATABASE> /
 *              --schema=<SNOWFLAKE SCHEMA>  /
 *              --storageIntegration=<SNOWFLAKE STORAGE INTEGRATION NAME> /
 *              --stagingBucketName=<GCS BUCKET NAME> /
 *              --runner=<DirectRunner/DataflowRunner> /
 *              --project=<FOR DATAFLOW RUNNER: GCP PROJECT NAME> /
 *              --gcpTempLocation=<FOR DATAFLOW RUNNER: GCS TEMP LOCATION STARTING> /
 *              --region=<FOR DATAFLOW RUNNER: GCP REGION> /
 *              --appName=<OPTIONAL: DATAFLOW JOB NAME PREFIX>"
 *
 */
public class SnowflakeWordCount {

    public static void main(String[] args) {
        SnowflakeWordCountOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(SnowflakeWordCountOptions.class);

        runWritingToSnowflake(options);
        runReadingFromSnowflake(options);
    }


    private static void runWritingToSnowflake(SnowflakeWordCountOptions options) {
        Pipeline p = Pipeline.create(options);

        p.apply("Reading files", TextIO.read().from(options.getInputFile()))
                .apply("Counting words", new CountWords())
                .apply("Writing counts to Snowflake", createSnowflakeWriteTransform(options));

        p.run().waitUntilFinish();
    }

    private static void runReadingFromSnowflake(SnowflakeWordCountOptions options) {
        Pipeline p = Pipeline.create(options);

        p.apply("Reading from Snowflake", createSnowflakeReadTransform(options))
                .apply(MapElements.via(new FormatAsTextFn()))
                .apply("Writing counts to GCP", TextIO.write().to(options.getOutput()));

        p.run().waitUntilFinish();
    }

    private static PTransform<PCollection<WordCountRow>, PDone> createSnowflakeWriteTransform(SnowflakeWordCountOptions options) {
        Location location = Location.of(options);

        SnowflakeIO.DataSourceConfiguration dataSourceConfiguration = createSnowflakeConfiguration(options);

        SnowflakeIO.UserDataMapper<WordCountRow> userDataMapper = (SnowflakeIO.UserDataMapper<WordCountRow>)
                (WordCountRow row) -> new Object[]{row.getWord(), row.getCount()};

        return SnowflakeIO.<WordCountRow>write()
                .withDataSourceConfiguration(dataSourceConfiguration)
                .withWriteDisposition(WriteDisposition.TRUNCATE)
                .withUserDataMapper(userDataMapper)
                .to(options.getTableName())
                .via(location)
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withTableSchema(
                        SFTableSchema.of(
                                SFColumn.of("word", SFString.of()),
                                SFColumn.of("count", SFNumber.of())));
    }

    private static PTransform<PBegin, PCollection<WordCountRow>> createSnowflakeReadTransform(SnowflakeWordCountOptions options) {
        Location location = Location.of(options);
        SnowflakeIO.DataSourceConfiguration dataSourceConfiguration = createSnowflakeConfiguration(options);

        SnowflakeIO.CsvMapper<WordCountRow> csvMapper = (SnowflakeIO.CsvMapper<WordCountRow>)
                parts -> new WordCountRow(parts[0], Long.valueOf(parts[1]));

        return SnowflakeIO.<WordCountRow>read()
                .withDataSourceConfiguration(dataSourceConfiguration)
                .fromTable(options.getTableName())
                .via(location)
                .withCsvMapper(csvMapper)
                .withCoder(SerializableCoder.of(WordCountRow.class));
    }

    private static SnowflakeIO.DataSourceConfiguration createSnowflakeConfiguration(SnowflakeWordCountOptions options) {
        return SnowflakeIO.DataSourceConfiguration.create(SnowflakeCredentialsFactory.of(options))
                .withDatabase(options.getDatabase())
                .withServerName(options.getServerName())
                .withSchema(options.getSchema());
    }

    public static class FormatAsTextFn extends SimpleFunction<WordCountRow, String> {
        @Override
        public String apply(WordCountRow wordCountRow) {
            return wordCountRow.getWord() + ": " + wordCountRow.getCount();
        }
    }

    private static class CountWords
            extends PTransform<PCollection<String>, PCollection<WordCountRow>> {
        @Override
        public PCollection<WordCountRow> expand(PCollection<String> lines) {
            return lines
                    .apply(ParDo.of(new ExtractWordsFn()))
                    .apply(Count.perElement())
                    .apply(MapElements.via(
                            new SimpleFunction<KV<String, Long>, WordCountRow>() {
                                @Override
                                public WordCountRow apply(KV<String, Long> line) {
                                    return new WordCountRow(line.getKey(), line.getValue());
                                }
                            }));

        }
    }

    private static class ExtractWordsFn extends DoFn<String, String> {
        private final String TOKENIZER_PATTERN = "[^\\p{L}]+";
        private final Counter emptyLines = Metrics.counter(ExtractWordsFn.class, "emptyLines");
        private final Distribution lineLenDist =
                Metrics.distribution(ExtractWordsFn.class, "lineLenDistro");

        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> receiver) {
            lineLenDist.update(element.length());
            if (element.trim().isEmpty()) {
                emptyLines.inc();
            }

            String[] words = element.split(TOKENIZER_PATTERN, -1);

            for (String word : words) {
                if (!word.isEmpty()) {
                    receiver.output(word);
                }
            }
        }
    }

}
