package batching;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.io.snowflake.credentials.SnowflakeCredentialsFactory;
import org.apache.beam.sdk.io.snowflake.data.SnowflakeColumn;
import org.apache.beam.sdk.io.snowflake.data.SnowflakeTableSchema;
import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeString;
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
 * Check main README for more information.
 */
public class WordCountExample {

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

        SnowflakeIO.DataSourceConfiguration dataSourceConfiguration = createSnowflakeConfiguration(options);

        SnowflakeIO.UserDataMapper<WordCountRow> userDataMapper = (SnowflakeIO.UserDataMapper<WordCountRow>)
                (WordCountRow row) -> new Object[]{row.getWord(), row.getCount()};

        return SnowflakeIO.<WordCountRow>write()
                .withDataSourceConfiguration(dataSourceConfiguration)
                .withWriteDisposition(WriteDisposition.TRUNCATE)
                .withUserDataMapper(userDataMapper)
                .toTable(options.getTable())
                .withStorageIntegrationName(options.getStorageIntegrationName())
                .withStagingBucketName(options.getStagingBucketName())
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withTableSchema(
                        SnowflakeTableSchema.of(
                                SnowflakeColumn.of("word", SnowflakeString.of()),
                                SnowflakeColumn.of("count", SnowflakeString.of())));
    }

    private static PTransform<PBegin, PCollection<WordCountRow>> createSnowflakeReadTransform(SnowflakeWordCountOptions options) {
        SnowflakeIO.DataSourceConfiguration dataSourceConfiguration = createSnowflakeConfiguration(options);

        SnowflakeIO.CsvMapper<WordCountRow> csvMapper = (SnowflakeIO.CsvMapper<WordCountRow>)
                parts -> new WordCountRow(parts[0], Long.valueOf(parts[1]));

        return SnowflakeIO.<WordCountRow>read()
                .withDataSourceConfiguration(dataSourceConfiguration)
                .fromTable(options.getTable())
                .withStagingBucketName(options.getStagingBucketName())
                .withStorageIntegrationName(options.getStorageIntegrationName())
                .withCsvMapper(csvMapper)
                .withCoder(SerializableCoder.of(WordCountRow.class));
    }

    public static SnowflakeIO.DataSourceConfiguration createSnowflakeConfiguration(SnowflakeWordCountOptions options) {
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
