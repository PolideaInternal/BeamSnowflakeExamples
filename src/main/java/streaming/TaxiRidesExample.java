package streaming;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.snowflake.Location;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.io.snowflake.SnowflakePipelineOptions;
import org.apache.beam.sdk.io.snowflake.credentials.SnowflakeCredentialsFactory;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ToString;
import org.joda.time.Duration;

/**
 * An example is streaming taxi rides from PubSub into Snowflake.
 *
 * Check main README for more information.
 */

public class TaxiRidesExample {

    private static final String PUBSUB_TAX_RIDES = "projects/pubsub-public-data/topics/taxirides-realtime";

    public static void main(String[] args) {
        SnowflakePipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(SnowflakePipelineOptions.class);

        Pipeline p = Pipeline.create(options);

        Location location = Location.of(options);

        SnowflakeIO.DataSourceConfiguration dataSourceConfiguration = createSnowflakeConfiguration(options);

        p.apply("Reading from PubSub",
                PubsubIO.readStrings()
                        .fromTopic(PUBSUB_TAX_RIDES))
                .apply(ToString.elements())
                .apply(
                        "Writing into Snowflake",
                        SnowflakeIO.<String>write()
                                .via(location)
                                .withDataSourceConfiguration(dataSourceConfiguration)
                                .withUserDataMapper(getStreamingCsvMapper())
                                .withSnowPipe(options.getSnowPipe())
                                .withFlushTimeLimit(Duration.millis(3000))
                                .withFlushRowLimit(100)
                                .withQuotationMark("")
                                .withShardsNumber(1));

        p.run().waitUntilFinish();
    }

    public static SnowflakeIO.DataSourceConfiguration createSnowflakeConfiguration(SnowflakePipelineOptions options) {
        return SnowflakeIO.DataSourceConfiguration.create(SnowflakeCredentialsFactory.of(options))
                .withDatabase(options.getDatabase())
                .withServerName(options.getServerName())
                .withSchema(options.getSchema());
    }

    public static SnowflakeIO.UserDataMapper<String> getStreamingCsvMapper() {
        return (SnowflakeIO.UserDataMapper<String>)
                recordLine -> {
                    JsonParser jsonParser = new JsonParser();
                    JsonObject jo = (JsonObject) jsonParser.parse(recordLine);

                    return new String[]{
                            jo.get("ride_id").getAsString(),
                            String.valueOf(jo.get("latitude").getAsDouble()),
                            String.valueOf(jo.get("longitude").getAsDouble())
                    };
                };
    }

}
