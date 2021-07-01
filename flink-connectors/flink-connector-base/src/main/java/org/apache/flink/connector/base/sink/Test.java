package org.apache.flink.connector.base.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.sink.impl.AmazonKinesisDataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

import java.util.Properties;

public class Test {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(10_000);

//        DataStream<String> stream = env.readTextFile("s3://shausma-nyc-tlc/yellow-trip-data/taxi-trips.json/dropoff_year=2010/part-00000-cdac5fe4-b823-4576-aeb7-7327b077476e.c000.json");

        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfigConstants.AWS_REGION, "eu-west-1");
        consumerConfig.put(ConsumerConfigConstants.AWS_CREDENTIALS_PROVIDER, "AUTO");
        consumerConfig.put(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "TRIM_HORIZON");

        DataStream<String> stream = env.addSource(new FlinkKinesisConsumer<>("test", new SimpleStringSchema(), consumerConfig));

        stream.sinkTo(new AmazonKinesisDataStreamSink<>("test-out"));

        /*
         * Here, you can start creating your execution plan for Flink.
         *
         * Start with getting some data from the environment, like
         * 	env.readTextFile(textPath);
         *
         * then, transform the resulting DataStream<String> using operations
         * like
         * 	.filter()
         * 	.flatMap()
         * 	.join()
         * 	.coGroup()
         *
         * and many more.
         * Have a look at the programming guide for the Java API:
         *
         * https://flink.apache.org/docs/latest/apis/streaming/index.html
         *
         */

        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }

}
