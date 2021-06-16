package com.amazonaws.samples.flink.api.sink;

import com.amazonaws.samples.flink.api.sink.impl.AmazonKinesisDataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(60_000);

//        DataStream<String> stream = env.readTextFile("s3://shausma-demo/part-10000.json");
        DataStream<String> stream = env.readTextFile("s3://shausma-nyc-tlc/yellow-trip-data/taxi-trips.json/dropoff_year=2010/part-00000-cdac5fe4-b823-4576-aeb7-7327b077476e.c000.json");

        stream.sinkTo(new AmazonKinesisDataStreamSink<>("test"));

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
