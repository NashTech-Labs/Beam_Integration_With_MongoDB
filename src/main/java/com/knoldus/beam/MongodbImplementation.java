package com.knoldus.beam;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.bson.Document;
import java.util.HashMap;
import java.util.Map;

public class MongodbImplementation {

    /**
     * @param args
     */
    public static void main(final String[] args) {
        Pipeline pipeline = Pipeline.create();
        PCollection<String> pCollection = pipeline
                .apply(TextIO.read()
                .from("src/main/resources/User.csv"));

        PCollection<Document> pDocument = pCollection
                .apply(ParDo.of(new DoFn<String, Document>() {
            @ProcessElement
            public void processElement(final ProcessContext c) {
                String[] arr = c.element().split(",");
                Map<String, Object> map = new HashMap<String, Object>();
                map.put("userId", arr[0]);
                map.put("OrderId", arr[1]);
                map.put("Name", arr[2]);
                map.put("ProductId", arr[3]);
                map.put("Amount", arr[4]);
                map.put("Order_Date", arr[5]);
                map.put("Country", arr[6]);

                Document d1 = new Document(map);
                c.output(d1);

            }
        }));

        pDocument.apply(MongoDbIO.write().withUri("mongodb://localhost:27017")
                .withDatabase("beam").withCollection("output"));


        pipeline.run();
    }

}

