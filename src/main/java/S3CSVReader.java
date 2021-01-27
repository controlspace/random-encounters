import java.io.*;
import java.util.*;

import com.opencsv.*;
import com.opencsv.exceptions.CsvValidationException;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;

public class S3CSVReader {

    private final static String BUCKET = "buddha-test-bucket";
    private final static String PATH = "buddha/output/out.txt";
    private final static String AWS_PROFILE = "aws-profile";

    public static void main(String... args) throws IOException, CsvValidationException {
        // Example Usage
        S3CSVReader reader = new S3CSVReader();
        List<Map<String, String>> records = reader.getS3Records(BUCKET, PATH);
        System.out.println(records);
    }

    public List<Map<String, String>> getS3Records(String bucket, String key) throws IOException, CsvValidationException {
        List<Map<String, String>> records = new ArrayList<>();
        try (CSVReaderHeaderAware reader = getReader(bucket, key)) {
            Map<String, String> values;
            while ((values = reader.readMap()) != null) {
                records.add(values);
            }
            return records;
        }
    }

    private CSVReaderHeaderAware getReader(String bucket, String key) {
        CSVParser parser = new CSVParserBuilder().withSeparator('\t').build();
        S3Object object = getS3().getObject(bucket, key);
        var br = new InputStreamReader(object.getObjectContent());
        return (CSVReaderHeaderAware) new CSVReaderHeaderAwareBuilder(br)
                .withCSVParser(parser)
                .build();
    }

    private AmazonS3 getS3() {
        return AmazonS3ClientBuilder.standard()
                .withCredentials(new ProfileCredentialsProvider(AWS_PROFILE))
                .withRegion(Regions.US_WEST_2)
                .build();
    }


}
