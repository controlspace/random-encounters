import java.io.*;
import java.util.*;
import java.nio.charset.StandardCharsets;

import com.opencsv.CSVWriter;

import com.amazonaws.services.s3.*;
import com.amazonaws.regions.Regions;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.model.ObjectMetadata;

public class S3CSVWriter {

    private final static String BUCKET = "buddha-test-bucket";
    private final static String PATH = "buddha/output/out.txt";
    private final static String AWS_PROFILE = "aws-profile";

    public static void main(String... args) throws IOException {
        // Example Usage
        S3CSVWriter writer = new S3CSVWriter();

        List<String[]> lines = Arrays.asList(
                new String[] { "col1", "col2", "col3" },
                new String[] { "1", "large", "5" },
                new String[] { "2", "small", "2" }
        );
        writer.writeRecords(lines);
    }

    public void writeRecords(List<String[]> lines) throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        OutputStreamWriter streamWriter = new OutputStreamWriter(stream, StandardCharsets.UTF_8);
        try (CSVWriter writer = buildCSVWriter(streamWriter)) {
            writer.writeAll(lines);
            writer.flush();
            ObjectMetadata meta = new ObjectMetadata();
            meta.setContentLength(stream.toByteArray().length);
            getS3().putObject(BUCKET, PATH, new ByteArrayInputStream(stream.toByteArray()), meta);
        }
    }

    private CSVWriter buildCSVWriter(OutputStreamWriter streamWriter) {
        return new CSVWriter(streamWriter,
                             ',',
                             Character.MIN_VALUE,
                             '"',
                             System.lineSeparator());
    }

    private AmazonS3 getS3() {
        return AmazonS3ClientBuilder.standard()
                                    .withCredentials(new ProfileCredentialsProvider(AWS_PROFILE))
                                    .withRegion(Regions.US_WEST_2)
                                    .build();
    }
}
