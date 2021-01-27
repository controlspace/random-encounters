import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import com.opencsv.exceptions.CsvValidationException;

public class S3CSVReaderTest {

    @Test
    public void simpleTest() throws IOException, CsvValidationException {
        S3CSVReader reader = new S3CSVReader();
        List<Map<String, String>> records = reader.getS3Records("",
                                                                  "");
        System.out.println(records);
    }
}
