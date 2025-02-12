package com.example.demo;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

@Service
public class CsvService {
    private static final Logger log = LoggerFactory.getLogger(CsvService.class);
    private final UserRepository userRepository;

    public CsvService(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    public void uploadCsv(MultipartFile file) throws IOException, CsvException {
        log.info("Starting to upload CSV file...");

        try {
            CSVReader csvReader = new CSVReader(new InputStreamReader(file.getInputStream()));
            List<String[]> records = csvReader.readAll();

            if (!records.isEmpty()) {
                records.removeFirst();
            }

            log.debug("CSV file parsed successfully. Total records found: {}", records.size());

            List<User> users = new ArrayList<>();
            for (String[] record : records) {
                User user = new User(record[0], Integer.parseInt(record[1]), record[2]);
                users.add(user);
            }

            log.debug("Persisting {} users to the database", users.size());
            userRepository.saveAll(users);

            log.info("CSV file processed and users persisted successfully.");
        } catch (Exception e) {
            log.error("Error processing CSV file: {}", e.getMessage(), e);
            throw e;
        }
    }
}
