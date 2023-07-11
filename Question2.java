import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;

class TotalMoneySpentByEachUser implements Callable<HashMap<String, Double>> {
    private final HashMap<String, Double> totalMoneySpentByUser = new HashMap<>();
    private final List<List<String>> dataset;
    private final List<String> headers;

    public TotalMoneySpentByEachUser(List<List<String>>dataset, List<String> headers) {
        this.dataset = dataset;
        this.headers = headers;
    }

    @Override
    public HashMap<String, Double> call() throws Exception {
        for (List<String> row : dataset) {
            // Extract the relevant values from the row
            String userId = row.get(headers.indexOf("user_id"));
            int sharesBought = Integer.parseInt(row.get(headers.indexOf("share_bought")));
            double sharePrice = Double.parseDouble(row.get(headers.indexOf("share_price")));

            // Calculate the total money spent for the current transaction
            double transactionAmount = sharesBought * sharePrice;

            // Check if the user ID exists in the HashMap
            if (totalMoneySpentByUser.containsKey(userId)) {
                // User already exists, add the current transaction amount to the existing total
                double currentTotal = totalMoneySpentByUser.get(userId);
                totalMoneySpentByUser.put(userId, currentTotal + transactionAmount);
            } else {
                // User doesn't exist, create a new entry in the HashMap
                totalMoneySpentByUser.put(userId, transactionAmount);
            }
        }
        return totalMoneySpentByUser;
    }
}

public class Question2 {
    public static void main(String[] args) {
        String csvFile = "C:\\Users\\User\\Desktop\\ccp\\assignment\\src\\20042800.csv";
        String line;
        List<String> headers = null;

        List<List<String>> rows = new ArrayList<>();

        // convert csv into List of rows, each row is a List of String
        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            // save the column headers, we will need it when calling the threads
            headers = Arrays.asList(br.readLine().split(","));

            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                List<String> row = Arrays.asList(values);
                rows.add(row);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        int rowsPerInstance = rows.size() / 4;

        ExecutorService executorService = Executors.newFixedThreadPool(4);
        TotalMoneySpentByEachUser firstQuarter = new TotalMoneySpentByEachUser(rows.subList(0, rowsPerInstance-1), headers);
        TotalMoneySpentByEachUser secondQuarter = new TotalMoneySpentByEachUser(rows.subList(rowsPerInstance, rowsPerInstance*2-1), headers);
        TotalMoneySpentByEachUser thirdQuarter = new TotalMoneySpentByEachUser(rows.subList(rowsPerInstance*2, rowsPerInstance*3-1), headers);
        TotalMoneySpentByEachUser lastQuarter = new TotalMoneySpentByEachUser(rows.subList(rowsPerInstance*3, rows.size()), headers);

        Future<HashMap<String, Double>> future1 = executorService.submit(firstQuarter);
        Future<HashMap<String, Double>> future2 = executorService.submit(secondQuarter);
        Future<HashMap<String, Double>> future3 = executorService.submit(thirdQuarter);
        Future<HashMap<String, Double>> future4 = executorService.submit(lastQuarter);

        HashMap<String, Double> result1;
        HashMap<String, Double> result2;
        HashMap<String, Double> result3;
        HashMap<String, Double> result4;

        List<HashMap<String, Double>> listOfHashMapsToBeCombined = new ArrayList<>();
        try {
            listOfHashMapsToBeCombined.add(future1.get());
            listOfHashMapsToBeCombined.add(future2.get());
            listOfHashMapsToBeCombined.add(future3.get());
            listOfHashMapsToBeCombined.add(future4.get());
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        executorService.shutdown();

        HashMap<String, Double> grandTotalOfMoneySpentByEachUser = hashMapCombineHelper(listOfHashMapsToBeCombined);
        // Print the data
        DecimalFormat decimalFormat = new DecimalFormat("#.##");
        for (HashMap.Entry<String, Double> record : grandTotalOfMoneySpentByEachUser.entrySet()) {
            String userId = record.getKey();
            Double totalSpent = record.getValue();
            System.out.println("User ID: " + userId + ", Total Money Spent: " + decimalFormat.format(totalSpent));
        }
    }

    // used to combine the output of multiple threads
    public static HashMap<String, Double> hashMapCombineHelper(List<HashMap<String, Double>> listOfHashMapsToBeCombined) {
        // Create a final HashMap to store the grand total money spent by each user
        HashMap<String, Double> grandTotalByUser = new HashMap<>();

        // for each individualHashMap in listOfHashMapsToBeCombined
        for(HashMap<String, Double> individualHashMap :listOfHashMapsToBeCombined) {
            // for each entry in individualHashMap
            for (HashMap.Entry<String, Double> entry : individualHashMap.entrySet()) {
                // get name and transactional amount for each entry
                String userId = entry.getKey();
                double totalSpent = entry.getValue();

                // add records in new hashmap, if transactional record for a user has been added, increase users spending total
                if (grandTotalByUser.containsKey(userId)) {
                    double currentTotal = grandTotalByUser.get(userId);
                    grandTotalByUser.put(userId, currentTotal + totalSpent);
                } else {
                    grandTotalByUser.put(userId, totalSpent);
                }
            }
        }

        return grandTotalByUser;
    }
}
