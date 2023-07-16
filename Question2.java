import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;
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

class TotalMoneySpentOnEachStock implements Callable<HashMap<String, Double>> {
    private final HashMap<String, Double> totalMoneySpentOnEachStock = new HashMap<>();
    private final List<List<String>> dataset;
    private final List<String> headers;

    public TotalMoneySpentOnEachStock(List<List<String>>dataset, List<String> headers) {
        this.dataset = dataset;
        this.headers = headers;
    }

    @Override
    public HashMap<String, Double> call() throws Exception {
        for (List<String> row : dataset) {
            // Extract the relevant values from the row;
            String stock_symbol = row.get(headers.indexOf("stock_symbol"));
            int sharesBought = Integer.parseInt(row.get(headers.indexOf("share_bought")));
            double sharePrice = Double.parseDouble(row.get(headers.indexOf("share_price")));

            // Calculate the total money spent for the current transaction
            double transactionAmount = sharesBought * sharePrice;

            // Check if the user ID exists in the HashMap
            if (totalMoneySpentOnEachStock.containsKey(stock_symbol)) {
                // User already exists, add the current transaction amount to the existing total
                double currentTotal = totalMoneySpentOnEachStock.get(stock_symbol);
                totalMoneySpentOnEachStock.put(stock_symbol, currentTotal + transactionAmount);
            } else {
                // User doesn't exist, create a new entry in the HashMap
                totalMoneySpentOnEachStock.put(stock_symbol, transactionAmount);
            }
        }
        return totalMoneySpentOnEachStock;
    }
}

public class Question2 {
    public static void main(String[] args) {
        // xlsx provided in elearn is converted to csv
        // path of csv file, you can change it to any according to your folder structure
        String csvFile = "C:\\Users\\User\\Desktop\\ccp\\assignment\\src\\20042800.csv";
        String line;
        List<String> headers = null;

        List<List<String>> rows = new ArrayList<>();

        // convert csv into List of rows, each row is a List of String
        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            // save the column headers, we will need it when calling the threads
            headers = Arrays.asList(br.readLine().split(","));
            for (int i = 0; i < headers.size(); i++) {
                String header = headers.get(i);
                header = header.replace("\uFEFF", ""); // Remove \uFEFF from the header, first column is "\uFEFFstock_symbol"
                headers.set(i, header);
            }

            // read every line of the csv and store the data
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                List<String> row = Arrays.asList(values);
                rows.add(row);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // get an estimate of how much rows we want to process with each thread
        // 4 threads is used for each type of calculation
        int rowsPerInstance = rows.size() / 4;

        ExecutorService executorServiceForTotalMoneySpentByEachUser = Executors.newFixedThreadPool(4);
        ExecutorService executorServiceForTotalMoneySpentOnEachStock = Executors.newFixedThreadPool(4);

        // separate rows in csv into similar sized portions
        List<List<String>> firstQuarter = rows.subList(0, rowsPerInstance-1);
        List<List<String>> secondQuarter = rows.subList(rowsPerInstance, rowsPerInstance*2-1);
        List<List<String>> thirdQuarter = rows.subList(rowsPerInstance*2, rowsPerInstance*3-1);
        List<List<String>> lastQuarter = rows.subList(rowsPerInstance*3, rows.size());

        long startTime = System.currentTimeMillis();
        Future<HashMap<String, Double>> future1ForTotalMoneySpentByEachUser = executorServiceForTotalMoneySpentByEachUser.submit(new TotalMoneySpentByEachUser(firstQuarter, headers));
        Future<HashMap<String, Double>> future2ForTotalMoneySpentByEachUser = executorServiceForTotalMoneySpentByEachUser.submit(new TotalMoneySpentByEachUser(secondQuarter, headers));
        Future<HashMap<String, Double>> future3ForTotalMoneySpentByEachUser = executorServiceForTotalMoneySpentByEachUser.submit(new TotalMoneySpentByEachUser(thirdQuarter, headers));
        Future<HashMap<String, Double>> future4ForTotalMoneySpentByEachUser = executorServiceForTotalMoneySpentByEachUser.submit(new TotalMoneySpentByEachUser(lastQuarter, headers));

        Future<HashMap<String, Double>> future1ForTotalMoneySpentOnEachStock = executorServiceForTotalMoneySpentOnEachStock.submit(new TotalMoneySpentOnEachStock(firstQuarter, headers));
        Future<HashMap<String, Double>> future2ForTotalMoneySpentOnEachStock = executorServiceForTotalMoneySpentOnEachStock.submit(new TotalMoneySpentOnEachStock(secondQuarter, headers));
        Future<HashMap<String, Double>> future3ForTotalMoneySpentOnEachStock = executorServiceForTotalMoneySpentOnEachStock.submit(new TotalMoneySpentOnEachStock(thirdQuarter, headers));
        Future<HashMap<String, Double>> future4ForTotalMoneySpentOnEachStock = executorServiceForTotalMoneySpentOnEachStock.submit(new TotalMoneySpentOnEachStock(lastQuarter, headers));

        List<HashMap<String, Double>> listOfTotalMoneySpentByEachUserHashMapsToBeCombined = new ArrayList<>();
        List<HashMap<String, Double>> listOfTotalMoneySpentOnEachStockHashMapsToBeCombined = new ArrayList<>();

        try {
            listOfTotalMoneySpentByEachUserHashMapsToBeCombined.add(future1ForTotalMoneySpentByEachUser.get());
            listOfTotalMoneySpentByEachUserHashMapsToBeCombined.add(future2ForTotalMoneySpentByEachUser.get());
            listOfTotalMoneySpentByEachUserHashMapsToBeCombined.add(future3ForTotalMoneySpentByEachUser.get());
            listOfTotalMoneySpentByEachUserHashMapsToBeCombined.add(future4ForTotalMoneySpentByEachUser.get());
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        try {
            listOfTotalMoneySpentOnEachStockHashMapsToBeCombined.add(future1ForTotalMoneySpentOnEachStock.get());
            listOfTotalMoneySpentOnEachStockHashMapsToBeCombined.add(future2ForTotalMoneySpentOnEachStock.get());
            listOfTotalMoneySpentOnEachStockHashMapsToBeCombined.add(future3ForTotalMoneySpentOnEachStock.get());
            listOfTotalMoneySpentOnEachStockHashMapsToBeCombined.add(future4ForTotalMoneySpentOnEachStock.get());
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        executorServiceForTotalMoneySpentByEachUser.shutdown();
        executorServiceForTotalMoneySpentOnEachStock.shutdown();

        // combine the answers from the output of the executor service
        HashMap<String, Double> grandTotalOfMoneySpentByEachUser = hashMapCombineHelper(listOfTotalMoneySpentByEachUserHashMapsToBeCombined);
        HashMap<String, Double> grandTotalOfMoneySpentOnEachShare = hashMapCombineHelper(listOfTotalMoneySpentOnEachStockHashMapsToBeCombined);

        // Print the data
        DecimalFormat decimalFormat = new DecimalFormat("#.##");
        for (HashMap.Entry<String, Double> record : grandTotalOfMoneySpentByEachUser.entrySet()) {
            String userId = record.getKey();
            Double totalSpent = record.getValue();
            System.out.println("User ID: " + userId + ", Total Money Spent: " + decimalFormat.format(totalSpent));
        }
        System.out.println("-----------------------------------------------------------------------------------------");
        for (HashMap.Entry<String, Double> record : grandTotalOfMoneySpentOnEachShare.entrySet()) {
            String stockSymbol = record.getKey();
            Double totalSpent = record.getValue();
            System.out.println("Stock Symbol: " + stockSymbol + ", Total Money Spent: " + decimalFormat.format(totalSpent));
        }
        long endTime = System.currentTimeMillis();
        System.out.println("Time used: " + Long.toString(endTime - startTime));
    }

    // used to combine the output of multiple threads
    public static HashMap<String, Double> hashMapCombineHelper(List<HashMap<String, Double>> listOfHashMapsToBeCombined) {
        // Create a final HashMap to store the grand total money spent by each user/stock
        // Key can be any unique identifier
        HashMap<String, Double> grandTotalByKey = new HashMap<>();

        // for each individualHashMap in listOfHashMapsToBeCombined
        for(HashMap<String, Double> individualHashMap :listOfHashMapsToBeCombined) {
            // for each entry in individualHashMap
            for (HashMap.Entry<String, Double> entry : individualHashMap.entrySet()) {
                // get name and transactional amount for each entry
                String key = entry.getKey();
                double totalSpent = entry.getValue();

                // add records in new hashmap, if transactional record for a user/stock has been added, increase user/stock spending total
                if (grandTotalByKey.containsKey(key)) {
                    double currentTotal = grandTotalByKey.get(key);
                    grandTotalByKey.put(key, currentTotal + totalSpent);
                } else {
                    grandTotalByKey.put(key, totalSpent);
                }
            }
        }

        return grandTotalByKey;
    }
}
