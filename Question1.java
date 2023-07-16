import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;
import java.util.Random;

class Seat {
    private String seatName;
    private Customer customerBooked = null;
    private ReentrantLock lock = new ReentrantLock();

    // to check if any errors has happened
    private int timesBooked = 0;

    public Seat(String seatName) {
        this.seatName = seatName;
    }
    public Boolean bookSeat(Customer customer) {
        if(customerBooked == null) {
            customerBooked = customer;
            timesBooked += 1;
            return true;
        }
        return false;
    }

    public int getTimesBooked() {
        return timesBooked;
    }

    public String getNameOfCustomerBooked() {
        if(customerBooked == null) {
            return "";
        }
        return customerBooked.getName();
    }

    public ReentrantLock getLock() {
        return lock;
    }
}

class Theatre {
    private String theatreName;
    private ArrayList<Seat> seats = new ArrayList<>();
    private int numOfAvailableSeats;

    public Theatre(String theatreName, int seatCapacity) {
        if (seatCapacity < 1)
            throw new IllegalArgumentException("Seat capacity must be greater than or equal to 1.");

        this.theatreName = theatreName;
        this. numOfAvailableSeats = seatCapacity;
        for (int i = 0; i < seatCapacity; i++) {
            seats.add(new Seat(theatreName + "-Seat-" + Integer.toString(i)));
        }
    }

    public Seat getSeat(int seatNum) {
        return seats.get(seatNum);
    }

    public ArrayList<Seat> getSeats() {
        return seats;
    }

    public int getNumOfAvailableSeats() {
        return numOfAvailableSeats;
    }

    public void decreaseNumOfAvailableSeats() {
        numOfAvailableSeats -= 1;
    }
}

class Cinema {
    private ArrayList<Theatre> theatres = new ArrayList<>();

    public Cinema(int numberOfTheatres) {
        for (int i = 0; i < numberOfTheatres; i++) {
            theatres.add(new Theatre("Theatre-"+Integer.toString(i), 20));
        }
    }

    public Theatre getTheatre(int theatreNum) {
        return theatres.get(theatreNum);
    }

    public ArrayList<Theatre> getTheatres() {
        return theatres;
    }
}

class Customer implements Runnable {
    private Cinema cinemaToVisit;
    private ArrayList<Seat> seatsBooked = new ArrayList<>();
    private String name;

    public Customer(Cinema cinema, String name){
        this.cinemaToVisit = cinema;
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public void run() {
        Random rand = new Random();

        int theatreNum = rand.nextInt(cinemaToVisit.getTheatres().size());
        int seatsRequired = rand.nextInt(3) + 1;
        Theatre theatreToBook = cinemaToVisit.getTheatre(theatreNum);

        // "Each customer will select one of the
        // theatres and in between 1 to 3 seats at one time randomly.

        // tracking of number of available seats is not that important to accuracy, some threads will run a few times more and discover there are no more seats
        // if tracking of number of available seats in each theatre needs to be synchronized users cant start to book seats at the same time, as they will have to wait for another thread to finish checking/updating the number of seats available first
        while (seatsRequired != seatsBooked.size() && theatreToBook.getNumOfAvailableSeats() > 0){
            int seatNumToBook = rand.nextInt(20);
            
            Seat seat = theatreToBook.getSeat(seatNumToBook);

            ReentrantLock seatLock = seat.getLock();
            boolean seatLockAcquired = seatLock.tryLock();

            // lock has been tried to acquire but cant get lock means the seat is already taken
            if(seatLockAcquired){
                Boolean seatSuccessfullyBooked = seat.bookSeat(this);
                if(seatSuccessfullyBooked) {
                    seatsBooked.add(seat);
                    theatreToBook.decreaseNumOfAvailableSeats();
                }
                seatLock.unlock();
            }
        }
    }
}

public class Question1 {
    public static void main(String[] args) {
        Cinema cinema1 = new Cinema(3);

        for (int i = 0; i < 100; i++) {
            new Thread(new Customer(cinema1, "customer-name-" + Integer.toString(i))).start();
        }

        for (int i = 0; i < cinema1.getTheatres().size(); i++) {
            System.out.println("====================== Theatre " + Integer.toString(i) + " =======================");
            for (int j = 0; j < cinema1.getTheatre(i).getSeats().size(); j++) {
                System.out.print("Seat: " + Integer.toString(j));
                System.out.println("  Booked by: " + cinema1.getTheatre(i).getSeat(j).getNameOfCustomerBooked() + "; seat is booked " + Integer.toString(cinema1.getTheatre(i).getSeat(j).getTimesBooked()) + " times");
            }
        }
    }
}

