import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

/**
 * A Hadoop MapReduce program that finds the average delay in
 * two-leg flights from ORD -> JFK airports. Amongst other filters,
 * the data is filtered to only include flights between June 2007
 * & May 2008.
 */
public class SECONDARY {
    // Indexes of desired data
    private static final int yearIndex = 0;
    private static final int monthIndex = 2;
    private static final int flightDateIndex = 5;
    public static final int airlineIndex = 6;
    private static final int destinationIndex = 17;
    private static final int arrDelayMinutesIndex = 37;
    private static final int cancelledIndex = 41;
    private static final int expectedYear = 2007;
    private static final int numReducers = 10;

    /**
     * The main method that sets up the Hadoop MapReduce job configuration.
     * It specifies the Mapper, Reducer, Partitioner, and other job parameters.
     *
     * @param args Command line arguments: input and output file paths.
     * @throws Exception If an error occurs during job configuration or execution.
     */
    public static void main(String[] args) throws Exception {
        // Jop 1 Configurations
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Monthly flight delay per airline & month");

        job.setJarByClass(SECONDARY.class);
        job.setMapperClass(FlightMapper.class);
        job.setReducerClass(FlightReducer.class);
        job.setOutputKeyClass(FlightKey.class);
        job.setOutputValueClass(Text.class);
        job.setSortComparatorClass(FlightKeyComparator.class);
        job.setGroupingComparatorClass(FlightGroupComparator.class);

        job.setPartitionerClass(FlightPartitioner.class);
        job.setNumReduceTasks(numReducers);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Wait for job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * Mapper class for the first MapReduce job.
     * Filters and transforms flight data from a CSV file.
     * Emits each relevant flight leg as key-value pairs based on flight origin and destination.
     */
    public static class FlightMapper extends Mapper<Object, Text, FlightKey, DoubleWritable> {
        private final FlightKey compositeKey = new FlightKey();
        private final DoubleWritable delayValue = new DoubleWritable();
        private final CSVParser csvParser = new CSVParserBuilder().withSeparator(',').build();

        /**
         * Check to see if the given string is valid.
         *
         * @param str A string.
         * @return Whether the string is valid.
         */
        private static boolean invalidString(String str) {
            return str == null || str.isEmpty();
        }

        /**
         * The map method processes each line of input, filters by specified conditions,
         * and emits flight leg details for valid flights.
         *
         * @param key     The input key (usually the byte offset).
         * @param value   The input value (a line of text).
         * @param context The context for writing output key-value pairs.
         * @throws IOException If an I/O error occurs.
         */
        public void map(Object key, Text value, Context context) throws IOException {
            try {
                String[] fields = csvParser.parseLine(value.toString());

                int year = Integer.parseInt(fields[yearIndex]);
                int month = Integer.parseInt(fields[monthIndex]);
                String airline = fields[airlineIndex];
                String flightDate = fields[flightDateIndex];
                String destination = fields[destinationIndex];
                String cancelled = fields[cancelledIndex];
                String arrDelayMinutes = fields[arrDelayMinutesIndex];

                // Filter by year expected year of 2008
                if (year != expectedYear || invalidString(flightDate) || invalidString(destination)) {
                    return;
                }

                // Filter out records with cancelled flights
                if (!cancelled.equals("0.00")) return;

                // Check that we have a valid airline
                if (invalidString(airline)) return;

                // Check valid month and non-empty delay
                if (month < 0 || month > 12 || invalidString(arrDelayMinutes)) return;

                compositeKey.setAirline(airline);
                compositeKey.setMonth(month);
                delayValue.set(Double.parseDouble(arrDelayMinutes));
                context.write(compositeKey, delayValue);
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * A custom Partitioner class that partitions words by their
     * first character. Words starting with 'm' go to reducer 0,
     * 'n' to reducer 1, and so on.
     */
    public static class FlightPartitioner extends Partitioner<FlightKey, DoubleWritable> {
        /**
         * Assigns a partition to each word based on its first character.
         *
         * @param flightKey     The word to partition.
         * @param delay         The count associated with the word.
         * @param numPartitions The total number of partitions (reducers).
         * @return The partition number for the word.
         */
        @Override
        public int getPartition(FlightKey flightKey, DoubleWritable delay, int numPartitions) {
            return Math.abs(flightKey.getAirline().hashCode()) % numPartitions;
        }
    }

    public static class FlightKeyComparator extends WritableComparator {
        protected FlightKeyComparator() {
            super(FlightKey.class, true);
        }

        @Override
        public int compare(Object a, Object b) {
            FlightKey flightKeyA = (FlightKey) a;
            FlightKey flightKeyB = (FlightKey) b;

            return flightKeyA.compareTo(flightKeyB);
        }
    }

    public static class FlightGroupComparator extends WritableComparator {
        protected FlightGroupComparator() {
            super(FlightKey.class, true);
        }

        @Override
        public int compare(Object a, Object b) {
            FlightKey flightKeyA = (FlightKey) a;
            FlightKey flightKeyB = (FlightKey) b;

            return flightKeyA.getAirline().compareTo(flightKeyB.getAirline());
        }
    }

//    /**
//     * Reducer class for the first MapReduce job.
//     * This reducer is responsible for pairing two-leg flights (ORD -> X and X -> JFK),
//     * where flights from ORD to an intermediary airport (F1) are paired with flights
//     * from that airport to JFK (F2). It calculates the total delay and counts the
//     * number of valid paired routes for each date.
//     */
//    public static class FlightReducer extends Reducer<FlightKey, DoubleWritable, Text, Text> {
//        private final Text airline = new Text();
//        private final Text avgMonthlyDelay = new Text();
//
//        private static String getAvgDelayAsString(int[] countFlights, double[] result) {
//            StringBuilder avgDelay = new StringBuilder();
//            for (int i = 0; i < 12; i++) {
//                if (countFlights[i] > 0) { // Avoid division by zero
//                    avgDelay.append(String.format("(%d, %d)", i + 1, Math.round(result[i] / countFlights[i])));
//                } else {
//                    avgDelay.append(String.format("(%d, 0)", i + 1)); // Default to 0 if no flights for the month
//                }
//                if (i < 11) {
//                    avgDelay.append(", "); // Add a comma between entries, but not after the last one
//                }
//            }
//            return avgDelay.toString();
//        }
//
//        /**
//         * The reduce method processes each date and airport pair, grouping flights from ORD to X (F1)
//         * and from X to JFK (F2). It finds valid flight pairs with an F1 arrival followed by an F2 departure.
//         * For each valid pair, it calculates the total combined delay and increments the flight count.
//         *
//         * @param flightKey    The key representing the date and intermediary airport (e.g., "2007-06-15|ATL").
//         * @param flightDelays The iterable list of Text values representing flight leg data (arrival or departure times and delay).
//         * @param context      The context for writing the output key-value pairs (total delay and count).
//         * @throws IOException          If an I/O error occurs.
//         * @throws InterruptedException If the reducer is interrupted.
//         */
//        public void reduce(FlightKey flightKey, Iterable<DoubleWritable> flightDelays, Context context) throws IOException, InterruptedException {
//            double[] result = new double[12];
//            int[] countFlights = new int[12];
//
//            int month = flightKey.getMonth();
//            for (DoubleWritable flightDelay : flightDelays) {
//                result[month - 1] += flightDelay.get();
//                countFlights[month - 1] += 1;
//            }
//
//            String avgDelay = getAvgDelayAsString(countFlights, result);
//
//            airline.set(flightKey.getAirline());
//            avgMonthlyDelay.set(avgDelay);
//            context.write(airline, avgMonthlyDelay);
//        }
//    }

    /**
     * Reducer class for the first MapReduce job.
     * This reducer is responsible for pairing two-leg flights (ORD -> X and X -> JFK),
     * where flights from ORD to an intermediary airport (F1) are paired with flights
     * from that airport to JFK (F2). It calculates the total delay and counts the
     * number of valid paired routes for each date.
     */
    public static class FlightReducer extends Reducer<FlightKey, DoubleWritable, Text, Text> {
        private final Text airline = new Text();
        private final Text avgMonthlyDelay = new Text();
        private final HashMap<String, int[]> carrierDelayMap = new HashMap<>();

        private static String getAvgDelayAsString(int[] countFlights, double[] result) {
            StringBuilder avgDelay = new StringBuilder();
            for (int i = 0; i < 12; i++) {
                if (countFlights[i] > 0) { // Avoid division by zero
                    avgDelay.append(String.format("(%d, %d)", i + 1, Math.round(result[i] / countFlights[i])));
                } else {
                    avgDelay.append(String.format("(%d, 0)", i + 1)); // Default to 0 if no flights for the month
                }
                if (i < 11) {
                    avgDelay.append(", "); // Add a comma between entries, but not after the last one
                }
            }
            return avgDelay.toString();
        }

        /**
         * The reduce method processes each date and airport pair, grouping flights from ORD to X (F1)
         * and from X to JFK (F2). It finds valid flight pairs with an F1 arrival followed by an F2 departure.
         * For each valid pair, it calculates the total combined delay and increments the flight count.
         *
         * @param flightKey    The key representing the date and intermediary airport (e.g., "2007-06-15|ATL").
         * @param flightDelays The iterable list of Text values representing flight leg data (arrival or departure times and delay).
         * @param context      The context for writing the output key-value pairs (total delay and count).
         * @throws IOException          If an I/O error occurs.
         * @throws InterruptedException If the reducer is interrupted.
         */
        public void reduce(FlightKey flightKey, Iterable<DoubleWritable> flightDelays, Context context) throws IOException, InterruptedException {
            double delay = 0.00;
            int countFlights = 0;

            int month = flightKey.getMonth();
            String carrier = flightKey.getAirline();
            for (DoubleWritable flightDelay : flightDelays) {
                delay += flightDelay.get();
                countFlights += 1;
            }

            // Check if the carrier exists in the map
            if (!carrierDelayMap.containsKey(carrier)) {
                // If not, create a new array for the carrier
                carrierDelayMap.put(carrier, new int[12]);
            }

            int avgDelay = (int) Math.ceil(delay / countFlights);
            carrierDelayMap.get(carrier)[month - 1] += avgDelay;

            airline.set(flightKey.getAirline());
            avgMonthlyDelay.set(Arrays.toString(carrierDelayMap.get(carrier)));
            context.write(airline, avgMonthlyDelay);
        }
    }

    public static class FlightKey implements WritableComparable<FlightKey> {
        private String airline;
        private int month;

        // Default constructor (required for Hadoop serialization)
        public FlightKey() {
        }

        // Getters and Setters
        public String getAirline() {
            return airline;
        }

        public void setAirline(String airline) {
            this.airline = airline;
        }

        public int getMonth() {
            return month;
        }

        public void setMonth(int month) {
            this.month = month;
        }

        // Serialization
        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(airline);
            out.writeInt(month);
        }

        // Deserialization
        @Override
        public void readFields(DataInput in) throws IOException {
            airline = in.readUTF();
            month = in.readInt();
        }

        // Comparison for sorting
        @Override
        public int compareTo(FlightKey other) {
            int cmp = this.airline.compareTo(other.airline);
            if (cmp != 0) {
                return cmp; // Compare by airline first
            }
            return Integer.compare(this.month, other.month); // Then compare by month
        }

        // Comparison for grouping
        public int groupComparator(FlightKey other) {
            return this.airline.compareTo(other.airline);
        }

        // For proper partitioning and grouping in Hadoop
        public int hashCode(int numPartitions) {
            return Math.abs(airline.hashCode()) % numPartitions;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            FlightKey other = (FlightKey) obj;
            return airline.equals(other.airline) && month == other.month;
        }

        @Override
        public String toString() {
            return airline + "," + month;
        }
    }
}