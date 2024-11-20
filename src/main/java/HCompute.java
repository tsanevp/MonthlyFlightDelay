import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;

public class HCompute {
    // Indexes of desired data
    public static final int airlineIndex = 6;
    private static final int yearIndex = 0;
    private static final int monthIndex = 2;
    private static final int flightDateIndex = 5;
    private static final int destinationIndex = 17;
    private static final int arrDelayMinutesIndex = 37;
    private static final int cancelledIndex = 41;
    private static final int expectedYear = 2008;
    private static final int numReducers = 10;

    public static void main(String[] args) throws Exception {
        // Declare conf
        Configuration conf = HBaseConfiguration.create();
        conf.addResource(new File("/etc/hbase/conf/hbase-site.xml").toURI().toURL());
        
        // Declare job
        Job job = Job.getInstance(conf, "HCompute");
        job.setJarByClass(HCompute.class);

        // Declare scan conf
        Scan scan = getHBaseTableScanConf();
        TableMapReduceUtil.initTableMapperJob("FlightDelays", scan, HComputeMapper.class, FlightKey.class, Text.class, job);

        // Define classes
        job.setPartitionerClass(FlightPartitioner.class);
        job.setNumReduceTasks(numReducers);
        job.setReducerClass(HComputeReducer.class);
        job.setSortComparatorClass(FlightKeyComparator.class);
        job.setGroupingComparatorClass(FlightGroupComparator.class);

        // Set output types
        job.setMapOutputKeyClass(FlightKey.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(FlightKey.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static Scan getHBaseTableScanConf() {
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);

        // Define filters
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        SingleColumnValueFilter yearFilter = new SingleColumnValueFilter("delayInfo".getBytes(), "Year".getBytes(), CompareOperator.EQUAL, Bytes.toBytes(2008));
        filterList.addFilter(yearFilter);

        SingleColumnValueFilter cancelledFilter = new SingleColumnValueFilter("delayInfo".getBytes(), "Cancelled".getBytes(), CompareOperator.EQUAL, "0".getBytes());
        filterList.addFilter(cancelledFilter);

        // Set scan filters
        scan.setFilter(filterList);
        return scan;
    }

    public static class HComputeMapper extends TableMapper<ImmutableBytesWritable, IntWritable> {

        private static final String HBASE_TABLE_NAME = "FlightDelays"; // HBase table name
        private static final String FAMILY_NAME = "delayInfo"; // Column family

        private Connection connection;
        private Table table;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Setup the HBase connection and table
            Configuration conf = HBaseConfiguration.create();
            connection = ConnectionFactory.createConnection(conf);
            table = connection.getTable(TableName.valueOf(HBASE_TABLE_NAME));
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            // The key from input file is not used, so we focus on reading from HBase

            // Extract necessary fields from the value (we'll assume it's in CSV format)
            String[] fields = value.toString().split(",");
            if (fields.length == 0) return;

            String carrier = fields[6];  // Carrier
            String year = fields[0];     // Year
            String month = fields[2];    // Month
            String flightDate = fields[5]; // Flight date
            String origin = fields[11];  // Origin
            String cancelled = fields[41]; // Cancelled flag

            // Skip rows that are cancelled
            if ("1".equals(cancelled)) {
                return;
            }

            // Construct the row key for HBase (e.g., "AA_2008_06_01_LAX")
            String rowKey = carrier + "_" + year + "_" + month + "_" + flightDate + "_" + origin;

            // Get the row from HBase
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);

            if (result.isEmpty()) {
                return; // Skip empty rows
            }

            // Extract the ArrDelayMinutes value from HBase (stored in "delayInfo:ArrDelayMinutes")
            String delay = Bytes.toString(result.getValue(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes("ArrDelayMinutes")));

            if (delay != null && !delay.isEmpty()) {
                // Output the airline and month combination as the key
                String outputKey = carrier + "_" + year + "_" + month; // Key: airline_year_month
                String outputValue = delay; // Value: delay for the flight

                // Write the result (this is an intermediate key-value pair)
                context.write(new Text(outputKey), new Text(outputValue));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Cleanup the connection to HBase
            table.close();
            connection.close();
        }
    }

    public static class HComputeReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Prepare arrays to store total delay and count for each month (1 to 12)
            double[] totalDelays = new double[12];
            int[] flightCounts = new int[12];

            // Iterate through the values (delays) for each airline/year/month combination
            for (Text value : values) {
                try {
                    double delay = Double.parseDouble(value.toString());
                    String[] keyParts = key.toString().split("_");
                    int month = Integer.parseInt(keyParts[2]) - 1; // month is zero-indexed (0 to 11)

                    // Add the delay to the appropriate month
                    totalDelays[month] += delay;
                    flightCounts[month]++;
                } catch (NumberFormatException e) {
                    // If the value is not a valid number, skip it
                    continue;
                }
            }

            // Construct the result string for the airline/year combination
            StringBuilder result = new StringBuilder();

            for (int i = 0; i < 12; i++) {
                if (flightCounts[i] > 0) {
                    // Calculate the average delay for each month
                    double averageDelay = totalDelays[i] / flightCounts[i];
                    result.append(String.format("(%d, %.2f)", i + 1, averageDelay)); // month is 1-indexed
                } else {
                    result.append(String.format("(%d, 0)", i + 1)); // No flights for this month, so delay is 0
                }

                if (i < 11) {
                    result.append(", "); // Add a comma between months
                }
            }

            // Write the result to context in the format: airline_year, (1, avg_delay), (2, avg_delay), ...
            context.write(key, new Text(result.toString()));
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
            double[] result = new double[12];
            int[] countFlights = new int[12];

            int month = flightKey.getMonth();
            for (DoubleWritable flightDelay : flightDelays) {
                result[month - 1] += flightDelay.get();
                countFlights[month - 1] += 1;
            }

            String avgDelay = getAvgDelayAsString(countFlights, result);

            airline.set(flightKey.getAirline());
            avgMonthlyDelay.set(avgDelay);
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
