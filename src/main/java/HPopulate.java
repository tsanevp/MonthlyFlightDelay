import java.io.File;
import java.io.IOException;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class HPopulate {
    private static void createTable(Configuration conf) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(conf); Admin admin = connection.getAdmin()) {
            TableName tableName = TableName.valueOf("FlightDelays");

            // Delete table if it exists
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }

            // Define column family
            ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.of("delayInfo");

            // Create table descriptor
            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName).setColumnFamily(columnFamilyDescriptor).build();

            // Define split keys for five regions: "E", "I", "N", and "T"
            byte[][] splitKeys = {Bytes.toBytes("E"), Bytes.toBytes("I"), Bytes.toBytes("N"), Bytes.toBytes("T")};

            // Create table with split keys
            admin.createTable(tableDescriptor, splitKeys);
        }
    }

    public static void main(String[] args) throws Exception {
        // Declare CSVParser.jar path
        Path csvParserPath = new Path("s3://a3b/opencsv.jar");

        Configuration conf = HBaseConfiguration.create();
        String hbaseSite = "/etc/hbase/conf/hbase-site.xml";
        conf.addResource(new File(hbaseSite).toURI().toURL());

        Job job = Job.getInstance(conf, "HPopulate");

        // Create the table
        createTable(conf);

        job.addFileToClassPath(csvParserPath);
        job.setJarByClass(HPopulate.class);
        job.setMapperClass(HPopulateMapper.class);
        TableMapReduceUtil.initTableReducerJob("FlightDelays", null, job);
        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class HPopulateMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        private Connection connection = null;
        private Table table = null;
        private final CSVParser csvParser = new CSVParserBuilder().withSeparator(',').build();

        @Override
        protected void setup(Context context) throws IOException {
            // Establish the connection and open the table
            Configuration conf = context.getConfiguration();
            this.connection = ConnectionFactory.createConnection(conf);
            this.table = connection.getTable(TableName.valueOf("FlightDelays"));
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = csvParser.parseLine(value.toString());

            // Skip invalid records (check if the line is empty or has no fields)
            if (fields.length == 0) return;

            String year = fields[0];
            String month = fields[2];
            String flightDate = fields[5];
            String carrier = fields[6];
            String origin = fields[11];
            String arrDelay = fields[37];
            String cancelled = fields[41];

            // Create row key as "carrier_year_month" (e.g., "AA_2008_01")
            String rowKey = carrier + "_" + year + "_" + month + "_" + flightDate + "_" + origin;
            ImmutableBytesWritable hbaseRowKey = new ImmutableBytesWritable(Bytes.toBytes(rowKey));

            Put put = new Put(Bytes.toBytes(rowKey));

            put.addColumn(Bytes.toBytes("delayInfo"), Bytes.toBytes("EntireRow"), Bytes.toBytes(value.toString()));
            put.addColumn(Bytes.toBytes("delayInfo"), Bytes.toBytes("Carrier"), Bytes.toBytes(carrier));
            put.addColumn(Bytes.toBytes("delayInfo"), Bytes.toBytes("Year"), Bytes.toBytes(year));
            put.addColumn(Bytes.toBytes("delayInfo"), Bytes.toBytes("Month"), Bytes.toBytes(month));
            put.addColumn(Bytes.toBytes("delayInfo"), Bytes.toBytes("ArrDelayMinutes"), Bytes.toBytes(arrDelay));
            put.addColumn(Bytes.toBytes("delayInfo"), Bytes.toBytes("Cancelled"), Bytes.toBytes(cancelled));

            context.write(hbaseRowKey, put);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Close the table and connection after the job
            if (this.table != null) {
                this.table.close();
            }

            if (this.connection != null) {
                this.connection.close();
            }
        }
    }
}
