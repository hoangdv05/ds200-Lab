import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class B4AgeGroupRatingAnalysis {

    public static class AgeMapper extends Mapper<Object, Text, IntWritable, Text> {
        private final Map<Integer, Integer> userAge = new HashMap<>();
        private final IntWritable outKey = new IntWritable();
        private final Text outValue = new Text();

        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null) return;

            for (URI uri : cacheFiles) {
                String path = uri.getPath();
                if (path != null && path.endsWith("users.txt")) {
                    loadUsers("users.txt");
                }
            }
        }

        private void loadUsers(String fileName) throws IOException {
            try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;

                    String[] parts = line.split(",");
                    if (parts.length < 3) continue;

                    try {
                        int userId = Integer.parseInt(parts[0].trim());
                        int age = Integer.parseInt(parts[2].trim());
                        userAge.put(userId, age);
                    } catch (NumberFormatException e) {
                    }
                }
            }
        }

        private String getAgeGroup(int age) {
            if (age <= 18) return "0-18";
            if (age <= 35) return "18-35";
            if (age <= 50) return "35-50";
            return "50+";
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(",");
            if (parts.length < 4) return;

            try {
                int userId = Integer.parseInt(parts[0].trim());
                int movieId = Integer.parseInt(parts[1].trim());
                double rating = Double.parseDouble(parts[2].trim());

                Integer age = userAge.get(userId);
                if (age == null) return;

                outKey.set(movieId);
                outValue.set(getAgeGroup(age) + "\t" + rating);
                context.write(outKey, outValue);
            } catch (NumberFormatException e) {
            }
        }
    }

    public static class AgeReducer extends Reducer<IntWritable, Text, Text, NullWritable> {
        private final Map<Integer, String> movieMap = new HashMap<>();
        private final DecimalFormat df = new DecimalFormat("0.00");

        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null) return;

            for (URI uri : cacheFiles) {
                String path = uri.getPath();
                if (path != null && path.endsWith("movies.txt")) {
                    loadMovies("movies.txt");
                }
            }
        }

        private void loadMovies(String fileName) throws IOException {
            try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;

                    int firstComma = line.indexOf(",");
                    int lastComma = line.lastIndexOf(",");

                    if (firstComma == -1 || lastComma == -1 || firstComma == lastComma) continue;

                    try {
                        int movieId = Integer.parseInt(line.substring(0, firstComma).trim());
                        String title = line.substring(firstComma + 1, lastComma).trim();
                        movieMap.put(movieId, title);
                    } catch (NumberFormatException e) {
                    }
                }
            }
        }

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            double s1 = 0.0, s2 = 0.0, s3 = 0.0, s4 = 0.0;
            int c1 = 0, c2 = 0, c3 = 0, c4 = 0;

            for (Text val : values) {
                String[] parts = val.toString().split("\t");
                if (parts.length != 2) continue;

                String group = parts[0].trim();
                double rating = Double.parseDouble(parts[1].trim());

                switch (group) {
                    case "0-18":
                        s1 += rating; c1++; break;
                    case "18-35":
                        s2 += rating; c2++; break;
                    case "35-50":
                        s3 += rating; c3++; break;
                    case "50+":
                        s4 += rating; c4++; break;
                    default:
                        break;
                }
            }

            String title = movieMap.getOrDefault(key.get(), "UnknownMovie(" + key.get() + ")");
            String g1 = c1 == 0 ? "N/A" : df.format(s1 / c1);
            String g2 = c2 == 0 ? "N/A" : df.format(s2 / c2);
            String g3 = c3 == 0 ? "N/A" : df.format(s3 / c3);
            String g4 = c4 == 0 ? "N/A" : df.format(s4 / c4);

            String output = title + "\t[0-18: " + g1 + ", 18-35: " + g2 + ", 35-50: " + g3 + ", 50+: " + g4 + "]";
            context.write(new Text(output), NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("Usage: hadoop jar ds200-java.jar B4AgeGroupRatingAnalysis <ratings_1> <ratings_2> <output> <users_file> <movies_file>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "B4 Age Group Rating Analysis");

        job.setJarByClass(B4AgeGroupRatingAnalysis.class);
        job.setMapperClass(AgeMapper.class);
        job.setReducerClass(AgeReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.addCacheFile(new URI(args[3] + "#users.txt"));
        job.addCacheFile(new URI(args[4] + "#movies.txt"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
