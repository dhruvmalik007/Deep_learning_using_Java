/* Importing the libraries , Here   The Prerequisites are Apache Hadoop (check out the link :- https://hadoop.apache.org/docs/r2.8.0/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html)*/ 
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import java.io.IOException;
import java.util.*;
import java.lang.Runnable;
import javax.naming.NamingException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.jobcontrol.JobControl;           
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;   
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;          
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/* implementing the FriendRecommendation Algo , For recommending the Key value pairs for the values having Higher relation/similarity */ 

public class FriendRecommendation extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      if (args.length!=2){
          System.out.println("Usage: FriendRecommendation <input dir> <output dir> \n");
          System.exit(-1);
      }
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new FriendRecommendation(), args);
     
      System.exit(res);
   }
  
   public int run(String[] args) throws Exception {
       System.out.println(Arrays.toString(args));
      /* getting the chunk of data and converting to corresponding Key Value Pairs */
       @SuppressWarnings("deprecation")
       String intermediateFileDir = "tmp";
       String intermediateFileDirFile =intermediateFileDir +"/part-r-00000";
       JobControl control = new JobControl("ChainMapReduce");
       ControlledJob step1 = new ControlledJob(jobListFriends(args[0], intermediateFileDir), null);
       ControlledJob step2 = new ControlledJob(jobRecommendFriends(intermediateFileDirFile, args[1]), Arrays.asList(step1));
       control.addJob(step1);
       control.addJob(step2);
       Thread workFlowThread =  new Thread(control, "workflowthread");
       workFlowThread.setDaemon(true);
       workFlowThread.start();  
       return 0;
   }

/* The code above is the driver for the chained MapReduce jobs.  critical issue is that we need to specify the dependency in "ControlledJob step2" so that MapReduce job 2 will only start to run after the first MapReduce job completes. */



   private Job jobListFriends(String inputPath, String outputPath) throws IOException, InterruptedException, ClassNotFoundException{      
       Job job = new Job();
       job.setJarByClass(WordCount.class);
       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(IntWritable.class);

       job.setMapperClass(Map.class);
       job.setReducerClass(Reduce.class);

       job.setInputFormatClass(KeyValueTextInputFormat.class);   // Need to change the import
       job.setOutputFormatClass(TextOutputFormat.class);

       FileInputFormat.addInputPath(job, new Path(inputPath));
       FileOutputFormat.setOutputPath(job, new Path(outputPath));

       job.waitForCompletion(true);

       return job;
   }


/* The code above setup all the configuration for MapReduce job 1. */



   private Job jobRecommendFriends(String inputPath, String outputPath) throws IOException, InterruptedException, ClassNotFoundException{     
       Job job1 = new Job();
       job1.setJarByClass(WordCount.class);
       job1.setOutputKeyClass(Text.class);
       job1.setOutputValueClass(Text.class);
      
       job1.setMapperClass(MapRecommendation.class);
       job1.setReducerClass(ReduceRecommendation.class);
      
       job1.setOutputFormatClass(TextOutputFormat.class);
       job1.setInputFormatClass(KeyValueTextInputFormat.class);

       FileInputFormat.addInputPath(job1, new Path(inputPath));
       FileOutputFormat.setOutputPath(job1, new Path(outputPath));

       job1.waitForCompletion(true);

       return job1;
      
   }


/* The code above setup all the configuration for MapReduce job 2. */
 
   public static class Map extends Mapper<Text, Text, Text, IntWritable> {
      public final static IntWritable ZERO = new IntWritable(0);
      public final static IntWritable ONE = new IntWritable(1);

      @Override
      public void map(Text key, Text value, Context context)
              throws IOException, InterruptedException{
          ArrayList <String> friendList = new ArrayList<String>();
          if (!key.toString().isEmpty()){
         for (String token: value.toString().split(",")) {
             friendList.add(token);
             context.write(new Text(key + "," + token), ZERO);
         }
        
         for (int i=0; i<friendList.size();i++){
             for (int j=0;j<friendList.size();j++){
                 if (i!=j){
                     context.write(new Text(friendList.get(i) + "," + friendList.get(j)), ONE);
                 }
             }
         }
          }
      }
   }

/* The code above is the Mapper for the first MapReduce job. We need add a mark "zero" here if the pair <userID1, userID2> are found to be friends already which will be discared in the Reducer stage. */
 
   public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
      public void reduce(Text key, Iterable<IntWritable> value, Context context)
              throws IOException, InterruptedException {
         int sum = 0;
         for (IntWritable val : value) {
            if (val.get()== 0){
                sum=0;
                break;
            }
            else if (val.get()== 1){
             sum += 1;
            }
      }
         if (sum!=0){
         context.write(key, new IntWritable(sum));
         }
   }
    //  public void cleanup(javax.naming.Context context) throws IOException, InterruptedException, NamingException{
    //      context.close();
    //  }
   }

   /* The code above is the Reducer for the first MapReduce job. We need delete the pair <userID1, userID2> which are found to be friends already and marked with "Zero" in the value of the input in the Mapper stage. */


   public static class MapRecommendation extends Mapper<Text, Text, Text, Text> {
       @Override
       public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
           String line = key.toString();
           String valueNumber = value.toString();
           int id_position = line.indexOf(",");
           context.write(new Text(line.substring(0, id_position)), new Text(line.substring(id_position+1,line.length())+","+valueNumber));
       }
   }
   
/* The Mapper for the second MapReduce job */
  
   public static class ReduceRecommendation extends Reducer<Text, Text, Text, Text> {
      
       public void reduce (Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
           ArrayList<FriendInformation> friendRec =  new ArrayList<FriendInformation>();
           for (Text friendList: value){
               FriendInformation friendCandidate = new FriendInformation();
               friendCandidate.addFriend(friendList.toString().substring(0, friendList.toString().indexOf(",")), Integer.parseInt(friendList.toString().substring(friendList.toString().indexOf(",")+1,friendList.toString().length())));
               friendRec.add(friendCandidate); 
           }
            CompareRec sortFriendRecommendation = new CompareRec();
               Collections.sort(friendRec, sortFriendRecommendation);
               String writeRecommendation = "";
               int N;
            if (friendRec.size()>10){
                N = 10;
            }
            else N = friendRec.size();
         
           for (int i=0; i<N; i++){
               if (i!=N-1){
                   writeRecommendation+=friendRec.get(i).secondLinkFriendID;
                   writeRecommendation+=",";                      
               }
               else writeRecommendation+=friendRec.get(i).secondLinkFriendID;
   
           }
           context.write(key, new Text(writeRecommendation));          
       }

      
/* The Reducer for the second MapReduce job */
        class FriendInformation {
           int secondLinkFriendNumber;
           String secondLinkFriendID;
       
           void addFriend(String secondLinkFriendID, int secondLinkFriendNumber){
               this.secondLinkFriendID = secondLinkFriendID;
               this.secondLinkFriendNumber = secondLinkFriendNumber;
           }
          
           int getsecondLinkFriendNumber(){
               return secondLinkFriendNumber;
           }
          
           String getsecondLinkFriendID(){
               return secondLinkFriendID;
           }             
    }
       

/* The code above constructs the class "FriendInformation" with a string variable and a int variable to store the recommendated userID and the number of common friends respectively. The reason of constructing this class is to facilitate the process of selecting the top 10 recommended friends. */

        class CompareRec implements Comparator<Object>{
           public int compare(Object arg0, Object arg1){
               FriendInformation friend1 = (FriendInformation)arg0;
               FriendInformation friend2 = (FriendInformation)arg1;
               int flag = Integer.valueOf(friend2.getsecondLinkFriendNumber()).compareTo(Integer.valueOf(friend1.getsecondLinkFriendNumber()));
               if (flag!=0){
                   return flag;   
               }
               else{
                   return  Integer.valueOf(Integer.parseInt(friend1.getsecondLinkFriendID())).compareTo(Integer.valueOf(Integer.parseInt(friend2.getsecondLinkFriendID())));
               }
           }
    } 
      
/* The code above constructs the comparator "CompareRec". It implements the sorting criteria for selecting the top 10 recommended friends. */
       
           }
       }

