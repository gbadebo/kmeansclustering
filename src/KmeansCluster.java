import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

// hadoop jar kmean.jar KmeansCluster /user/hue/center data kmeanout
//hdfs dfs -cat /user/hue/center/*
//hdfs dfs -cat /user/hue/kmeanout_5/*
// hdfs dfs -ls /user/hue/

//hdfs dfs -rm -r -f kmeanout*
// hdfs dfs -rm -r -f center/*
//VERY IMPORTANT

//hdfs dfs -put center.txt center 

public class KmeansCluster {

	/**
	 * @param args
	 */

	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		private Text center = new Text();  // type of output key 
		private Text datapoint = new Text();
		
		ArrayList<CenterPoints> myCenterList;
		class CenterPoints{
			public double x;
			public double y;
			public CenterPoints(double x, double y){
				this.x = x;
				this.y = y;
			}
			
		}
		
		class DataPoints{
			public double cx;
			public double cy;
			public double dx; 
			public double dy;
			public DataPoints(double cx,double cy,double dx,double dy){
				this.cx = cx;
				this.cy = cy;
				this.dx = dx;
				this.dy = dy;
			}
			
		}
		public double calculateDist(double x1,double y1, double x2, double y2){
			double d = (x2 - x1)* (x2 - x1) + (y2 - y1)*(y2 - y1);
			return Math.sqrt(d);
		}
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//from data
			//for each data point calculate distance to the centroids and key track of the nearest to the centroid
			//output the closent centroid with the data
			String[] mydata = value.toString().split(" ");
		 //mydata[2],mydata[3]
			DataPoints temp = new DataPoints(0.0, 0.0, 0.0, 0.0);
			double tempMinDist = Double.MAX_VALUE;
			for (CenterPoints center: myCenterList){
				
				
				double dist = calculateDist(center.x,center.y, Double.parseDouble(mydata[2]), Double.parseDouble(mydata[3]));
				if (dist < tempMinDist ){
					
					temp.cx = center.x;
					temp.cy = center.y;
					temp.dx = Double.parseDouble(mydata[2]);
					temp.dy = Double.parseDouble(mydata[3]);
					tempMinDist = dist;
					
					
					
				}
			}
			center.set(""+temp.cx + " "+temp.cy);
			datapoint.set(temp.dx+" " +temp.dy);
			context.write(center, datapoint);
			
		}
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			//read data to memory on the mapper.
			myCenterList = new ArrayList<>();
			Configuration conf = context.getConfiguration();
			String mycentroid = conf.get("centroidpath");
			//e.g /user/hue/input/
			Path part=new Path("hdfs://cshadoop1"+mycentroid);//Location of file in HDFS
			
			
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(part);
		    for (FileStatus status : fss) {
		        Path pt = status.getPath();
		        
		        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
		        String line;
		        line=br.readLine();
		        while (line != null){
		            System.out.println(line);
		            //do what you want with the line read
		            line=br.readLine();
		        }
		       
		    }
			
			
			
	       
	    }
		
		
		
		
	}

	public static class Reduce extends Reducer<Text,Text,Text,Text> {
		Text out=new Text();
		Text k=new Text();
		
		//reducer writes out the centroid and the data in format cx,cy,dx,dy
		public void reduce(Text key, Iterable<Text> values,Context context ) throws IOException, InterruptedException {
			
			for(Text t : values){
				out.set(key.toString() + " " + t.toString());
				k.set("");
				context.write(k, out);
			}
		}
	}

	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();		// get all args
		if (otherArgs.length != 3) {
			System.err.println("Usage: KmeansCluster <center> <data> <output>");
			System.exit(2);
		}
		
		conf.set("centroidpath", otherArgs[0]);
		// create a job with name "kmeans"
		Job job = new Job(conf, "kmeans");
		job.setJarByClass(KmeansCluster.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		// set output key type 
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);
		
		 FileSystem fs = FileSystem.get(conf);
		
		if (fs.exists(new Path(otherArgs[2]+"_0")))
            fs.delete(new Path(otherArgs[2]+"_0"), true); //if output exist delete it
		
		
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		// set the HDFS path for the output 
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]+"_0"));
		
		//Wait till job completion
		job.waitForCompletion(true);
		
		
		//recalculate centroids=======================================
		
		//take data from output of the previous job to recalculate centroid
		Job jobc = new Job(conf, "kmeans");
		jobc.setJarByClass(KmeansCluster.class);
		
		jobc.setMapperClass(MapRecal.class);
		jobc.setReducerClass(ReduceRecal.class);
		
		// set output key type 
		jobc.setOutputKeyClass(Text.class);
		// set output value type
		jobc.setOutputValueClass(Text.class);
		
		
		if (fs.exists(new Path(otherArgs[0])))
            fs.delete(new Path(otherArgs[0]), true);
		
		
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(jobc, new Path(otherArgs[2]+"_0"));
		// set the HDFS path for the output 
		FileOutputFormat.setOutputPath(jobc, new Path(otherArgs[0]));
		
		//Wait till job completion
		jobc.waitForCompletion(true);
		
		
	
		//============================================================
		//redu clustering.
		int count=0;
		
		while(count < 5){
			conf.set("centroidpath", otherArgs[0]);
			// create a job with name "kmeans"
			Job job1 = new Job(conf, "kmeans");
			job1.setJarByClass(KmeansCluster.class);
			
			job1.setMapperClass(Map.class);
			job1.setReducerClass(Reduce.class);
			
			// set output key type 
			job1.setOutputKeyClass(Text.class);
			// set output value type
			job1.setOutputValueClass(Text.class);
			
			
			
			
			//set the HDFS path of the input data
			FileInputFormat.addInputPath(job1, new Path(otherArgs[2]+"_"+count));
			// set the HDFS path for the output 
			FileOutputFormat.setOutputPath(job1, new Path(otherArgs[2]+"_"+(count+1)));
			
			//Wait till job completion
			job1.waitForCompletion(true) ;
			
						
			//recalculate centroids=======================================
			
			
			Job jobd = new Job(conf, "kmeans");
			jobd.setJarByClass(KmeansCluster.class);
			
			jobd.setMapperClass(MapRecal.class);
			jobd.setReducerClass(ReduceRecal.class);
			
			// set output key type 
			jobd.setOutputKeyClass(Text.class);
			// set output value type
			jobd.setOutputValueClass(Text.class);
			
			
			if (fs.exists(new Path(otherArgs[0])))
	            fs.delete(new Path(otherArgs[0]), true);
			
			
			//set the HDFS path of the input data
			FileInputFormat.addInputPath(jobd, new Path(otherArgs[2]+"_"+(count+1)));
			// set the HDFS path for the output 
			FileOutputFormat.setOutputPath(jobd, new Path(otherArgs[0]));
			
			//Wait till job completion
			jobd.waitForCompletion(true);
			
			
			
			
			
			
			count++;
			}
		System.exit(0);
		
	}
	
	public static class MapRecal extends Mapper<LongWritable, Text, Text, Text>{
		private Text center = new Text();  // type of output key 
		private Text datapoint = new Text();
		//recalcuate centroid
		// take the group all data points by the centroid and pair it with corresponding data.
		
		
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//from data
			
		String[] mydata = value.toString().split(" ");
		//mydata[2],mydata[3]
		
			center.set(mydata[0].trim()+" "+mydata[1].trim());  //centroid
			datapoint.set(mydata[2].trim()+" "+mydata[3].trim());//data points. You get a list of data that coresponds to the center.
			context.write(center, datapoint);
		}
		
		
		
		
		
	}

	public static class ReduceRecal extends Reducer<Text,Text,Text,Text> {
		Text out=new Text();
		Text k=new Text("");
		//get the list and find the average of dx and dy and write out new centroid
		public void reduce(Text key, Iterable<Text> values,Context context ) throws IOException, InterruptedException {
			
			//calculate avg
			double sumx = 0.0;
			double sumy = 0.0;
			double count = 0.0;
			for(Text value: values){
				
				String[] mydata = value.toString().split(" ");
				sumx = sumx + (Double.parseDouble(mydata[0].trim()));
				sumy = sumy + (Double.parseDouble(mydata[1].trim()));
				count++;
			}
			String result = "" + (sumx/count) + " " + (sumy/count);
			out.set(result);
			context.write(k, out);
			
		}
	}


}
