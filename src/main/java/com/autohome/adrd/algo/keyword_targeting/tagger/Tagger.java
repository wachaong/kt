package com.autohome.adrd.algo.keyword_targeting.tagger;


import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.autohome.adrd.algo.keyword_targeting.io.AbstractProcessor;
import com.autohome.adrd.algo.keyword_targeting.utility.CommonFunc;


/**
 * 
 * @author [Wangchao: wangchao@autohome.com.cn ]
 * 
 * version 2
 * input:  ods_search_searchlog mds_autohome_pvlog
 * 		   dict: 
 * 			 	query:tags 
 * 				doc:tags
 * 				tag:query_idf:doc_idf
 * 				
 * multioutput: cookie1  [tag1:weight1,tag2:weight2,...]  //tf
 * 
 */

public class Tagger extends AbstractProcessor {
		
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

		private Map<String, String> query_tags = null;
		private Map<String, String> doc_tags = null;
		private Map<String, Double> query_cnt = null;
		private Map<String, Double> doc_cnt = null;
		private static String day_end;
		private static double decay;
		private static double query_weight;

		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			
			query_tags = CommonFunc.readMaps("query_tags", ":", 0, 1, "utf8");
			doc_tags = CommonFunc.readMaps("doc_tags", ":", 0, 1, "utf8");
			query_cnt = CommonFunc.readDoubleMaps("tags_cnt", ":", 0, 1, "utf8");
			doc_cnt = CommonFunc.readDoubleMaps("tags_cnt", ":", 0, 2, "utf8");	
			day_end = context.getConfiguration().get("day_end");
			decay = context.getConfiguration().getDouble("decay", 0.9);
			query_weight = context.getConfiguration().getDouble("query_weight", 5.0);
		}
		
		
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			
			String cookie = "";
			boolean if_query = false, if_doc = false;
			
			String [] tags = null;
			String [] segs = value.toString().split("\t", -1);
			if(segs.length == 32)
				if_doc = true;
			if(segs.length == 29)
				if_query = true;
												
			if(if_query)
			{
				String query = segs[11].trim();
				if(query_tags.containsKey(query))
				{
					tags = query_tags.get(query).split(",");
				}
				else
					return;
				if(segs[4].length()>36)
					cookie = segs[4].substring(0,36);
				else
					cookie = segs[4];
			}
			if(if_doc)
			{
				String article_id = segs[8].trim();
				if(doc_tags.containsKey(article_id))
				{
					tags = doc_tags.get(article_id).split(",");
				}
				else
					return;
				if(segs[29].length()>36)
					cookie = segs[29].substring(0,36);
				else
					cookie = segs[29];
			}				
															
			if(cookie == null  || cookie.isEmpty())
				return;
			
			String path=((FileSplit)context.getInputSplit()).getPath().toString();
			String date = path.split("/")[4];
			Date d;
			java.text.DecimalFormat df = new java.text.DecimalFormat("###.####"); 
			
			try {
				
				d = new SimpleDateFormat("yyyyMMdd").parse(date);
				Date d2 = new SimpleDateFormat("yyyyMMdd").parse(day_end);
				long diff = d2.getTime() - d.getTime();
				long days_diff = diff/(1000*60*60*24);
				double score = Math.pow(decay,days_diff);
				if(if_query)
					score = score * query_weight;
				
				//tf idf
				for(String tag: tags)
				{				
					double idf = 0.0;
					if(if_query)
						idf = Math.log(query_cnt.get(tag));
					if(if_doc)
						idf = Math.log(doc_cnt.get(tag));
					context.write(new Text(cookie), new Text(tag + ":" + df.format(score / idf)));
				}
				
			} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
			}
		}
	}


	public static class HReduce extends Reducer<Text, Text, Text, Text> {		
		
						
		private void string2dict(String str, HashMap<String, Double> dic) {

			String[] tmp = str.trim().split(":",2);
			double score = Double.valueOf(tmp[1]);
			if(dic.containsKey(tmp[0])) {
				dic.put(tmp[0], dic.get(tmp[0]) + score);
			}
			else {
				dic.put(tmp[0], score);
			}
		}				
		
		private String output_map(HashMap<String, Double> map) {
			StringBuilder sb = new StringBuilder();
			java.text.DecimalFormat df = new java.text.DecimalFormat("###.###");  
			int i = 0;
			for(Map.Entry<String, Double> entry : map.entrySet()) {
				if(i > 0)
					sb.append("\t");
				i++;
				sb.append(entry.getKey());
				sb.append(":");
				double val = entry.getValue();

				sb.append(df.format(val));				
			}
			return sb.toString();
		}


		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			HashMap<String, Double> dic = new HashMap<String, Double>();
			
			for (Text value : values) {
				if(value.toString().trim().isEmpty())
					continue;
				string2dict(value.toString(), dic);
			}
			if(!dic.isEmpty())			
			    context.write(key, new Text(output_map(dic)));
			
		}
	}

	@Override
	protected void configJob(Job job) {
		job.getConfiguration().set("mapred.job.priority", "VERY_HIGH");
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(HReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	}
}