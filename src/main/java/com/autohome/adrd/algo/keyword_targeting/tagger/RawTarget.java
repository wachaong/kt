package com.autohome.adrd.algo.keyword_targeting.tagger;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
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
 * version1
 * input : ods_search_searchlog
 * 		   dict query:tags
 * output: cookie1  [tag1:weight1,tag2:weight2,...]
 * 
 */

public class RawTarget extends AbstractProcessor {
		
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

		private Map<String, String> query_tags = null;

		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			query_tags = CommonFunc.readMaps("query_tags", "\t", 0, 1, "utf8");	
		}
		
		private void add(String fea, HashMap<String, Double> map, double score) {
			if(map.containsKey(fea)) {
				map.put(fea, map.get(fea) + score);
			}
			else
				map.put(fea, score);	
		}
		

		private String output_map(HashMap<String, Double> map) {
			StringBuilder sb = new StringBuilder();
			int i = 0;
			for(Map.Entry<String, Double> entry : map.entrySet()) {
				if(i > 0)
					sb.append("\t");
				i++;
				sb.append(entry.getKey());
			    sb.append(":");
				double val = entry.getValue();
				if(val > 50)
					val = 50;
				sb.append(val);									
			}
			return sb.toString();
		}

		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			
			
			List<PvlogOperation.AutoPVInfo> pvList = new ArrayList<PvlogOperation.AutoPVInfo>();
			List<SaleleadsInfoOperation.SaleleadsInfo> saleleadsList = new ArrayList<SaleleadsInfoOperation.SaleleadsInfo>();
			
			decode(key, value);

			pvList = (List<PvlogOperation.AutoPVInfo>) list.get(CG_PV);
			saleleadsList = (List<SaleleadsInfoOperation.SaleleadsInfo>) list.get(CG_SALE_LEADS);
			
			String cookie = (String) list.get("user");
			
			String path=((FileSplit)context.getInputSplit()).getPath().toString();
			String date = path.split("sessionlog")[1].split("part")[0].replaceAll("/", "");
			Date d;
			Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
			
			try {
				
				d = new SimpleDateFormat("yyyyMMdd").parse(date);
				Date d2 = new SimpleDateFormat("yyyyMMdd").parse(pred_train_start.replaceAll("/", ""));
				long diff = d2.getTime() - d.getTime();
				long days_train = diff/(1000*60*60*24);
				long days_test = 999;
				String series="";
				String spec="";
				if(! pred_test_start.equals("no"))
				{
					d2 = new SimpleDateFormat("yyyyMMdd").parse(pred_test_start.replaceAll("/", ""));
					diff = d2.getTime() - d.getTime();
					days_test = diff/(1000*60*60*24);
				}
				
				if(pvList != null && pvList.size() > 0)
				{
					HashMap<String, Double> dc = new HashMap<String, Double>();
					for(PvlogOperation.AutoPVInfo pvinfo : pvList) {
						
						if((!pattern.matcher(pvinfo.getSeriesid()).matches()) || (pvinfo.getSeriesid().equals("")))
							continue;
						if((!pattern.matcher(pvinfo.getSpecid()).matches()) || (pvinfo.getSpecid().equals("")))
							continue;
							
						series = pvinfo.getSeriesid();	
						spec = pvinfo.getSpecid();
																		
						String province = pvinfo.getProvinceid().trim();
						String city = pvinfo.getCityid().trim();
						add("province" + "@" + province, dc, 1.0);
						add("city" + "@" + city, dc, 1.0);
						
						for(String part : days_history.split(","))
						{
							int day = Integer.valueOf(part.split(":",2)[0]);
							double decay = Double.valueOf(part.split(":",2)[1]);
							if( (days_train > 0) && (days_train <= day) )
							{
								double score = Math.pow(decay,days_train);
								if(! pvinfo.getSeriesid().equals("0"))
									add("tr_series_" + String.valueOf(day) + "@" + series, dc, score);
								if(! pvinfo.getSpecid().equals("0"))
									add("tr_spec_"+ String.valueOf(day) + "@" + spec, dc, score);																								
								
								if(pattern.matcher(pvinfo.getSite1Id()).matches() && pattern.matcher(pvinfo.getSite2Id()).matches() )
									add("tr_channel_" + String.valueOf(day) + "@" + pvinfo.getSite1Id()+"#"+pvinfo.getSite2Id(), dc, score);
							}
							if( days_test <= day )
							{
								double score = Math.pow(decay,days_test);
								if(! pvinfo.getSeriesid().equals("0"))
									add("te_series_" + String.valueOf(day) + "@" + series, dc, score);
								if(! pvinfo.getSpecid().equals("0"))
									add("te_spec_"+ String.valueOf(day) + "@" + spec, dc, score);
								
								if(pattern.matcher(pvinfo.getSite1Id()).matches() && pattern.matcher(pvinfo.getSite2Id()).matches() )
									add("te_channel_" + String.valueOf(day) + "@" + pvinfo.getSite1Id()+"#"+pvinfo.getSite2Id(), dc, score);
							}																											
						}																							
					}
					
					if (saleleadsList != null && saleleadsList.size() != 0)
					{
						for(String part : days_history.split(","))
						{
							int day = Integer.valueOf(part.split(":",2)[0]);
							double decay = Double.valueOf(part.split(":",2)[1]);
							if( (days_train > 0) && (days_train <= day) )
							{
								double score = Math.pow(decay,days_train);
								add("tr_hissaleleads_" + String.valueOf(day), dc, score);								
							}
							if( days_test <= day )
							{
								double score = Math.pow(decay,days_test);
								add("te_hissaleleads_" + String.valueOf(day), dc, score);								
							}																											
						}						
					}
					
					if(cookie != null  && !cookie.isEmpty() && !dc.isEmpty())
						context.write(new Text(cookie), new Text(output_map (dc)));	
					
				}
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	}
	}

	/*
	 * 为map阶段的所有特征增加如下特征：
	 */
	public static class HReduce extends Reducer<Text, Text, Text, Text> {
		private Map<String,Double> spec_price_map = new HashMap<String,Double>();
				
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			String spec_price_map_file = context.getConfiguration().get("spec_price");
			Scanner in = new Scanner(new File(spec_price_map_file));
			while(in.hasNext()) {
				spec_price_map.put(in.next(), Double.valueOf(in.next()));
			}
		}
		
		private void string2dict(String str, HashMap<String, Double> dic, HashSet<String> types) {
			if(str == null)
				return;
			String key = null;
			double val;
			String[] tmp = str.trim().split("\t");
			for(int i = 0; i < tmp.length; i ++ )
			{
				key = tmp[i].split(":",2)[0];			
				val = Double.parseDouble(tmp[i].split(":",2)[1]);
				if(key.indexOf("@") != -1)
				{
					types.add(key.split("@")[0]+"@");
				}
				if(dic.containsKey(key)) {
					dic.put(key, dic.get(key) + val);
				}
				else {
					dic.put(key, val);
				}
			}
		}
		
		private List<Entry<String, Double>> filter_sort_dict(String filter, HashMap<String, Double> dic) {
			HashMap<String, Double> subset = new HashMap<String, Double>();
			for(Map.Entry<String, Double> entry : dic.entrySet()) {
				if(entry.getKey().trim().indexOf(filter) != -1)
				{
					subset.put(entry.getKey(), entry.getValue());
				}
			 }
			
			List<Map.Entry<String, Double>> dic_lst = new ArrayList<Map.Entry<String, Double>>(subset.entrySet());
			Collections.sort(dic_lst, new Comparator<Map.Entry<String, Double>>() {   
			    public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {      
			        return (int) (o2.getValue() - o1.getValue());			        
			    }
			});
			
			return dic_lst;			
		}
		
		private Map<String, Double> filter_from_dict(String filter, HashMap<String, Double> dic) {
			HashMap<String, Double> subset = new HashMap<String, Double>();
			for(Map.Entry<String, Double> entry : dic.entrySet()) {
				if(entry.getKey().trim().indexOf(filter) != -1)
				{
					subset.put(entry.getKey(), entry.getValue());
				}
			 }			
			
			return subset;			
		}
		
		private HashMap<String, Double> ratio_features(String prefix, List<Map.Entry<String, Double>> sort_lst, 
				Map<String,Double> spec_price_map) {
			HashMap<String, Double> new_feas = new HashMap<String, Double>();
			
			double ratio_top1 = 0.0, ratio_top3 = 0.0, sum = 0.0, price_mean = 0.0, price_var = 0.0;
			int cnt = 0, cnt_var = 0, sum_var = 0;
			boolean do_price = false;
			if( sort_lst.get(0).getKey().indexOf("spec") != -1 )
				do_price = true;
			
			if(( sort_lst.get(0).getKey().indexOf("province") != -1 ) || ( sort_lst.get(0).getKey().indexOf("city") != -1 ))
			{
				new_feas.put(sort_lst.get(0).getKey() , 1.0);
				return new_feas;
			}
			
			for (int i = 0; i < sort_lst.size(); i++) {
			    if(i == 0)
			    	ratio_top1 += sort_lst.get(i).getValue();
			    
			    if(i > 10)  //去掉低频兴趣
			    	break;
			    new_feas.put(sort_lst.get(i).getKey() , sort_lst.get(i).getValue());
			    
			    if(i<3)
			    {
			    	ratio_top3 += sort_lst.get(i).getValue();
			    	if(do_price)
			    	{
				    	if(spec_price_map.containsKey(sort_lst.get(i).getKey().split("@")[1])) {
				    		price_mean += Double.valueOf(spec_price_map.get(sort_lst.get(i).getKey().split("@")[1]));
				    		cnt_var ++;
				    	}
			    	}
			    		
			    }
			    sum += sort_lst.get(i).getValue();
			    if(sort_lst.get(i).getValue() > 0.1)
			    	cnt++;			    
			}
			if(sum > 0) {
				ratio_top1 = ratio_top1 / sum;
				ratio_top3 = ratio_top3 / sum;				
			}
			String prefix_trim = prefix.split("@")[0];
			new_feas.put(prefix_trim + "_Ratio1" , ratio_top1);
			new_feas.put(prefix_trim + "_Ratio3" , ratio_top3);
			new_feas.put(prefix_trim + "_Cnt" , (double) cnt);
			
			if(do_price)
			{
				if(cnt_var > 0)
					price_mean /= cnt_var;
				for (int i = 0; i < sort_lst.size(); i++) {
					if(i>=3)
						break;
					if(spec_price_map.containsKey(sort_lst.get(i).getKey().split("@")[1]))
						sum_var += Math.pow(Double.valueOf(spec_price_map.get(sort_lst.get(i).getKey().split("@")[1])) - price_mean, 2);				
				}
				if(cnt_var > 0)
				{
					price_var = Math.sqrt(sum_var/cnt_var);
					new_feas.put(prefix_trim + "_Mean" , price_mean);
					new_feas.put(prefix_trim + "_Var" , price_var);
				}
			}
										
			return new_feas;
		}
		
		
		private String output_map(HashMap<String, Double> map) {
			StringBuilder sb = new StringBuilder();
			java.text.DecimalFormat df = new java.text.DecimalFormat("#.00");  
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
			HashMap<String, Double> dic_tmp, dic_add;
			List<Map.Entry<String, Double>> sort_lst;
			HashSet<String> types = new HashSet<String>();
			
			for (Text value : values) {
				if(value.toString().trim().isEmpty())
					continue;
				string2dict(value.toString(), dic, types);
			}
			
			/*
			 * 用户是否决定购买某款车型 还是在选择多款车型的阶段
			 *  
			 * */
			dic_tmp = new HashMap<String, Double>();
			for(String type : types)
			{
				sort_lst = filter_sort_dict(type, dic);
				dic_add = ratio_features(type, sort_lst, spec_price_map);
				dic_tmp.putAll(dic_add);
				dic_add.clear();
			}
			Map<String, Double> saleleads = filter_from_dict("hissaleleads", dic);
			dic_tmp.putAll(saleleads);
			
			if(!dic.isEmpty())			
			    context.write(key, new Text(output_map(dic_tmp)));
		}
	}

	@Override
	protected void configJob(Job job) {
		job.getConfiguration().set("mapred.job.priority", "VERY_HIGH");
		job.setMapperClass(RCFileMapper.class);
		job.setReducerClass(HReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	}
}