package com.autohome.adrd.algo.keyword_targeting.utility;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

public class SparseVector {
	private HashMap<Integer, Double> _data = null; 
	
	public SparseVector() {
		_data = new HashMap<Integer, Double>();
	}
	
	public void clear() {
		_data = new HashMap<Integer, Double>();
	}
	
	public SparseVector(final SparseVector v) {
		_data = new HashMap<Integer, Double>();
		_data.putAll(v._data);

	}
	
	public HashMap<Integer, Double> getData() {
		return _data;
	}
	
	public void setData(HashMap<Integer, Double> data) {
		_data = data;
	}

	public double getValue(int i) {
		if(this.getData().containsKey(i)) {
			return _data.get(i);
		}
		else
			return 0.0;
	}
	
	public void setValue(int i, double value) {
		_data.put(i, value);
	}
	
	
	// this = this * alpha
	public SparseVector scale(double alpha) {
		SparseVector ans = new SparseVector();
		Integer i = null;
		Double val = null;
		for(Map.Entry<Integer, Double> elem : _data.entrySet()) {
			i = elem.getKey();
			val = elem.getValue();
			ans._data.put(i, val * alpha);
		}
		return ans;
	}
	
	//Assignments
	public void plusAssign(SparseVector v) {
		plusAssign(1.0, v);
	}
	
	// this = this + alpha * v
	public void plusAssign(double alpha, final SparseVector v) {
		Integer i = null;
		Double val = null;
		for(Map.Entry<Integer, Double> elem : ((SparseVector)v)._data.entrySet()) {
			i = elem.getKey();
			val = elem.getValue();
			_data.put(i, this.getValue(i) + alpha * val);
		}
	}

	public void minusAssign(SparseVector v) {
		plusAssign(-1.0, v);
		
	}

	public void minusAssign(double alpha, SparseVector v) {
		plusAssign(-alpha, v);
		
	}

	public void scaleAssign(double alpha) {
		Integer i = null;
		Double val = null;
		for(Map.Entry<Integer, Double> elem : _data.entrySet()) {
			i = elem.getKey();
			val = elem.getValue();
			_data.put(i, val * alpha);
		}
		
	}
		
	// this^T * v
	public double dot(final SparseVector v)  {
		double ans = 0.0;
		Integer i = null;
		Double val = null;
		if(_data.size() < ((SparseVector)v)._data.size()) {
			for(Map.Entry<Integer, Double> elem : _data.entrySet()) {
				i = elem.getKey();
				val = elem.getValue();
				ans += val * ((SparseVector)v)._data.get(i);
			}
		}
		else {
			for(Map.Entry<Integer, Double> elem : ((SparseVector)v)._data.entrySet()) {
				i = elem.getKey();
				val = elem.getValue();
				ans += val * _data.get(i);
			}
		}
		return ans;
	}
	
	public double norm_1() {
		double ans = 0.0;
		for(Double val : _data.values()) {
			ans += Math.abs(val);
		}
		return ans;
	}
	
	public double norm_2() {
		double ans = 0.0;
		for(Double val : _data.values()) {
			ans += val * val;
		}
		return Math.sqrt(ans);
	}
	
	public double square() {
		double ans = 0.0;
		for(Double val : _data.values()) {
			ans += val * val;
		}
		return ans;
	}
	
	public int norm_inf_index() {
		Integer ans = -1;
		Double max_abs_value = -1.0;
		Integer i = null;
		Double val = null;
		for(Map.Entry<Integer, Double> elem : _data.entrySet()) {
			i = elem.getKey();
			val = elem.getValue();
			if(Math.abs(val) > max_abs_value) {
				max_abs_value = val;
				ans = i;
			}
		}
		return ans;
	}
	
	public void assign(final SparseVector v) {
		_data.clear();
		_data.putAll(v._data);
	}
	
	
	public void assignTmp(SparseVector v) {
		_data = v._data;
		v._data = null;
	}
	
	public void swap(SparseVector v) {
		HashMap<Integer, Double> tmp = _data;
		_data = v._data;
		v._data = tmp;
	}
	
	public Object clone() {
		SparseVector ans = new SparseVector(this);
		return ans;
	}
	
	public String toString() {
		StringBuilder str = new StringBuilder();
		DecimalFormat df = new DecimalFormat("#.000000000000000E0");
		for(int i : _data.keySet()) {
			str.append(i + ":" + df.format(_data.get(i)) + ",");
		}
		return str.toString();
	}
	
	public static SparseVector fromString(String str) {
		SparseVector ans = new SparseVector();
		if(str==null || "".equals(str))
			return ans;
		String[] tmp = str.split(",", -1);
		int id;
		double value;
		for(int i = 0; i < tmp.length-1; ++i) {
			String[] item = tmp[i].split(":");
			id = Integer.parseInt(item[0]);
			value = Double.parseDouble(item[1]);
			ans.setValue(id, value);
		}
		return ans;
	}

	public boolean has_key(int i) {
		// TODO Auto-generated method stub
		if(this.getData().containsKey(i)) {
			return true;
		}
		return false;
	}

}
