package org.apache.hadoop.mapred;

import java.text.*;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

//The format of PermitPattern is *;*;*;*;* just like crontab 
//From left to right, each * means minute of hour,  hour of day, day of month, month of year and day of week
//At the position of *, a comma separated list of intervals can be specified. However, * means always permit
//An interval is a pair of integers which are connected by '-', such as 10-20(i.e. from 10(included) to 20(excluded))
//However, if the second integer is 0, the interval becomes a point in time, such as 5-0(i.e. just at 5, as same as 5-6)

//Here are some examples:
//*;*;*;*;*             ----  Always permit
//10-30;*;*;*;*			----  Permit the job while the minute of submission time is 10 to 29
//*;1-0;*;*;*;*   		----  Permit the job while the hour of submission time is 1(i.e. 1:00-1:59 of everyday)
//0-30;0-9;*;*;*        ----  Permit the job while the first half hour is from 0:00 to 8:59 of everyday

public class TimePattern {

	private String timePattern;
	private List<PermitInterval> minuteInterval;
	private List<PermitInterval> hourInterval;
	private List<PermitInterval> dayInterval;
	private List<PermitInterval> monthInterval;
	private List<PermitInterval> weekInterval;
	private static Log LOG = LogFactory.getLog(TimePattern.class);

	private PermitInterval parseInterval(String interval) {
		int offset = interval.indexOf('-');
		if (offset == -1) {
			int timePoint;
			try {
				timePoint = Integer.parseInt(interval.trim());
			} catch (Exception e) {
				LOG.error("format error with interval " + interval);
				return new PermitInterval(0, 60);
			}
			return new PermitInterval(timePoint, 0);
		}
		int begin = 0;
		int end = 60;
		try {
			begin = Integer.parseInt(interval.substring(0,offset).trim());
			end = Integer.parseInt(interval.substring(offset + 1).trim());
		} catch (Exception e) {
			LOG.error("format error with interval " + interval);
			return new PermitInterval(0, 60);
		}

		return new PermitInterval(begin, end);
	}

	private List<PermitInterval> parsePattern(String pattern) {

		List<PermitInterval> parsed = new ArrayList<PermitInterval>();

		if (pattern.equals("*"))
			return parsed;

		String intervals[] = pattern.split("\\s*,\\s*");

		for (int i = 0; i < intervals.length; i++)
			parsed.add(parseInterval(intervals[i]));
		return parsed;
	}

	public TimePattern(String pattern) {

		this.timePattern = pattern;
		String intervals[] = pattern.split("\\s*;\\s*");
		try {
			minuteInterval = parsePattern(intervals[0]);
			hourInterval = parsePattern(intervals[1]);
			dayInterval = parsePattern(intervals[2]);
			monthInterval = parsePattern(intervals[3]);
			weekInterval = parsePattern(intervals[4]);
		} catch (Exception e) {
			LOG.error("format error with pattern " + pattern);
			timePattern = "*;*;*;*;*";
			minuteInterval = new ArrayList<PermitInterval>();
			hourInterval = new ArrayList<PermitInterval>();
			dayInterval = new ArrayList<PermitInterval>();
			monthInterval = new ArrayList<PermitInterval>();
			weekInterval = new ArrayList<PermitInterval>();
		}
	}

	private boolean pitting(int value, List<PermitInterval> interval) {

		boolean gotCha = true;

		for (PermitInterval intvl : interval) {
			if (intvl.notGotcha(value))
				gotCha = false;
			else
				return true;
		}
		return gotCha;
	}

	private boolean gotcha(Calendar cal) {

		boolean minuteGot = pitting(cal.get(Calendar.MINUTE), minuteInterval);

		boolean hourGot = pitting(cal.get(Calendar.HOUR_OF_DAY), hourInterval);

		boolean dayGot = pitting(cal.get(Calendar.DAY_OF_MONTH), dayInterval);

		boolean monthGot = pitting(cal.get(Calendar.MONTH) + 1, monthInterval);

		int value = cal.get(Calendar.DAY_OF_WEEK) - 1;
		if (value == 0)
			value = 7;

		boolean weekGot = pitting(value, weekInterval);

		return minuteGot && hourGot && dayGot && monthGot && weekGot;
	}

	public boolean isCanSubmitNow() {

		Calendar cal = Calendar.getInstance();

		return gotcha(cal);
	}
	
	public String toString() {
		return timePattern;
	}
}
class PermitInterval {
	
	//interval form begin(included) to end(excluded) when end isn't 0
	//a point in time at begin when end is 0
	private int begin;
	private int end;
	
	public PermitInterval(int begin, int end){
		this.begin = begin;
		this.end = end;
	}
	
	public boolean gotcha(int value){
		if(end!=0){
			if(value>=begin && value<end)
				return true;
			return false;
		}
		if(value==begin)
			return true;
		return false;
	}
	
	public boolean notGotcha(int value){
		return !gotcha(value);
	}
	
	public String toString(){
		if(end!=0)
			return begin+" to "+end;
		return "at "+begin;
	}
}

