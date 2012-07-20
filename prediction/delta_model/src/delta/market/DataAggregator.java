package delta.market;
import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Date;

import org.ini4j.Wini;
import au.com.bytecode.opencsv.CSVReader;

/*
 * Here is the idea. 
 * 1. Find how many sections (files)
 * 	1a. Make another class for each file
 * 2. Read first line of every file, compare dates
 * 3. Combine files, reading line by line
 */

/*PRECONDITIONS:
 * 	All have the same starting date, all start with header
 */

public class DataAggregator {

	public class OneFile{
		
		CSVReader parser;
		String filename;
		int date_ind;
		int hour_ind;
		private String[] last_line;		
		private DateFormat format;
		
		public OneFile(String filename, int date, int hour, String date_format) throws IOException{
			this.filename = filename;
			this.parser = new CSVReader(new FileReader(filename));
			this.format = new SimpleDateFormat(date_format);
			//setup first line
			this.last_line = parser.readNext();
			this.date_ind = date;
			this.hour_ind = hour;
			/*String[] line = parser.readNext();
			while(line != null){
				System.out.println(line[0] + " " + line[1]);
				line = parser.readNext();
			}*/
		}
		
		public boolean changeDate() throws ParseException, IOException{
			if(last_line == null){
				return true;
			}
			Date date;
			//are we before the given date?
			System.out.println(this.last_line[this.date_ind]);
			try{
				date = (Date)this.format.parse(this.last_line[this.date_ind]);
			}
			catch (ParseException e){
				this.last_line = this.parser.readNext();
				return false;
			}
			System.out.println(date);
			/*if(date.getYear() == otherDate.getYear() &&
					date.getMonth() == otherDate.getMonth() && 
					date.getDay() == otherDate.getDay()){
				return true;
			}*/
			
			this.last_line = this.parser.readNext();
			
			return false;
		}
		
		public boolean eof(){
			return (last_line == null);
		}
		
		public void closeFile(){
			
		}
		
	}
	
	public static void main(String[] args) throws IOException, ParseException{
		System.out.println("test");
		DataAggregator x = new DataAggregator();
		String path = "/home/tom/financial_data/";
		String filename = "RL.csv";
		String ini_file = "test.ini";
		int hour;
		int date;
		String fileish;
		Wini ini = new Wini(new File(path + ini_file));
		//int age = ini.get("bashful", "age", int.class);
		Set<String> sections = ini.keySet();
		OneFile[] files = new OneFile[sections.size()];
		int i = 0;
		Iterator iter = sections.iterator();
		while(iter.hasNext()){
			String section = iter.next().toString();
			System.out.println(section);
			Map<String, String> map = ini.get(section);
			Set<String> one_section = map.keySet();
			Iterator iter_next = one_section.iterator();
			files[i] = x.new OneFile(path + ini.get(section, "filename"), ini.get(section, "date", int.class), ini.get(section, "hour", int.class), ini.get(section, "format"));

			/*while(iter_next.hasNext()){
				System.out.println("\t" + iter_next.next());
				
			}*/
		}
		/*
		CSVReader parser = new CSVReader(new FileReader(path + filename));
		String[] line = parser.readNext();
		while(line != null){
			System.out.println(line[0] + " " + line[1]);
			line = parser.readNext();
		}*/
	}
	
}
