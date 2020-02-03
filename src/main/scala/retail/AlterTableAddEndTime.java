package com.home.practice.maven_example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;

public class AlterTableAddEndTime {

	public static void main(String[] args) throws IOException {
		
		BufferedReader br = new BufferedReader(new FileReader("/Users/RAMA/Desktop/hadoop/Sr_DE_Takehome/csvoriginal/DataEngineerTakehomedataset-dataset.csv"));
		
		try {
		    StringBuilder sb = new StringBuilder();
		    String line = br.readLine();
		    sb.append(line+",end_timestamp");
		    int count=0;
		    int randomMinutes = 0;
		    Random random = new Random();
		    while (line != null) {
		    		++count;
		    		if(count ==1) {line = br.readLine();continue;} // for skipping the header
		    		 sb.append(System.lineSeparator());
		        ZonedDateTime startTime = ZonedDateTime.parse(line.split(",")[0], DateTimeFormatter.ISO_DATE_TIME);
		        randomMinutes = random.nextInt(7) + 2; // Adding random minutes in range [2....8]
		        ZonedDateTime endTime = startTime.plusMinutes(randomMinutes);
		        sb.append(line+","+endTime);
		        line = br.readLine();
		    }
		    PrintWriter writer = new PrintWriter("/Users/RAMA/Desktop/hadoop/Sr_DE_Takehome/csv/DataEngineerModified-dataset.csv", "UTF-8");
		    writer.print(sb.toString());
		    writer.close();
		} finally {
		    br.close();
		}

	}

}
