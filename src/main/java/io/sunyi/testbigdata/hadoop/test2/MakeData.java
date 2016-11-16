package io.sunyi.testbigdata.hadoop.test2;

import java.io.*;
import java.math.BigDecimal;
import java.util.Random;

/**
 * @author sunyi
 */
public class MakeData {

	public static void main(String args[]) throws IOException {

		File data = new File(MakeData.class.getClassLoader().getResource("").getFile() + "data.txt");

		System.out.println(data.getAbsolutePath());

		if (data.exists()) {
			data.delete();
		}

		data.createNewFile();

		BufferedWriter writer = new BufferedWriter(new FileWriter(data));
//		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(System.out));


		Random user = new Random();
		Random amount = new Random();


		for (int i = 0; i < 1000000; i++) {
			writer.write(String.valueOf(100000 + user.nextInt(100)) + " " + new BigDecimal(amount.nextInt(1000000)).abs());
			writer.newLine();
		}

		writer.close();


	}

}
