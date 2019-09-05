import java.io.*;
import java.util.*;

public class Aggregate  
{
	public static void showUsage(){
		System.err.printf("Usage: java Aggregate <function> <aggregation column> <csv file> <group column 1> <group column 2> ...\n");
		System.err.printf("Where <function> is one of \"count\", \"count_distinct\", \"sum\", \"avg\"\n");	
	}

	public static void main(String[] args) throws IOException
	{
		//At least four arguments are needed
		if (args.length < 4){
			showUsage();
			return;
		}

		String agg_function = args[0];
		String agg_column = args[1];
		String csv_filename = args[2];
		String[] group_columns = new String[args.length - 3];
		for(int i = 3; i < args.length; i++)
			group_columns[i-3] = args[i];

		BufferedReader br = null;
		
		try{
			br = new BufferedReader(new FileReader(csv_filename));
		}catch( IOException e ){
			System.err.printf("Error: Unable to open file %s\n",csv_filename);
			return;
		}

		String header_line;
		try{
			header_line = br.readLine(); //The readLine method returns either the next line of the file or null (if the end of the file has been reached)
		} catch (IOException e){
			System.err.printf("Error reading file\n", csv_filename);
			return;
		}
		if (header_line == null){
			System.err.printf("Error: CSV file %s has no header row\n", csv_filename);
			return;
		}

		//Split the header_line string into an array of string values using a comma
		//as the separator.
		String[] column_names = header_line.split(",");

		for (String gc : group_columns ) {
			if (gc.equals(agg_column)) {
				System.err.println("Aggergate Column: " + gc + "\nWas a group column");
				System.exit(0);
			}
		}

		Boolean match = false;
		for (String cn : column_names ) {
			if (cn.equals(agg_column)) {
				match = true;
			}
		}
		if (!match) {
			System.err.println("Aggregate Column: " + agg_column + "\nDoes not exists in csv file");
			System.exit(0);
		}

		// create a list of the group_columns coresponding index values with header columns
		List<Integer> group_columns_index_list = GetGroupColumnIndexs(group_columns,column_names);	
		int agg_column_index = GetAggregateColumnIndex(agg_column,column_names);	
		List<String[]> input = ReadData(br);

		String[][] temp_data = new String[input.size()][0];
		temp_data = input.toArray(temp_data);
		List<String[]> temp = new ArrayList<String[]>();

		for (String[] row : temp_data) {
			if (!row[agg_column_index].equals("")) {
				temp.add(row);
			}
		}

		String[][] data = new String[temp.size()][0];
		data = temp.toArray(data);

		for (Integer i : group_columns_index_list ) {
			DataSortByIndexs(data,i);
		}
	
		String[][] group_column_rows = GroupColumnJoin(group_columns_index_list,data);
		
		//Array type conversions in the following if statements are adapted from
		//https://stackoverflow.com/questions/3619850/converting-an-int-array-to-a-string-array

		if (agg_function.equals("count")) {
			String[] count = Arrays.toString(Count(agg_column_index,group_columns_index_list,group_column_rows,data)).split("[\\[\\]]")[1].split(", ");
			group_column_rows = RemoveDuplicates(group_columns_index_list,group_column_rows,data);
			group_column_rows = AggregationJoin(count,group_column_rows);
		}

		if (agg_function.equals("sum")) {
			String[] sum = Arrays.toString(Sum(agg_column_index,group_columns_index_list,group_column_rows,data)).split("[\\[\\]]")[1].split(", ");
			group_column_rows = RemoveDuplicates(group_columns_index_list,group_column_rows,data);
			group_column_rows = AggregationJoin(sum,group_column_rows);
		}

		if (agg_function.equals("avg")) {
			String[] avg = Arrays.toString(Average(agg_column_index,group_columns_index_list,group_column_rows,data)).split("[\\[\\]]")[1].split(", ");
			group_column_rows = RemoveDuplicates(group_columns_index_list,group_column_rows,data);
			group_column_rows = AggregationJoin(avg,group_column_rows);
		}

		if (agg_function.equals("count_distinct")) {
			String[] countD = Arrays.toString(CountDistinct(agg_column_index,group_columns_index_list,group_column_rows,data)).split("[\\[\\]]")[1].split(", ");
			List<Integer> group_columns_index_list_no_agg_column = new ArrayList<Integer>();
			for (Integer e: group_columns_index_list ) {
				if (e != agg_column_index) {
					group_columns_index_list_no_agg_column.add(e);
				}
			}
			group_column_rows = RemoveDuplicates(group_columns_index_list_no_agg_column,group_column_rows,data);
			group_column_rows = AggregationJoin(countD,group_column_rows);
		}

		if (!agg_function.equals("count") && !agg_function.equals("count_distinct") && !agg_function.equals("sum") && !agg_function.equals("avg")){
			showUsage();
			group_column_rows = RemoveDuplicates(group_columns_index_list,group_column_rows,data);	
		}

		DataSort(group_column_rows);
		String[] output_header = new String[group_columns.length+1];		
		int a = 0;

		for (Integer b : group_columns_index_list) {
			output_header[a] = column_names[b];
			a++;
		}
		output_header[output_header.length-1] = String.format("%s(%s)", agg_function, agg_column);	

		String[][] temp_arr = new String[group_column_rows.length+1][group_column_rows[0].length];
		int j = 1;

		for (String[] row : group_column_rows ) {
			temp_arr[j] = row;
			j++;
		}

		temp_arr[0] = output_header;
		group_column_rows = temp_arr;

		List<String[]> output = Arrays.asList(group_column_rows);

		for (String[] row : output ) {
			System.out.println(Arrays.toString(row).replace("[","").replace("]",""));
		}
		PrintDataToCsv(output);
	}

//Data reading and preprocessing
	public static List<String[]> ReadData(BufferedReader csvFile) throws IOException { 
   		int count = 0;
    	ArrayList<String[]> content = new ArrayList<String[]>();
    	try{

        	String line = "";
        	while ((line = csvFile.readLine()) != null) {
        		if (line.trim().equals("") || line.trim().equals("\n")) {
        			continue;
        		}
        		String is_empty_row = line.replace(",","");
        		if (is_empty_row.trim().equals("")){
        			continue;
        		}
            	content.add(line.split(",",-1)); //This trialing empty row handeling is adapted from https://www.chrisnewland.com/java-stringsplit-include-empty-trailing-strings--156
        	}
    	} catch (FileNotFoundException e) {
    		System.err.println(e);
      		System.out.println("file: " + csvFile +" does not exist");
    	}

    	return content;
	}

	public static void PrintDataToCsv(List<String[]> list) throws IOException {
		BufferedWriter bw  = new BufferedWriter(new FileWriter("output.csv"));
		StringBuilder sb = new StringBuilder();
		for (String[] row: list) { //worst case n*100001
			int i = 0;
			for (String string : row ) {
				sb.append(string);
				if (i != row.length-1) {
					sb.append(",");
				}
				i++;
			}
			sb.append("\n");
		}
		bw.write(sb.toString());
		bw.close();
	}

	public static void DataSort(String[][] data) {
        Arrays.sort(data, new Comparator<String[]>() {
    		public int compare(String[] a, String[] b) {   		
            		return Arrays.toString(a).compareTo(Arrays.toString(b)); 
    		}
		});

	}

	public static void DataSortByIndexs(String[][] data, final int col) {
		 Arrays.sort(data, new Comparator<String[]>() {
    		public int compare(String[] a, String[] b) {   		
            		return a[col].compareTo(b[col]); 
    		}
		});
	}

	public static List<Integer> GetGroupColumnIndexs (String[] groupColumns, String[] headerLine){// has not been tested for duplicate row names

		List<Integer> indexs = new ArrayList<Integer>();
		List<String> matched_columns = new ArrayList<String>();
		List<String> non_matched_columns = new ArrayList<String>();

		for (int header = 0; header < headerLine.length; header++) { //worst case n*100001
			int group = 0;
			while (group < groupColumns.length) {
				if (headerLine[header].equals(groupColumns[group])) {
					indexs.add(header);
					matched_columns.add(headerLine[header]);
				}
				group++;
			}
		}

		List<String> groupColumns_list = Arrays.asList(groupColumns);

		if (!matched_columns.containsAll(groupColumns_list)) {
			System.err.println("The following group columns do not match with header values in the csv file:");
			for (String gc : groupColumns_list ) {
				Boolean match = false;
				for (String mc  : matched_columns) {
					if (mc.equals(gc)) {
						match = true;
					}
				}
				if(match == false){
					non_matched_columns.add(gc);
				}
			}

			for (String nmc : non_matched_columns ) {
				System.out.println(nmc);
			}
			System.exit(0);
		}
		return indexs;		
	}

	public static int GetAggregateColumnIndex (String aggregateColumn, String[] headerLine){
		Boolean column_found = false;
		int agg_column = 0;

		for (int i = 0; i < headerLine.length; i++) {
			if (aggregateColumn.equals(headerLine[i])) {
				agg_column = i;
				column_found = true;
				break;
			}
		}
		if (!column_found) {
			System.err.println("The following column: " + aggregateColumn + " does not have a coresponding column in the csv file");
			System.exit(0);
		}
		return agg_column;
	}

	public static String[][] GroupColumnJoin(List<Integer> groupColumnsIndexs, String[][] data) { //Should unit test
		List<String[]> group_columns_bucket = new ArrayList<String[]>();

		for (Integer i : groupColumnsIndexs) {

			String[] arr = new String[data.length];
			int pos = 0;

			for (int row = 0; row < data.length; row++) {
				arr[pos] = data[row][i];
				pos++;
			}

			group_columns_bucket.add(arr);
		} 
				
		List<String[]> group_columns_parings = new ArrayList<String[]>();

		for (int x = 0; x < data.length; x++ ) { 
			String[] arr = new String[group_columns_bucket.size()];
			int t = 0;
			for (String[] string: group_columns_bucket) {
				arr[t] = string[x];
				t++;
			}
			group_columns_parings.add(arr);
		}

		String[][] array = new String[group_columns_parings.size()][0];
		array = group_columns_parings.toArray(array);

		return array;
	}

// end of preprocessing	
	public static float[] Count(int aggreateColumnIndex,List<Integer> group_column_indexs,String[][] group_columns, String[][] data) {
		List<Float> freq = new ArrayList<Float>();
		float count = 0;

		for (int i = 0; i < data.length-1; i++ ) {
			List<String> lista = new ArrayList<String>();
			for (Integer n : group_column_indexs ) {
				lista.add(data[i][n]);
			}
			List<String> listb = new ArrayList<String>();
			listb= Arrays.asList(group_columns[i+1]); //O(1)
			Boolean match = Arrays.toString(lista.toArray()).equals(Arrays.toString(listb.toArray()));
			if(match){ //worst runs 100,001 times
				count = count +1;
			}
			else if (!match ) {
				count = count +1;	
				freq.add(count);
				count = 0;	
			}
			if (i == data.length-2) {
				count = count + 1;		
				freq.add(count);		
			}		
		}
		float[] array = new float[freq.size()];
		int t = 0;
		for (float f : freq ) {array[t++] = f;};
		return array;
	}

	public static float[] Sum (int aggreateColumnIndex,List<Integer> group_column_indexs,String[][] group_columns, String[][] data) {
		
		int s = 0;
		List<Float> sum = new ArrayList<Float>();
		float total = 0; 
		for (int j = 0; j < data.length-1; j++ ) {
			List<String> lista = new ArrayList<String>();
			for (Integer n : group_column_indexs ) {
				lista.add(data[j][n]);
			}
			List<String> listb = new ArrayList<String>();
			listb= Arrays.asList(group_columns[j+1]); //O(1)
			Boolean match = Arrays.toString(lista.toArray()).equals(Arrays.toString(listb.toArray()));
			try{
				
				if (match) {
					total = total + Float.parseFloat(data[j][aggreateColumnIndex]);
				}
				else {
					total = total + Float.parseFloat(data[j][aggreateColumnIndex]);
					sum.add(total);
					total = 0;
				}

				if (j == data.length-2) {
					if (match) {
						total = total + Float.parseFloat(data[j+1][aggreateColumnIndex]);
						sum.add(total);
					}
					else {
						sum.add(Float.parseFloat(data[j+1][aggreateColumnIndex]));
					}
				}
				
			} 
			catch(NumberFormatException e) {
				System.err.println(e);
				System.err.println(data[j][aggreateColumnIndex] + " Could not be converted into an integer");
				System.exit(0);
			}
		}

		float[] array = new float[sum.size()];
		int t = 0;
		for (Float f : sum ) {array[t++] = f;}
		return array;
	}

	public static float[] Average (int aggreateColumnIndex,List<Integer> group_column_indexs,String[][] group_columns, String[][] data) {
		float[] count = Count(aggreateColumnIndex,group_column_indexs,group_columns,data);
		float[] sum = Sum(aggreateColumnIndex,group_column_indexs,group_columns,data);
		float[] avg = new float[count.length];

		for (int i = 0; i < sum.length ; i++ ) {
			avg[i] = sum[i]/count[i];
		}

		return avg;
	}

	public static float[] CountDistinct (int aggreateColumnIndex, List<Integer> group_column_indexs, String[][] group_columns, String[][] data) {

		group_column_indexs.add(0,aggreateColumnIndex);

		String[][] columns_to_be_removed = GroupColumnJoin(group_column_indexs,data);
		
		for (Integer i : group_column_indexs ) {
			DataSortByIndexs(data,i);
		}

		for (int a = 0; a < columns_to_be_removed[0].length ; a++) {
			DataSortByIndexs(columns_to_be_removed,a);
		}
		
		group_column_indexs.remove(0);

		List<String[]> data_no_duplicates = new ArrayList<String[]>();
		List<String[]> group_columns_no_duplicates = new ArrayList<String[]>();
		for (int x = 0; x < data.length-1 ; x++) {
			List<String> lista = Arrays.asList(data[x+1]); //O(1)
			List<String> listb = Arrays.asList(columns_to_be_removed[x]); //O(1)
			if (!lista.containsAll(listb)) { //O(1) because worst case does 100001 traversals since it comparing row values				
				data_no_duplicates.add(data[x]);
				group_columns_no_duplicates.add(columns_to_be_removed[x]);
			}
		}

//List to array conversion adapted from https://stackoverflow.com/questions/9572795/convert-list-to-array-in-java
		data_no_duplicates.add(data[data.length-1]);
		String [][] free_duplicate_data = new String[data_no_duplicates.size()][0];
		free_duplicate_data = data_no_duplicates.toArray(free_duplicate_data);

		String[][] free_duplicate_group_columns = GroupColumnJoin(group_column_indexs,free_duplicate_data);
		
		for (int q = 0; q < free_duplicate_group_columns[0].length; q++ ) {
			DataSortByIndexs(free_duplicate_group_columns,q);	
		}

		group_column_indexs.add(0,aggreateColumnIndex);
		for (Integer r : group_column_indexs ) {
			DataSortByIndexs(free_duplicate_data,r);	
		}

		group_column_indexs.remove(0);

		float[] count = Count(aggreateColumnIndex,group_column_indexs,free_duplicate_group_columns,free_duplicate_data);
		return count;
	}

//Helper methods

	public static String[][] RemoveDuplicates(List<Integer> group_column_indexs,String[][] group_columns,String[][] data){ //unit test

		List<String[]> group_columns_no_duplicates = new ArrayList<String[]>();
		for (int x = 0; x < data.length-1 ; x++) {
			List<String> lista = new ArrayList<String>();
			for (Integer n : group_column_indexs ) {
				lista.add(data[x][n]);
			}
			List<String> listb = new ArrayList<String>();
			listb= Arrays.asList(group_columns[x+1]); //O(1)
			Boolean match = Arrays.toString(lista.toArray()).equals(Arrays.toString(listb.toArray()));
			if (!match) { //O(1) because worst case does 100001 traversals since it comparing row values				
				group_columns_no_duplicates.add(group_columns[x]);
			}
		}

//List to array conversion adapted from https://stackoverflow.com/questions/9572795/convert-list-to-array-in-java
		group_columns_no_duplicates.add(group_columns[group_columns.length-1]);
		String[][] array = new String[group_columns_no_duplicates.size()][0];
		array = group_columns_no_duplicates.toArray(array);

		return array;	
	}

	public static String[][] AggregationJoin(String[] arr, String[][] group_columns) {

		List<String[]> aggregated_rows = new ArrayList<String[]>();

		for (int x = 0; x < arr.length ; x++ ) {
			String[] temp_arr = Arrays.copyOf(group_columns[x],group_columns[0].length+1);
			temp_arr[temp_arr.length-1] = arr[x];
			aggregated_rows.add(temp_arr);
		}

		String[][] array = new String[aggregated_rows.size()][0];
		array = aggregated_rows.toArray(array);

		return array;
	}
}