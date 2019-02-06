package input;

import global.SystemDefs;
import java.util.Scanner;

public class ColumnarMain {

	public static void main(final String args[]) throws Exception {
		System.out.println("Enter your command to the Minibase ColumnarDB:");
		Scanner input = new Scanner(System.in);
		System.out.print("> ");
		String line;
		while ((line=input.nextLine()) != null) {
			try {
				String[] commandargs = line.split(" ");
				switch (commandargs[0]) {
					case "batchinsert":
						BatchInsert batchInsert = new BatchInsert();
						String[] insertArgs = new String[commandargs.length-1];
						System.arraycopy(commandargs, 1, insertArgs, 0, commandargs.length-1);
						batchInsert.insert(insertArgs);
						break;
					case "index":
						String[] indexArgs = new String[commandargs.length-1];
						System.arraycopy(commandargs, 1, indexArgs, 0, commandargs.length-1);
						Index index = new Index();
						index.createIndex(indexArgs);
						break;
					case "query":
                        String[] queryArgs = new String[commandargs.length-1];
                        System.arraycopy(commandargs, 1, queryArgs, 0, commandargs.length-1);
						Query query = new Query();
						query.execute(queryArgs);
						break;
					case "delete_query":
						String[] delqueryArgs = new String[commandargs.length-1];
						System.arraycopy(commandargs, 1, delqueryArgs, 0, commandargs.length-1);
						DeleteQuery delquery = new DeleteQuery();
						delquery.execute(delqueryArgs);
						break;
					case "sort":
						String[] sortArgs = new String[commandargs.length - 1];
						System.arraycopy(commandargs, 1, sortArgs, 0, commandargs.length - 1);
						ColumnarSort columnarSort = new ColumnarSort();
						columnarSort.execute(sortArgs);
						break;
					case "bmj":
						String[] bmequijoinArgs = new String[commandargs.length-1];
						System.arraycopy(commandargs, 1, bmequijoinArgs, 0, commandargs.length-1);
						BitMapQuery bitmapQuery = new BitMapQuery();
						bitmapQuery.execute(bmequijoinArgs);
						break;
					case "nlj":
						String[] nljqueryArgs = new String[commandargs.length-1];
						System.arraycopy(commandargs, 1, nljqueryArgs, 0, commandargs.length-1);
						NljQuery nljQuery = new NljQuery();
						nljQuery.execute(nljqueryArgs);
						break;
					case "indexes_query":
						String[] cnfQueryArgs = new String[commandargs.length-1];
                        System.arraycopy(commandargs, 1, cnfQueryArgs, 0, commandargs.length-1);
						MultiIndexQuery multiIndQuery = new MultiIndexQuery();
						multiIndQuery.execute(cnfQueryArgs);
						break;						
					case "exit":
						if(SystemDefs.JavabaseBM!=null) {
							try {
								SystemDefs.JavabaseBM.flushAllPages();
							} catch (Exception e) {
							}
						}
						System.exit(0);
					case "":
						break;
					default:
						System.out.println("Invalid command. Re-enter input command to the Minibase ColumnarDB");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

			System.out.print("> ");
		}
	}

}
