package edu.buffalo.cse.cse486586.simpledynamo;

import java.net.ServerSocket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.util.Log;

class DB extends SQLiteOpenHelper {
	final static int DB_VERSION = 1;
	final static String DB_NAME = "haribol ";
	Context context;

	public DB(Context context) {
		super(context, DB_NAME, null, DB_VERSION);
		// Store the context for later use
		// this.context = context;
	}

	@Override
	public void onCreate(SQLiteDatabase db) {
		db.execSQL("CREATE TABLE Msg (key TEXT PRIMARY KEY, value TEXT)");

	}

	@Override
	public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {

	}

}
/*NodeList : 5562 --> 5556 --> 5554 --> 5558 --> 5560*/
public class SimpleDynamoProvider extends ContentProvider {

	public static SQLiteDatabase sqlite;
	public static Context context;
	public static ContentResolver myContentResolver;
	public static DB mydb;
	public static Cursor cursor = null;
	public static String portStr, pred1, pred2;
	public static String IP = "10.0.2.2";
	static Uri mUri = buildUri("content",
			"edu.buffalo.cse.cse486586.simpledht.provider");
	public static MatrixCursor matcursor;
	static boolean tempflag = false;
	static boolean flag= false;
	double wait=0;
	static TreeMap<String, Integer> nodeList = new TreeMap<String, Integer>();
	public static HashMap<String, String> hashmap = new HashMap<String, String>();
	public static HashMap<String, String> hashmapstar = new HashMap<String, String>();
	public static Hashtable<String, Message> table = new Hashtable<String, Message>();
	 
	private String TAG = SimpleDynamoProvider.class.getSimpleName();

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {

		sqlite.delete("Msg", null, null);
		
		for (String k : nodeList.keySet())

		{
			Message msg = new Message();
			msg.type = "delete";
			msg.Querytype = "all";
			Log.v("Delete *", "sending to " + nodeList.get(k));
			msg.destination = Integer.toString(nodeList.get(k));
			Runnable r = new Client(msg);
			Thread th = new Thread(r);
			th.start();
		}
		

		return 0;
	}

	private static Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	@Override
	public String getType(Uri uri) {
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		try {

			String k = values.getAsString("key");
			String v = values.getAsString("value");
			// int position = getcorrectNodeId(genHash(k));
			Message m = new Message();
			m.key = k;
			m.value = v;
			if (nodeList.higherEntry(genHash(k)) != null)
				m.destination = Integer.toString(nodeList.higherEntry(
						genHash(k)).getValue());
			else
				m.destination = Integer.toString(nodeList.firstEntry()
						.getValue());

			// m.destination=Integer.toString(position);
			Log.v("Insert provider", " Destination is " + m.destination);
			m.type = "insert";
			m.state = "original";
			m.originalID=m.destination;
			table.put(m.key, m);
			Runnable r = new Client(m);
			Thread th = new Thread(r);
			th.setPriority(Thread.MAX_PRIORITY);
			th.start();
			

			if (m.destination.equalsIgnoreCase("5554")) {
				Message m2 = new Message();
				m2.key = m.key;
				m2.value = m.value;
				m2.type = m.type; m2.rep1="5560"; m2.rep2="5558";
				m2.originalID=m.destination;
				m2.destination = "5560";
				m2.state = "replica";
				
				Runnable r2 = new Client(m2);
				Thread th2 = new Thread(r2);
				Log.v("Replication ", "Sending " + m2.key + "  to "
						+ m.destination);
				th2.setPriority(Thread.MAX_PRIORITY);
				th2.start();
				Message m3 = new Message();
				m3.key = m.key;
				m3.value = m.value;
				m3.type = m.type;
				m3.originalID=m.destination;
				m3.destination = "5558";
				m3.rep1="5560"; m3.rep2="5558";
				m3.state = "replica";
				Log.v("Replication ", "Sending " + m3.key + "  to "
						+ m.destination);
				Runnable r3 = new Client(m3);
				Thread th3 = new Thread(r3);
				th3.setPriority(Thread.MAX_PRIORITY);
				th3.start();

			}
			else if (m.destination.equalsIgnoreCase("5556")) {

				Message m2 = new Message();
				m2.key = m.key;
				m2.value = m.value;
				m2.type = m.type;
				m2.originalID=m.destination;
				m2.destination = "5558";
				m2.state = "replica";
				m2.rep1="5554"; m2.rep2="5558";
				Log.v("Replication ", "Sending " + m2.key + "  to "
						+ m.destination);
				Runnable r2 = new Client(m2);
				Thread th2 = new Thread(r2);
				th2.setPriority(Thread.MAX_PRIORITY);
				th2.start();
				Message m3 = new Message();
				m3.key = m.key;
				m3.value = m.value;
				m3.type = m.type;
				m3.state = "replica";
				m3.originalID=m.destination;
				m3.destination = "5554";
				m3.rep1="5554"; m3.rep2="5558";
				Log.v("Replication ", "Sending " + m3.key + "  to "
						+ m.destination);
				Runnable r3 = new Client(m3);
				Thread th3 = new Thread(r3);
				th3.setPriority(Thread.MAX_PRIORITY);	
				th3.start();

			}
			else if (m.destination.equalsIgnoreCase("5558")) {

				Message m2 = new Message();
				m2.key = m.key;
				m2.value = m.value;
				m2.type = m.type;
				m2.state = "replica";
				m2.originalID=m.destination;
				m2.destination = "5560";
				m2.rep1="5560"; m2.rep2="5562";
				Log.v("Replication ", "Sending " + m2.key + "  to "
						+ m.destination);
				Runnable r2 = new Client(m2);
				Thread th2 = new Thread(r2);
				th2.setPriority(Thread.MAX_PRIORITY);
				th2.start();
				Message m3 = new Message();
				m3.key = m.key;
				m3.value = m.value;
				m3.type = m.type;
				m3.state = "replica";
				m3.originalID=m.destination;
				m3.destination = "5562";
				m3.rep1="5562"; m3.rep2="5560";
				Log.v("Replication ", "Sending to " + m.destination);
				Runnable r3 = new Client(m3);
				Thread th3 = new Thread(r3);
				th3.setPriority(Thread.MAX_PRIORITY);
				th3.start();

			}
			else if (m.destination.equalsIgnoreCase("5560")) {

				Message m2 = new Message();
				m2.key = m.key;
				m2.value = m.value;
				m2.type = m.type;
				m2.state = "replica";
				m2.originalID=m.destination;
				m2.destination = "5562";
				m2.rep1="5562"; m2.rep2="5556";
				Log.v("Replication ", "Sending to " + m.destination);
				Runnable r2 = new Client(m2);
				Thread th2 = new Thread(r2);
				th2.setPriority(Thread.MAX_PRIORITY);
				th2.start();
				Message m3 = new Message();
				m3.key = m.key;
				m3.value = m.value;
				m3.type = m.type;
				m3.state = "replica";
				m3.originalID=m.destination;
				m3.destination = "5556";
				m3.rep1="5556"; m3.rep2="5562";
				Log.v("Replication ", "Sending to " + m.destination);
				Runnable r3 = new Client(m3);
				Thread th3 = new Thread(r3);
				th3.setPriority(Thread.MAX_PRIORITY);
				th3.start();

			}
			else if (m.destination.equalsIgnoreCase("5562")) {

				Message m2 = new Message();
				m2.key = m.key;
				m2.value = m.value;
				m2.type = m.type;
				m2.state = "replica";
				m2.originalID=m.destination;
				m2.destination = "5554";
				m2.rep1="5556"; m2.rep2="5554";
				Log.v("Replication ", "Sending " + m2.key + "  to "
						+ m.destination);
				Runnable r2 = new Client(m2);
				Thread th2 = new Thread(r2);
				th2.setPriority(Thread.MAX_PRIORITY);
				th2.start();
				Message m3 = new Message();
				m3.key = m.key;
				m3.value = m.value;
				m3.type = m.type;
				m3.state = "replica";
				m3.originalID=m.destination;
				m3.destination = "5556";
				m3.rep1="5556"; m3.rep2="5554";
				Log.v("Replication ", "Sending to " + m.destination);
				Runnable r3 = new Client(m3);
				Thread th3 = new Thread(r3);
				th3.setPriority(Thread.MAX_PRIORITY);
				th3.start();

			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// try{Thread.sleep(6000);}catch(Exception e){}
		}
		return null;
	}

	@Override
	public boolean onCreate() {
		mydb = new DB(getContext());
		sqlite = mydb.getWritableDatabase();
		context = getContext();
		myContentResolver = context.getContentResolver();
		TelephonyManager tel = (TelephonyManager) context
				.getSystemService(Context.TELEPHONY_SERVICE);
		portStr = tel.getLine1Number().substring(
				tel.getLine1Number().length() - 4);
		if(portStr.equalsIgnoreCase("5554"))
		{
			pred1="5556"; pred2="5562";
		}
		if(portStr.equalsIgnoreCase("5556"))
		{
			pred1="5562"; pred2="5560";
		}
		if(portStr.equalsIgnoreCase("5558"))
		{
			pred1="5554"; pred2="5556";
		}
		if(portStr.equalsIgnoreCase("5560"))
		{
			pred1="5558"; pred2="5554";
		}
		if(portStr.equalsIgnoreCase("5562"))
		{
			pred1="5560"; pred2="5558";
		}
		try {
			nodeList.put(genHash(Integer.toString(5554)), 5554);
			nodeList.put(genHash(Integer.toString(5556)), 5556);
			nodeList.put(genHash(Integer.toString(5558)), 5558);
			nodeList.put(genHash(Integer.toString(5560)), 5560);
			nodeList.put(genHash(Integer.toString(5562)), 5562);

			ServerSocket serverSocket = new ServerSocket(10000);
			Runnable r = new Server(serverSocket);
			Thread the = new Thread(r);
			the.start();
			Thread.sleep(1000);
			synchronized (sqlite) {
				/*tempflag = false;
				matcursor = new MatrixCursor(new String[] { "key", "value" });
				{
					for (String k : nodeList.keySet())

					{
						Message msg = new Message();
						msg.type = "query";
						msg.key = "@";
						msg.Querytype = "all";
						msg.source = portStr;
						Log.v("Recovery Query *",
								"sending to " + nodeList.get(k));
						msg.destination = Integer.toString(nodeList.get(k));
						Runnable ro = new Client(msg);
						Thread th = new Thread(ro);
						th.setPriority(Thread.MIN_PRIORITY);

						th.start();
					}
					Thread.sleep(6000);
					while (tempflag == false) {
						Thread.sleep(100);
					}
					matcursor.moveToFirst();
					while (matcursor.moveToNext()) {
						String position;
						String keyy = matcursor.getString(matcursor
								.getColumnIndex("key"));
						String vall = matcursor.getString(matcursor
								.getColumnIndex("value"));
						if (nodeList.higherEntry(genHash(keyy)) != null)
							position = Integer.toString(nodeList.higherEntry(
									genHash(keyy)).getValue());
						else
							position = Integer.toString(nodeList.firstEntry()
									.getValue());
						if (position.equalsIgnoreCase(portStr)
								|| position.equalsIgnoreCase(pred1)
								|| position.equalsIgnoreCase(pred2)) {
							ContentValues cv = new ContentValues();
							cv.put("key", keyy);
							cv.put("value", vall);
							SimpleDynamoProvider.sqlite.insertWithOnConflict(
									"Msg", null, cv,
									SQLiteDatabase.CONFLICT_REPLACE);
						}

					}
					tempflag = false;
					matcursor = new MatrixCursor(
							new String[] { "key", "value" });
					for (String k : nodeList.keySet())

					{
						Message msg = new Message();
						msg.type = "query";
						msg.key = "@";
						msg.Querytype = "all";
						msg.source = portStr;
						Log.v("Recovery Query *",
								"sending to " + nodeList.get(k));
						msg.destination = Integer.toString(nodeList.get(k));
						Runnable ro = new Client(msg);
						Thread th = new Thread(ro);
						th.setPriority(Thread.MIN_PRIORITY);

						th.start();
					}
					Thread.sleep(6000);
					while (tempflag == false) {
						Thread.sleep(100);
					}
					matcursor.moveToFirst();
					while (matcursor.moveToNext()) {
						String position;
						String keyy = matcursor.getString(matcursor
								.getColumnIndex("key"));
						String vall = matcursor.getString(matcursor
								.getColumnIndex("value"));
						if (nodeList.higherEntry(genHash(keyy)) != null)
							position = Integer.toString(nodeList.higherEntry(
									genHash(keyy)).getValue());
						else
							position = Integer.toString(nodeList.firstEntry()
									.getValue());
						if (position.equalsIgnoreCase(portStr)
								|| position.equalsIgnoreCase(pred1)
								|| position.equalsIgnoreCase(pred2)) {
							ContentValues cv = new ContentValues();
							cv.put("key", keyy);
							cv.put("value", vall);
							SimpleDynamoProvider.sqlite.insertWithOnConflict(
									"Msg", null, cv,
									SQLiteDatabase.CONFLICT_REPLACE);
						}

					}
					tempflag = false;
					matcursor = new MatrixCursor(
							new String[] { "key", "value" });
					for (String k : nodeList.keySet())

					{
						Message msg = new Message();
						msg.type = "query";
						msg.key = "@";
						msg.Querytype = "all";
						msg.source = portStr;
						Log.v("Recovery Query *",
								"sending to " + nodeList.get(k));
						msg.destination = Integer.toString(nodeList.get(k));
						Runnable ro = new Client(msg);
						Thread th = new Thread(ro);
						th.setPriority(Thread.MIN_PRIORITY);

						th.start();
					}
					Thread.sleep(6000);
					while (tempflag == false) {
						Thread.sleep(100);
					}
					matcursor.moveToFirst();
					while (matcursor.moveToNext()) {
						String position;
						String keyy = matcursor.getString(matcursor
								.getColumnIndex("key"));
						String vall = matcursor.getString(matcursor
								.getColumnIndex("value"));
						if (nodeList.higherEntry(genHash(keyy)) != null)
							position = Integer.toString(nodeList.higherEntry(
									genHash(keyy)).getValue());
						else
							position = Integer.toString(nodeList.firstEntry()
									.getValue());
						if (position.equalsIgnoreCase(portStr)
								|| position.equalsIgnoreCase(pred1)
								|| position.equalsIgnoreCase(pred2)) {
							ContentValues cv = new ContentValues();
							cv.put("key", keyy);
							cv.put("value", vall);
							SimpleDynamoProvider.sqlite.insertWithOnConflict(
									"Msg", null, cv,
									SQLiteDatabase.CONFLICT_REPLACE);
						}

					}
				}*/
				for (String k : nodeList.keySet())

				{
					Message msg = new Message();
					msg.type = "tableReq";
					msg.key = "@";
					msg.Querytype = "all";
					msg.source = portStr;
					Log.v("Recovery Query *",
							"sending to " + nodeList.get(k));
					msg.destination = Integer.toString(nodeList.get(k));
					Runnable ro = new Client(msg);
					Thread th = new Thread(ro);
					th.setPriority(Thread.MIN_PRIORITY);

					th.start();
				}
				
			}
	
		} catch (Exception e) {
		}
		return false;
	}

	@Override
	public synchronized Cursor query(Uri uri, String[] projection,
			String selection, String[] selectionArgs, String sortOrder) {
		try {
			// Thread.sleep(7000);
			
			Log.i(TAG, "Query key: " + selection);
			if (selection.equals("@")) {
				table.clear();
				Thread.sleep(6000);
				return sqlite.query("Msg", null, null, null, null, null, null);
			}
			if (selection.equals("*")) {
				cursor = null;
				tempflag = false;
				matcursor = new MatrixCursor(new String[] { "key", "value" });
				for (String k : nodeList.keySet())

				{
					Message msg = new Message();
					msg.type = "query";
					msg.key = "@";
					msg.Querytype = "all";
					msg.source = portStr;
					Log.v("Query *", "sending to " + nodeList.get(k));
					msg.destination = Integer.toString(nodeList.get(k));
					Runnable r = new Client(msg);
					Thread th = new Thread(r);
					th.start();
				}
				Thread.sleep(6000);

				while (tempflag == false) {
					Thread.sleep(100);
				}
				return matcursor;
			}
			if ((!selection.equalsIgnoreCase("*"))
					&& (!selection.equalsIgnoreCase("@"))) {
				cursor = null;
				tempflag = false;
				matcursor = new MatrixCursor(new String[] { "key", "value" });
				Message msg = new Message();
				msg.type = "query";
				msg.key = selection;
				// int position = getcorrectNodeId((selection));
				if (nodeList.higherEntry(genHash(msg.key)) != null)
					msg.destination = Integer.toString(nodeList.higherEntry(
							genHash(msg.key)).getValue());
				else
					msg.destination = Integer.toString(nodeList.firstEntry()
							.getValue());

				msg.source = portStr;
				Log.v("Provider query", "Sending " + selection + " to "
						+ msg.destination);
				Runnable r = new Client(msg);
				Thread th = new Thread(r);
				th.start();
				if(msg.destination.equals("5554"))
				{
					Message m1=new Message();
					m1.key=msg.key; m1.type=msg.type; m1.source=msg.source;
					m1.destination="5558";
					Log.d("failure query", "Sending replica query to "+ m1.destination);
					Runnable ro = new Client(m1);
					Thread tho = new Thread(ro);
					tho.start();
				}
				else if(msg.destination.equals("5556"))
				{

					Message m1=new Message();
					m1.key=msg.key; m1.type=msg.type; m1.source=msg.source;
					m1.destination="5554";
					Log.d("failure query", "Sending replica query to "+ m1.destination);
					Runnable ro = new Client(m1);
					Thread tho = new Thread(ro);
					tho.start();
				}
				else if(msg.destination.equals("5558"))
				{

					Message m1=new Message();
					m1.key=msg.key; m1.type=msg.type; m1.source=msg.source;
					m1.destination="5560";
					Log.d("failure query", "Sending replica query to "+ m1.destination);
					Runnable ro = new Client(m1);
					Thread tho = new Thread(ro);
					tho.start();
				}
				else if(msg.destination.equals("5560"))
				{
					Message m1=new Message();
					m1.key=msg.key; m1.type=msg.type; m1.source=msg.source;
					m1.destination="5562";
					Log.d("failure query", "Sending replica query to "+ m1.destination);
					Runnable ro = new Client(m1);
					Thread tho = new Thread(ro);
					tho.start();
				}
				else if(msg.destination.equals("5562"))
				{

					Message m1=new Message();
					m1.key=msg.key; m1.type=msg.type; m1.source=msg.source;
					m1.destination="5556";
					Log.d("failure query", "Sending replica query to "+ m1.destination);
					Runnable ro = new Client(m1);
					Thread tho = new Thread(ro);
					tho.start();
				}
				while (tempflag == false) {
					Thread.sleep(100);
					wait++;
					if(wait>10000)
					{
						Log.d(TAG, "Wait exceeded: " + wait);
						break;
					}
						
					
					}
				wait=0;
				Thread.sleep(1000);
				matcursor.moveToFirst();
				Log.d("Query result", "Returning "+ matcursor.getString(matcursor.getColumnIndex("key")));
				return matcursor;
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	private void cleanup( ) {
//db.delete(MYDATABASE_TABLE, "username = ?", new String[] { userName });
		for (Entry<String, Message> entry : table.entrySet())
		{
			Message m=entry.getValue();
			boolean f=(m.originalID.equals(portStr) || m.rep1.equals(portStr)||m.rep2.equals(portStr) );
			if(f)
			{
				continue;
			}
			else
			{
				Log.d("Deleting", "Deleting "+ entry.getKey() + "  "+ entry.getValue());
				sqlite.delete("Msg", "key = ?", new String[] { entry.getKey() });
			}
		}
		
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		return 0;
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		@SuppressWarnings("resource")
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	int getcorrectNodeId(String hash)

	{
		try {
			if (hash.compareTo(genHash(Integer.toString(5560))) < 0
					&& hash.compareTo(genHash(Integer.toString(5562))) > 0)
				return 5560;
			else if (hash.compareTo(genHash(Integer.toString(5556))) < 0
					&& hash.compareTo(genHash(Integer.toString(5560))) > 0)
				return 5556;
			else if (hash.compareTo(genHash(Integer.toString(5554))) < 0
					&& hash.compareTo(genHash(Integer.toString(5556))) > 0)
				return 5554;
			else if (hash.compareTo(genHash(Integer.toString(5558))) < 0
					&& hash.compareTo(genHash(Integer.toString(5554))) > 0)
				return 5558;
			else if (hash.compareTo(genHash(Integer.toString(5562))) < 0
					&& hash.compareTo(genHash(Integer.toString(5558))) > 0)
				return 5562;

		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;

	}

}