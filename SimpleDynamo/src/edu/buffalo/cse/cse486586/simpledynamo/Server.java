package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedInputStream;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import android.content.ContentValues;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.util.Log;

public class Server implements Runnable {
	ServerSocket serverSocket = null;
	Message msg;

	Server(ServerSocket sc) {
		this.serverSocket = sc;
	}

	private Socket socket = null;

	public void run() {
		try {
			while (true) {

				{

					socket = serverSocket.accept();
					ObjectInputStream ois = new ObjectInputStream(
							new BufferedInputStream(socket.getInputStream()));
					msg = (Message) ois.readObject();
				}
				if (msg.type.equalsIgnoreCase("insert")) {
					insert(msg);
				}
				if (msg.type.equalsIgnoreCase("query")) {
					query(msg);
				}
				if (msg.type.equalsIgnoreCase("cursor")) {
					sendbackCursor(msg);
				}
				if(msg.type.equalsIgnoreCase("tableReq"))
				{
					sendTable(msg);
				}
				if(msg.type.equalsIgnoreCase("table"))
				{
					copyContents2(msg);
				}
				if(msg.type.equalsIgnoreCase("delete"))
				{
					delete();
				}

			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void delete() {

		SimpleDynamoProvider.sqlite.delete("Msg", null, null);
	}

	private void copyContents2(Message msg2) {

		for(String k: msg2.hashtable.keySet())
		{
			Message m=new Message();
			m=msg2.hashtable.get(k);
			String ke = m.key;
			String v = m.value;
			
			long id;
			String position="";
			try {
				if (SimpleDynamoProvider.nodeList.higherEntry(genHash(ke)) != null)
					position = Integer.toString(SimpleDynamoProvider.nodeList.higherEntry(
							genHash(ke)).getValue());
				else
					position = Integer.toString(SimpleDynamoProvider.nodeList.firstEntry()
							.getValue());
			} catch (NoSuchAlgorithmException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (position.equalsIgnoreCase(SimpleDynamoProvider.portStr)
					|| position.equalsIgnoreCase(SimpleDynamoProvider.pred1)
					|| position.equalsIgnoreCase(SimpleDynamoProvider.pred2)) {
				ContentValues cv = new ContentValues();
				cv.put("key", ke);
				cv.put("value", v);
				SimpleDynamoProvider.sqlite.insertWithOnConflict(
						"Msg", null, cv,
						SQLiteDatabase.CONFLICT_REPLACE);
			}

 		}
	}
 
	private void copyContents(Message msg2) {
		if(!(msg2.hashtable.isEmpty()))
		{
			String p="pred"; 
			String q=msg2.subtype;
			Log.wtf("Rec", p + " "+ q);
			SimpleDynamoProvider.table.putAll(msg2.hashtable);
			for(String k: SimpleDynamoProvider.table.keySet())
			{
				Message m=SimpleDynamoProvider.table.get(k);
				if(p.equals(q))
				{
					if(m.state.equalsIgnoreCase("original"))
					{
						String key = m.key;
						String v = m.value;
						ContentValues cv = new ContentValues();
						cv.put("key", key);
						Log.i("Recovery pred", "Putting "+ key+ " in "+ SimpleDynamoProvider.portStr);
						cv.put("value", v);
						SimpleDynamoProvider.table.put(key, m);
						SimpleDynamoProvider.sqlite.insertWithOnConflict("Msg", null, cv,
								SQLiteDatabase.CONFLICT_REPLACE);
						replicate(m, SimpleDynamoProvider.portStr);
						SimpleDynamoProvider.flag=true;

					}
				}
				else
				{
					String s1=m.originalID; String s2=SimpleDynamoProvider.portStr;
					if(m.originalID.equals(SimpleDynamoProvider.portStr))
					{
						String key = m.key;
						String v = m.value;
						ContentValues cv = new ContentValues();
						cv.put("key", key);
						cv.put("value", v);
						Log.i("Recovery succ", "Putting "+ key+ " in "+ SimpleDynamoProvider.portStr);
						SimpleDynamoProvider.table.put(key, m);
						SimpleDynamoProvider.sqlite.insertWithOnConflict("Msg", null, cv,
								SQLiteDatabase.CONFLICT_REPLACE);
						replicate(m, SimpleDynamoProvider.portStr);
						SimpleDynamoProvider.flag=true;
					}
				}
			}
		
			
		/*	SimpleDynamoProvider.table.putAll(msg2.hashtable);
			for(String k: SimpleDynamoProvider.table.keySet())
			{
				Message m=SimpleDynamoProvider.table.get(k);
				String key = m.key;
				String v = m.value;
				ContentValues cv = new ContentValues();
				cv.put("key", key);
				cv.put("value", v);
				SimpleDynamoProvider.table.put(key, m);
				SimpleDynamoProvider.sqlite.insertWithOnConflict("Msg", null, cv,
						SQLiteDatabase.CONFLICT_REPLACE);
				SimpleDynamoProvider.flag=true;

*/

			
		}
		
			
			
		
	}

	private void replicate(Message ms, String portStr) {
		Message m=new Message();
		m=ms;
		m.originalID=ms.destination;
		if (portStr.equalsIgnoreCase("5554")) {
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
			th3.start();

		}
		if (portStr.equalsIgnoreCase("5556")) {

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
			th3.start();

		}
		if( portStr.equalsIgnoreCase("5558")) {

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
			th3.start();

		}
		if (portStr.equalsIgnoreCase("5560")) {

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
			th3.start();

		}
		if (portStr.equalsIgnoreCase("5562")) {

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
			th3.start();

		}

	}

	private void sendTable(Message msg2) {
		
		/*String[] columns = { "key", "value" };

		Cursor cursor = SimpleDynamoProvider.sqlite.query("Msg",
				columns, null, null, null, null, null);
		Message mCursor = new Message();
		mCursor.destination = msg2.source;
		mCursor.type="table";
		Log.v("Sending table" , "to "+ mCursor.destination);
		while (cursor.moveToNext()) {
			int key = cursor.getColumnIndex("key");
			int value = cursor.getColumnIndex("value");
			mCursor.key=cursor.getString(key); mCursor.value=cursor.getString(value);
			mCursor.hashtable.put(cursor.getString(key),
					mCursor);
		}
		mCursor.type="table";*/
		Message mCursor=new Message();
		mCursor.destination=msg2.source;
		mCursor.type="table";
		mCursor.hashtable.putAll(SimpleDynamoProvider.table);
		Runnable r=new Client(mCursor);
		Thread t=new Thread(r);
		t.start();
		
	}

	private void sendbackCursor(Message msg2) {

		if (msg2.Querytype.equalsIgnoreCase("basic")) {
			SimpleDynamoProvider.hashmap.putAll(msg2.hashmap);
			Log.v("Server Query", "hashmap updated " + msg2.hashmap);
			Iterator<Entry<String, String>> it = msg2.hashmap.entrySet()
					.iterator();
			Log.v("Provider query", "hashmap here " + msg2.hashmap);
			while (it.hasNext()) {
				@SuppressWarnings("rawtypes")
				Map.Entry pairs = (Map.Entry) it.next();
				if (!(pairs.getValue().equals(""))) {
					SimpleDynamoProvider.matcursor.addRow(new Object[] {
							pairs.getKey(), pairs.getValue() });
				}
				Log.v("provider query", "matcursor updated " + pairs.getKey());
				// it.remove(); // avoids a ConcurrentModificationException

			}
			SimpleDynamoProvider.tempflag = true;
			Log.v("Server query", "Cursor updated lock released");
		} else if (msg2.Querytype.equalsIgnoreCase("all")) {
			SimpleDynamoProvider.hashmapstar.putAll(msg2.hashmap);
			Log.v("Server Query", "hashmap updated " + msg2.hashmap);
			Iterator<Entry<String, String>> it = msg2.hashmap.entrySet()
					.iterator();
			Log.v("Provider query", "hashmap here " + msg2.hashmap);
			while (it.hasNext()) {
				@SuppressWarnings("rawtypes")
				Map.Entry pairs = (Map.Entry) it.next();
				SimpleDynamoProvider.matcursor.addRow(new Object[] {
						pairs.getKey(), pairs.getValue() });
				Log.v("provider query", "matcursor updated " + pairs.getValue());
				// it.remove(); // avoids a ConcurrentModificationException

			}
			SimpleDynamoProvider.tempflag = true;
			Log.v("Server query", "Cursor updated lock released");
		}
	}

	private void query(Message msg2) {
		// synchronized(SimpleDynamoProvider.sqlite)
		{
			if (msg2.Querytype.equalsIgnoreCase("basic")) {
				String[] columns = { "key", "value" };
				String selection = msg2.key;
				Message mCursor = new Message();
				mCursor.key = msg2.key;
				mCursor.value = "";

				Cursor cursor = SimpleDynamoProvider.sqlite.query("Msg",
						columns, "key = " + "'" + selection + "'", null, null,
						null, null);

				mCursor.destination = msg2.source;
				cursor.moveToNext();
				int key = cursor.getColumnIndex("key");
				int value = cursor.getColumnIndex("value");
				if (mCursor!=null && cursor.moveToFirst()) {
					mCursor.value = cursor.getString(value);
				} else
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				while (mCursor.value.equals(null)) {
					cursor = SimpleDynamoProvider.sqlite.query("Msg", columns,
							"key = " + "'" + selection + "'", null, null, null,
							null);

					mCursor.destination = msg2.source;
					cursor.moveToNext();
					key = cursor.getColumnIndex("key");
					value = cursor.getColumnIndex("value");
					mCursor.value = cursor.getString(value);
				}

				Log.v("Server query in", "Key :" + mCursor.key + " Value: "
						+ mCursor.value + " in "+ SimpleDynamoProvider.portStr);
				mCursor.type = "cursor";

				mCursor.hashmap.put(mCursor.key, mCursor.value);
				Log.v("Server Query", "Returning cursor");
				Runnable r = new Client(mCursor);
				Thread th = new Thread(r);
				th.start();

			} else if (msg2.Querytype.equalsIgnoreCase("all")) {
				
				String[] columns = { "key", "value" };
				String selection = msg2.key;
				
					try {
						Cursor cursor = SimpleDynamoProvider.sqlite.query(
								"Msg", columns, null, null, null, null, null);
						Message mCursor = new Message();
						mCursor.destination = msg2.source;
						while (cursor.moveToNext()) {
							int key = cursor.getColumnIndex("key");
							int value = cursor.getColumnIndex("value");
							mCursor.hashmap.put(cursor.getString(key),
									cursor.getString(value));
						}
						mCursor.type = "cursor";
						Log.v("Server Query", "Returning cursor");
						Runnable r = new Client(mCursor);
						Thread th = new Thread(r);
						th.start();
					} catch (Exception e) {
					}
				
			}
		}

	}

	private void insert(Message msg2) {
		long id = 0;

		synchronized (SimpleDynamoProvider.sqlite) {
			String k = msg2.key;
			String v = msg2.value;
			ContentValues cv = new ContentValues();
			cv.put("key", k);
			cv.put("value", v);
			Log.v("Insert", "Insert received at "
					+ SimpleDynamoProvider.portStr);
			Log.v("Insert", k + " =key " + v + " =value" + " State= "
					+ msg2.state);
			SimpleDynamoProvider.table.put(k, msg2);
			id = SimpleDynamoProvider.sqlite.insertWithOnConflict("Msg", null,
					cv, SQLiteDatabase.CONFLICT_REPLACE);
			// else Log.v("zhol", "Wrong val picked up");
			Log.v("Insertion finally done", Long.toString(id));
		}

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


}