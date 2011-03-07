package org.apache.hadoop.mapred;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class HistoryIndexManager {

	public static class Document {
		public String jobID;
		public String jobName;
		public String user;
		public String group;
		public String date;
		public String status;

		public Document(String jobID, String user, String group, String date, String status) {
			this.jobID = jobID;
			this.user = user;
			this.group = group;
			this.date = date;
			this.status = status;
			jobName = jobID.substring(jobID.lastIndexOf('_')+1);
			jobName = jobName.replaceAll("\\+", " ");
		}
	}

	private static String indexFile;
	private static Map<String, List<Document>> jobIDAndJobMap;
	private static Map<String, List<Document>> jobNameAndJobMap;
	private static Map<String, List<Document>> userAndJobMap;
	private static Map<String, List<Document>> groupAndJobMap;
	private static Map<String, List<Document>> dateAndJobMap;
	private static Map<String, List<Document>> statusAndJobMap;
	private static List<Document> docList = null;
	private static int numSaved;

	public HistoryIndexManager(String file) {
		indexFile = file;
		jobIDAndJobMap = new HashMap<String, List<Document>>();
		jobNameAndJobMap = new HashMap<String, List<Document>>();
		userAndJobMap = new HashMap<String, List<Document>>();
		groupAndJobMap = new HashMap<String, List<Document>>();
		dateAndJobMap = new HashMap<String, List<Document>>();
		statusAndJobMap = new HashMap<String, List<Document>>();
		docList = new LinkedList<Document>();
		numSaved = 0;
	}

	public boolean load() {
		synchronized (this) {
			try {
				BufferedReader br = new BufferedReader(
						new FileReader(indexFile));
				String line;
				while (true) {
					line = br.readLine();
					if (line == null)
						break;
					Document doc = stringToDoc(line);
					innerAdd(doc);
				}
				numSaved = docList.size();
				return true;
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				return false;
			} catch (IOException e) {
				e.printStackTrace();
				return false;
			}
		}
	}

	public boolean add(Document doc) {
		synchronized (this) {
			return innerAdd(doc);
		}
	}

	private boolean innerAdd(Document doc) {
		updateIndex(doc, doc.jobID, jobIDAndJobMap);
		updateIndex(doc, doc.jobName, jobNameAndJobMap);
		updateIndex(doc, doc.user, userAndJobMap);
		updateIndex(doc, doc.group, groupAndJobMap);
		updateIndex(doc, doc.date.split(" ")[0], dateAndJobMap);
		updateIndex(doc, doc.status, statusAndJobMap);
		docList.add(0, doc);
		return true;
	}

	public synchronized boolean save() {
		synchronized (this) {
			int numUnSaved = docList.size() - numSaved;
			if (numUnSaved <= 0)
				return true;
			try {
				BufferedWriter bw = new BufferedWriter(new FileWriter(
						indexFile, true));
				List<Document> listToSave = docList.subList(0, numUnSaved);
				for (int i = numUnSaved - 1; i >= 0; i--) {
					bw.write(docToString(listToSave.get(i)) + "\n");
				}
				numSaved = docList.size();
				bw.flush();
				bw.close();
				return true;
			} catch (IOException e) {
				e.printStackTrace();
				return false;
			}
		}
	}

    public List<Document> search(String user, String group, String date,
            String status, String jobId, String jobName) {

        user = user.trim();
        group = group.trim();
        date = date.trim();
        status = status.trim();
        jobId = jobId.trim();
        jobName = jobName.trim();
        
        if(jobId.isEmpty() && jobName.isEmpty() && user.isEmpty()
                && group.isEmpty() && date.isEmpty() && status.isEmpty()) {
            return docList;
        }

        List<Document> result = null;

        if (jobId != null && !jobId.isEmpty()) {
            result = parse(jobId, jobIDAndJobMap, result);
        }
        
        if (jobName != null && !jobName.isEmpty()) {
            result = parse(jobName, jobNameAndJobMap, result);
        }

        if (user != null && !user.isEmpty()) {
            result = parse(user, userAndJobMap, result);
        }

        if (group != null && !group.isEmpty()) {
            result = parse(group, groupAndJobMap, result);
        }

        if (date != null && !date.isEmpty()) {
            result = parse(date, dateAndJobMap, result);
        }

        if (status != null && !status.isEmpty()) {
            result = parse(status, statusAndJobMap, result);
        }

        return result;
    }


	public Document stringToDoc(String line) {
		String[] var = line.split("\t");
		return new Document(var[0], var[1], var[2], var[3], var[4]); 
	}

	public String docToString(Document doc) {
		String s = doc.jobID + "\t" + doc.user + "\t" + doc.group + "\t"
				+ doc.date + "\t" + doc.status;
		return s;
	}

	private void updateIndex(Document jobInfo, String key,
			Map<String, List<Document>> map) {
		if (!map.containsKey(key)) {
			List<Document> docList = new LinkedList<Document>();
			docList.add(jobInfo);
			map.put(key, docList);
		} else {
			List<Document> docList = map.get(key);
			docList.add(0, jobInfo);
			map.put(key, docList);
		}
	}
	
	/*public static void main(String[] args) throws IOException {
		if (args.length == 2 && args[0].equalsIgnoreCase("build")) {
			HistoryIndexManager him = new HistoryIndexManager(args[1]);
			if (!him.load()) {
				him.save();
			}
			BufferedReader br = new BufferedReader(new InputStreamReader(
					System.in));
			String line;
			while ((line = br.readLine()) != null) {
				him.add(him.stringToDoc(line));
			}
			him.save();
			br.close();
		} else if (args.length == 6 && args[0].equalsIgnoreCase("search")) {
			HistoryIndexManager him = new HistoryIndexManager(args[1]);
			him.load();
			List<Document> docList = him.search(args[2], args[3], args[4], args[5]);
			for (int i = 0; i < docList.size(); i++) {
				System.out.println(him.docToString(docList.get(i)));
			}
		} else {
			System.out.println("Usage: HistoryIndexManager build file\n"
					+ "       HistoryIndexManager search file user group date status");
		}
	}*/
	
    private List<Document> fuzzyParse(String content,
            Map<String, List<Document>> map) {
        List<Document> result = new LinkedList<Document>();
        Iterator<String> keyIter = map.keySet().iterator();
        content = content.trim().toLowerCase();
        while (keyIter.hasNext()) {
            String tmp = keyIter.next();
            if (tmp.toLowerCase().contains(content))
                result = mergeList(result, map.get(tmp));
        }
        return result;
    }
    
    private List<Document> mergeList(List<Document> result, List<Document> append) {
        if(result.size() == 0) {
            for(Document doc : append)
              result.add(doc);
            return result;
        }
        if(append.size() == 0)
            return result;
        
        for(Document doc : append) {
            if(result.indexOf(doc) == -1) {
                result.add(doc);
            }
        }
        return result;
    }

    private List<Document> parse(String content, Map<String, List<Document>> map,
            List<Document> result) {
        List<Document> tmpList = new LinkedList<Document>();
        String contents[] = content.split("\\s+");
        
        for (String ct : contents)
            tmpList = mergeList(tmpList, fuzzyParse(ct, map));

        if(result != null) 
            result.retainAll(tmpList);
        else
            result = tmpList;
        
        return result;
    }

}
