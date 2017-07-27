import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Part3 {

    public static void main(String[] args) {

        ////////////
        // INPUT:
        ///////////

        Scanner reader = new Scanner (System.in);
        System.out.println("Enter folder of txt files:");
        File fileLocation = new File(""+reader.next());
        System.out.println("Enter number of threads:");
        int inputThreads = Integer.parseInt(reader.next());
        Map<String, String> input = new HashMap<String, String>();
        String wordString= "";
        for(File file : fileLocation.listFiles()){
            try{
                BufferedReader br = new BufferedReader(new FileReader(file));
                String line= "";
                int i = 0;
                while (line != null){
                    try{
                        line = br.readLine();
                        wordString +=line;
                        i++;
                    }catch(IOException e){
                        e.printStackTrace();
                    }
                }
                input.put(file.toString(), wordString);
            }catch(FileNotFoundException e){
                e.printStackTrace();
            }
            input.put(file.toString(), wordString);
        }
//------------------------------------------------------------------------------------------------------------------------------
        System.out.println();
        System.out.println("APPROACH 1");
        System.out.println();

        long startTime = System.nanoTime();


        // APPROACH #1: Brute force
        {
            Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();

            Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
            while(inputIter.hasNext()) {
                Map.Entry<String, String> entry = inputIter.next();
                String file = entry.getKey();
                String contents = entry.getValue();

                String[] words = contents.trim().split("\\s+");

                for(String word : words) {

                    Map<String, Integer> files = output.get(word);
                    if (files == null) {
                        files = new HashMap<String, Integer>();
                        output.put(word, files);
                    }

                    Integer occurrences = files.remove(file);
                    if (occurrences == null) {
                        files.put(file, 1);
                    } else {
                        files.put(file, occurrences.intValue() + 1);
                    }
                }
            }

            // show me:
            //System.out.println(output);

        }

        long stopTime1 = System.nanoTime();
        long timer = stopTime1 - startTime;
        System.out.printf("Time time elapsed for approach 1 in nanoseconds: %d\n", timer);
//------------------------------------------------------------------------------------------------------------------------------
        System.out.println();
        System.out.println("APPROACH 2");
        System.out.println();

        // APPROACH #2: MapReduce
        {
            Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();

            // MAP:

            List<MappedItem> mappedItems = new LinkedList<MappedItem>();

            Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
            while(inputIter.hasNext()) {
                Map.Entry<String, String> entry = inputIter.next();
                String file = entry.getKey();
                String contents = entry.getValue();

                map(file, contents, mappedItems);
            }

            // GROUP:

            Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();

            Iterator<MappedItem> mappedIter = mappedItems.iterator();
            while(mappedIter.hasNext()) {
                MappedItem item = mappedIter.next();
                String word = item.getWord();
                String file = item.getFile();
                List<String> list = groupedItems.get(word);
                if (list == null) {
                    list = new LinkedList<String>();
                    groupedItems.put(word, list);
                }
                list.add(file);
            }

            // REDUCE:

            Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
            while(groupedIter.hasNext()) {
                Map.Entry<String, List<String>> entry = groupedIter.next();
                String word = entry.getKey();
                List<String> list = entry.getValue();

                reduce(word, list, output);
            }

            //System.out.println(output);
        }

        long stopTime2 = System.nanoTime();
        timer = stopTime2 - stopTime1;
        System.out.printf("Time time elapsed for approach 2 in nanoseconds: %d\n", timer);
//------------------------------------------------------------------------------------------------------------------------------
        System.out.println();
        System.out.println("APPROACH 3");
        System.out.println();


        // APPROACH #3: Distributed MapReduce
        {
            // MAP:
            final List<MappedItem> mappedItems = new CopyOnWriteArrayList<MappedItem>();

            Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();


            ExecutorService mapThreadPool = Executors.newFixedThreadPool(inputThreads);

            while(inputIter.hasNext()) {
                Map.Entry<String, String> entry = inputIter.next();
                final String file = entry.getKey();
                final String contents = entry.getValue();

                mapThreadPool.execute( new Thread(new Runnable() {
                    @Override
                    public void run() {
                        map(file, contents, mappedItems);
                    }
                }));

            }

            mapThreadPool.shutdown();
            while(!mapThreadPool.isTerminated()){}

            // wait for mapping phase to be over:
           /* for(Thread t : mapCluster) {
                try {
                    t.join();
                } catch(InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }*/

            // GROUP:

            Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();

            Iterator<MappedItem> mappedIter = mappedItems.iterator();
            while(mappedIter.hasNext()) {
                MappedItem item = mappedIter.next();
                String word = item.getWord();
                String file = item.getFile();
                List<String> list = groupedItems.get(word);
                if (list == null) {
                    list = new LinkedList<String>();
                    groupedItems.put(word, list);
                }
                list.add(file);
            }

            // REDUCE:
            final Map<String, Map<String, Integer>> output = new ConcurrentHashMap<String, Map<String, Integer>>();

            Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();

            ExecutorService reduceThreadPool = Executors.newFixedThreadPool(inputThreads);

            while(groupedIter.hasNext()) {
                Map.Entry<String, List<String>> entry = groupedIter.next();
                final String word = entry.getKey();
                final List<String> list = entry.getValue();

                reduceThreadPool.execute(new Thread(new Runnable() {
                    @Override
                    public void run() {
                        reduce(word, list, output);
                    }
                }));
            }


            reduceThreadPool.shutdown();

            while(!reduceThreadPool.isTerminated()){}

            // wait for reducing phase to be over:
           /* for(Thread t : reduceCluster) {
                try {
                    t.join();
                } catch(InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }*/

            //System.out.println(output);
        }

        long stopTime3 = System.nanoTime();
        timer = stopTime3 - stopTime2;
        System.out.printf("Time time elapsed for approach 3 in nanoseconds: %d\n", timer);
//------------------------------------------------------------------------------------------------------------------------------
    }

    public static void map(String file, String contents, List<MappedItem> mappedItems) {
        String[] words = contents.trim().split("\\s+");
        for(String word: words) {
            mappedItems.add(new MappedItem(word, file));
        }
    }

    public static void reduce(String word, List<String> list, Map<String, Map<String, Integer>> output) {
        Map<String, Integer> reducedList = new HashMap<String, Integer>();
        for(String file: list) {
            Integer occurrences = reducedList.get(file);
            if (occurrences == null) {
                reducedList.put(file, 1);
            } else {
                reducedList.put(file, occurrences.intValue() + 1);
            }
        }
        output.put(word, reducedList);
    }

    public static void map(String file, String contents, CopyOnWriteArrayList<MappedItem> mappeditems) {
        String[] words = contents.trim().split("\\s+");
        List<MappedItem> results = new ArrayList<MappedItem>(words.length);
        for(String word: words) {
            results.add(new MappedItem(word, file));
        }
        mappeditems.addAll(results);
    }

    public static void reduce(String word, List<String> list, ConcurrentHashMap<String, Map<String, Integer>> output) {

        Map<String, Integer> reducedList = new HashMap<String, Integer>();
        for(String file: list) {
            Integer occurrences = reducedList.get(file);
            if (occurrences == null) {
                reducedList.put(file, 1);
            } else {
                reducedList.put(file, occurrences.intValue() + 1);
            }
        }
        output.put(word, reducedList);
    }

    private static class MappedItem {

        private final String word;
        private final String file;

        public MappedItem(String word, String file) {
            this.word = word;
            this.file = file;
        }

        public String getWord() {
            return word;
        }

        public String getFile() {
            return file;
        }

        @Override
        public String toString() {
            return "[\"" + word + "\",\"" + file + "\"]";
        }
    }
} 
