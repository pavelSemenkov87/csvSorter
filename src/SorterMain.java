import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

public class SorterMain {
    private static TreeMap<Integer, List<String>> sortedRows = new TreeMap<>();

    public static void main(String[] args) {
        String relation = "test"; // the code takes care of the .CSV
        externalSort(relation);
        System.out.println("The relation was sorted");
    }

    private static void externalSort(String relation) {
        try {
            FileReader relationInput = new FileReader(relation + ".csv");
            BufferedReader initRelationReader = new BufferedReader(relationInput);

            boolean haveLine = true;
            int numFiles = 0;
            while (haveLine) {
                // get 10k rows
                for (int i = 0; i < 10000; i++) {
                    String line = initRelationReader.readLine();
                    if (line == null) {
                        haveLine = false;
                        break;
                    }
                    sortedRows.computeIfAbsent(getKey(line), k -> new ArrayList<>()).add(line);
                }
                if (sortedRows.isEmpty()) break;

                // write to disk
                FileWriter fw = new FileWriter(relation + "_chunk" + numFiles + ".csv");
                BufferedWriter bw = new BufferedWriter(fw);
                sortedRows.values()
                        .forEach(rows ->
                                rows.forEach(r -> {
                                    try {
                                        bw.append(r).append("\n");
                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                }));
                bw.close();
                numFiles++;
                sortedRows.clear();
            }

            mergeFiles(relation, numFiles);


            initRelationReader.close();
            relationInput.close();

        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(-1);
        }


    }

    private static void mergeFiles(String relation, int countFiles) {
        List<ReaderFile> readersFile = new ArrayList<>();
        boolean someFileStillHasRows = false;

        try {
            FileWriter fw = new FileWriter(relation + "_sorted.csv");
            BufferedWriter bw = new BufferedWriter(fw);

            for (int i = 0; i < countFiles; i++) {
                readersFile.add(new ReaderFile(relation + "_chunk" + i + ".csv"));
                someFileStillHasRows = true;
            }
            int min = -1;
            int minIndex = 0;

            while (someFileStillHasRows) {
                min = readersFile.get(0).getKey();
                minIndex = 0;
                for (int i = 0; i < readersFile.size(); i++) {
                    ReaderFile reader = readersFile.get(i);
                    if (min > reader.getKey()) {
                        min = reader.getKey();
                        minIndex = i;
                    }
                }
                bw.append(readersFile.get(minIndex).getLine()).append("\n");
                if (!readersFile.get(minIndex).haveNextLine()) {
                    readersFile.get(minIndex).close();
                    readersFile.remove(minIndex);
                    if (readersFile.size() == 0) {
                        someFileStillHasRows = false;
                    }
                }
            }
            bw.close();
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(-1);
        }

    }

    private static class ReaderFile {
        private Integer key;
        private String line;
        private String fileName;
        private FileReader fileReader;
        private BufferedReader bufferedReader;

        public ReaderFile(String fileName) throws IOException {
            this.fileName = fileName;
            this.fileReader = new FileReader(fileName);
            this.bufferedReader = new BufferedReader(fileReader);
            this.line = bufferedReader.readLine();
            this.key = SorterMain.getKey(line);
        }

        public void close() throws IOException {
            bufferedReader.close();
        }

        public boolean haveNextLine() throws IOException {
            String newLine = bufferedReader.readLine();
            if (newLine != null) {
                line = newLine;
                key = SorterMain.getKey(line);
                return true;
            }
            return false;
        }

        public Integer getKey() {
            return key;
        }

        public void setKey(Integer key) {
            this.key = key;
        }

        public String getLine() {
            return line;
        }

        public void setLine(String line) {
            this.line = line;
        }
    }

    private static Integer getKey(String row) {
        return Integer.parseInt(row.substring(0, row.indexOf(",")));
    }
}
