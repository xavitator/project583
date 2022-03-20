package B.threads;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Vector;


public class Threads implements Runnable {

    public static final int NB_THREADS = 5;

    private int id;
    private Vector<Double> vector;

    public Threads(int id, Vector<Double> vector){
        this.id = id;
        this.vector = vector;
    }

    @Override
    public void run() {
        FileReader fr = null;
        BufferedReader bf = null;
        try {
            fr = new FileReader("data/edgelist" + id + ".txt"  );
            bf = new BufferedReader(fr);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        int line_nb = 0;
        if(id != 0) {
            Path path = Paths.get("data/edgelist" + (id-1) + ".txt");
            try {
                line_nb = (int)Files.lines(path).count();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        String line;
        try {
            while((line = bf.readLine()) !=null){

                String[] lineArray = line.split(" ");

                double sum = 0.0;
                for(String s : lineArray) {
                    sum += vector.get(Integer.parseInt(s));
                }
                vector.set(line_nb, sum);
                line_nb++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}