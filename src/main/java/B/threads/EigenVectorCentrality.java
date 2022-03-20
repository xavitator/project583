package B.threads;
import java.util.Collections;
import java.util.Vector;

public class EigenVectorCentrality {

    public static void main(String[] args) throws InterruptedException {

        final int size = 64375;
        final int nb_threads = 5;
        Vector<Double> vector = new Vector<Double>();

        for(int i = 0; i < size; i++){
            vector.add(1.0/size);
        }

        Thread[] threads = new Thread[nb_threads];

        int converge = 0;
        while (converge < 500) {
            for(int i = 0; i < nb_threads; i++) {
                threads[i] = new Thread(new Threads(i, vector));
                threads[i].start();
            }
            for(int i = 0; i < nb_threads; i++) {
                threads[i].join();
            }
            double norm = 0;
            for(double i : vector) {
                norm+=i*i;
            }
            norm = Math.sqrt(norm);
            for(int i = 0; i < vector.size(); i++) {
                vector.set(i, vector.get(i)/norm);
            }
            converge++;
        }

        System.out.println("The index of the maximum of the collection is : "+vector.indexOf(Collections.max(vector)));
        System.out.println("The maximum is : "+ Collections.max(vector));
    }
}