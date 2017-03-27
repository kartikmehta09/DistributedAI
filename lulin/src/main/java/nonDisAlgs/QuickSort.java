package nonDisAlgs;

/**
 * Created by Administrator on 2017/3/25.
 */
public class QuickSort {

//    private  <T extends Comparable<? super T>>;

//    private <T extends Comparable<? super T>>;

    public static void QSort(int[] data, int left, int right) {
        int i = left, j = right;
        int key = data[i];

        while (i < j) {
            while (i < j && data[j] >= key) j--;
            if(i < j) {
                data[i] = data[j];
                i++;
            }

            while (i < j && data[i] <= key) i++;
            if(i < j) {
                data[j] = data[i];
                j--;
            }
        }
        // i == j
        data[i] = key;

        if(i > left) {
            QSort(data, left, i-1);
        }

        if(j < right) QSort(data, j+1, right);

    }

    public static void main(String[] args) {
        int[] test = new int[]{3,4,2,1,5,6};
        QSort(test, 0, test.length-1);

        for(int i =0; i<test.length;i++) System.out.println(test[i] + " ");

    }

}
