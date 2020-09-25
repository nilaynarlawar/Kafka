package Demo1;

public class demo {
    public static void main(String[] args) {
        int[] arr = {4,5,2,3,8,1};
        bubbleSort(arr);
    }
    public static void bubbleSort(int[] arr){
        int length = arr.length;
        for(int i= 0 ; i < length; i++){
            for(int j=0;j<(length-i-1); j++){
                if(arr[j] < arr[j+1]){
                    int temp = arr[j];
                    arr[j] = arr[j+1];
                    arr[j+1] = temp;
                }
            }
        }
        for(int i=0;i<length;i++){
            System.out.println(arr[i]);
        }
    }
}
