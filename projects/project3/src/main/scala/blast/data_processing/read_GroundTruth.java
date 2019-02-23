package blast.data_processing;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.HashSet;
import java.lang.Math;
import DataStructures.IdDuplicates;


public class read_GroundTruth {
    private static HashSet<IdDuplicates> hashSet;
    public static HashSet<Integer> listofHashValues = new HashSet<Integer>();
    public static HashSet<IdDuplicates> get_the_original_hashset () {return hashSet;}


    public static HashSet<IdDuplicates> read_groundData(String path) {
        HashSet<IdDuplicates> duplicates=null ;
        try (ObjectInputStream ois_ground = new ObjectInputStream(new FileInputStream(path))) {
            duplicates = (HashSet<IdDuplicates>) (ois_ground.readObject());
        } catch (Exception e) {
            e.printStackTrace();
        }

        hashSet = duplicates;
       return duplicates;
}

public static HashSet<Integer> get_the_hashValues(){
        for (IdDuplicates dup : hashSet){
            listofHashValues.add((Integer) (dup.hashCode()));
        }
        return listofHashValues;
}
}
