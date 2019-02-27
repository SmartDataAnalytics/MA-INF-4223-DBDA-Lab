package DataStructures;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import java.util.Set;
import java.util.List;
import scala.collection.JavaConversions;
import scala.collection.Seq;

/**
 * Classes obtained from: http://sourceforge.net/projects/erframework/files/CleanCleanERDatasets/
 *
 * Modified
 */


public class DatasetReader {


    public static Seq<EntityProfile> readDataset(String path) {
        ArrayList<EntityProfile> read_data=null;
        Object temp = null;

        try {
            ObjectInputStream ois = new ObjectInputStream(
                    new FileInputStream(path));
            temp = ois.readObject();
            read_data = (ArrayList<EntityProfile>) (temp);
            // updating entity urls so they include the index of the entity in the source file.
            // importing for validating
            for(int i = 0; i < read_data.size(); i++) {
                String new_id = String.valueOf(i) + ":" + read_data.get(i).getEntityUrl();
                read_data.get(i).setEntityUrl(new_id);
            }
        }  catch (IOException e) {
        e.printStackTrace();
        }
        catch (ClassNotFoundException e){
        e.printStackTrace();
        }
        //checkAttr(read_data);
        return JavaConversions.asScalaBuffer(read_data).toList();
        //return read_data;
    }



}