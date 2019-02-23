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


public class DatasetReader {


    public static Seq<EntityProfile> readDataset(String path) {
        ArrayList<EntityProfile> read_data=null;
        Object temp = null;

        try {
            ObjectInputStream ois = new ObjectInputStream(
                    new FileInputStream(path));
            temp = ois.readObject();
            read_data = (ArrayList<EntityProfile>) (temp);
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


    public static void  checkAttr(List<EntityProfile> l){
        Set<String> attr = new  HashSet();

        //getting attribute names from first entity
        for(Attribute at : l.get(0).getAttributes()){
            attr.add(at.getName());
            System.out.println("attr:"+at.getName());
        }


        for(EntityProfile ep: l){
            Set<String> attr2 = new  HashSet();
            Set<String> attr2Val = new  HashSet();

            //getting attribute names from first entity
            for( Attribute at : ep.getAttributes()){
                attr2.add(at.getName());
                attr2Val.add(at.getValue());
            }
            if(attr.equals(attr2) == false){
                System.out.println("ERORR");
                System.out.println(attr.toString() + " "+ attr2.toString() + " " + attr2Val.toString());
                System.out.println(ep.getEntityUrl());
            }
        }

    }


    public static List<EntityProfile> read(String d1, String d2, String grnd){
        ArrayList<EntityProfile> read_data;
        ArrayList<EntityProfile> read_data2;
        HashSet<IdDuplicates> duplicates;
        Integer count = 4;
        Object tempOBJ = null;
        Object tempOBJ2 = null;
        Object grTruth = null;

        try {
            ObjectInputStream ois = new ObjectInputStream(
                    new FileInputStream(d1));
            tempOBJ = ois.readObject();

            ObjectInputStream ois2 = new ObjectInputStream(
                    new FileInputStream(d2));
            tempOBJ2 = ois2.readObject();

            ObjectInputStream ois3 = new ObjectInputStream(
                    new FileInputStream(grnd));
            grTruth = ois3.readObject();

            System.out.println("object read");
            read_data = (ArrayList<EntityProfile>) (tempOBJ);
            read_data2 = (ArrayList<EntityProfile>) (tempOBJ2);
            duplicates = (HashSet<IdDuplicates>) grTruth;

            System.out.println("dblp:"+read_data.size()+"\tacm:"+read_data2.size()+"\tDuplicates:"+duplicates.size());

            System.out.println("DBLP");
            for ( Attribute a:read_data.get(1).getAttributes()) {
                System.out.println(a.getName()+":"+a.getValue());
            }
            System.out.println("acm");
            for (Attribute a:read_data2.get(1).getAttributes()) {
                System.out.println(a.getName()+":"+a.getValue());
            }
            Iterator<IdDuplicates> iterate_Dup = duplicates.iterator();
            IdDuplicates example_duplicate = iterate_Dup.next();
            System.err.println("Ground Truth");
            System.out.println("ID1:"+example_duplicate.getEntityId1()+"\tID2:"+example_duplicate.getEntityId2()
                    +"\tHASH:"+example_duplicate.hashCode());


        } catch (IOException e) {
            e.printStackTrace();
        }
        catch (ClassNotFoundException e){
            e.printStackTrace();
        }

        return null;
    }
}