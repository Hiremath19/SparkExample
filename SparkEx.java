import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.*;

public class SparkEx
{

    public static void main(String args[])
    {
        SparkConf conf=new SparkConf().setMaster("local").setAppName("sparktraining");
        JavaSparkContext sc= new JavaSparkContext(conf);

        JavaRDD<Integer> rd1 =sc.parallelize(Arrays.asList(1,2,2,2,3,4,5,6,1,4,3));

        System.out.println(rd1.collect());

        System.out.println("Distinct Transformation");

        System.out.println(rd1.distinct().collect());


        JavaRDD<Integer> rdd2 =rd1.map(new Function<Integer,Integer>() {
                                           @Override
                                           public Integer call(Integer x) throws Exception {
                                               return x * x * x;
                                           }


                                       });
        System.out.println(rdd2.collect());
        JavaRDD<Integer> oddRDD=rd1.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer i) throws Exception {
                return (i>3);
            }
        });

        JavaRDD<String> strDD = sc.parallelize(Arrays.asList("Welcome to sankir Technology","Hello India"));
       ArrayList<String> wordsList =new ArrayList<>();
       JavaRDD<String> strDD2=strDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] words= s.split(",");
                ArrayList<String> strList = new ArrayList<>();
                for (String word : words)
                {
                    strList.add(word);
                }
                return strList.iterator();

            }
        });
        System.out.println(strDD2.collect());

        System.out.println("Distinct on a RDD:");
        System.out.println(strDD2.collect());
        System.out.println(oddRDD.collect());
        Scanner scanner=new Scanner(System.in);
        System.out.println("Enter q to Quit:");
        String s= scanner.next();
    }
}
