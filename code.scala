val df1 = sc.textFile("Chess.csv")
//df1: org.apache.spark.rdd.RDD[String] = Chess.csv MapPartitionsRDD[1] at textFile at <console>:24

val df_1 = df1.map(_.replaceAll("[^0-9A-Za-z,/\\s]*",""))
//df_1: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[2] at map at <console>:26

val data = df_1.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
//data: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[3] at mapPartitionsWithIndex at <console>:28

data.take(5).foreach(println)
/*1,Carlsen,2862,2021 Jan,30
2,Caruana,2823,2021 Jan,28
3,Ding Liren,2791,2021 Jan,28
4,Nepomniachtchi,2789,2021 Jan,30
5,VachierLagrave,2784,2021 Jan,30*/

val chess_rdd=data.map{l=>
     |     val s=l.split(",")
     |     val (position,name,elo,date,age)=(s(0).toInt,s(1),s(2).toInt,s(3),s(4).toInt)
     |     (position,name,elo,date,age)}
//chess_rdd: org.apache.spark.rdd.RDD[(Int, String, Int, String, Int)] = MapPartitionsRDD[4] at map at <console>:30

chess_rdd.toDF("position","name","elo","date","age").show()
/*
+--------+---------------+----+--------+---+
|position|           name| elo|    date|age|
+--------+---------------+----+--------+---+
|       1|        Carlsen|2862|2021 Jan| 30|
|       2|        Caruana|2823|2021 Jan| 28|
|       3|     Ding Liren|2791|2021 Jan| 28|
|       4| Nepomniachtchi|2789|2021 Jan| 30|
|       5| VachierLagrave|2784|2021 Jan| 30|
|       6|        Aronian|2781|2021 Jan| 38|
|       7|       Grischuk|2777|2021 Jan| 37|
|       8|     Mamedyarov|2770|2021 Jan| 35|
|       9|             So|2770|2021 Jan| 27|
|      10|       Radjabov|2765|2021 Jan| 33|
|      11|           Giri|2764|2021 Jan| 26|
|      12|       Wang Hao|2763|2021 Jan| 31|
|      13|        Rapport|2759|2021 Jan| 24|
|      14|Dominguez Perez|2758|2021 Jan| 37|
|      15|       Karjakin|2757|2021 Jan| 30|
|      16|          Anand|2753|2021 Jan| 51|
|      17|        Kramnik|2753|2021 Jan| 45|
|      18|       Firouzja|2749|2021 Jan| 17|
|      19|           Duda|2743|2021 Jan| 22|
|      20|       Nakamura|2736|2021 Jan| 33|
+--------+---------------+----+--------+---+
only showing top 20 rows


val age = chess_rdd.filter{ case(position,name,elo,date,age)=>(position == 1 || position == 2 || position == 3 || position == 4 || position == 5)}
age: org.apache.spark.rdd.RDD[(Int, String, Int, String, Int)] = MapPartitionsRDD[8] at filter at <console>:32

age.toDF("position","name","elo","date","age").show()
+--------+--------------+----+--------+---+
|position|          name| elo|    date|age|
+--------+--------------+----+--------+---+
|       1|       Carlsen|2862|2021 Jan| 30|
|       2|       Caruana|2823|2021 Jan| 28|
|       3|    Ding Liren|2791|2021 Jan| 28|
|       4|Nepomniachtchi|2789|2021 Jan| 30|
|       5|VachierLagrave|2784|2021 Jan| 30|
|       1|       Carlsen|2872|2020 Jan| 29|
|       2|       Caruana|2822|2020 Jan| 27|
|       3|    Ding Liren|2805|2020 Jan| 27|
|       4|      Grischuk|2777|2020 Jan| 36|
|       5|Nepomniachtchi|2774|2020 Jan| 29|
|       1|       Carlsen|2835|2019 Jan| 28|
|       2|       Caruana|2828|2019 Jan| 26|
|       3|    Mamedyarov|2817|2019 Jan| 33|
|       4|    Ding Liren|2813|2019 Jan| 26|
|       5|          Giri|2783|2019 Jan| 24|
|       1|       Carlsen|2834|2018 Jan| 27|
|       2|       Caruana|2811|2018 Jan| 25|
|       3|    Mamedyarov|2804|2018 Jan| 32|
|       4|       Aronian|2797|2018 Jan| 35|
|       5|VachierLagrave|2793|2018 Jan| 27|
+--------+--------------+----+--------+---+ */
only showing top 20 rows


val age = chess_rdd.filter{ case(position,name,elo,date,age)=>(name == "Carlsen")}
//age: org.apache.spark.rdd.RDD[(Int, String, Int, String, Int)] = MapPartitionsRDD[12] at filter at <console>:32

age.toDF("position","name","elo","date","age").show()
/*
+--------+-------+----+--------+---+
|position|   name| elo|    date|age|
+--------+-------+----+--------+---+
|       1|Carlsen|2862|2021 Jan| 30|
|       1|Carlsen|2872|2020 Jan| 29|
|       1|Carlsen|2835|2019 Jan| 28|
|       1|Carlsen|2834|2018 Jan| 27|
|       1|Carlsen|2840|2017 Jan| 26|
|       1|Carlsen|2844|2016 Jan| 25|
|       1|Carlsen|2862|2015 Jan| 24|
|       1|Carlsen|2872|2014 Jan| 23|
|       1|Carlsen|2861|2013 Jan| 22|
|       1|Carlsen|2835|2012 Jan| 21|
|       1|Carlsen|2814|2011 Jan| 20|
|       1|Carlsen|2810|2010 Jan| 19|
|       4|Carlsen|2776|2009 Jan| 18|
|      13|Carlsen|2733|2008 Jan| 17|
+--------+-------+----+--------+---+


val chess_rdd_new=chess_rdd.map{
     |     case (position,name,elo,date,age)=>(name,elo,date)
     |     }
chess_rdd_new: org.apache.spark.rdd.RDD[(String, Int, String)] = MapPartitionsRDD[16] at map at <console>:32

chess_rdd_new.toDF("name","elo","date").show()
+---------------+----+--------+
|           name| elo|    date|
+---------------+----+--------+
|        Carlsen|2862|2021 Jan|
|        Caruana|2823|2021 Jan|
|     Ding Liren|2791|2021 Jan|
| Nepomniachtchi|2789|2021 Jan|
| VachierLagrave|2784|2021 Jan|
|        Aronian|2781|2021 Jan|
|       Grischuk|2777|2021 Jan|
|     Mamedyarov|2770|2021 Jan|
|             So|2770|2021 Jan|
|       Radjabov|2765|2021 Jan|
|           Giri|2764|2021 Jan|
|       Wang Hao|2763|2021 Jan|
|        Rapport|2759|2021 Jan|
|Dominguez Perez|2758|2021 Jan|
|       Karjakin|2757|2021 Jan|
|          Anand|2753|2021 Jan|
|        Kramnik|2753|2021 Jan|
|       Firouzja|2749|2021 Jan|
|           Duda|2743|2021 Jan|
|       Nakamura|2736|2021 Jan|
+---------------+----+--------+ */
only showing top 20 rows


val maxKey = chess_rdd_new.takeOrdered(2)(Ordering[Int].reverse.on(_._2))
//maxKey: Array[(String, Int, String)] = Array((Carlsen,2872,2020 Jan), (Carlsen,2872,2014 Jan))

val minKey = chess_rdd_new.takeOrdered(1)(Ordering[Int].on(_._2))
//minKey: Array[(String, Int, String)] = Array((Svidler,2672,2000 Jan))

val chess_rdd_new_1=chess_rdd.map{
     |     case (position,name,elo,date,age)=>(name,age,date)
     |     }
//chess_rdd_new_1: org.apache.spark.rdd.RDD[(String, Int, String)] = MapPartitionsRDD[22] at map at <console>:34

val maxKey = chess_rdd_new_1.takeOrdered(2)(Ordering[Int].reverse.on(_._2))
//maxKey: Array[(String, Int, String)] = Array((Karpov,51,2003 Jan), (Anand,51,2021 Jan))

val minKey = chess_rdd_new_1.takeOrdered(2)(Ordering[Int].on(_._2))
//minKey: Array[(String, Int, String)] = Array((Carlsen,17,2008 Jan), (Karjakin,17,2008 Jan))

val age_avg = chess_rdd.map{
     |     case (position,name,elo,date,age)=>(age)
     |     }
//age_avg: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[25] at map at <console>:34

val age_mean = age_avg.sum()/age_avg.count()
age_mean: Double = 30.25227272727273

 val elo_avg = chess_rdd.map{
     |     case (position,name,elo,date,age)=>(elo)
     |     }
//elo_avg: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[27] at map at <console>:32

 val elo_mean = elo_avg.sum()/elo_avg.count()
//elo_mean: Double = 2751.0636363636363

val elo_info = chess_rdd.map{
     |     case (position,name,elo,date,age)=>(elo,name,date)
     |     }
//elo_info: org.apache.spark.rdd.RDD[(Int, String, String)] = MapPartitionsRDD[29] at map at <console>:32

elo_info.filter(x=>x._1==2781).toDF("elo","name","date").show()
/*
+----+--------+--------+
| elo|    name|    date|
+----+--------+--------+
|2781| Aronian|2021 Jan|
|2781|Nakamura|2018 Jan|
|2781| Caruana|2013 Jan|
|2781| Aronian|2010 Jan|
+----+--------+--------+ */