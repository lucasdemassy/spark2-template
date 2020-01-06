package sql_practice

import org.apache.spark.sql.functions._
import spark_helpers.SessionBuilder

object examples {
  def exec1(): Unit ={
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")
    toursDF.show

    println(toursDF
      .select(explode($"tourTags"))
      .groupBy("col")
      .count()
      .count()
    )

    toursDF
      .select(explode($"tourTags"), $"tourDifficulty")
      .groupBy($"col", $"tourDifficulty")
      .count()
      .orderBy($"count".desc)
      .show(10)

    toursDF.select($"tourPrice")
      .filter($"tourPrice" > 500)
      .orderBy($"tourPrice".desc)
      .show(20)


  }

  def exec2(): Unit ={
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val demography = spark.read
      .option("multiline", false)
      .option("mode", "PERMISSIVE")
      .json("data/input/demographie_par_commune.json")

    println("How many inhabitants has France ?")
    demography.agg(sum($"population")).show

    println("What are the top highly populated departments in France ? (Just a code name)")
    demography.groupBy($"departement").agg(sum($"population")).orderBy($"sum(population)".desc).show

    val departement = spark.read
      .csv("data/input/departements.txt")
      .select($"_c0".as("Nom"), $"_c1".as("Code"))

    println("What are the top highly populated departments in France ?  (use Join to dispaly the names)")
    demography.groupBy($"departement").agg(sum($"population")).join(departement, $"departement"===$"Code", "inner").orderBy($"sum(population)".desc).select($"Nom", $"departement", $"sum(population)").show

  }

  def exec3(): Unit ={
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val sample_07 = spark.read
      .option("delimiter", "\t")
      .option("header", false)
      .csv("data/input/sample_07")
      .select($"_c0".as("Code"), $"_c1".as("Description"), $"_c2".as("total_emp").cast("int"), $"_c3".as("Salary").cast("int"))

    val sample_08 = spark.read
      .option("delimiter", "\t")
      .option("header", false)
      .csv("data/input/sample_08")
      .select($"_c0".as("Code"), $"_c1".as("Description"), $"_c2".as("total_emp").cast("int"), $"_c3".as("Salary").cast("int"))


    println("Top salaries in 2007 which are above $100k")
    sample_07.where($"Salary">=100000).orderBy($"Salary".desc).show(100, false)

    val jointure = sample_07.join(sample_08, Seq("Code"))
    println("Salary growth (sorted) from 2007 to 2008")
    jointure.select($"Code",
      sample_07("Description"),
      sample_07("Salary").as("2007 salary"),
      sample_08("Salary").as("2008 salary"),
      (sample_08("Salary")-sample_07("Salary")).cast("int").as("growth"))
      .orderBy($"growth".desc).show

    println("Job evolution from 2007 to 2008 among $100k salaries in 2007")
    jointure.where(sample_07("Salary")>=100000).select($"Code",
      sample_07("Description"),
      sample_07("Salary").as("2007 salary"),
      sample_07("total_emp").as("2007 employees"),
      sample_08("total_emp").as("2008 employees"),
      (sample_08("total_emp")-sample_07("total_emp")).cast("int").as("Job evolution"))
      .orderBy($"Job evolution".asc).show(100, false)
  }

  def exec4(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")
    toursDF.show

    println("How many unique levels of difficulties ? " + toursDF.select($"tourDifficulty").distinct().count)
    toursDF.select($"tourDifficulty").distinct().show

    println("What is the min/avg/max of tour prices ?")
    toursDF.select(min($"tourPrice"), avg($"tourPrice"), max($"tourPrice")).show

    println("What is the min/avg/max of price of each level of difficulty ?")
    toursDF.groupBy($"tourDifficulty").agg(min($"tourPrice"), avg($"tourPrice"), max($"tourPrice")).show

    println("What is the min/avg/max of price and min/avg/max of duration for each level of difficulty ?")
    toursDF.groupBy($"tourDifficulty").agg(min($"tourPrice"), avg($"tourPrice"), max($"tourPrice"), min($"tourLength"), avg($"tourLength"), max($"tourLength")).show

    println("Top 10 'tourTags'")
    toursDF.select(explode($"tourTags")).groupBy($"col").count().orderBy($"count".desc).show(10)

    println("Relationship between the top 10 'tourTags' and 'tourDifficulty'")
    toursDF
      .select(explode($"tourTags"), $"tourDifficulty")
      .groupBy($"col", $"tourDifficulty")
      .count()
      .orderBy($"count".desc)
      .show(10)

    println("What is the min/avg/max of price in 'tourTags' and 'tourDifficulty' relationship ? (sorted by average)")
    toursDF
      .select(explode($"tourTags"), $"tourDifficulty", $"tourPrice")
      .groupBy($"col", $"tourDifficulty")
      .agg(min($"tourPrice"), avg($"tourPrice"), max($"tourPrice"))
      .orderBy($"avg(tourPrice)".desc)
      .show

  }
}
