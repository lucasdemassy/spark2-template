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
}
