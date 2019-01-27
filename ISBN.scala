import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkConf
object ISBN {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("ISBN decoder")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    // function to validate it the number is a valid isbn number
    def validateISBN(n:String): Boolean ={
      var i = 0
      var ss = 0
      // check if its a 13 digit number and multiple digits with 1 and 3 alternatively and take the sum
      // divide this dum by 10 and if the reminder is 0 then its a valid isbn
      if (n.length()==13) {
        for (a <- n){
          if(i == 0) {
            ss = ss + ((a-'0') * 1)
            i = 1
          }
          else if (i==1) {
            ss = ss + ((a-'0') * 3)
            i = 0
          }
        }
        if (ss%10 == 0)
          return true
        else
          return false
      }
      else
        return false
    }

    //val df = spark.read.option("header", "true").csv("C:\\Users\\User\\Desktop\\isbn\\ISBN.csv")

    // dataframe with the data that needs to be processed
    val df = spark.sparkContext.parallelize(List(("Learning Spark: Lightning-Fast Big Data Analysis","2015","ISBN: 978-1449358624")
    )).toDF("name", "year", "isbn")
    val Raw_isbn = df.select("ISBN").first.getString(0)

    // get only the numbers from the third column
    val isbn_digit = ("""\d+""".r findAllIn Raw_isbn).toList.mkString("")
    println(isbn_digit)
    if (validateISBN(isbn_digit)) {
      val name = df.select("Name").first.getString(0)
      val yy = df.select("Year").first.getString(0)
      //val newRow = List(name,yy,"ISBN-EAN: "+isbn_digit.slice(0,3))
      val sc = spark.sparkContext.parallelize(List((name, yy, "ISBN-EAN: " + isbn_digit.slice(0, 3)),
        (name, yy, "ISBN-GROUP: " + isbn_digit.slice(3, 5)), (name, yy, "ISBN-PUBLISHER: " + isbn_digit.slice(5, 9)),
        (name, yy, "ISBN-TITLE: " + isbn_digit.slice(9, 12)))).toDF("name", "year", "isbn")
      val finalDF = df.union(sc)
      println(finalDF.show(truncate = false))
    //finalDF.foreach { row =>
      //println(row) }
    }
    else
      println("invalid ISBN code")
}}
