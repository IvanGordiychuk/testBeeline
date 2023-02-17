import org.apache.spark.SparkContext.getOrCreate
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructType}

import java.util.Date

object TestSpark {
  def main(args: Array[String]) {
    val customerPath = "logs/customer.csv" // Should be some file on your system
    val orderPath = "logs/order.csv"
    val productPath = "logs/product.csv"
    val customerSchema = new StructType()
      .add("id",IntegerType,true)
      .add("name",StringType,true)
      .add("email",StringType,true)
      .add("joinDate", DateType, true)
      .add("status",StringType, true)

    val productSchema = new StructType()
      .add("id",IntegerType,true)
      .add("name",StringType,true)
      .add("price",DoubleType, true)
      .add("numberOfProducts",IntegerType,true)

    val orderSchema = new StructType()
      .add("customerID", IntegerType, true)
      .add("orderID",IntegerType,true)
      .add("productID",IntegerType, true)
      .add("numberOfProduct", IntegerType,true)
      .add("orderDate",DoubleType,true)
      .add("status",StringType,true)

    val spark = SparkSession.builder.appName("Simple Application").master("local[4]").getOrCreate()
    val customerData = spark.read.option("delimiter","\t").schema(customerSchema).option("header","true").csv(customerPath).cache()
    val orderData = spark.read.option("delimiter","\t").schema(orderSchema).option("header","true").csv(orderPath).cache()
    val productData = spark.read.option("delimiter","\t").schema(productSchema).option("header","true").csv(productPath).cache()

    customerData.createOrReplaceTempView("customer");
    orderData.createOrReplaceTempView("order")
    productData.createOrReplaceTempView("product")


    val GroupedData = spark.sql("SELECT customer.name as customer, product.name as product, SUM(order.numberOfProduct) as sumOfProduct " +
      "FROM order  INNER JOIN customer ON  customer.id " +
      "= order.customerID JOIN product ON product.id = order.productID " +
      "GROUP BY customer.name, product.name"
    ).toDF();

    val windowSpec = Window.partitionBy("customer").orderBy(col("sumOfProduct").desc);
    val finDF = GroupedData.withColumn("row_num",row_number().over(windowSpec)).where("row_num=1").toDF();

    finDF.write.format("csv").save("groupcsv")

    //val orderCustomerProduct = orderData.join(customerData, customerData("id") === orderData("customerID"),"inner")
    //.join(productData, productData("id") === orderData("productID"))//.show(false)
    //orderCustomerProduct.show(false);
    //println(customerData.show())
    //println(orderData.show())
    //println(productData.show())
    spark.stop()
  }
}
