# Databricks notebook source
data = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/scratue2000@gmail.com/dada12345-1.csv")
data.display()

# COMMAND ----------

from pyspark.sql.functions import col,asc,sum,count,filter,avg,max

# COMMAND ----------

#Show the first 5 rows of the DataFrame
data.show(5)

# COMMAND ----------

#Display the schema of the DataFrame.
data.printSchema()

# COMMAND ----------

#Select and show only the TransactionID, Date, and Amount columns.
display(data.select("TransactionID", "Date", "Amount"))

# COMMAND ----------

# Filter the DataFrame to show only transactions where the amount is greater than 500.
data.select(col("TransactionID")).filter(col("Amount") > 500).show()

# COMMAND ----------

#Sort the DataFrame by Date in ascending order.
ascending_data=data.sort("Date").show()

# COMMAND ----------

#Sort the DataFrame by Amount in descending order
desending_data=data.orderBy("Amount",ascending = True ).show()
#display(desending_data)

# COMMAND ----------

#Add a new column TotalValue which is the product of Amount and Quantity

cre_col=data.withColumn("Total",col("Quantity")*col("Amount"))
display(cre_col)

# COMMAND ----------

#Rename the CustomerID column to ClientID.
col_rename=data.withColumnRenamed("CustomerID","ClientID")
display(col_rename)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC total=df1.agg(sum(col("toatal_amt")).alias("total")).show()
# MAGIC print(f"total amt:{total}")

# COMMAND ----------

#Group the DataFrame by Category and calculate the total Amount for each category.
Grouped=cre_col.groupBy(col("Category")).sum("Total").show()

# COMMAND ----------


#-> Group the DataFrame by StoreID and count the number of transactions for each store.
grpupstor=data.groupBy("StoreID").count().show()


# COMMAND ----------

# -> Filter transactions to show only those that occurred in StoreID 1.
fildata=data.select("TransactionID","StoreID").filter(col("StoreID") == 1 ).show()

# COMMAND ----------

# MAGIC %md
# MAGIC groupby=df1.groupBy("product_id").agg(sum("quantity").alias("total q"))
# MAGIC groupby.show()

# COMMAND ----------

#  -> Calculate the average Amount for each Product
pro_avg=cre_col.groupBy("Product").agg(avg(col("Total"))).show()

# COMMAND ----------

#  -> Find the maximum Amount for transactions in the Electronics category.
maxmu=cre_col.filter("Category=='Electronics'").agg(max("Total")).show()

# COMMAND ----------




# -> Filter the DataFrame to show only transactions that occurred on or after 2023-01-05.
fdata=data.select("TransactionID").filter(col("Date") > "05-01-2023").show()


# COMMAND ----------

# -> Create a new DataFrame with an additional column DiscountedAmount which is Amount with a 10% discount applied.
dis_col=cre_col.withColumn("Discount",col("Total")* 0.9).show()

# COMMAND ----------

  #-> Filter and show transactions where the Quantity is greater than 1 and the Category is Accessories.
greater_col=data.filter((col("Quantity")>1) & (col("category") == 'Accessories')).select("TransactionID","category","Quantity").show()


# COMMAND ----------

 #-> Count the number of unique ClientID values in the DataFrame.
data.select("CustomerID").distinct().count()

# COMMAND ----------

#-> Filter transactions where the Product starts with the letter 'L'.
