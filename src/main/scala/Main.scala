import org.apache.spark.SparkConf

object Main {
  def main(args: Array[String]): Unit = {

    // Take the kafka topic to consume from as a input from user
    print("Enter Kafka Topic Name to consume:-")

    // Read the kafka topic as string
    val kafkaTopic = scala.io.StdIn.readLine()


    //display the options to user
    println("Enter the algorithm to run (1-8):-")

    println("1. Sum of Mass in Each Frame\n" +
      "2. Moment of Inertia in each Frame\n" +
      "3. Dipole Moment\n" +
      "4. Center of Mass\n" +
      "5. Average Charge\n" +
      "6. Radius of Gyration along X axis\n"+
      "7. Radius of Gyration along Y axis\n"+
      "8. Radius of Gyration along Z axis\n")

    print("Choice? ")


    //take the algorithm to run as an input from user
    val algoToRun = scala.io.StdIn.readInt()

    if(algoToRun<1 || algoToRun>8){
      throw new IllegalArgumentException("Enter the algorithm to run choice between 1-8")
    }

    //run the pipeline to consume the data from kafka topic based on the algorithm user has selected.
    StreamingPipeline.run(kafkaTopic, algoToRun)

  }
}
