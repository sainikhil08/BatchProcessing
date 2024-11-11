import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import scala.util.Random

import scala.math.sqrt


object StreamingPipeline {

  /**
    @param kafkaTopic   The name of kafka topic to consume from
    @param algorithmIndex Run the scientific algorithm on molecular data based on the user selection

    @return   The streaming function which consumes the data from kafka topic and runs the selected scientific algorithm.
   **/
  def run(kafkaTopic:String, algorithmIndex: Int): Unit = {


    //create the random class instance
    val random = new Random()



    //build the kafka config.
    //Assigning the random integer to consumer group id.
    val kafkaParams = Map(
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer].getName,
      "value.deserializer" -> classOf[StringDeserializer].getName,
      "group.id" -> s"${random.nextInt()}",
      "auto.offset.reset" -> "earliest"
    )

    // Suppressing INFO-level logs for Spark
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)


    //build the sparkConf
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("KafkaStreamProcessing")

    //build the streaming context
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //set the checkpoint location for the stateful streaming operation.
    ssc.checkpoint("./checkpoint") // Set a checkpoint directory for updateStateByKey


    //broadcast the name of kafka topic to consume from to all the executors.
    val broadcastVar1: Broadcast[String] = ssc.sparkContext.broadcast(kafkaTopic)

    //broadcast the index of algorithm to run from to all the executors.
    val algoInd: Broadcast[Int] = ssc.sparkContext.broadcast(algorithmIndex)



    val sparkUIUrl = ssc.sparkContext.uiWebUrl.getOrElse("No url available")

    println(sparkUIUrl)


    //create the DStream by consuming the data from kafka topic given by the user
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set(broadcastVar1.value), kafkaParams)
    )



    //Split the each row at the space and extract the columns.
    var dataStream = stream.map(record => record.value().split("\\s+")).filter(x=> (!x(0).equals("HEAD")))
      .map(elem=>{
        (elem(1).toString, (elem(4).toDouble, elem(5).toDouble, elem(6).toDouble,elem(7).toDouble, elem(8).toDouble, 1))
      })

    
    //cache the datastream as it is used repeatedly.
    dataStream = dataStream.cache()


    //run the algorithm bases on the based on the algorithm index
    val result = if(algoInd.value==1){

      // produces the total sum of mass of atoms in each frame
       dataStream.updateStateByKey(updateFunc = updateStateSumOfMass)
      } else if(algoInd.value==2){

      //produces moment of intertia of atoms in each frame
       dataStream.updateStateByKey(updateFunc = updateStateMoemntOfInertia)
      } else if (algoInd.value == 3) {

      //produces the dipole moment of atoms in each frame
        dataStream.updateStateByKey(updateFunc = updateStateDipoleMoment)
      } else if (algoInd.value == 4) {

      //produces the ceneter of mass of atoms in each time frame
        dataStream.updateStateByKey(updateFunc = updateStateCenterOfMass)
      } else if (algoInd.value == 5) {

      //produces the average charge of atoms in each frame
       dataStream.updateStateByKey(updateFunc = updateStateAverageCharge)
      } else if(algoInd.value == 6) {

      //produces the radius of gyration along x-axis in each frame
         dataStream.updateStateByKey(updateFunc = updateStateRGX)
      } else if (algoInd.value == 7) {

      //produces the radius of gyration along y-axis in each frame
      dataStream.updateStateByKey(updateFunc = updateStateRGY)
    } else{

      //produces the radius of gyration along z-axis in each frame
      dataStream.updateStateByKey(updateFunc = updateStateRGZ)
    }

  if (algoInd.value == 4) {
      val data = result.map(x => {
        val a1 = x._1
        val s2 = x._2 match {
          case (a: Double, b: Double, c: Double, _: Double) => Some((a, b, c))
          case _ => None
        }
        (a1, s2.get)
      })
      data.print()

    } else if (algoInd.value >=5) {

      val data = result.map(x => {
        val a1 = x._1
        val s2 = x._2 match {
          case (a: Double, _: Double) => Some((a))
          case _ => None
        }
        (a1, s2.get)
      })
      data.print()
    } else {
     result.print()
    }




    //start the streaming
    ssc.start()

    //wait for the execution to stop
    ssc.awaitTermination()
  }


  /**
   *
   * @param newValues List of (X-position, Y-position, Z-position, charge, mass, count) of atoms for given fame-id
   * @param currentState moment of inertia of given frame id if available in the state
   * @return moment of inertia of atoms in the frame
   */
  def updateStateMoemntOfInertia(newValues: Seq[(Double, Double, Double, Double, Double, Int)], currentState: Option[(Double, Double, Double)]) = {
    val currState = currentState.getOrElse((0.0, 0.0, 0.0))
    val d = newValues.foldLeft(0.0, 0.0, 0.0)((a, b) => {
      (b._5 * b._1 + a._1,
        b._5 * b._2 + a._2,
        b._5 * b._3 + a._3)
    })
    Some((currState._1+d._1,currState._2+d._2, currState._3+d._3))
  }

  /**
   *
   * @param newValues    List of (X-position, Y-position, Z-position, charge, mass, count) of atoms for given fame-id
   * @param currentState sum of mass of given frame id if available in the state
   * @return sum of mass of atoms in the frame
   */
  def updateStateSumOfMass(newValues: Seq[(Double, Double, Double, Double, Double, Int)], currentState: Option[Double]) = {
    val currStateV = currentState.getOrElse((0.0))
    val d = newValues.map(_._5).foldLeft(0.0)((a, b) => {
      a+b
    })
    Some(d+currStateV)
  }

  /**
   *
   * @param newValues    List of (X-position, Y-position, Z-position, charge, mass, count) of atoms for given fame-id
   * @param currentState center of mass of given frame id if available in the state
   * @return center of mass of atoms in the frame
   */
  def updateStateCenterOfMass(newValues: Seq[(Double, Double, Double, Double, Double, Int)], currentState: Option[(Double, Double, Double, Double)]) = {
    val currStateV = currentState.getOrElse((0.0, 0.0, 0.0, 0.0))
    val d = newValues.foldLeft(0.0, 0.0, 0.0, 0.0)((a, b) => {
      (b._5 * b._1 + a._1,
        b._5 * b._2 + a._2,
        b._5 * b._3 + a._3, a._4+b._5)
    })
    val t = d._4+currStateV._4
    Some((currStateV._1 *t+d._1)/t,(currStateV._2 *t+d._2)/t, (currStateV._3 *t+d._3)/t,t)
  }

  /**
   *
   * @param newValues    List of (X-position, Y-position, Z-position, charge, mass, count) of atoms for given fame-id
   * @param currentState Dipole Moment of given frame id if available in the state
   * @return Dipole Moment of atoms in the frame
   */
  def updateStateDipoleMoment(newValues: Seq[(Double, Double, Double, Double, Double, Int)], currentState: Option[(Double, Double, Double)]) = {
    val currState = currentState.getOrElse((0.0, 0.0, 0.0))
    val d = newValues.foldLeft(0.0, 0.0, 0.0)((a, b) => {
      (b._4 * b._1 + a._1,
        b._4 * b._2 + a._2,
        b._4 * b._3 + a._3)
    })
    Some((currState._1 + d._1, currState._2 + d._2, currState._3 + d._3))
  }

  /**
   *
   * @param newValues    List of (X-position, Y-position, Z-position, charge, mass, count) of atoms for given fame-id
   * @param currentState (Average Charge iof atoms, total_atoms_count) in given frame id if available in the state
   * @return (Average charge of atoms, total_atoms_count)  in the frame
   */
  def updateStateAverageCharge(newValues: Seq[(Double, Double, Double, Double, Double, Int)], currentState: Option[(Double, Double)]) = {
    val currState = currentState.getOrElse((0.0, 0.0))
    val d = newValues.map(x=>(x._4, x._6)).foldLeft(0.0, 0.0)((a, b) => {
      (a._1+b._1, a._2+b._2)
    })
    val totalCount = currState._2 + d._2
    Some((d._1+(currState._1*currState._2))/totalCount, totalCount)
  }

  /**
   *
   * @param newValues    List of (X-position, Y-position, Z-position, charge, mass, count) of atoms for given fame-id
   * @param currentState (Radius of Gyration along X-axis, totalMass) in given frame id if available in the state
   * @return (Radius of Gyration along X-axis, totalMass)  in the frame
   */
  def updateStateRGX(newValues: Seq[(Double, Double, Double, Double, Double, Int)], currentState: Option[(Double, Double)]) = {
    val currState = currentState.getOrElse((0.0, 0.0))
    var d = (0.0,0.0)
    if(newValues.nonEmpty) {
      d = newValues.map(x => (x._1 * x._5, x._5)).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
    }
    val totalMass = currState._2 + d._2
    val stateMOI = currState._1*currState._1*currState._2
    val totalMOI = stateMOI+d._1
    Some(sqrt((totalMOI)/totalMass), totalMass)
  }

  /**
   *
   * @param newValues    List of (X-position, Y-position, Z-position, charge, mass, count) of atoms for given fame-id
   * @param currentState (Radius of Gyration along Y-axis, totalMass) in given frame id if available in the state
   * @return (Radius of Gyration along Y-axis, totalMass)  in the frame
   */
  def updateStateRGY(newValues: Seq[(Double, Double, Double, Double, Double, Int)], currentState: Option[(Double, Double)]) = {
    val currState = currentState.getOrElse((0.0, 0.0))
    var d = (0.0, 0.0)
    if (newValues.nonEmpty) {
      d = newValues.map(x => (x._2 * x._5, x._5)).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
    }
    val totalMass = currState._2 + d._2
    val stateMOI = currState._1 * currState._1 * currState._2
    val totalMOI = stateMOI + d._1
    Some(sqrt((totalMOI) / totalMass), totalMass)
  }

  /**
   *
   * @param newValues    List of (X-position, Y-position, Z-position, charge, mass, count) of atoms for given fame-id
   * @param currentState (Radius of Gyration along Z-axis, totalMass) in given frame id if available in the state
   * @return (Radius of Gyration along Z-axis, totalMass)  in the frame
   */
  def updateStateRGZ(newValues: Seq[(Double, Double, Double, Double, Double, Int)], currentState: Option[(Double, Double)]) = {
    val currState = currentState.getOrElse((0.0, 0.0))
    var d = (0.0, 0.0)
    if (newValues.nonEmpty) {
      d = newValues.map(x => (x._3 * x._5, x._5)).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
    }
    val totalMass = currState._2 + d._2
    val stateMOI = currState._1 * currState._1 * currState._2
    val totalMOI = stateMOI + d._1
    Some(sqrt((totalMOI) / totalMass), totalMass)
  }

}
