
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io._
import scala.collection.mutable.PriorityQueue
import scala.collection.mutable.ListBuffer
object Chirumamilla_Lakshmi_clustering {
  
  def main(args: Array[String]){
  
    val conf = new SparkConf().setAppName("CF").setMaster("local[1]")
    val sparkContext = new SparkContext(conf)
   
     if (args.length <2) 
        {
          println("Please provide sufficient arguments from command line as per Description file")
          System.exit(1)
        } 
       val inputFile =  sparkContext.textFile(args(0))
        val NoOfClusters = args(1).toInt
     
      
    val inputData = inputFile.filter(!_.isEmpty()).map(line=>line.split(",")).map(line=>(line(0).toDouble, line(1).toDouble, line(2).toDouble, line(3).toDouble, line(4).toString)).collect()   
    val clustersData = scala.collection.mutable.Map[Int,((Double,Double,Double,Double),scala.collection.mutable.Map[Int,(Double,Double,Double,Double)])]()
    val output = scala.collection.mutable.Map[Int,scala.collection.mutable.Map[Int,(Double,Double,Double,Double)]]()
    val pointClassData  = scala.collection.mutable.Map[Int, String]()
     var pointNewClass  =  scala.collection.mutable.Map[Int, String]()

   
    //use inputdata without converting to array , it wont work Iterating through an RDD and updating a mutable structure defined outside the loop is a Spark anti-pattern.
    var k = 0;
    inputData.foreach{        
      x=> 
        {
          k = k +1
          var tempMap =  scala.collection.mutable.HashMap((k,(x._1,x._2,x._3,x._4)))             
           clustersData.put(k,((x._1,x._2,x._3,x._4),tempMap)) 
           output.put(k,tempMap)  //fix for bug nto working for othr values of k like 6, 8 
           pointClassData.put(k,x._5)         
          }     
    }
       
    case class Pair(c1: Int, c2:Int, distance: Double)
    def PairOrder(p: Pair) = -p.distance    
    var priorityQ: PriorityQueue[Pair] = PriorityQueue.empty[Pair](Ordering.by(PairOrder))
    var i = 1; 
    var j = 2;
    
     for( i <- 1 to k){
       for (j <-i+1 to k){
         var x = clustersData(i)._1
         var y = clustersData(j)._1
         var dist = math.sqrt(math.pow(x._1 - y._1, 2) + math.pow(x._2 - y._2, 2)+ math.pow(x._3 - y._3, 2)+ math.pow(x._4 - y._4, 2))
         priorityQ += (Pair(i,j, dist))      
      }
     }   // intiial priority Queue formation
    
    //hierarchical algo
      while ( output.size > NoOfClusters ) //priorityQ.size >NoOfClusters
    {
        
        k = k+1
       val pairDequeued: Pair = priorityQ.dequeue()
       // println(s"element dequeued = $pairDequeued")
       priorityQ = priorityQ.filterNot(x=>x.c1 == pairDequeued.c1 | x.c1 == pairDequeued.c2 | x.c2 == pairDequeued.c1 | x.c2 == pairDequeued.c2 )
       
       //clusters to be joined       
       var cluster1 = clustersData(pairDequeued.c1)._2
         var cluster2 = clustersData(pairDequeued.c2)._2  // _.1 contains centroid, _.2 is the list of points in the cluster         
        
      var tempMap =  scala.collection.mutable.HashMap[Int, (Double,Double,Double,Double)]() 
       // iterate over each cluster 
      cluster1.foreach{
          x=> tempMap.put(x._1,x._2)        
          
        }
         cluster2.foreach{
         x=> tempMap.put(x._1,x._2)          
        }         
         
      val tempList = tempMap.values.toList
      //calculate centroid of new cluster 
   var ting =    tempList.foldLeft((0.0,0.0,0.0,0.0)) { case ((p, q,r, s), (a, b,c,d)) => (p + a, q + b, r+c, s+d) }
   val newClusterSize = tempList.size
 
     clustersData.put(k,((ting._1/newClusterSize,ting._2/newClusterSize,ting._3/newClusterSize,ting._4/newClusterSize),tempMap))  //adding the combined cluster  
   
     if(output.contains(pairDequeued.c1))
       output -= pairDequeued.c1
       
     if(output.contains(pairDequeued.c2))
       output -= pairDequeued.c2
     output.put(k,tempMap)
     
     // add new pairs into PriorityQueue 
      var remOthers = scala.collection.mutable.Set[Int]()
      priorityQ.foreach{
      x=> 
        {
          remOthers += x.c1
          remOthers += x.c2   }
     }
   
   //calculate distances of new cluster with each other remaining in remOthers
   remOthers.foreach
   {
     r=> 
       {
        var x = clustersData(r)._1
         var y = clustersData(k)._1
         var dist = math.sqrt(math.pow(x._1 - y._1, 2) + math.pow(x._2 - y._2, 2)+ math.pow(x._3 - y._3, 2)+ math.pow(x._4 - y._4, 2))
         priorityQ += (Pair(r,k, dist))  
       }
   }
   
    
   } //end of while loop
     
  
  val outputFile = new PrintWriter(new File("Lakshmi_Chirumamilla_"+NoOfClusters+".txt"))  
  
    output.values.foreach{
     
       t=> 
          var classCount =  scala.collection.mutable.Map[String,Int]()
     
      var finalClass =""
      
         t.foreach{
         
         p =>
           {
             var className = pointClassData(p._1)
            if (!classCount.contains(className)) {
          classCount.put(className, 1)

        }
        else {
          classCount.update(className, classCount(className) + 1)
        }
           
             
           }
           
         
       }
      
     finalClass =  classCount.maxBy(_._2)._1 //we will take the classname majority of the records represent
     
     t.foreach{
       e=>  pointNewClass.put(e._1,finalClass)
       
     }
         outputFile.println("cluster:"+ finalClass)
         var tempPoints = t.toSeq.sortBy(_._1)
         tempPoints.foreach{
     
       tp => outputFile.println("["+tp._2._1+", "+tp._2._2+", "+tp._2._3+", "+tp._2._4+", '"+ pointClassData(tp._1)+"']")
       }
         
         outputFile.println("Number of points in this cluster:"+t.size)         
      
          outputFile.write( System.getProperty("line.separator") )         
       
          
   }
  
    var nonMatch =  (sparkContext.parallelize(pointNewClass.toSeq).join(sparkContext.parallelize(pointClassData.toSeq))).filter(x=>x._2._1 !=x._2._2)
   
    if(nonMatch.count > 0)
        {
          outputFile.println("Number of points wrongly assigned:"+nonMatch.count)         
        }
  outputFile.close()
 
  }
  
}