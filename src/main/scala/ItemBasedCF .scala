

import java.io.{File, PrintWriter}

import breeze.numerics.{abs, pow, sqrt}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext, rdd}
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import scala.collection.mutable.ListBuffer

object ItemBasedCF {

  def get_prime(list: List[Int]): List[Int] = list match {
    case Nil => Nil
    case x :: xs => List(x) ::: get_prime(xs.filter(_ % x != 0))
  }

  def prime(n: Int) = {
    val list = (2 to n).toList
    get_prime(list)
  }

  val prime_nums = prime(1000)

  var a_list = ListBuffer.empty[Int]
  var b_list = ListBuffer.empty[Int]
  var m_list = ListBuffer.empty[Int]

  val hash_num = 600

  for(_ <- 0 until hash_num){

    val a_index = scala.util.Random.nextInt(150)+1
    val m_index = scala.util.Random.nextInt(150)+1
    val b = scala.util.Random.nextInt(400)+1

    b_list+= b
    a_list += prime_nums(a_index)
    m_list += prime_nums(m_index)

  }



  def main(args: Array[String]) {
    val t0 = System.currentTimeMillis()



    val conf = new SparkConf()
    conf.setAppName("CF_UserBased")
    conf.setMaster("local[*]")
    conf.set("spark.executor.memory", "1g")
    conf.set("driver-memory", "4g")
    conf.set("executor-cores", "2")


    val sc = new SparkContext(conf)
    val storageLevel = StorageLevel.MEMORY_ONLY


    var total_file = sc.textFile(args(0)).cache()
      .map(line => line.split(","))
    val total_header = total_file.first()
    total_file = total_file.filter(_ (0) != total_header(0))

    val total = total_file.map(line => (line(1).toInt, (line(0).toInt, line(2).toDouble)))


    var testing_file = sc.textFile(args(1)).cache().map(line => line.split(","))
    val testing_header = testing_file.first()
    testing_file = testing_file.filter(_ (0) != testing_header(0))

    val testing_rdd = testing_file.map(line => (line(1).toInt, (line(0).toInt, line(2).toDouble)))


    val training_rdd = total.subtract(testing_rdd)



    //list of distinct product
    val product_num = training_rdd.map(x => x._1).distinct()


    val maxIndex = product_num.max + 1

    val userProductRatings = training_rdd
      .groupByKey().map(line => (line._1, line._2.toSeq)) //(productID,((userID1,1),(userID2,1),(userID3,1)...))

    //construct upr map: pro->(user->rating)

    val upr_map = userProductRatings.map(line=>(line._1,line._2.toMap)).collect().toMap


    //construct uplist : pro->(rating list)
    val up_list = userProductRatings.map(line=>(line._1,line._2.toMap.keysIterator.toSet)).collect()

    val sparseVectorData = userProductRatings.map { line =>
      val sv: Vector = Vectors.sparse(maxIndex.toInt, line._2)

      (line._1, sv)
    }
    val rdd_minhash = sparseVectorData.map { line =>

      var list_after_hash = ListBuffer.empty[Int]

      val user_index_list = line._2.toSparse.indices.toList


      val minhash_list = minhash(user_index_list)

      val rating_list = line._2.toSparse.values.toList

      (line._1,(minhash_list,user_index_list,rating_list))//(productID,(signature,sp_index))
    }



    val band_num = 100
    val row_num = hash_num/band_num



    val rdd_band = rdd_minhash.map{case(pId,(signature,sp_index,rating_list)) =>
      var band_list = ListBuffer.empty[Tuple2[ListBuffer[Int],Int]]// (sub_list,band number)


      for(i <-0 until band_num){
        val sub_list = signature.slice(i*row_num,i*row_num+row_num)
        band_list += Tuple2(sub_list,i)
      }

      ((pId,sp_index,rating_list),band_list)   //(productID,sp_index),band_list)
    }



    val band_divide = rdd_band.flatMap{ case ((pId,sp_index,rating_list),band_list)  =>

      for(elem <- band_list) yield (elem,(pId,sp_index,rating_list))//(band,(productID,sp_index))

    }
      .groupByKey()
      .map(line =>line._2.toList)
      .filter(line =>line.size>=2)
    // .map(line=>line.combinations(2))


   // band_divide.collect().foreach(println)


    val pair_rdd = band_divide.map { line =>

      var pair_list = ListBuffer.empty[Tuple2[(Int,List[Int],List[Double]),(Int,List[Int],List[Double])]]
      for(i <- 0 until line.size){
        for( j<- i+1 until line.size){
          pair_list += Tuple2(line(i),line(j))
        }
      }
      pair_list
    }

    val cdd_pair = pair_rdd.collect().flatten.toSet

    println(cdd_pair.size)

    // calculate pearson smilarity between cdd pair
   val pair_pearson=  sc.parallelize(cdd_pair.toSeq)
      .map{case((pID_a,uIDList_a,rateList_a),(pID_b,uIDList_b,rateList_b))=>

       var  up_map_a = Map.empty[Int,Double]
        for(i<- 0 until uIDList_a.size ){
          up_map_a += (uIDList_a(i)-> rateList_a(i))
        }

        var  up_map_b = Map.empty[Int,Double]
        for(i<- 0 until uIDList_b.size ){
          up_map_b += (uIDList_b(i)-> rateList_b(i))
        }

          val co_rate_users = uIDList_a.intersect(uIDList_b)


        val avg_a = up_map_a.filter{case(product,rating)=>
          co_rate_users.contains(product)
        }.valuesIterator.sum/up_map_a.size

        val avg_b = up_map_b.filter{case(product,rating)=>
          co_rate_users.contains(product)
        }.valuesIterator.sum/up_map_b.size


        var numerator = 0.0
        var denominator_1= 0.0
        var denominator_2= 0.0
          for(  elem <-co_rate_users ){
            numerator += (up_map_a(elem)-avg_a)*(up_map_b(elem)-avg_b)

            denominator_1+= pow((up_map_a(elem)-avg_a),2)
            denominator_2+= pow((up_map_b(elem)-avg_b),2)
          }

        var pearson = 0.0
        if(denominator_1*denominator_2 != 0){
          pearson = numerator/sqrt(denominator_1*denominator_2)
        }
        (Set(pID_a,pID_b),pearson)
      }.filter(line=>line._2 != 0).collect().toMap

    pair_pearson.toMap
      //.foreach(println)



    // predict

    val prediction = testing_rdd.map{case(proA,(theUser,og_rating))=>



      val corate_up = up_list.filter { case (proB, user_rating_list) =>
        Set(theUser).subsetOf(user_rating_list)
      }// product has been purchased by user = 》user_rating_list contain theUser



      var predict_numerator = 0.0
      var predict_denominator = 0.0
      corate_up.map { case (proB, user_rating_list) =>

        var pearson = 0.0
        if (pair_pearson.contains(Set(proA,proB))) {
          pearson = pair_pearson(Set(proA, proB))
        }
        predict_numerator += pearson * (upr_map(proB))(theUser)
        predict_denominator += abs(pearson)

      }
        var predict = 0.0
        if(predict_denominator != 0){
          predict = predict_numerator/predict_denominator
      }else{
          //assign the avg
          val avg_a = (upr_map(proA)).valuesIterator.toList.sum/(upr_map(proA)).valuesIterator.toList.size
          predict = avg_a
        }


      ((theUser,proA),(og_rating,predict))
    }.sortBy(x=>(x._1._1,x._1._2))

    prediction.foreach(println)

    val prediction_result = prediction.collect()


    val diff= prediction_result.map { case ((user, product), (r1, r2)) => math.abs(r1 - r2)}

    var num1=0
    var num2=0
    var num3=0
    var num4=0
    var num5=0
    for ( x <- diff) {
      x match {
        case x if (x>=0 && x<1) => num1+=1;
        case x if (x>=1 && x<2)=> num2+=1;
        case x if (x>=2 && x<3) => num3+=1;
        case x if (x>=3 && x<4) => num4+=1;
        case x if (x>=4 ) => num5+=1;
      }
    }

    println(">=0 and <1:"+ num1)
    println(">=1 and <2:"+ num2)
    println(">=2 and <3:"+ num3)
    println(">=3 and <4:"+ num4)
    println(">=4 :"+ num5)


    var MSE = 0.0

    val calc_MSE = prediction_result.map { case ((user, product), (r1, r2)) =>
      MSE += scala.math.pow(r1-r2,2)
    }
    val RMSE = sqrt(MSE/prediction_result.size)

    println("RMSE:"+ RMSE)



    val output_file = new File(args(2))
    val out = new PrintWriter(output_file)
    for (elem <- prediction_result){

      out.write(elem._1._1 + "," + elem._1._2 +","+ elem._2._2 +"\n")

    }

    out.close()

    val t1 = System.currentTimeMillis()
    println("Time : " + (t1-t0)/1000+ "sec")


  }//main


  def minhash(user_list: List[Int]): ListBuffer[Int] = {
    var minhash_list = ListBuffer.empty[Int]
    for(i <- 0 until hash_num){
      var list_after_hash = ListBuffer.empty[Int]
      for(num <- user_list){


        val new_index = (a_list(i) * num + b_list(i)) % m_list(i)

        list_after_hash += new_index
      }

      minhash_list+= list_after_hash.min
    }
    return minhash_list
  }

}//object
