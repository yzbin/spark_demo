package com.robin.sparkcore

/**
  * Created with IntelliJ IDEA.
  * Description: 
  * User: bin
  * Date: 2017-12-19
  *
  */

class SecondSortKey(val first:Int, val second:Int)
  extends Ordered[SecondSortKey] with Serializable{
  override def compare(that: SecondSortKey) = {
    if (this.first - that.first != 0){
      this.first - that.first
    }else{
      this.second - that.second
    }
  }
}
