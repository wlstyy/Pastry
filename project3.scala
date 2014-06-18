import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.ActorRef
import akka.actor.Props
case object JOIN
case object JOINT
case object UPDATE
case object ROUTE
case object HELLO
case object FAIL
case object ACK
case object ASK
case object RESPONSE


object project3 {

  def main(args: Array[String]): Unit = {
    val numNode = args(0).toInt
    val numRequest =args(1).toInt
    val system = ActorSystem("pastry")
    var master = system.actorOf(Props[PastryMaster]);
    master ! (numNode, numRequest)
  }
}

class PastryMaster extends Actor {
  var count = 0
  var nNodes = 0
  var nRequests = 0
  var hopSum = 0
  val b = 2
  val l = 16
  val L=16
  val range = 1 << b
  var pastryNodes: FixedArrayList = null
  def receive = {
    case (numNode: Int, numRequest: Int) => {
      println("Pastry System Initialization")
      nNodes = numNode
      nRequests = numRequest
      pastryNodes = new FixedArrayList(numNode)
      //l=Math.ceil(Math.log(nNodes)/Math.log(range)).toInt
     // l=32;
    //  println("length:"+l)
      var id = Array.fill(l)((Math.random * (range)).toInt)
      context.actorOf(Props[PastryNode], id.mkString) ! (range, l, L, id)
    }
    case (JOIN, id: Array[Int]) => {
      if (pastryNodes.length == 0) {
        pastryNodes.add(id)
        count += 1
        var newid: Array[Int] = null //=Array.fill(l)((Math.random*(range)).toInt)

        var i = 0
        var flag = false
        while (!flag) {
          newid = Array.fill(l)((Math.random * (range)).toInt)
          var iid = newid.mkString.toLong
          flag = true
          for (i <- 0 to pastryNodes.length - 1) {
            if (pastryNodes.get(i).mkString.toLong == iid) {
              flag = false
            }
          }
        }
        context.actorOf(Props[PastryNode], newid.mkString) ! (range, l,L, newid)
      } else {
        var randomIndex = (Math.random * pastryNodes.length).toInt
        context.actorSelection(pastryNodes.get(randomIndex).mkString) ! (UPDATE, id, Array[Array[ActorRef]](),-1,false)
      }
    }
    case (JOINT, id: Array[Int]) => {
      pastryNodes.add(id)
      count += 1
      if (count >= nNodes) {
        println("BEGIN PROTOCOL")
        count = 0
        var i=0
        for(i<-0 to nRequests-1){
          var orgIndex = (Math.random * pastryNodes.length).toInt
          var destIndex=(Math.random * pastryNodes.length).toInt
          while(destIndex==orgIndex){
            destIndex=(Math.random * pastryNodes.length).toInt
          }
          context.actorSelection(pastryNodes.get(orgIndex).mkString) ! (ROUTE,0,pastryNodes.get(destIndex),false)
        }
       
      } else {
        var id: Array[Int] = null //=Array.fill(l)((Math.random*(range)).toInt)

        var i = 0
        var flag = false
        while (!flag) {
          id = Array.fill(l)((Math.random * (range)).toInt)
          var iid = id.mkString.toLong
          flag = true
          for (i <- 0 to pastryNodes.length - 1) {
            if (pastryNodes.get(i).mkString.toLong == iid) {
              flag = false
            }
          }
        }
        context.actorOf(Props[PastryNode], id.mkString) ! (range, l, L,id)
      }
    }

    case nHops: Int => {
     // println("NHOPS:"+nHops)
      count += 1
      hopSum += nHops
      if(count>=nRequests){
        println("HOPSUM:"+hopSum)
        println("NUMREQUESTS:"+nRequests)
        println("Average hops: "+hopSum*1.0/count)
        context.system.shutdown
      }
    }
    case FAIL=>{
     // println("FAIL")
      nRequests-=1
    }

  }

}

class PastryNode extends Actor {
  var id: Array[Int] = null;
  var rtable: Array[Array[ActorRef]] = null
  var left: FixedArrayList = null
  var leftRef: Array[ActorRef] = null
  var rightRef: Array[ActorRef] = null
  var right: FixedArrayList = null
  var ackCount=0
  var ackLimit=0;
  def receive = {
    case (range: Int, l: Int, ll: Int, xid: Array[Int]) => {
      id = xid
      rtable = Array.ofDim[ActorRef](l, range)
      left = new FixedArrayList(ll / 2)
      right = new FixedArrayList(ll / 2)
      leftRef = new Array[ActorRef](ll / 2)
      rightRef = new Array[ActorRef](ll / 2)
      ackLimit=rtable.length*rtable(0).length+leftRef.length+rightRef.length+1
      context.parent ! (JOIN,id)
    }
    case (ROUTE, nhops: Int, destId: Array[Int], closest: Boolean) => {
      var isClosest=closest
      if(id.mkString.toLong==destId.mkString.toLong)
        isClosest=true
      if (isClosest) {
    	  context.parent ! nhops
      } else {
        var hops=nhops+1
    	  var iDestId=destId.mkString.toLong
    	  var iMyId=id.mkString.toLong
    	  if(iDestId>iMyId){
    	    if(right.length>0&&iDestId<=right.get(right.length-1).mkString.toLong){
    	      rightRef(findClosest(right,destId)) ! (ROUTE,hops,destId,true)
    	    }
    	    else{
    	      var next=findNext(destId)
    	      if(next==self){
    	        self ! (ROUTE,nhops,destId,true)
    	      }
    	      else if(next==null){
    	      
    	        if(right.length>0){
    	          var dest:ActorRef=null
    	          var i=1
    	          var l=shl(id,destId)
    	          for(i<-0 to right.length-1){
    	            if(shl(right.get(i),destId)>=l){
    	              dest=rightRef(i)
    	            }
    	          }
    	          if(dest ==null){
    	            dest=rightRef(right.length-1)
    	          }
    	          
    	        	  dest ! (ROUTE,hops,destId,false)
    	          
    	        }
    	        else{
    	        	context.parent ! FAIL
    	        }
    	        	
    	      }else{
    	        next ! (ROUTE,hops,destId,false)
    	      }
    	    }
    	  }
    	  else{
    	    if(left.length>0&&iDestId>=left.get(left.length-1).mkString.toLong){
    	      leftRef(findClosest(left,destId)) ! (ROUTE,hops,destId,true)
    	    }
    	    else{
    	      var next=findNext(destId)
    	      if(next==self){
    	        self ! (ROUTE,nhops,destId,true)
    	      }
    	      else if(next==null){
    	        if(left.length>0){
    	          var dest:ActorRef=null
    	          var i=1
    	          var l=shl(id,destId)
    	          for(i<-0 to left.length-1){
    	            if(shl(left.get(i),destId)>=l){
    	              dest=leftRef(i)
    	            }
    	          }
    	          if(dest ==null){
    	            dest=leftRef(left.length-1)
    	          }
    	          
    	        	  dest ! (ROUTE,hops,destId,false)
    	        }
    	        else{
    	        	context.parent ! FAIL
    	        }
    	        	
    	      }else{
    	        next ! (ROUTE,hops,destId,false)
    	      }
    	    }
    	  }
      }
    }
    case (UPDATE, destId: Array[Int], rt: Array[Array[ActorRef]], level: Int, closest: Boolean) => {
      var table = rt
      var newlevel = level
     if(level==(-1)){
       newlevel=shl(id,destId)
       table=Array.ofDim[ActorRef](rtable.length,rtable(0).length)
       var i=0
       for(i<-0 to newlevel-1){
         table(i)=rtable(i).clone
       }
       
     }
     else if (level < rt.length - 1) {
        newlevel=shl(id,destId)
        if(newlevel>level)
        	table(newlevel) = rtable(newlevel).clone
      }
      if (closest) {
        deliver(destId, table)
      } else {
        var iDestId = destId.mkString.toLong
        var iMyId = id.mkString.toLong
        if (iDestId > iMyId) {
          updateNext(destId, iDestId, iMyId, table, newlevel, right, rightRef,false)
        } else {
          updateNext(destId, iDestId, iMyId, table, newlevel, left, leftRef,true)
        }
      }
    }
    case (rt: Array[Array[ActorRef]], smaller: FixedArrayList, bigger: FixedArrayList, smallRef: Array[ActorRef], bigRef: Array[ActorRef], closestId: Array[Int]) => {
      sender ! (ACK,id)
      rtable = rt
      left = smaller.copy
      right = bigger.copy
      leftRef = smallRef.clone
      rightRef = bigRef.clone
      updateTable(closestId,sender)
      
      if (closestId.mkString.toLong > id.mkString.toLong) {
        updateLeafSet(right, rightRef, closestId,sender)
      } else {
        updateLeafSet(left, leftRef, closestId,sender)
      }
      var i = 0
      for (i <- 0 to leftRef.length - 1) {
        if(leftRef(i)!=null)
        	leftRef(i) ! (HELLO, id)
        else
          //self ! ACK
          ackLimit-=1
      }
      for (i <- 0 to rightRef.length - 1) {
        if(rightRef(i)!=null)
        	rightRef(i) ! (HELLO, id)
        else
          //self ! ACK
          ackLimit-=1
      }
      
      var j=0
      for(i<-0 to rtable.length-1){
        for(j<-0 to rtable(0).length-1){
          if(rtable(i)(j)!=null)
            rtable(i)(j) ! (ACK,id)
          else
            //self ! ACK
            ackLimit-=1
        }
      }
    }
    case ACK=>{
      ackCount+=1
      if(ackCount==ackLimit){//rtable.length*rtable(0).length+leftRef.length+rightRef.length+1){
        ackCount=0
         context.parent ! (JOINT,id)
      }
    }
    case (HELLO, cid: Array[Int]) => {
     updateTable(cid, sender)
      if (cid.mkString.toLong > id.mkString.toLong) {
        updateLeafSet(right, rightRef, cid,sender)
      } else {
        updateLeafSet(left, leftRef, cid,sender)
      }
      sender ! ACK
    }
    case (ACK,cid:Array[Int])=>{
      updateTable(cid,sender)
      sender ! ACK
    }
    case ASK=>{
      sender ! (RESPONSE,id)
    }
    case (RESPONSE,xid:Array[Int])=>{
      updateTable(xid,sender)
      if(xid.mkString.toLong>id.mkString.toLong&&right.length<right.size){
        updateLeafSet(right,rightRef,xid,sender)
      }else if(xid.mkString.toLong<id.mkString.toLong&&left.length<left.size){
        updateLeafSet(left,leftRef,xid,sender)
      }
    }
  }

  def shl(myid:Array[Int],xid: Array[Int]): Int = {
    var i = 0
    var result = xid.length
    while (i < xid.length && result == xid.length) {
      if (xid(i) != myid(i))
        result = i
      i += 1
    }
    result
  }
  def deliver(destId: Array[Int], table: Array[Array[ActorRef]]) = {
    var sdesId = destId.mkString;
    var dest = context.actorSelection("../" + sdesId)
    dest ! (table, left, right, leftRef, rightRef, id)
  }
  
  

  def updateNext(destId: Array[Int], iDestId: Long, iMyId: Long, table: Array[Array[ActorRef]], level: Int, leafSet: FixedArrayList, leafRef: Array[ActorRef],isLeft:Boolean) = {
    if (leafSet.length == 0) {
      deliver(destId, table)
    } else if (((iDestId <= leafSet.get(leafSet.length-1).mkString.toLong)&&(!isLeft))||((iDestId >= leafSet.get(leafSet.length-1).mkString.toLong)&&(isLeft))/**||(leafSet.length<leafSet.size)*/) {
      var index = findClosest(leafSet, destId)
      if (Math.abs(iMyId - iDestId) <= Math.abs(leafSet.get(index).mkString.toLong - iDestId)) {
        deliver(destId, table)
      } else {
        leafRef(index) ! (UPDATE, destId, table, level, true)
      }
    }
    else {
      var next=findNext(destId)
      if(next!=null){
        next ! (UPDATE, destId, table, level, false)
      }
      else{
    	//println("cannot find next")
        //leafRef(leafSet.length-1) ! (UPDATE, destId, table, level, false)
        /** START*/
        var dest:ActorRef=null
    	          var i=1
    	          var l=shl(id,destId)
    	          for(i<-0 to leafSet.length-1){
    	            if(shl(leafSet.get(i),destId)>=l){
    	              dest=leafRef(i)
    	            }
    	          }
    	          if(dest ==null){
    	            dest=leafRef(right.length-1)
    	          }
    	          
    	        	  dest ! (UPDATE, destId, table, level, false)
         
        /** END */
        
        context.actorSelection("../"+destId.mkString) ! ASK
      }
    }
  }
  def findNext(destId: Array[Int]): ActorRef = {
    var row=shl(id,destId)
    if(row<destId.length)
    	rtable(row)(destId(row))
    else
      self
  }
  def findClosest(leafSet: FixedArrayList, destId: Array[Int]): Int = {
    var iDestId=destId.mkString.toLong
    var index=0
    var min=Math.abs(leafSet.get(0).mkString.toLong-iDestId)
    var i=0
    for(i<-1 to leafSet.length-1){
      var temp=Math.abs(leafSet.get(i).mkString.toLong-iDestId)
      if(temp<min){
        index=i
        min=temp
      }
    }
    index
  }
  def updateLeafSet(leafSet: FixedArrayList, leafRefSet: Array[ActorRef], cid: Array[Int],ref:ActorRef) = {
	var iCid=cid.mkString.toLong
	var iMyId=id.mkString.toLong
	var i=0
	var index=leafSet.length
	var diff=Math.abs(iCid-iMyId)
	while(i<leafSet.length&&index==leafSet.length){
	  var temp=Math.abs(leafSet.get(i).mkString.toLong-iMyId);
	  if(temp>=diff){
	    index=i
	  }
	  i+=1
	}
	var temp1=cid
	var temp2=ref
	for(i<-index to leafSet.length-1){
	  var tmp1=leafSet.get(i)
	  var tmp2=leafRefSet(i)
	  leafSet.set(i,temp1)
	  leafRefSet(i)=temp2
	  temp1=tmp1
	  temp2=tmp2
	}
	if(leafSet.length<leafSet.size){
	  leafRefSet(leafSet.length)=temp2
	  leafSet.set(leafSet.length,temp1)
	}	
	
  }
  def updateTable(exId: Array[Int], ref: ActorRef) = {
	var row=shl(id,exId)
	if(row<exId.length)
		rtable(row)(exId(row))=ref
  }

}

class FixedArrayList(capacity: Int) {
  var array: Array[Array[Int]] = new Array[Array[Int]](capacity)
  var length = 0
  var size = capacity

  def add(xarray: Array[Int]) = {
    array(length) = xarray
    length += 1
  }
  def set(index:Int,xarray:Array[Int])={
    array(index)=xarray
    if(index>=length){
      length=index+1
    }
  }

  def get(i: Int) = {
    array(i)
  }
  def copy={
    var result=new FixedArrayList(size)
    result.array=array.clone
    result.length=length
    result
  }
}