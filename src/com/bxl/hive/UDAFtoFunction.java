package com.bxl.hive;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.FloatWritable;

/**
 * UDAF是输入多个数据行，产生一个数据行
 * 用户自定义UDAF必须继承UDFA，必须提供一个实现了UDAFEvaluator接口的内部类

 */

/**
 *
 *  1：函数类需要继承UDAF类，并且该函数类中必须提供一个实现了UDAFEvaluator接口的内部类。

 2.内部类需要实现init()、iterate()、terminatePartial()、merge()、terminate()这几个函数。

 2.1：init()函数实现接口UDAFEvaluator中的init()函数，主要是负责初始化计算函数并且重设其内部状态，一般就是重设其内部字段。一般在静态类中定义一个内部字段存放最终结果。

 2.2：iterate()函数接收传入的参数，并进行内部轮转。其返回类型为boolean，每一次对一个新值进行聚集计算时候都会调用该方法，计算函数会根据聚集计算结果更新内部状态。当输入值合法或者正确计算了，则返回true。

 2.3：terminatePartial()无参数，其为iterate()函数轮转结束后，返回轮转数据，terminatePartial()类似于Hadoop的Combiner，Hive需要部分结果的时候会调用该方法，必须要返回一个封装了聚集计算当前状态的对象。

 2.4：merge接收terminatePartial()的返回结果，进行数据merge()操作，其返回类型为boolean，Hive进行合并一个部分聚集和另一个部分聚集的时候会调用该方法。

 2.5：terminate()返回最终的聚集函数结果，Hive最终聚集结果的时候会调用该方法。计算函数需要把状态作为一个值返回给用户。

 注：部分聚集结果的数据类型和最终结果的数据类型可以不同。
 *
 */

public class UDAFtoFunction extends UDAF{

	public static class MaxIdToUDADEvaluator implements UDAFEvaluator{

		//最终结果
		private FloatWritable result;

		//负责初始化计算函数并设置它的内部状态，result是存放最终结果的
		@Override
		public void init() {
			result=null;
		}

		//每次对一个新值进行聚集计算都会调用iterate方法
		public boolean iterate(FloatWritable value){
			if(value==null)
				return false;
			if(result==null)
				result = new FloatWritable(value.get());
			else
				result.set(Math.max(result.get(), value.get()));
			return true;
		}

		//Hive需要部分聚集结果的时候会调用该方法
		//会返回一个封装了聚集计算当前状态的对象
		public FloatWritable terminatePartial(){
			return result;
		}

		//合并两个部分聚集值会调用这个方法
		public boolean merge(FloatWritable other){
			return iterate(other);
		}

		//Hive需要最终聚集结果时候会调用该方法
		public FloatWritable terminate(){
			return result;
		}

	}

}
