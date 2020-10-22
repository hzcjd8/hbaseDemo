package lession1;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class hbaseDemo {
	
	Random r = new Random();
	SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

	
	HBaseAdmin admin;
	HTable htable;
	String TN = "phone";//生成的表名为phone
	
	/**
	 * 建立连接
	 * @throws Exception
	 */
	@Before
	public void init() throws Exception {
		try{
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "192.168.21.153");
		admin = new HBaseAdmin(conf);
		htable = new HTable(conf, TN.getBytes());
		}catch (Exception e){
			 e.printStackTrace();
			}
			
	}		
		
		
		
		/**
		 * 创建表
		 * @throws Exception 
		 */
		@Test
		public void creatTable() throws Exception {
		try{
			if (admin.tableExists(TN)) {
				admin.disableTable(TN);
				admin.deleteTable(TN);
			}

			// 表描述
			HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(TN));
			HColumnDescriptor cf = new HColumnDescriptor("cf".getBytes());//定义列族名为cf
			desc.addFamily(cf);
			admin.createTable(desc);
			}catch (Exception e){
			 e.printStackTrace();
		}
//			
	 
		}

		
		
		

	/**
	 * 新增与查询一条数据
	 * @throws Exception
	 */
	@Test
	public void insertDB()  throws Exception{
		//新增一条数据
		String rowKey = "1231231313";
		Put put = new Put(rowKey.getBytes());
		put.add("cf".getBytes(), "name".getBytes(), "xiaohong".getBytes());
		put.add("cf".getBytes(), "age".getBytes(), "23".getBytes());
		put.add("cf".getBytes(), "sex".getBytes(), "women".getBytes());
		htable.put(put);
		//根据rowKey查询
		Get get = new Get(rowKey.getBytes());
		get.addColumn("cf".getBytes(), "name".getBytes());
		get.addColumn("cf".getBytes(), "age".getBytes());
		get.addColumn("cf".getBytes(), "sex".getBytes());
		Result rs = htable.get(get);
		Cell cell = rs.getColumnLatestCell("cf".getBytes(), "name".getBytes());
		Cell cell2 = rs.getColumnLatestCell("cf".getBytes(), "age".getBytes());
		Cell cell3 = rs.getColumnLatestCell("cf".getBytes(), "sex".getBytes());
		System.out.println("rowKey:"+rowKey+",cf:name"+new String(CellUtil.cloneValue(cell))+",cf:age"+new String(CellUtil.cloneValue(cell2))+",cf:sex"+new String(CellUtil.cloneValue(cell3)));
		//输入结果：rowKey:1231231313,cf:namexiaohong,cf:age23,cf:sexwomen
	}
	
	
	
	/**
	 * 随机产生100条记录
	 * 
	 * @throws Exception
	 */
	@Test
	public void insertDB2() throws Exception {
		List<Put> puts = new ArrayList<Put>();
		for (int i = 0; i < 10; i++) {
			String phoneNum = "18695165434";
			for (int j = 0; j < 100; j++) {
				String dnum = getPhoneNum("158");
				String length = r.nextInt(99) + "";
				String type = r.nextInt(2) + "";
				String dateStr = getDate("2018");
				String rowkey = phoneNum + "_" + (Long.MAX_VALUE - sdf.parse(dateStr).getTime());
				Put put = new Put(rowkey.getBytes());
				put.add("cf".getBytes(), "dnum".getBytes(), dnum.getBytes());
				put.add("cf".getBytes(), "length".getBytes(), length.getBytes());
				put.add("cf".getBytes(), "type".getBytes(), type.getBytes());
				put.add("cf".getBytes(), "date".getBytes(), dateStr.getBytes());
				puts.add(put);
			}
		}
		htable.put(puts);
	}
	
	
	
	/**
	 * 
	 * 统计二月份到三月份并且type=0的通话记录 
	 * san 指定范围查询
	 * @throws Exception
	 */
	@Test
	public void scan() throws Exception {
		String phoneNum = "18695165434";
		String startRow = phoneNum + "_" + (Long.MAX_VALUE - sdf.parse("20180301000000").getTime());
		String stopRow = phoneNum + "_" + (Long.MAX_VALUE - sdf.parse("20180201000000").getTime());
		SingleColumnValueFilter filter2 = new SingleColumnValueFilter("cf".getBytes(), "type".getBytes(),CompareOp.EQUAL, "0".getBytes());
		FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);	
		list.addFilter(filter2);
		
		Scan scan = new Scan();
		scan.setStartRow(startRow.getBytes());
		scan.setStopRow(stopRow.getBytes());
		scan.setFilter(list);
		ResultScanner rss = htable.getScanner(scan);
		for (Result rs : rss) {
			System.out
					.print(new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "dnum".getBytes()))));
			System.out.print("-"
					+ new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "length".getBytes()))));
			System.out.print(
					"-" + new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "type".getBytes()))));
			System.out.println(
					"-" + new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "date".getBytes()))));
		}
		//结果：15811949601-40-0-20180202010217
		//      15893694342-3-0-20180201044840
		//       ...
	}
	
	/**
	 * 关闭连接
	 * @throws Exception
	 */
	@After
	public void destory() throws Exception {
		if (admin != null) {
			admin.close();
		}
	}
	
	/**
	 * 生成随机年月
	 * @param year
	 * @return
	 */
	private String getDate(String year) {
		return year + String.format("%02d%02d%02d%02d%02d",
				new Object[] { r.nextInt(12) + 1, r.nextInt(31) + 1, r.nextInt(24), r.nextInt(60), r.nextInt(60) });
	}
	
	/**
	 * 生成随机的手机号码
	 * 
	 * @param string
	 * @return
	 */
	private String getPhoneNum(String string) {
		return string + String.format("%08d", r.nextInt(99999999));
	}
}
