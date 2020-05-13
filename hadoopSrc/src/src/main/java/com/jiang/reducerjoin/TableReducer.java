package com.jiang.reducerjoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class TableReducer extends Reducer<Text,TableBean,TableBean, NullWritable> {
    ArrayList<TableBean> orderList = new ArrayList<TableBean>();
    TableBean pdBean = new TableBean();


    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        for (TableBean value : values) {
            TableBean tmp = new TableBean();
            if (value.getFlag().equals("order")) {
                try {
                    BeanUtils.copyProperties(tmp, value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                orderList.add(tmp);
            } else {
                try {
                    BeanUtils.copyProperties(pdBean, value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }
        for (TableBean tableBean : orderList) {
            tableBean.setPname(pdBean.getPname());
            context.write(tableBean, NullWritable.get());
        }
    }
}
