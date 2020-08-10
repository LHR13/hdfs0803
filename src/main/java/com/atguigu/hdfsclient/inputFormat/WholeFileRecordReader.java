package com.atguigu.hdfsclient.inputFormat;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.IOException;

/**
 * 定义RR，处理一个文件，将其处理成一个KV值
 */
public class WholeFileRecordReader extends RecordReader<Text, BytesWritable> {

    private boolean notread = true;
    private Text key = new Text();
    private BytesWritable value = new BytesWritable();
    private FSDataInputStream inputStream;
    private FileSplit fs;

    /**
     * 初始化方法
     * @param inputSplit
     * @param taskAttemptContext
     * @throws IOException
     * @throws InterruptedException
     */
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //转换切片到文件切片
        fs = (FileSplit) inputSplit;
        //通过切片获取路径
        Path path = fs.getPath();
        //通过路径获取文件系统
        FileSystem fileSystem = path.getFileSystem(taskAttemptContext.getConfiguration());
        //开流
        inputStream = fileSystem.open(path);
    }

    /**
     * 读取下一组KV值
     * @return 读到了返回true
     * @throws IOException
     * @throws InterruptedException
     */
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (notread) {
            //具体读文件过程，因为此方法只处理一整个文件，所以只会读到一次，读到了就返回true
            //读key
            key.set(fs.getPath().toString());

            //读value
            byte[] buf = new byte[(int) fs.getLength()];
            inputStream.read(buf);
            value.set(buf, 0, buf.length);

            notread = false;
            return true;
        } else {
            return false;
        }
    }

    /**
     * 获取当前读到的key
     * @return key
     * @throws IOException
     * @throws InterruptedException
     */
    public Text getCurrentKey() throws IOException, InterruptedException {
        return this.key;
    }

    /**
     * 获取当前读到的value
     * @return value
     * @throws IOException
     * @throws InterruptedException
     */
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return this.value;
    }

    /**
     * 返回当前的读取进度
     * @return 0.0-1.0之间的小数
     * @throws IOException
     * @throws InterruptedException
     */
    public float getProgress() throws IOException, InterruptedException {
        //每次只读一个文件，所以只有读了和没读两种情况，不存在其他情况
        return notread ? 0 : 1;
    }

    /**
     * 关闭资源
     * @throws IOException
     */
    public void close() throws IOException {
        IOUtils.closeStream(inputStream);
    }
}
