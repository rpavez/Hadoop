import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class AnagramMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
{

  // Input Key: Line Number
  // Input Value: Full line
  //
  // Output Key: First Word
  // Output Value: Second Word (Separated by a space)
  @Override
  public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException
  {

    String wordString = value.toString().trim();
    
    //Split the line using ';' separator
    String[] splittedWord = wordString.split(":");
    
    output.collect(new Text(splittedWord[0]), new Text(splittedWord[1]));
  }
}
