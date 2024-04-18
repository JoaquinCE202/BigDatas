import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

  // Clase Mapper que procesa texto para convertirlo en pares clave-valor.
  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

    // Objeto constante para representar el valor '1'.
    private final static IntWritable one = new IntWritable(1);
    // Objeto de texto para almacenar cada palabra como clave.
    private Text word = new Text();

    // Método de mapeo que se llama con cada línea de texto de entrada.
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      // Utiliza StringTokenizer para descomponer la línea de texto en palabras.
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        // Establece la palabra actual como clave.
        word.set(itr.nextToken());
        // Escribe un par clave-valor (palabra, 1) en el contexto.
        context.write(word, one);
      }
    }
  }

  // Clase Reducer que suma todos los valores asociados a la misma clave.
  public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    // Objeto IntWritable para almacenar el resultado de la suma.
    private IntWritable result = new IntWritable();

    // Método de reducción que se llama con una clave y su lista de valores.
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      // Suma todos los valores para la misma clave.
      for (IntWritable val : values) {
        sum += val.get();
      }
      // Establece el resultado total como el valor de la clave.
      result.set(sum);
      // Escribe el par clave-valor final (palabra, suma total) en el contexto.
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");

    // Establece la clase Mapper para el trabajo. TokenizerMapper procesa texto de entrada en palabras.
    job.setMapperClass(TokenizerMapper.class);

    // Establece la clase Combiner para el trabajo. IntSumReducer actúa como un reducer local para sumar los conteos en el mismo nodo.
    job.setCombinerClass(IntSumReducer.class);

    // Establece la clase Reducer para el trabajo. IntSumReducer suma todos los conteos a través de todos los nodos.
    job.setReducerClass(IntSumReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
