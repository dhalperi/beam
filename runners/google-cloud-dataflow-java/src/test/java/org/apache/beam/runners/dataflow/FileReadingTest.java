package org.apache.beam.runners.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;

/**
 * Created by dhalperi on 10/24/16.
 */
public class FileReadingTest {

  public static void main(String [] args) {
    Pipeline p = Pipeline.create(PipelineOptionsFactory.fromArgs(args).create());
    p.apply(TextIO.Read
        .from("gs://sort_g/input_small_files/ascii_sort_1MB_input.00*")
        .withCoder(ByteArrayCoder.of()))
        .apply(Count.<byte[]>globally());
    p.run();
  }

}
