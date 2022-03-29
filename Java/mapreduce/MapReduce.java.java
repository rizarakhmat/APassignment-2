package assignment.mapreduce;

import assignment.utils.Pair;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.stream.Stream;

/**
 *
 * @author nicco
 * 
 * @param <R> is the type of the key-value pairs returned by the read method
 * @param <U> is the type of the key-value pairs returned by map and that will be processed by reduce
 * @param <T> is the type of the key-value pairs returned by reduce
 * 
 */
public abstract class MapReduce<R extends Pair,  U extends Pair, T extends Pair> {
    
    
    public final void compute(Path src, File dest) throws IOException{
        Stream i = read(src);
        Stream out = map(i);
        Stream sorted = compare(out);        
        Stream r = reduce(sorted);
        write(dest, r);
        
        
    }
    protected abstract  Stream<R> read(Path p) throws IOException;
    protected abstract Stream<U> map(Stream<R> in);
    protected abstract Stream<U> compare(Stream<U> s);
    protected abstract Stream<T> reduce(Stream<U> in);
    protected abstract  void write(File f, Stream<T> r ) throws IOException;
}