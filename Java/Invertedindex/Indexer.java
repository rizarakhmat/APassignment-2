/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package assignment.invertedindex;

import assignment.mapreduce.MapReduce;
import assignment.utils.Pair;
import assignment.utils.Reader;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 *
 * @author nicco
 */
public class Indexer extends MapReduce<Pair<String, List<String>>, Pair<String, Pair<String, Integer>>, Pair<String, Pair<String, Integer>>> {

    @Override
    protected Stream<Pair<String, List<String>>> read(Path p) throws IOException {
        Reader r = new Reader(p);
        return r.read();
    }

    @Override
    protected Stream<Pair<String, Pair<String, Integer>>> map(Stream<Pair<String, List<String>>> in) {
        
        return in.map(p -> numLines(p))
                .flatMap(w -> w
                .getValue()
                .stream()
                .map(x -> new Pair<>(w.getKey(), new Pair<>(x.getKey(), x.getValue())))
                );

    }

    private Pair<String, List<Pair<String, Integer>>> numLines(Pair<String, List<String>> p) {
        //return for each document the list of words with the line number where they appear
        String doc = p.getKey();
        List<Pair<String, Integer>> lines = new ArrayList<>();
        AtomicInteger i = new AtomicInteger(); //i needed an AtomicInteger to count lines inside a lambda
        p.getValue().forEach((String l) -> {
            int curr = i.getAndIncrement();
            Arrays.stream(l.split(" "))
                    .map(s -> s.toLowerCase().replaceAll("[^a-z0-9]", ""))
                    .filter(s -> s.length() > 3)
                    .forEach(s -> lines.add(new Pair<>(s, curr)));
        });
        return new Pair<>(doc, lines);

    }

    @Override
    protected Stream<Pair<String, Pair<String, Integer>>> compare(Stream<Pair<String, Pair<String, Integer>>> s) {
        //sort the words, than the documents and in the end the lines
        Comparator<Pair<String, Pair<String, Integer>>> c = (p1, p2) -> p1.getValue().getKey().compareTo(p2.getValue().getKey());
        c = c.thenComparing((p1, p2) -> p1.getKey().compareTo(p2.getKey()))
                .thenComparing((p1, p2) -> p1.getValue().getValue().compareTo(p2.getValue().getValue()));

        return s.sorted(c);
    }

    @Override
    protected Stream<Pair<String, Pair<String, Integer>>> reduce(Stream<Pair<String, Pair<String, Integer>>> in) {
        // produce the inverted index by swapping term and document position
        return in.
                map(p -> new Pair<>(p.getValue().getKey(), new Pair<>(p.getKey(), p.getValue().getValue())));

    }

    @Override
    protected void write(File f, Stream<Pair<String, Pair<String, Integer>>> r) throws IOException {
        try ( PrintStream ps = new PrintStream(f)) {
            r.forEach(p -> ps.println(p.getKey() + ", " + p.getValue().getKey() + ", " + p.getValue().getValue()));
        }
    }

}