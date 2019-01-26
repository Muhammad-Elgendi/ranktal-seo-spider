package com.seospider.seospider.crawler;

import com.github.s3curitybug.similarityuniformfuzzyhash.UniformFuzzyHash;
import com.github.s3curitybug.similarityuniformfuzzyhash.UniformFuzzyHashes;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.github.s3curitybug.similarityuniformfuzzyhash.ToStringUtils.maxLength;
import static com.github.s3curitybug.similarityuniformfuzzyhash.ToStringUtils.prepareIdentifiers;

public class Similarities {

    public static void main(String[] args) throws IOException {
        System.out.println("Hello");
        DecimalFormat df = new DecimalFormat("%");



        File html1 = new File("/media/muhammad/disk/projects/Similarity-Of-Pages/src/main/java/com/github/Elgendi/html1");
        File html2 = new File("/media/muhammad/disk/projects/Similarity-Of-Pages/src/main/java/com/github/Elgendi/html2");
        File csv = new File("/media/muhammad/disk/projects/Similarity-Of-Pages/src/main/java/com/github/Elgendi/simil.csv");

        UniformFuzzyHash obj1 = new UniformFuzzyHash(html1,61);
        UniformFuzzyHash obj2 = new UniformFuzzyHash(html2,61);

        Map<String,UniformFuzzyHash> map = new HashMap<>();
        map.put("html1",obj1);
        map.put("html2",obj2);
        Map simil= UniformFuzzyHashes.computeAllHashesSimilarities(map);
        saveAllHashesSimilarities(simil);

        System.out.println("#1 :"+obj1.toString());
        System.out.println("#2 :"+obj2.toString());

        double res1= obj1.similarity(obj2);
        double res2= obj2.similarity(obj1);

        System.out.println("1--->2 :"+df.format(res1));
        System.out.println("2--->1 :"+df.format(res2));
    }



    /**
     * Writes a table showing the similarity between all the hashes in a map of identified Uniform
     * Fuzzy Hashes into a CSV file, overwriting it.
     *
     * @param <T> Identifiers type.
     * @param similarities Map of identified similarities (As it is returned from the method
     *        computeAllHashesSimilarities).
     */
    public static <T> void saveAllHashesSimilarities(Map<T, Map<T, Double>> similarities){

        // Parameters check.
        if (similarities == null) {
            throw new NullPointerException("Map of similarities is null.");
        }

        if (similarities.isEmpty()) {
            return;
        }

        // Identifiers.
        Set<T> identifiers = similarities.keySet();
        List<String> preparedIdentifiers = prepareIdentifiers(identifiers, -1);
        int identifiersMaxLength = maxLength(preparedIdentifiers);

        // Generate CSV.
        try{

            int i = 0;
            Set<Map.Entry<T, Map<T, Double>>> entries = similarities.entrySet();
            for (Map.Entry<T, Map<T, Double>> entry : entries) {

                Map<T, Double> similarities1 = entry.getValue();
                String preparedIdentifier = preparedIdentifiers.get(i);

                System.out.println(preparedIdentifier);

                for (T identifier1 : identifiers) {
                    Double similarity = null;
                    if (similarities1 != null) {
                        similarity = similarities1.get(identifier1);
                    }
                    System.out.println(similarity);
                }
                i++;
            }
        }catch (Exception ex){

        }

    }
}
