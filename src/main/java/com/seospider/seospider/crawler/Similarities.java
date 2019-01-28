package com.seospider.seospider.crawler;

import com.github.s3curitybug.similarityuniformfuzzyhash.UniformFuzzyHash;
import com.github.s3curitybug.similarityuniformfuzzyhash.UniformFuzzyHashes;
import com.seospider.seospider.db.DBService;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;

import java.util.*;


import static com.github.s3curitybug.similarityuniformfuzzyhash.ToStringUtils.prepareIdentifiers;

public class Similarities {

    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(Similarities.class);

    public static void main(String[] args) throws IOException {
        DecimalFormat df = new DecimalFormat("%");

        File html1 = new File("/media/muhammad/disk/projects/Similarity-Of-Pages/src/main/java/com/github/Elgendi/html1.html");
        File html2 = new File("/media/muhammad/disk/projects/Similarity-Of-Pages/src/main/java/com/github/Elgendi/html2.html");
        File csv = new File("/media/muhammad/disk/projects/Similarity-Of-Pages/src/main/java/com/github/Elgendi/simil.csv");

        UniformFuzzyHash obj1 = new UniformFuzzyHash(html1,61);
        UniformFuzzyHash obj2 = new UniformFuzzyHash(html2,61);

        Map<String,UniformFuzzyHash> map = new HashMap<>();
        map.put("html1",obj1);
        map.put("html2",obj2);
        Map simil= UniformFuzzyHashes.computeAllHashesSimilarities(map);
        saveAllHashesSimilarities(simil,null);

        System.out.println("#1 :"+obj1.toString());
        System.out.println("#2 :"+obj2.toString());

        double res1= obj1.similarity(obj2);
        double res2= obj2.similarity(obj1);

        System.out.println("1--->2 :"+df.format(res1));
        System.out.println("2--->1 :"+df.format(res2));
    }

    /**
     * Wrapper to get the hash of html
     * @param html
     * @return String
     */
    public static String calculateHash(String html){
        UniformFuzzyHash hashObject = new UniformFuzzyHash(html,61);
        return hashObject.toString();
    }



    /**
     * Writes a table showing the similarity between all the hashes in a map of identified Uniform
     * Fuzzy Hashes into a CSV file, overwriting it.
     *
     * @param <T> Identifiers type.
     * @param similarities Map of identified similarities (As it is returned from the method
     *        computeAllHashesSimilarities).
     */
    public static <T> void saveAllHashesSimilarities(Map<T, Map<T, Double>> similarities, DBService postgresDBService){

        // Parameters check.
        if (similarities == null || similarities.isEmpty()) {
            logger.error("similarities are empty in saveAllHashesSimilarities()");
            return;
        }

        // Identifiers.
        Set<T> identifiers = similarities.keySet();
        List<String> preparedIdentifiers = prepareIdentifiers(identifiers, -1);

        // Loop over similarities

        int i = 0;
        Set<Map.Entry<T, Map<T, Double>>> entries = similarities.entrySet();
        for (Map.Entry<T, Map<T, Double>> entry : entries) {

            Map<T, Double> similarities1 = entry.getValue();
            String preparedIdentifier = preparedIdentifiers.get(i);
            for (T identifier1 : identifiers) {
                if (similarities1 != null && identifier1 != preparedIdentifier && similarities1.get(identifier1) > 0.85) {
//                        System.out.println("id 1: "+preparedIdentifier+"----> id 2: "+identifier1+" = "+similarities1.get(identifier1));

                    // store similarities
                    try {
                        postgresDBService.storeSimilarity(preparedIdentifier,identifier1.toString(),similarities1.get(identifier1).floatValue());
                    } catch (RuntimeException e) {
                        logger.error("Storing similarity in saveAllHashesSimilarities() failed", e);
                    }
                }
            }
            i++;
        }
    }
}
