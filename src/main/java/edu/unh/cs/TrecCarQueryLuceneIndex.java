package edu.unh.cs;

import com.sun.org.apache.xpath.internal.operations.Bool;
import edu.unh.cs.treccar_v2.Data;
import edu.unh.cs.treccar_v2.read_data.CborFileTypeException;
import edu.unh.cs.treccar_v2.read_data.CborRuntimeException;
import edu.unh.cs.treccar_v2.read_data.DeserializeData;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLOutput;
import java.util.*;

/*
 * User: dietz
 * Date: 1/4/18
 * Time: 1:23 PM
 */

/**
 * Example of how to build a lucene index of trec car paragraphs
 */
public class TrecCarQueryLuceneIndex {

    private static void usage() {
        System.out.println("Command line parameters: action OutlineCBOR LuceneINDEX\n" +
                "action is one of output-sections | paragraphs-run-sections | paragraphs-run-pages | pages-run-pages | iterate-topics");
        System.exit(-1);
    }

    static class MyQueryBuilder {

        private final EnglishAnalyzer analyzer;
        private List<String> tokens;

        public MyQueryBuilder(EnglishAnalyzer standardAnalyzer) {
            analyzer = standardAnalyzer;
            tokens = new ArrayList<>(128);
        }

        public BooleanQuery toQuery(String queryStr) throws IOException {

            TokenStream tokenStream = analyzer.tokenStream("text", new StringReader(queryStr));
            tokenStream.reset();
            tokens.clear();
            while (tokenStream.incrementToken()) {
                final String token = tokenStream.getAttribute(CharTermAttribute.class).toString();
                tokens.add(token);
            }
            tokenStream.end();
            tokenStream.close();
            BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();

            BooleanQuery.Builder booleanQuery1 = new BooleanQuery.Builder();
            for (String token : tokens) {
                booleanQuery1.add(new TermQuery(new Term("text", token)), BooleanClause.Occur.SHOULD);
            }
            BoostQuery unigramQuery = new BoostQuery(booleanQuery1.build(), 0.85f);
            booleanQuery.add(unigramQuery, BooleanClause.Occur.SHOULD);

            // oW1
            BooleanQuery.Builder booleanQuery2 = new BooleanQuery.Builder();
            for (int i = 0; i < tokens.size()-1; i++) {
                booleanQuery2.add(new PhraseQuery(0, "text", tokens.get(i), tokens.get(i+1)), BooleanClause.Occur.SHOULD);
            }
            BoostQuery bigramQuery = new BoostQuery(booleanQuery2.build(), 0.10f);
            booleanQuery.add(bigramQuery, BooleanClause.Occur.SHOULD);

            // uW8
            BooleanQuery.Builder booleanQuery3 = new BooleanQuery.Builder();
            for (int i = 0; i < tokens.size()-1; i++) {
                BooleanQuery.Builder booleanQuery31 = new BooleanQuery.Builder();
                booleanQuery31.add(new PhraseQuery(8, "text", tokens.get(i), tokens.get(i+1)), BooleanClause.Occur.SHOULD);
                booleanQuery31.add(new PhraseQuery(8, "text", tokens.get(i+1), tokens.get(i)), BooleanClause.Occur.SHOULD);
                booleanQuery3.add(booleanQuery31.build(), BooleanClause.Occur.SHOULD);
            }
            BoostQuery unorderedQuery = new BoostQuery(booleanQuery3.build(), 0.05f);
            booleanQuery.add(unorderedQuery, BooleanClause.Occur.SHOULD);

            return booleanQuery.build();
        }
    }

    public static void main(String[] args) throws IOException {
        System.setProperty("file.encoding", "UTF-8");

        if (args.length < 3)
            usage();


        String mode = args[0];
        String indexPath = args[2];

        if (mode.equals("output-sections")) {
            IndexSearcher searcher = setupIndexSearcher(indexPath, "paragraph.lucene");

            searcher.setSimilarity(new BM25Similarity());
            final MyQueryBuilder queryBuilder = new MyQueryBuilder(new EnglishAnalyzer());

            final String pagesFile = args[1];
            final FileInputStream fileInputStream3 = new FileInputStream(new File(pagesFile));
            for (Data.Page page : DeserializeData.iterableAnnotations(fileInputStream3)) {
                System.out.println("\n\nPage: " + page.getPageId());
                for (List<Data.Section> sectionPath : page.flatSectionPaths()) {
                    System.out.println();
                    System.out.println(Data.sectionPathId(page.getPageId(), sectionPath) + "   \t " + Data.sectionPathHeadings(sectionPath));

                    String queryStr = buildSectionQueryStr(page, sectionPath);

                    // get top 10 documents
                    TopDocs tops = searcher.search(queryBuilder.toQuery(queryStr), 10);
                    ScoreDoc[] scoreDoc = tops.scoreDocs;
                    System.out.println("Found " + scoreDoc.length + " results.");
                    for (ScoreDoc score : scoreDoc) {
                        final Document doc = searcher.doc(score.doc); // to access stored content
                        // print score and internal docid
                        System.out.println(doc.getField("paragraphid").stringValue() + " (" + score.doc + "):  SCORE " + score.score + "\n");
                        // access and print content
//                        System.out.println("  " +doc.getField("text").stringValue());
                    }

                }
                System.out.println();
            }
        } else if (mode.equals("paragraphs-run-sections")) {
            IndexSearcher searcher = setupIndexSearcher(indexPath, "paragraph.lucene");

            searcher.setSimilarity(new BM25Similarity());
            final MyQueryBuilder queryBuilder = new MyQueryBuilder(new EnglishAnalyzer());

            final String pagesFile = args[1];
            final FileInputStream fileInputStream3 = new FileInputStream(new File(pagesFile));
            for (Data.Page page : DeserializeData.iterableAnnotations(fileInputStream3)) {
                for (List<Data.Section> sectionPath : page.flatSectionPaths()) {
                    final String queryId = Data.sectionPathId(page.getPageId(), sectionPath); //TODO START FROM HERE

                    String queryStr = buildSectionQueryStr(page, sectionPath);

                    TopDocs tops = searcher.search(queryBuilder.toQuery(queryStr), 100);
                    ScoreDoc[] scoreDoc = tops.scoreDocs;
                    for (int i = 0; i < scoreDoc.length; i++) {
                        ScoreDoc score = scoreDoc[i];
                        final Document doc = searcher.doc(score.doc); // to access stored content
                        // print score and internal docid
                        final String paragraphid = doc.getField("paragraphid").stringValue();
                        final float searchScore = score.score;
                        final int searchRank = i + 1;

                        System.out.println(queryId + " Q0 " + paragraphid + " " + searchRank + " " + searchScore + " Lucene-BM25");
                    }

                }
            }
        } else if (mode.equals("paragraphs-run-pages")) {
            IndexSearcher searcher = setupIndexSearcher(indexPath, "paragraph.lucene");

            searcher.setSimilarity(new BM25Similarity());
            final MyQueryBuilder queryBuilder = new MyQueryBuilder(new EnglishAnalyzer());

            final String pagesFile = args[1];
            final FileInputStream fileInputStream3 = new FileInputStream(new File(pagesFile));
            for (Data.Page page : DeserializeData.iterableAnnotations(fileInputStream3)) {
                final String queryId = page.getPageId();

                String queryStr = buildSectionQueryStr(page, Collections.<Data.Section>emptyList());

                TopDocs tops = searcher.search(queryBuilder.toQuery(queryStr), 100);
                ScoreDoc[] scoreDoc = tops.scoreDocs;
                for (int i = 0; i < scoreDoc.length; i++) {
                    ScoreDoc score = scoreDoc[i];
                    final Document doc = searcher.doc(score.doc); // to access stored content
                    // print score and internal docid
                    final String paragraphid = doc.getField("paragraphid").stringValue();
                    final float searchScore = score.score;
                    final int searchRank = i + 1;

                    System.out.println(queryId + " Q0 " + paragraphid + " " + searchRank + " " + searchScore + " Lucene-BM25");
                }

            }
        } else if (mode.equals("pages-run-pages")) {
            IndexSearcher searcher = setupIndexSearcher(indexPath, "pages.lucene");

            searcher.setSimilarity(new BM25Similarity());
            final MyQueryBuilder queryBuilder = new MyQueryBuilder(new EnglishAnalyzer());

            final String pagesFile = args[1];
            final FileInputStream fileInputStream3 = new FileInputStream(new File(pagesFile));
            for (Data.Page page : DeserializeData.iterableAnnotations(fileInputStream3)) {
                final String queryId = page.getPageId();

                String queryStr = buildSectionQueryStr(page, Collections.<Data.Section>emptyList());

                TopDocs tops = searcher.search(queryBuilder.toQuery(queryStr), 100);
                ScoreDoc[] scoreDoc = tops.scoreDocs;
                for (int i = 0; i < scoreDoc.length; i++) {
                    ScoreDoc score = scoreDoc[i];
                    final Document doc = searcher.doc(score.doc); // to access stored content
                    // print score and internal docid
                    final String paragraphid = doc.getField("pageid").stringValue();
                    final float searchScore = score.score;
                    final int searchRank = i + 1;

                    System.out.println(queryId + " Q0 " + paragraphid + " " + searchRank + " " + searchScore + " Lucene-BM25");
                }

            }

        } else if (mode.equals("iterate-topics")) {

            IndexSearcher searcher = setupIndexSearcher(indexPath, "paragraph.lucene"); //Create IndexSearcher


            if ("bm25".equals(args[3])) {
                float k1 = Float.parseFloat(args[4]);
                float b = Float.parseFloat(args[5]);
                searcher.setSimilarity(new BM25Similarity(k1, b));
            } else if ("lmd".equals(args[3])) {
                float mu = Float.parseFloat(args[4]);
                searcher.setSimilarity(new LMDirichletSimilarity(mu));
            }

            final MyQueryBuilder queryBuilder = new MyQueryBuilder(new EnglishAnalyzer());

            final String topicsFile = args[1];    //Get test.benchmarkY1test.cbor.outlines file
            final FileInputStream fileInputStream3 = new FileInputStream(new File(topicsFile));

            for (Data.Page page : DeserializeData.iterableAnnotations(fileInputStream3)) {

                for (List<Data.Section> sectionPath : page.flatSectionPaths()) {
                    final String queryId = Data.sectionPathId(page.getPageId(), sectionPath); //Get QueryID

                    String queryStr = buildSectionQueryStr(page, sectionPath);  //Get queryString to search
//                    System.out.println(queryBuilder.toQuery(queryStr));
                    TopDocs tops = searcher.search(queryBuilder.toQuery(queryStr), 100);//Get 100 docs for the provided query

                    ScoreDoc[] scoreDoc = tops.scoreDocs;
                    HashSet<Object> seen = new HashSet<>(100);

                    for (int i = 0; i < scoreDoc.length; i++) {

                        ScoreDoc score = scoreDoc[i];
                        final Document doc = searcher.doc(score.doc); // to access stored content
                        final String paragraphid = doc.getField("paragraphid").stringValue();
                        final float searchScore = score.score;
                        final int searchRank = i + 1;

                        if (!seen.contains(paragraphid)) {
                            System.out.println(queryId + " Q0 " + paragraphid + " " + searchRank + " " + searchScore + " Lucene-BM25");
                            seen.add(paragraphid);
                        }
                    }

                }
            }
        }
    }

    @NotNull
    private static IndexSearcher setupIndexSearcher(String indexPath, String typeIndex) throws IOException {
        Path path = FileSystems.getDefault().getPath(indexPath, typeIndex);
        Directory indexDir = FSDirectory.open(path);
        IndexReader reader = DirectoryReader.open(indexDir);
        return new IndexSearcher(reader);
    }

    @NotNull
    private static String buildSectionQueryStr(Data.Page page, List<Data.Section> sectionPath) {
        StringBuilder queryStr = new StringBuilder();
        queryStr.append(page.getPageName());
        for (Data.Section section : sectionPath) {
            queryStr.append(" ").append(section.getHeading());
        }
//        System.out.println("queryStr = " + queryStr);
        return queryStr.toString();
    }

    private static Iterable<Document> toIterable(final Iterator<Document> iter) throws CborRuntimeException, CborFileTypeException {
        return new Iterable<Document>() {
            @Override
            @NotNull
            public Iterator<Document> iterator() {
                return iter;
            }
        };
    }

}